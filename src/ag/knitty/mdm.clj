(ns ag.knitty.mdm
  (:require [ag.knitty.deferred :as kd]
            [manifold.deferred :as md])
  (:import [clojure.lang Associative]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicReference AtomicReferenceArray]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defonce ^:private keyword-int-mapping (ref {}))
(defonce ^:private keyword-int-counter (ref 0))

(defn max-initd 
  ^long []
  @keyword-int-counter)

(defn keyword->intid
  ^long [k]
  (let [^java.lang.Long c
        (or
         (@keyword-int-mapping k)
         (dosync
           (when-not (qualified-keyword? k)
             (throw (java.lang.IllegalArgumentException. "yarn key must be a qualified keyword")))
           (let [c (long @keyword-int-counter)]
             (alter keyword-int-counter inc)
             (alter keyword-int-mapping assoc k c)
             c)))]
    (.longValue c)))


(deftype FetchResult [^boolean claimed 
                      ^manifold.deferred.IMutableDeferred value])


(definterface IMutableDeferredMap
  (mdmGet    [^clojure.lang.Keyword kkw ^long kid] "get value or ::none")
  (mdmFetch  [^clojure.lang.Keyword kkw ^long kid] "get or claim a deferred")
  (mdmFreeze [] "freeze map, deref all completed deferreds")
  (mdmCancel [] "freeze map, cancel all deferreds"))


(defmacro mdm-fetch! [mdm kw kid]
  (list '.mdmFetch (with-meta mdm {:tag "ag.knitty.mdm.IMutableDeferredMap"}) kw kid))

(defmacro mdm-freeze! [mdm]
  (list '.mdmFreeze (with-meta mdm {:tag "ag.knitty.mdm.IMutableDeferredMap"})))

(defmacro mdm-cancel! [mdm]
  (list '.mdmCancel (with-meta mdm {:tag "ag.knitty.mdm.IMutableDeferredMap"})))

(defmacro mdm-get! [mdm kw kid]
  (list '.mdmGet (with-meta mdm {:tag "ag.knitty.mdm.IMutableDeferredMap"}) kw kid))

(defmacro fetch-result-claimed? [r]
  (list '.-claimed (with-meta r {:tag "ag.knitty.mdm.FetchResult"})))

(defmacro fetch-result-value [r]
  (list '.-value (with-meta r {:tag "ag.knitty.mdm.FetchResult"})))

(defmacro none? [x]
  `(identical? ::none ~x))


(deftype KVCons [^clojure.lang.Keyword key
                 ^ag.knitty.deferred.KaDeferred val
                 next])


(deftype ConcurrentMapMDM [^Associative init
                           ^AtomicReference added
                           ^ConcurrentHashMap hm]
  IMutableDeferredMap

  (mdmFetch
    [_ k ki]
    (let [v (.valAt init k ::none)]
      (if-not (none? v)
        (FetchResult. false v) ;; from init
        (let [v (.get hm ki)]  ;; from hm or new
          (if (nil? v)
            ;; new
            (let [d (kd/ka-deferred)
                  p (.putIfAbsent hm ki d)
                  d (if (nil? p) d p)]
              (when (nil? p)
                (loop [g (.get added)]
                  (when-not (.compareAndSet added g (KVCons. k d g))
                    (recur (.get added)))))
              (FetchResult. true d))
            ;; from hm
            (FetchResult. false v))))))

  (mdmGet
   [_ k ki]
   (let [v (.valAt init k ::none)]
     (if (none? v)
       (.get hm ki)
       (kd/unwrap1' v))))

  (mdmFreeze
    [_]
    (let [a (.get added)]
      (if a
        (with-meta
          (if (instance? clojure.lang.IEditableCollection init)
            (loop [^KVCons a a, m (transient init)]
              (if a
                (recur (.-next a)
                       (assoc! m (.-key a) (kd/unwrap1' (.-val a))))
                (persistent! m)))
            (loop [^KVCons a a, m init]
              (if a
                (recur (.-next a)
                       (assoc m (.-key a) (kd/unwrap1' (.-val a))))
                m)))
          (meta init))
        init)))

  (mdmCancel
   [_]
   (loop [^KVCons a (.get added)]
     (when a
       (let [d (.-val a)]
         (when-not (md/realized? d)
           (kd/cancel! d)))
       (recur (.-next a))
       )))
  )


(def ^:private nil-deferred (kd/successed nil))


(defmacro ^:private arr-getset-lazy [a i v]
  `(let [^AtomicReferenceArray a# ~a
         i# ~i
         x# (.get a# i#)]
     (if x#
       x#
       (let [v# ~v]
         (if (.compareAndSet a# i# nil v#)
           v#
           (.get a# i#))))))


(deftype AtomicRefArrayMDM [^Associative init
                            ^AtomicReference added
                            ^AtomicReferenceArray a0]
  IMutableDeferredMap

  (mdmFetch
    [_ k ki]
    (let [i0 (bit-shift-right ki 5)
          i1 (bit-and ki 31)
          ^AtomicReferenceArray a1 (arr-getset-lazy a0 i0 (AtomicReferenceArray. 32))
          v (.get a1 i1)]
      (if (nil? v)

          ;; probably mdmGet was not called... (or in-progress by other thread)
        (let [x (.valAt init k ::none)]
          (if (none? x)
            (let [d (kd/ka-deferred)
                  p (.compareAndSet a1 i1 nil d)]
              (if p
                (do
                  (loop [g (.get added)]
                    (when-not (.compareAndSet added g (KVCons. k d g))
                      (recur (.get added))))
                  (FetchResult. true d))
                (recur k ki)  ;; interrupted by another mdmFetch or mdmGet
                ))
            (let [d (if (nil? x) nil-deferred x)]
              ;; copy from 'init'
              (.set a1 i1 d)
              (FetchResult. false d))))

        (if (none? v)
            ;; new item, was prewarmed by calling mdmGet 
          (let [d (kd/ka-deferred)
                p (.compareAndSet a1 i1 ::none d)]
            (if p
              (do
                (loop [g (.get added)]
                  (when-not (.compareAndSet added g (KVCons. k d g))
                    (recur (.get added))))
                (FetchResult. true d))
              (do
                  ;; ::none may change only to claimed deferred or value
                (FetchResult. false (.get a1 i1)))))
          (FetchResult. false v)))))

  (mdmGet
    [_ k ki]
    (let [i0 (bit-shift-right ki 5)
          i1 (bit-and ki 31)
          ^AtomicReferenceArray a1 (arr-getset-lazy a0 i0 (AtomicReferenceArray. 32))
          v (.get a1 i1)]
      (if-not (nil? v)
        (kd/unwrap1' v)
        (let [v (.valAt init k ::none)]
          (if (none? v)
            (do
              ;; maybe put ::none, so mdmFetch don't need to check 'init' again
              (.compareAndSet a1 i1 nil ::none)
              v)
            (let [d (if (nil? v) nil-deferred v)]
                ;; copy from 'init'
              (.set a1 i1 d)
              v))))))

  (mdmFreeze
    [_]
    (let [a (.get added)]
      (if a
        (with-meta
          (if (instance? clojure.lang.IEditableCollection init)
            (loop [^KVCons a a, m (transient init)]
              (if a
                (recur (.-next a)
                       (assoc! m (.-key a) (kd/unwrap1' (.-val a))))
                (persistent! m)))
            (loop [^KVCons a a, m init]
              (if a
                (recur (.-next a)
                       (assoc m (.-key a) (kd/unwrap1' (.-val a))))
                m)))
          (meta init))
        init)))

  (mdmCancel
    [_]
    (loop [^KVCons a (.get added)]
      (when a
        (let [d (.-val a)]
          (when (and (md/deferred? d) (not (md/realized? d)))
            (kd/cancel! d)))
        (recur (next a))))))


(defn create-mdm-chm [init]
  {:pre [(associative? init)]}
  (ConcurrentMapMDM.
   init
   (AtomicReference. nil)
   (ConcurrentHashMap.)))


(defn create-mdm-arr [init ^long mk]
  {:pre [(associative? init)]}
  (let [n (bit-shift-right (+ 31 mk) 5)]
    (AtomicRefArrayMDM.
     init
     (AtomicReference. nil)
     (AtomicReferenceArray. n))))


(defn create-mdm [init]
  (let [mk (max-initd)]
    (if (> mk 2048)
      (create-mdm-chm init)
      (create-mdm-arr init mk))))
