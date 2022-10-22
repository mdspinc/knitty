(ns ag.knitty.mdm
  (:require [ag.knitty.deferred :as kd]
            [manifold.deferred :as md])
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicReference AtomicReferenceArray]))


(set! *warn-on-reflection* true)


(defonce ^:private keywords-int-ids (atom [0 {}]))


(defn keyword->intid
  ^long [k]
  {:pre [(keyword? k)]}
  (long
   (or
    (-> keywords-int-ids deref (nth 1) k)
    (->
     (swap! keywords-int-ids
            (fn [[c m]]
              [(inc c)
               (assoc m k c)]))
     (nth 1)
     k))))


(definterface FetchResult
  ( mdmResult  [] "get deferred")
  (^boolean mdmClaimed [] "true if caller should put value into deferred"))


(definterface MutableDeferredMap
  (mdmGet    [^clojure.lang.Keyword kkw ^long kid] "get value or nil")
  (mdmFetch  [^clojure.lang.Keyword kkw ^long kid] "get or claim deferred")
  (mdmFreeze [] "freeze map, deref all completed deferreds")
  (mdmCancel [] "freeze map, cancel all deferreds"))


(definline mdm-fetch! [mdm kw kid]
  `(.mdmFetch ~(with-meta mdm {:tag "ag.knitty.mdm.MutableDeferredMap"}) ~kw ~kid))

(definline mdm-freeze! [mdm]
  `(.mdmFreeze ~(with-meta mdm {:tag "ag.knitty.mdm.MutableDeferredMap"})))

(definline mdm-cancel! [mdm]
  `(.mdmCancel ~(with-meta mdm {:tag "ag.knitty.mdm.MutableDeferredMap"})))

(definline mdm-get! [mdm kw kid]
  `(.mdmGet ~(with-meta mdm {:tag "ag.knitty.mdm.MutableDeferredMap"}) ~kw ~kid))


(defmacro none? [x]
  `(identical? ::none ~x))


(deftype KVCons [^clojure.lang.Keyword key
                 ^ag.knitty.deferred.KaDeferred val
                 next])


(deftype ETrue [v]
  FetchResult
  (mdmClaimed [_] true)
  (mdmResult [_] v))


(deftype EFalse [v]
  FetchResult
  (mdmClaimed [_] false)
  (mdmResult [_] v))


(deftype ConcurrentMapMDM [init
                           ^AtomicReference added
                           ^ConcurrentHashMap hm]
  MutableDeferredMap

  (mdmFetch
    [_ k ki]
    (let [v (init k ::none)]
      (if-not (none? v)
        (EFalse. v)    ;; from init
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
              (ETrue. d))
            ;; from hm
            (let [v' (md/success-value v v)]
              (EFalse. v')))))))

  (mdmGet
   [_ k ki]
   (let [v (init k ::none)]
     (if (none? v)
       (.get hm ki)
       (kd/successed v))))

  (mdmFreeze
    [_]
    (let [a (.get added)]
      (if a
        (with-meta
          (loop [^KVCons a a, m (transient init)]
            (if a
              (recur (.-next a)
                     (assoc! m (.-key a) (kd/unwrap1' (.-val a))))
              (persistent! m)))
          (meta init))
        init)))

  (mdmCancel
   [_]
   (loop [^KVCons a (.get added)]
     (when a
       (let [d (.-val a)]
         (when-not (md/realized? d)
           (kd/cancel! d))))))
  )


(defmacro ^:private arr-getset-lazy [a i v]
  `(let [^AtomicReferenceArray a# ~a
         i# ~i
         x# (.get a# i#)]
     (if x#
       x#
       (let [v# ~v]
         (if (.compareAndSet a# ~i nil v#)
           v#
           (.get a# i#))))))


(deftype AtomicRefArrayMDM [init
                            ^long max-ki
                            extra-mdm-delay
                            ^AtomicReference added
                            ^AtomicReferenceArray a0]
  MutableDeferredMap

  (mdmFetch
    [_ k ki]
    (if (> ki max-ki)
      (mdm-fetch! @extra-mdm-delay k ki)
      (let [i0 (bit-shift-right ki 5)
            i1 (bit-and ki 31) 
            ^AtomicReferenceArray a1 (arr-getset-lazy a0 i0 (AtomicReferenceArray. 32))
            v  (.get ^AtomicReferenceArray a1 i1)]
        (if (nil? v)

          ;; probably mdmGet was not called... (or in-progress by other thread)
          (let [x (get init k ::none)]
            (if (none? x)
              (let [d (kd/ka-deferred)
                    p (.compareAndSet a1 i1 nil d)]
                (if p
                  (do
                    (loop [g (.get added)]
                      (when-not (.compareAndSet added g (KVCons. k d g))
                        (recur (.get added))))
                    (ETrue. d))
                  (recur k ki)  ;; interrupted by another mdmFetch or mdmGet
                  ))
              (let [d (kd/successed x)]
                ;; copy from 'init'
                (.lazySet a1 i1 d)
                (EFalse. d))))

          (if (none? v)
            ;; new item, was prewarmed by calling mdmGet 
            (let [d (kd/ka-deferred)
                  p (.compareAndSet a1 i1 ::none d)]
              (if p
                (do
                  (loop [g (.get added)]
                    (when-not (.compareAndSet added g (KVCons. k d g))
                      (recur (.get added))))
                  (ETrue. d))
                (do
                  ;; ::none may change only to claimed deferred - no need to retry mdmFetch
                  (EFalse. (.get a1 i1)))))
            (EFalse. v))))))

  (mdmGet
    [_ k ki]
    (if (> ki max-ki)
      (mdm-get! @extra-mdm-delay k ki)
      (let [i0 (bit-shift-right ki 5)
            i1 (bit-and ki 31) 
            ^AtomicReferenceArray a1 (arr-getset-lazy a0 i0 (AtomicReferenceArray. 32))
            v (.get a1 i1)]
        (if (nil? v)
          (let [v (get init k ::none)]
            (if (none? v)
              (do
                ;; maybe put ::none, so mdmFetch don't need to check 'init' again
                (.compareAndSet a1 i1 nil ::none)
                nil)
              (let [d (kd/successed v)]
                ;; copy from 'init'
                (.lazySet a1 i1 d)
                d)))
          (when-not (none? v)
            v)))))

  (mdmFreeze
    [_]
    (let [a (.get added)
          init' (if (realized? extra-mdm-delay)
                  (mdm-freeze! @extra-mdm-delay)
                  init)]
      (if a
        (with-meta
          (loop [^KVCons a a, m (transient init')]
            (if a
              (recur (.-next a)
                     (assoc! m (.-key a) (kd/unwrap1' (.-val a))))
              (persistent! m)))
          (meta init))
        init)))

  (mdmCancel
    [_]
    (when (realized? extra-mdm-delay)
      (mdm-cancel! @extra-mdm-delay))
    (loop [^KVCons a (.get added)]
      (when a
        (let [d (.-val a)]
          (when-not (md/realized? d)
            (kd/cancel! d)))))))


(defn create-mdm-chm [init size-hint]
  (ConcurrentMapMDM.
   init
   (AtomicReference. nil)
   (ConcurrentHashMap. (int size-hint) 0.75 (int 2))))


(defn create-mdm-arr [init size-hint]
  (let [[mk] @keywords-int-ids
        ^int n (quot (+ 31 mk) 32)]
    (AtomicRefArrayMDM.
     init
     mk
     (delay (create-mdm-chm init size-hint))
     (AtomicReference. nil)
     (AtomicReferenceArray. n))))


(defn create-mdm [init size-hint]
  (create-mdm-arr init size-hint))