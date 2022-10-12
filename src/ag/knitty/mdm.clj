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
  (mdmFetch  [^clojure.lang.Keyword kkw ^long kid] "get value or deferred") 
  (mdmGet    [^clojure.lang.Keyword kkw ^long kid] "get value or nil")
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


(defn- unwrap-mdm-deferred [x]
  (if (md/deferred? x)
    (let [val (md/success-value x ::none)]
      (if (identical? val ::none)
        (do
          (alter-meta! x assoc
                       ::leakd true   ;; actual indicator of leaking
                       :type ::leakd  ;; use custom print-method
                       )
          x)
        (recur val)))
    x))


(definline none? [x]
  `(identical? ::none ~x))


(deftype KVCons [key val next])


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
            (let [d (md/deferred nil)
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
       (md/success-deferred v nil))))
  
  (mdmFreeze
    [_]
    (let [a (.get added)]
      (if a
        (with-meta
          (loop [^KVCons a a, m (transient init)]
            (if a
              (recur (.-next a)
                     (assoc! m (.-key a) (unwrap-mdm-deferred (.-val a))))
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


(defmacro ^:private arr-set [a i v]
  `(let [^AtomicReferenceArray a# ~a
         i# ~i
         v# ~v]
     (when (.compareAndSet a# i# nil v#)
       v#)))


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
      (let [v (init k ::none)]
        (if-not (none? v)
          (EFalse. v)               ;; from init
          (let [i0 (bit-shift-right ki 5)
                i1 (bit-and ki 31)
                a1 (arr-getset-lazy a0 i0 (AtomicReferenceArray. 32))
                v  (.get ^AtomicReferenceArray a1 i1)]

            (if (nil? v)
             ;; new
              (let [d (md/deferred nil)
                    p (arr-set a1 i1 d)]
                (when p
                  (loop [g (.get added)]
                    (when-not (.compareAndSet added g (KVCons. k d g))
                      (recur (.get added)))))
                (ETrue. d))
             ;; from hm
              (let [v' (md/success-value v v)]
                (EFalse. v'))))))))
  
  (mdmGet
   [_ k ki]
   (if (> ki max-ki)
     (mdm-get! @extra-mdm-delay k ki)
     (let [v (init k ::none)]
       (if-not (none? v)
         (md/success-deferred v nil)
         (let [i0 (bit-shift-right ki 5) 
               a1 (.get ^AtomicReferenceArray a0 i0)]
           (when a1
             (let [i1 (bit-and ki 31)
                   v (.get ^AtomicReferenceArray a1 i1)]
               v)))))))

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
                     (assoc! m (.-key a) (unwrap-mdm-deferred (.-val a))))
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
            (kd/cancel! d))))))
  )
 

(defmethod print-method ::leakd [y ^java.io.Writer w]
  (.write w "#ag.knitty/LeakD[")
  (let [error (md/error-value y nil)]
    (cond
      error
      (do
        (.write w ":error ")
        (print-method (class error) w)
        (.write w " ")
        (print-method (ex-message error) w))
      (md/realized? y)
      (do
        (.write w ":value ")
        (print-method (md/success-value y nil) w))
      :else
      (.write w "…")))
  (.write w "]"))


(defn create-mdm-chm [init size-hint]
  (ConcurrentMapMDM.
   init
   (AtomicReference. nil)
   (ConcurrentHashMap. (int size-hint) 0.75 (int 2))))


(defn create-mdm-arr [init size-hint]
  (let [[mk] @keywords-int-ids
        ^int n (inc (quot mk 32))]
    (AtomicRefArrayMDM.
     init
     mk
     (delay (create-mdm-chm init size-hint))
     (AtomicReference. nil)
     (AtomicReferenceArray. n))))


(defn create-mdm [init size-hint]
  (create-mdm-arr init size-hint))