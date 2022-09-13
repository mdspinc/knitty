(ns ag.knitty.mdm
  (:require [ag.knitty.deferred :as kd]
            [manifold.deferred :as md])
  (:import [java.util.concurrent ConcurrentHashMap]))


(set! *warn-on-reflection* true)


(defprotocol MutableDeferredMap
  (mdm-fetch! [_ k] "get value or deferred")
  (mdm-freeze! [_] "freeze map, deref all completed deferreds")
  (mdm-cancel! [_] "freeze map, cancel all deferreds")
  )


(defn- unwrap-mdm-deferred
  [d]
  (let [d (md/unwrap' d)]
    (cond
      (identical? d ::nil) nil
      (md/deferred? d)
      (let [sv (md/success-value d ::none)]
        (if (identical? ::none sv)
          (do
            (alter-meta! d assoc
                         ::leakd true   ;; actual indicator of leaking
                         :type ::leakd  ;; use custom print-method
                         )
            d)
          sv))
      :else d)))


(defmacro none? [x]
  `(identical? ::none ~x))


(deftype ConcurrentMapMDM [init added ^ConcurrentHashMap hm]

  MutableDeferredMap

  (mdm-fetch!
    [_ k]
    (let [v (init k ::none)]
      (if-not (none? v)
        [false v]             ;; from init
        (let [v (.get hm k)]  ;; from hm or new
          (if (nil? v)
            ;; new
            (let [d (md/deferred nil)
                  p (.putIfAbsent hm k d)
                  d (if (nil? p) d p)]
              (when (nil? p) (swap! added conj [k d]))
              [true d])
            ;; from hm
            (let [v' (md/success-value v v)]
              [false v']))))))

  (mdm-freeze!
   [_]
   (let [a @added]
     (if (seq a)
       (into init
             (map (fn [[k d]] [k (unwrap-mdm-deferred d)]))
             a)
       init)))

  (mdm-cancel!
   [_]
   (doseq [[_k d] @added]
     (when-not (md/realized? d)
       (kd/cancel! d))))
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


(defn create-mdm [init size-hint]
  (->ConcurrentMapMDM
   init
   (atom ())
   (ConcurrentHashMap. (int size-hint) 0.75 (int 2))))
