(ns ag.knitty.mdm
  (:require [manifold.deferred :as md])
  (:import [java.util Map HashMap]
           [java.util.concurrent ConcurrentMap ConcurrentHashMap]))


(set! *warn-on-reflection* true)

(defprotocol MutableDeferredMap
  (mdm-fetch! [_ k] "get value or claimed deferred")
  (mdm-freeze! [_] "freeze map, deref all completed deferreds"))


(defn- unwrap-mdm-deferred
  [d]
  (let [d (md/unwrap' d)]
    (cond
      (identical? d ::nil) nil
      (md/deferred? d) (let [sv (md/success-value d ::none)]
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


(defmacro to-nil [x]
  `(let [x# ~x] (when-not (identical? ::nil x#) x#)))


(defmacro de-nil [x]
  `(let [x# ~x] (if (nil? x#) ::nil x#)))

(deftype LockedMapMDM
         [init lock frozen ^Map hm]

  MutableDeferredMap

  (mdm-fetch!
    [_ k]
    (let [v (get init k ::none)]
      (if-not (none? v)
        [nil
         (if-not (md/deferred? v)
           v
           (let [vv (md/success-deferred v ::none)]
             (if (none? vv)
               v
               (locking lock 
                 (when-not @frozen (.put hm k vv)) vv))))]
        (locking lock
          (if-let [v (.get hm k)]
            (if (and (md/deferred? v) (md/realized? v) (not @frozen))
              (let [vv (md/success-value v v)]
                (when-not @frozen
                  (.put hm k (if (nil? vv) ::nil vv)))
                [nil vv])
              [nil (when-not (identical? ::nil v) v)])
            (let [d (md/deferred nil)
                  c (md/claim! d)]
              (when @frozen
                (throw (ex-info "fetch from frozen mdm" {:knitty/yarn k
                                                         ::mdm-frozen true})))
              (.put hm k d)
              [c d]))))))

  (mdm-freeze!
    [_]
    (locking lock
      (when @frozen
        (throw (ex-info "mdm is already frozen" {})))
      (vreset! frozen true))
    (if (.isEmpty hm)
      init
      (into init (map (fn [kv] [(key kv) (unwrap-mdm-deferred (val kv))])) hm))))


(deftype ConcurrentMapMDM
         [init ^ConcurrentMap hm]

  MutableDeferredMap

  (mdm-fetch!
    [_ k]
    (let [v (get init k ::none)]
      (if (none? v)

        ;; from hm or new
        (let [v (.get ^ConcurrentMap hm k)]
          (if (nil? v)
            ;; new
            (let [d (md/deferred nil)
                  p (.putIfAbsent hm k d)
                  d (to-nil (if (nil? p) d p))
                  c (when (md/deferred? d) (md/claim! d))]
              [c d])
            ;; from hm
            (let [v (to-nil v)
                  v' (if (md/deferred? v) (md/success-value v v) v)]
              (when-not (identical? v v')
                (.put hm k (de-nil v')))
              [nil v'])))

        ;; from init
        (if (md/deferred? v)
          (let [v' (md/success-value v v)]
            (when (not (identical? v v'))
              (.putIfAbsent hm k (de-nil v')))
            [nil v'])
          [nil v]))))

  (mdm-freeze!
   [_]
   (if (.isEmpty hm)
     init
     (into init
           (map (fn [kv] [(key kv) (unwrap-mdm-deferred (val kv))]))
           hm))))


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


(defn locked-hash-map-mdm [init hm-size]
  (->LockedMapMDM init (Object.) (volatile! false) (HashMap. (int hm-size))))

(defn concurrent-hash-map-mdm [init hm-size]
  (->ConcurrentMapMDM init (ConcurrentHashMap. (int hm-size) 0.75 (int 2))))

(defn create-mdm [init size-hint]
  (concurrent-hash-map-mdm init size-hint))
