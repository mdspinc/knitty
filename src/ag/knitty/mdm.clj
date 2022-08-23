(ns ag.knitty.mdm 
  (:require [manifold.deferred :as md]) 
  (:import [java.util Map HashMap]))


(set! *warn-on-reflection* true)

(defprotocol MutableDeferredMap
  (mdm-fetch! [_ k] "get value or claimed deferred")
  (mdm-freeze! [_] "freeze map, deref all completed deferreds"))


(defn- unwrap-mdm-deferred
  [d]
  (let [d (md/unwrap' d)]
    (when-not (= ::nil d)
      (when (md/deferred? d)
        (alter-meta! d assoc
                     ::leaked true   ;; actual indicator of leaking
                     :type ::leaked  ;; use custom print-method
                     ))
      d)))


(deftype LockedMapMDM
         [init lock frozen ^Map hm]

  MutableDeferredMap

  (mdm-fetch!
    [_ k]
    (if-let [kv (find init k)]
      (let [v (val kv)
            vv (md/unwrap' v)]
        (when-not (identical? vv v)
          (locking lock (.put hm k vv)))
        [false vv])
      (locking lock
        (if-let [v (.get hm k)]
          (if (and (md/deferred? v) (md/realized? v) (not @frozen))
            (let [vv (md/success-value v v)]
              (.put hm k (if (nil? vv) ::nil vv))
              [false vv])
            [false (when-not (identical? ::nil v) v)])
          (let [d (md/deferred nil)
                c (md/claim! d)]
            (when @frozen
              (throw (ex-info "fetch from frozen mdm" {:ag.knitty/yarn k
                                                       ::mdm-frozen true})))
            (.put hm k d)
            [c d])))))

  (mdm-freeze!
    [_]
    (locking lock
      (when @frozen
        (throw (ex-info "mdm is already frozen" {})))
      (vreset! frozen true))
    (if (.isEmpty hm)
      init
      (into init (map (fn [[k v]] [k (unwrap-mdm-deferred v)])) hm))))


(defmethod print-method ::leaked [y ^java.io.Writer w]
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


(defn locked-hmap-mdm [init hm-size]
  (LockedMapMDM. init (Object.) (volatile! false) (HashMap. (int hm-size))))

