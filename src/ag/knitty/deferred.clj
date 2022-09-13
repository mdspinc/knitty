(ns ag.knitty.deferred
  (:require [clojure.tools.logging :refer [error]]
            [manifold.deferred :as md])
  (:import [java.util.concurrent CancellationException TimeoutException]
           [manifold.deferred IDeferred]))

(set! *warn-on-reflection* true)


(defmacro realize [binds & body]
  {:pre [(every? simple-symbol? binds)]}
  (if (empty? binds)
    `(do ~@body)
    `(let [x# ~(first binds)]
       (md/chain'
        (md/->deferred x# x#)
        (fn [~(first binds)]
          (realize [~@(rest binds)] ~@body))))))


(defmacro realize' [binds & body]
  {:pre [(every? simple-symbol? binds)]}
  (if (empty? binds)
    `(do ~@body)
    `(md/chain'
      ~(first binds)
      (fn [~(first binds)]
        (realize' [~@(rest binds)] ~@body)))))


(def ^:private cancellation-exception
  (doto (CancellationException. (str ::cancel!))
    (.setStackTrace (make-array StackTraceElement 0))))


(defn cancel!
  ([d]
   (try
     (md/error! d cancellation-exception)
     (catch Exception e (error e "failure while cancelling"))))
  ([d claim-token]
   (try
     (md/error! d cancellation-exception claim-token)
     (catch Exception e (error e "failure while cancelling")))))


(defn revokation-exception? [e]
  (or (instance? CancellationException e)
      (instance? TimeoutException e)))


(defn connect'' [d1 d2]
  (if (md/deferred? d1)
    (md/on-realized
     d1
     #(md/success! d2 %)
     #(md/error! d2 %))
    (md/success! d2 d1)))


(defn revoke' [^IDeferred d c]
  (let [e (.executor d)
        d' (md/deferred e)
        cc (fn [_] (when-not
                    (md/realized? d)
                     (c)))]
    (connect'' d d')
    (md/on-realized d' cc cc)
    d'))


(defn revoke [d c]
  (revoke' (md/->deferred d) c))


(defn- chain-revoke*
  [revoke chain x fns]
  (let [abort (volatile! false)
        curd  (volatile! x)
        fnf (fn [f]
              (fn [d]
                (when-not @abort
                  (vreset! curd (f d)))))]
    (revoke
     (transduce (map fnf) chain x fns)
     (fn []
       (vreset! abort true)
       (when-let [d @curd]
         (when (md/deferred? d)
           (cancel! d)))))))


(defn chain-revoke
  [x & fns]
  (chain-revoke* revoke md/chain x fns))


(defn chain-revoke'
  [x & fns]
  (chain-revoke* revoke' md/chain' x fns))


(defn zip* 
  ([ds] (apply md/zip ds))
  ([a ds] (apply md/zip a ds))
  ([a b ds] (apply md/zip a b ds))
  ([a b c ds] (apply md/zip a b c ds))
  ([a b c d ds] (apply md/zip a b c d ds))
  ([a b c d e & ds] (zip* (apply list* a b c d e ds)))
  )
