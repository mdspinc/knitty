(ns ag.knitty.deferred
  (:require [clojure.tools.logging :as log]
            [manifold.deferred :as md])
  (:import [java.util.concurrent CancellationException TimeoutException]
           [java.util.concurrent.atomic AtomicInteger]
           [manifold.deferred IDeferred IMutableDeferred IDeferredListener]))

(set! *warn-on-reflection* true)


(definline success'!
  [d x]
  `(let [^IMutableDeferred d# ~d] (.success d# ~x)))


(definline error'!
  [d x]
  `(let [^IMutableDeferred d# ~d] (.error d# ~x)))


(defn await' [vals f]
  (let [ds (filterv #(and (md/deferred? %) (not (md/realized? %))) vals)
        c (count ds)]
    (case c
      0 (f)
      1 (md/chain' (nth ds 0) (fn [_] (f)))
      2 (md/chain' (nth ds 0) (fn [_] (nth ds 1)) (fn [_] (f)))
      (let [d (md/deferred nil)
            a (AtomicInteger. c)
            fv (fn await-okk [_]
                 (when (== 0 (.decrementAndGet a))
                   (try
                     (success'! d (f))
                     (catch Throwable e (error'! d e)))))
            fe (fn awaikt-err [e]
                 (md/error! d e))]
        (reduce #(md/on-realized %2 fv fe) nil ds)
        d))))


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
     (catch Exception e (log/error e "failure while cancelling"))))
  ([d claim-token]
   (try
     (md/error! d cancellation-exception claim-token)
     (catch Exception e (log/error e "failure while cancelling")))))


(defn revokation-exception? [e]
  (or (instance? CancellationException e)
      (instance? TimeoutException e)))


(defn connect'' [d1 d2]
  (if (md/deferred? d1)
    (md/on-realized
     d1
     #(connect'' % d2)
     #(error'! d2 %))
    (success'! d2 d1)))


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
  ([a b c d e & ds] (zip* (apply list* a b c d e ds))))


(defn- via-n [chain n k r => [expr & forms]]
  (let [x (gensym "x")
        ns (take n forms)
        rs (take r forms)
        ks (drop k forms)]
    (if (seq ks)
      `(~chain ~expr (fn [~x]
                       ~(via-n chain n k r =>
                               `((~=> ~x ~@ns)
                                 ~@rs
                                 ~@ks))))
      `(~chain (~=> ~expr ~@ns)))))


(defmacro via [chain [=> & forms]]
  (let [s (symbol (name =>))]
    (cond
      (#{'-> '->> 'some-> 'some->>} s) (via-n chain 1 1 0 => forms)
      (#{'cond-> 'cond->>} s)          (via-n chain 2 2 0 => forms)
      (#{'as->} s)                     (via-n chain 2 2 1 => forms)
      :else (throw (Exception. (str "unsupported arrow " =>))))))
