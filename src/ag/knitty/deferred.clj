(ns ag.knitty.deferred
  (:refer-clojure :exclude [await])
  (:require [clojure.tools.logging :as log]
            [manifold.deferred :as md])
  (:import [java.util.concurrent CancellationException TimeoutException]
           [manifold.deferred IDeferred IMutableDeferred]))

(set! *warn-on-reflection* true)


(definline success'! [d x] 
  `(let [^IMutableDeferred d# ~d] (.success d# ~x)))

(definline error'! [d x] 
  `(let [^IMutableDeferred d# ~d] (.error d# ~x)))

(definline unwrap1' [x]
  `(let [x# ~x]
     (if (md/deferred? x#)
       (md/success-value x# x#)
       x#)))

(definline successed [x] 
  `(manifold.deferred.SuccessDeferred. (unwrap1' ~x) nil nil))


(declare connect-d'')

(definline connect'' [d1 d2]
  `(let [d1# (unwrap1' ~d1)
         d2# ~d2]
     (if (md/deferred? d1#)
       (connect-d'' d1# d2#)
       (success'! d2# d1#))
     d1#))

(defn connect-d'' [d1 d2]
  (if (instance? manifold.deferred.IMutableDeferred d1)

    (.addListener
     ^manifold.deferred.IMutableDeferred d1
     (reify manifold.deferred.IDeferredListener
       (onSuccess [_ x] (connect'' x d2))
       (onError [_ e] (error'! d2 e))))
    
    (.onRealized
     ^manifold.deferred.IDeferred d1
     (fn conn-okk [x] (connect'' x d2))
     (fn conn-err [e] (error'! d2 e)))
  ))


(defn await**
  ([^objects da f]
   (await** (alength da) nil da f))
  ([^long i r ^objects da f]
   (if (== 0 i)

     (if r
       (try
         (connect'' (f) r)
         (catch Throwable e (error'! r e)))
       (f))

     (let [i (unchecked-dec i)
           v (aget da i)]
       (cond
         (not (md/deferred? v))
         (recur i r da f)

         (identical? ::none (md/success-value v ::none))
         (let [r (or r (md/deferred nil))]
           (if (instance? manifold.deferred.IMutableDeferred v)

             (.addListener
              ^manifold.deferred.IMutableDeferred v
              (reify manifold.deferred.IDeferredListener
                (onSuccess
                  [_ _]
                  (try
                    (await** i r da f)
                    (catch Throwable e (error'! r e))))
                (onError [_ e] (error'! r e))))

             (.onRealized
              ^manifold.deferred.IDeferred v
              (fn await-okk [_]
                (try
                  (await** i r da f)
                  (catch Throwable e (error'! r e))))
              (fn await-err [e]
                (error'! r e))))
           r)

         :else
         (recur i r da f))))))


(defn await'
  ([vs]
   (let [^objects a (into-array java.lang.Object vs)] 
     (await** (alength a) nil a #(do true))))
  ([vs f]
   (let [^objects a (into-array java.lang.Object vs)]
     (await** (alength a) nil a f))))


(defn await
  ([vs]
   (await vs #(do true)))
  ([vs f]
   (await' (eduction (map #(md/->deferred % %)) vs) f)))


(defmacro realize [binds & body]
  {:pre [(every? simple-symbol? binds)]}
  (if (empty? binds)
    `(do ~@body)
    `(let [x# (force ~(first binds))]
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


(defn zip*'
  ([ds] (apply md/zip' ds))
  ([a ds] (apply md/zip' a ds))
  ([a b ds] (apply md/zip' a b ds))
  ([a b c ds] (apply md/zip' a b c ds))
  ([a b c d ds] (apply md/zip' a b c d ds))
  ([a b c d e & ds] (zip*' (apply list* a b c d e ds))))


(defmacro via [chain [=> expr & forms]]
  (let [s (symbol (name =>))
        [n r] (cond
                (#{'-> '->> 'some-> 'some->>} s) [1 0]
                (#{'cond-> 'cond->>} s)          [2 0]
                (#{'as->} s)                     [1 1]
                :else (throw (Exception. (str "unsupported arrow " =>))))
        x (gensym)
        e (take r forms)
        fs (partition n (drop r forms))]
    (list*
     chain
     expr
     (for [a fs]
       `(fn [~x] (~=> ~x ~@e ~@a))))))


;; predefined popular varints of `via`
(defmacro chain-> [expr & forms] `(via md/chain (-> ~expr ~@forms)))
(defmacro chain->> [expr & forms] `(via md/chain (->> ~expr ~@forms)))
(defmacro chain-as-> [expr name & forms] `(via md/chain (as-> ~expr ~name ~@forms)))

(defmacro chain->' [expr & forms] `(via md/chain' (-> ~expr ~@forms)))
(defmacro chain->>' [expr & forms] `(via md/chain' (->> ~expr ~@forms)))
(defmacro chain-as->' [expr name & forms] `(via md/chain' (as-> ~expr ~name ~@forms)))


(defmacro let-chain-via*
  "simplified version of manifold.deferred/let-flow, resolve deferreds sequentially"
  [chain [v d & rs :as binds] & body]
  (if (empty? binds)
    `(do ~@body)
    `(let [~v ~d]
       (~chain ~v
               (fn [~v]
                 (let-chain-via* ~chain ~rs ~@body))))))

(defmacro let-chain [binds & body]
  `(let-chain-via* md/chain ~binds ~@body))

(defmacro let-chain' [binds & body]
  `(let-chain-via* md/chain' ~binds ~@body))

(defmacro let-chain-revoke [binds & body]
  `(let-chain-via* chain-revoke ~binds ~@body))

(defmacro let-chain-revoke' [binds & body]
  `(let-chain-via* chain-revoke' ~binds ~@body))


(defn maybe-success-value 
  ([x]
   (maybe-success-value x nil))
  ([x d]
   (cond
     (nil? x) d
     (md/deferred? x) (md/success-value x d)
     :else x)))


(defn also
  ([f] (fn [x] (f) x))
  ([f a] (fn [x] (f a) x))
  ([f a b] (fn [x] (f a b) x))
  ([f a b c] (fn [x] (f a b c) x))
  ([f a b c d] (fn [x] (f a b c d) x))
  ([f a b c d & rs] (fn [x] (apply f a b c d rs) x)))
