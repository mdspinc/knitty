(ns ag.knitty.deferred
  (:refer-clojure :exclude [await])
  (:require [clojure.tools.logging :as log]
            [manifold.deferred :as md])
  (:import [java.util.concurrent CancellationException TimeoutException CountDownLatch]
           [java.util.concurrent.atomic AtomicReference]
           [manifold.deferred IDeferred IMutableDeferred IDeferredListener]))

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

(definline listen'!
  [d ls]
  `(let [^IMutableDeferred d# ~d] (.addListener d# ~ls)))

(definline listen!
  [d ls]
  `(let [d# ~d
         ^IDeferredListener ls# ~ls]
     (if (instance? IMutableDeferred d#)
       (.addListener ^IMutableDeferred d# ls#)
       (md/on-realized d#
                       (fn ~'on-okk [x#] (.onSuccess ls# x#))
                       (fn ~'on-err [e#] (.onError ls# e#))))))


(deftype DeferredHookListener [^IDeferred d]
  manifold.deferred.IDeferredListener
  (onSuccess [_ x] (success'! d x))
  (onError [_ e] (error'! d e)))


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

    (if (instance? manifold.deferred.IDeferredListener d2)
      (listen'! d1 d2)
      (listen'! d1 (DeferredHookListener. d2)))

    (.onRealized
     ^manifold.deferred.IDeferred d1
     (fn conn-okk [x] (connect'' x d2))
     (fn conn-err [e] (error'! d2 e)))
  ))


(declare ka-deferred)


;; sequentially attaches itself as a listener
(deftype AwaiterListener [^:volatile-mutable ^long i
                          ^objects da
                          ^IMutableDeferred r
                          callback]
  
  manifold.deferred.IDeferredListener
  (onSuccess
   [this _]
   (if (== 0 i)
     (try
       (connect'' (callback) r)
       (catch Throwable e (error'! r e)))
     (do
       (set! i (unchecked-dec i))
       (let [v (aget da i)]
         (cond

           (not (md/deferred? v))
           (recur nil)

           (identical? ::none (md/success-value v ::none))
           (listen! v this)

           :else
           (recur nil))))))
  (onError
   [_ e]
   (error'! r e)))


(defn await*
  [^objects ds f]
  (let [r (ka-deferred)
        a (AwaiterListener. (alength ds) ds r f)]
    (.onSuccess a nil)
    (unwrap1' r)))


(defn await'
  ([vs]
   (let [^objects a (into-array java.lang.Object vs)]
     (await* a #(do true))))
  ([vs f]
   (let [^objects a (into-array java.lang.Object vs)]
     (await* a f))))


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


(deftype RevokeListener 
  [^IDeferred deferred
   ^clojure.lang.IFn canceller]
  
  manifold.deferred.IDeferredListener
  (onSuccess [_ _] (when-not (md/realized? deferred) (canceller)))
  (onError [_ _] (when-not (md/realized? deferred) (canceller)))
)


(defn revoke' [^IDeferred d c]
  (let [d' (ka-deferred)]
    (connect'' d d')
    (md/add-listener! d' (RevokeListener. d c))
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


(defmacro do-chain [& body]
  (list* md/chain () (for [b body] `(fn [_#] ~b))))

(defmacro do-chain' [& body]
  (list* md/chain' () (for [b body] `(fn [_#] ~b))))


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


;; >> Knitty-aware deferred
;; slimmer version of manifold.deferred.IMutableDeferred
;; produces smaller stacktraces (just a nice touch) & works a tiny bit faster
;; also custom deferred class allows us to introduce any custom field for MDM

(deftype ListenerCons [listener next])
(def ^:const empty-listener-cons (ListenerCons. nil nil))

(defmacro ^:private kd-deferred-deref
  [this await-form recur-form]
  `(let [x# ~'valokk]
     (if-not (identical? ::none x#)
       x#
       (let [y# ~'valerr]
         (if-not (identical? ::none y#)
           (throw y#)
           (let [cdl# (CountDownLatch. 1)
                 f# (fn [_#] (.countDown cdl#))]
             (.onRealized ~this f# f#)
             (-> cdl# ~await-form)
             ~recur-form))))))

(defmacro ^:private kd-deferred-onrealized
  [_this on-okk on-err listener]
  `(when-not
    (loop [s# (.get ~'callbacks)]
      (when s#
        (or
         (.compareAndSet ~'callbacks s# (ListenerCons. ~listener s#))
         (recur (.get ~'callbacks)))))
     (loop []
       (let [x# ~'valokk]
         (if (identical? x# ::none)
           (let [y# ~'valerr]
             (if (identical? y# ::none)
               (do
                 (Thread/yield)
                 (recur))
               (->> y# ~on-err)))
           (->> x# ~on-okk))))))

(defmacro ^:private kd-deferred-succerr
  [_this refield x ondo]
  `(when-let [cs# (.getAndSet ~'callbacks nil)]
     (set! ~refield ~x)
     (loop [^ListenerCons cs# cs#]
       (let [^IDeferredListener c# (.-listener cs#)]
         (when c#
           (try
             (-> c# ~ondo)
             (catch Throwable e#
               (log/error e# "error in deferred handler")))
           (recur (.-next cs#)))))
     true))

(deftype KaDeferred
         [^AtomicReference   callbacks
          ^:volatile-mutable valokk
          ^:volatile-mutable valerr
          ^:volatile-mutable mta]

  clojure.lang.IDeref
  (deref [this] (kd-deferred-deref this (.await) (recur)))

  clojure.lang.IBlockingDeref
  (deref [this time timeout] (kd-deferred-deref this (.await time timeout) (recur time timeout)))

  clojure.lang.IPending
  (isRealized [_] (nil? (.get callbacks)))

  clojure.lang.IReference
  (meta [_] mta)
  (resetMeta [_ m] (set! mta m))
  (alterMeta [_ f args] (locking callbacks (set! mta (apply f mta args))))

  manifold.deferred.IDeferred
  (realized [_] (nil? (.get callbacks)))
  (onRealized [this on-okk on-err] (kd-deferred-onrealized this on-okk on-err (md/listener on-okk on-err)))
  (successValue [_ default] (let [x valokk] (if (identical? ::none x) default x)))
  (errorValue [_ default] (let [x valerr] (if (identical? ::none x) default x)))

  manifold.deferred.IDeferredListener
  (onSuccess [this x] (kd-deferred-succerr this valokk x (.onSuccess x)))
  (onError [this x]   (kd-deferred-succerr this valerr x (.onError x)))

  manifold.deferred.IMutableDeferred
  (addListener [this t] (kd-deferred-onrealized this (.onSuccess ^IDeferredListener t) (.onError ^IDeferredListener t) t))
  (success [this x] (kd-deferred-succerr this valokk x (.onSuccess x)))
  (error [this x]   (kd-deferred-succerr this valerr x (.onError x)))
  (claim [_] nil)
  (cancelListener [_ _l] (throw (UnsupportedOperationException.)))
  (success [_ _x _token] (throw (UnsupportedOperationException.)))
  (error [_ _x _token] (throw (UnsupportedOperationException.)))

  ;;
  )

(defn ka-deferred
  "fast lock-free deferred (no executor/claim support)"
  []
  (KaDeferred. (AtomicReference. empty-listener-cons) ::none ::none nil))


(defmethod print-method KaDeferred [y ^java.io.Writer w]
  (.write w "#ag.knitty/Deferred[")
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
        (print-method (successed y) w))
      :else
      (.write w "…")))
  (.write w "]"))

;; >> Knitty aware deferred