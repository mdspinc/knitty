(ns ag.knitty.deferred
  (:refer-clojure :exclude [await])
  (:require [clojure.tools.logging :as log]
            [manifold.deferred :as md]
            [ag.knitty.deferred :as kd])
  (:import [java.util.concurrent CancellationException TimeoutException]
           [manifold.deferred IDeferred IMutableDeferred IDeferredListener]
           [ag.knitty KaDeferred]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare connect-to-ka-deferred)
(declare ka-deferred)
(declare connect-two-deferreds)


(KaDeferred/setExceptionLogFn
 (fn log-ex [e] (log/error e "error in deferred handler")))

(definline success'! [d x]
  `(let [^IMutableDeferred d# ~d] (.success d# ~x)))

(definline error'! [d x]
  `(let [^IMutableDeferred d# ~d] (.error d# ~x)))

(definline unwrap1' [x]
  `(let [x# ~x]
     (if (md/deferred? x#)
       (md/success-value x# x#)
       x#)))

(definline listen'!
  [d ls]
  `(let [^IMutableDeferred d# ~d] (.addListener d# ~ls)))


(definline maybe-listen'!
  [d ls]
  `(let [d# ~d
         ^IDeferredListener ls# ~ls]
     (if (instance? IMutableDeferred d#)
       (.addListener ^IMutableDeferred d# ls#)
       (.onSuccess ls# d#))))


(definline listen!
  [d ls]
  `(let [d# ~d
         ^IDeferredListener ls# ~ls]
     (if (instance? IMutableDeferred d#)
       (.addListener ^IMutableDeferred d# ls#)
       (md/on-realized d#
                       (fn ~'on-okk [x#] (.onSuccess ls# x#))
                       (fn ~'on-err [e#] (.onError ls# e#))))))


(defmacro connect-to-ka-deferred [v ka-deferred]
  `(let [d1# ~v
         d2# ~ka-deferred]
     (if (md/deferred? d1#)
       (listen'! d1# d2#)
       (success'! d2# d1#))))



(defmacro chain-listener [next on-succ]
  `(reify manifold.deferred.IDeferredListener
     (onSuccess [_ _#] ~on-succ)
     (onError [_ e#] (.onError ~next e#))))


(defn await-all!
  [^IDeferredListener ls, ^objects ds]
  (let [n (alength ds)]
    (case n
      0 (.onSuccess ls nil)
      1 (maybe-listen'! (aget ds 0) ls)
      (KaDeferred/awaitAll ls ds))))


(defmacro await-ary*
  ([ls]
   `(.onSuccess ~ls nil))
  ([ls x1]
   `(maybe-listen'! ~x1 ~ls))
  ([ls x1 x2]
   `(let [ls# ~ls]
      (maybe-listen'! ~x2 (chain-listener ls# (maybe-listen'! ~x1 ls#)))))
  ([ls x1 x2 & xs]
   (let [xs (list* x1 x2 xs)
         n (count xs)
         df (gensym)]
     `(await-all!
       ~ls
       (let [~df (object-array ~n)]
         ~@(for [[i x] (map vector (range) xs)]
             `(aset ~df ~i ~x))
         ~df)))))


(def ^:private cancellation-exception
  (doto (CancellationException. (str ::cancel!))
    (.setStackTrace (make-array StackTraceElement 0))))


(defn cancel! [d]
  (try
    (md/error! d cancellation-exception)
    (catch Exception e (log/error e "failure while cancelling"))))


(defn revokation-exception? [e]
  (or (instance? CancellationException e)
      (instance? TimeoutException e)))


(deftype RevokeListener
         [^IDeferred deferred
          ^clojure.lang.IFn canceller]

  manifold.deferred.IDeferredListener
  (onSuccess [_ _] (when-not (md/realized? deferred) (canceller)))
  (onError [_ _] (when-not (md/realized? deferred) (canceller))))


(defn revoke' [^IDeferred d c]
  (let [d' (ka-deferred)]
    (connect-to-ka-deferred d d')
    (md/add-listener! d' (RevokeListener. d c))
    d'))

(definline ka-deferred
  "fast lock-free deferred (no executor/claim support)"
  []
  `(KaDeferred.))


(defmethod print-method KaDeferred [y ^java.io.Writer w]
  (.write w "#ag.knitty/Deferred[")
  (let [error (md/error-value y ::none)
        value (md/success-value y ::none)]
    (cond
      (not (identical? error ::none))
      (do
        (.write w ":error ")
        (print-method (class error) w)
        (.write w " ")
        (print-method (ex-message error) w))
      (not (identical? value ::none))
      (do
        (.write w ":value ")
        (print-method value w))
      :else
      (.write w "…")))
  (.write w "]"))

;; >> Knitty aware deferred

(deftype DeferredHookListener [^IMutableDeferred d]
  manifold.deferred.IDeferredListener
  (onSuccess [_ x] (success'! d x))
  (onError [_ e] (error'! d e)))


(defn connect-two-deferreds [^IDeferred d1 ^IDeferred d2]
  (if (instance? KaDeferred d2)
    (listen'! d1 d2)
    (listen'! d1 (DeferredHookListener. d2))))


(deftype KaSuccess
         [val
          ^:volatile-mutable mta]

  clojure.lang.IReference
  (meta [_] mta)
  (resetMeta [_ m] (set! mta m))
  (alterMeta [this f args] (locking this (set! mta (apply f mta args))))

  IMutableDeferred
  (claim [_] false)
  (addListener [_ listener] (.onSuccess ^IDeferredListener listener val) true)
  (cancelListener [_ _listener] false)
  (success [_ _x] false)
  (success [_ _x _token] false)
  (error [_ _x] false)
  (error [_ _x _token] false)

  IDeferred
  (executor [_] nil)
  (realized [_] true)
  (onRealized [_ on-success _on-error] (on-success val))
  (successValue [_ _default-value] val)
  (errorValue [_ default-value] default-value)

  clojure.lang.IPending
  (isRealized [_] true)

  clojure.lang.IDeref
  clojure.lang.IBlockingDeref
  (deref [_] val)
  (deref [_ _ _] val))


(defmethod print-method KaSuccess [y ^java.io.Writer w]
  (.write w "#ag.knitty/Success[")
  (print-method (md/success-value y ::fail) w)
  (.write w "]"))


(definline as-deferred [v]
  `(let [v# ~v]
     (if (md/deferred? v#)
       v#
       (KaSuccess. v# nil))))
