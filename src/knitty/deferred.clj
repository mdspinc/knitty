(ns knitty.deferred
  (:refer-clojure :exclude [await])
  (:require [manifold.deferred :as md]
            [clojure.tools.logging :as log]
            [knitty.javaimpl :as ji])
  (:import [java.util.concurrent CancellationException TimeoutException]
           [manifold.deferred IDeferred IMutableDeferred IDeferredListener]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(definline success'! [d x t]
  `(let [^IMutableDeferred d# ~d] (.success d# ~x ~t)))

(definline error'! [d x t]
  `(let [^IMutableDeferred d# ~d] (.error d# ~x ~t)))

(definline unwrap1' [x]
  `(let [x# ~x]
     (if (md/deferred? x#)
       (md/success-value x# x#)
       x#)))


(deftype FnSucc [^IDeferredListener ls]
  clojure.lang.IFn
  (invoke [_ x] (.onSuccess ls x)))


(deftype FnErrr [^IDeferredListener ls]
  clojure.lang.IFn
  (invoke [_ x] (.onError ls x)))


(definline listen!
  [d ls]
  `(let [d# ~d, ^IDeferredListener ls# ~ls]
     (if (instance? IMutableDeferred d#)
       (.addListener ^IMutableDeferred d# ls#)
       (.onRealized ^IDeferred d# (FnSucc. ls#) (FnErrr. ls#)))))


(defmacro await-ary*
  ([ls]
   `(.onSuccess ~ls nil))
  ([ls x1]
   `(ji/kd-await ~ls ~x1))
  ([ls x1 x2]
   `(ji/kd-await ~ls ~x1 ~x2))
  ([ls x1 x2 x3]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3))
  ([ls x1 x2 x3 x4]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4))
  ([ls x1 x2 x3 x4 x5]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5))
  ([ls x1 x2 x3 x4 x5 x6]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6))
  ([ls x1 x2 x3 x4 x5 x6 x7]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13 ~x14))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13 ~x14 ~x15))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16]
   `(ji/kd-await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13 ~x14 ~x15 ~x16))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 & xs]
   (let [xs (list* x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 xs)
         n (count xs)
         df (gensym)]
     `(knitty.javaimpl.KDeferredAwaiter/awaitArr
       ~ls
       (let [~df (knitty.javaimpl.KDeferredAwaiter/createArr ~n)]
         ~@(for [[i x] (map vector (range) xs)]
             `(knitty.javaimpl.KDeferredAwaiter/setArrItem ~df ~i ~x))
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
  (let [d' (ji/create-kd)]
    (listen! d' (RevokeListener. d c))
    (ji/kd-chain-from d' d nil)
    ))


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
  (.write w "#knitty/Success[")
  (print-method (md/success-value y ::fail) w)
  (.write w "]"))


(definline as-deferred [v]
  `(let [v# ~v]
     (if (md/deferred? v#)
       v#
       (KaSuccess. v# nil))))
