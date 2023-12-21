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


(defn await-all-array
  [^IDeferredListener ls, ^objects ds]
  (let [n (alength ds)]
    (case n
      0 (.onSuccess ls nil)
      1 (ji/kd-await ls (aget ds 0))
      2 (ji/kd-await ls (aget ds 0) (aget ds 1))
      3 (ji/kd-await ls (aget ds 0) (aget ds 1) (aget ds 2))
      (ji/kd-await-all ls ds)
      )))


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
  ([ls x1 x2 x3 x4 & xs]
   (let [xs (list* x1 x2 x3 x4 xs)
         n (count xs)
         df (gensym)]
     `(ji/kd-await-all
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
  (let [d' (ji/create-kd)]
    (listen! d' (RevokeListener. d c))
    (ji/kd-chain-from d' d)))


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
