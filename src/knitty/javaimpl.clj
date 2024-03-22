(import 'knitty.javaimpl.KnittyLoader)
(KnittyLoader/touch)

(ns knitty.javaimpl
  (:require [manifold.deferred :as md]
            [clojure.tools.logging :as log])
  (:import [knitty.javaimpl KDeferred KAwaiter KwMapper]))


(KDeferred/setExceptionLogFn
 (fn log-ex [e msg] (log/error e msg)))


(definline regkw [k]
  `(KwMapper/regkw ~k))

(definline maxid []
  `(KwMapper/maxi))

(defrecord YarnInfo
           [type key deps body-sexp])

(defmacro decl-yarn
  ([ykey deps bodyf]
   (assert (qualified-keyword? ykey))
   (regkw ykey)
   `(decl-yarn ~(symbol (name ykey)) ~ykey ~deps ~bodyf))
  ([fnname ykey deps [_fn [ctx dst] & body]]
   `(fn
      ~fnname
      ([] ~(if (and (keyword? ykey)
                    (set? deps))
             (->YarnInfo
              :knitty/yarn-info
              ykey
              deps
              body)
             (list
              `->YarnInfo
              :knitty/yarn-info
              ykey
              deps
              (list `quote body))))
      ([~(vary-meta ctx assoc :tag "knitty.javaimpl.YankCtx")
        ~(vary-meta dst assoc :tag "knitty.javaimpl.KDeferred")]
       ~@body))))


(definline yarn-deps [y]
  `(:deps (~y)))

(definline yarn-key [y]
  `(:key (~y)))

(definline yarn-yank [y ctx d]
  `(~y ~ctx ~d))

(definline kd-success! [d x t]
  `(let [^KDeferred d# ~d] (.success d# ~x ~t)))

(definline kd-error! [d x t]
  `(let [^KDeferred d# ~d] (.error d# ~x ~t)))

(definline kd-realized? [d]
  `(let [^KDeferred d# ~d] (.realized d#)))

(defmacro kd-create
  ([] `(KDeferred/create))
  ([token] `(KDeferred/create ~token)))

(definline kd-wrap [x]
  `(KDeferred/wrap ~x))

(definline kd-unwrap [kd]
  `(let [^KDeferred x# ~kd] (.unwrap x#)))

(definline kd-get [kd]
  `(let [^KDeferred x# ~kd] (.get x#)))

(definline kd-revoke [d cancel err-handler]
  `(KDeferred/revoke ~d ~cancel ~err-handler))

(definline kd-revoke-to [kd kd0]
  `(let [^KDeferred x# ~kd] (.revokeTo x# ~kd0)))

(definline kd-chain-from [kd d token]
  `(let [^KDeferred x# ~kd] (.chain x# ~d ~token)))

(definline kd-bind [d vf ef token]
  `(let [^KDeferred x# ~d] (.bind x# ~vf ~ef ~token)))

(defmacro kd-after* [d & body]
  `(kd-bind ~d
            (fn [x#] (do ~@body) x#)
            (fn [e#] (do ~@body) (KDeferred/wrapErr e#))
            nil))

(defmacro kd-await
  ([ls]
   `(KAwaiter/await ~ls))
  ([ls x1]
   `(KAwaiter/await ~ls ~x1))
  ([ls x1 x2]
   `(KAwaiter/await ~ls ~x1 ~x2))
  ([ls x1 x2 x3]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3))
  ([ls x1 x2 x3 x4]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4))
  ([ls x1 x2 x3 x4 x5]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5))
  ([ls x1 x2 x3 x4 x5 x6]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6))
  ([ls x1 x2 x3 x4 x5 x6 x7]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13 ~x14))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13 ~x14 ~x15))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16]
   `(KAwaiter/await ~ls ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13 ~x14 ~x15 ~x16))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 & xs]
   (let [xs (list* x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 xs)
         n (count xs)
         df (gensym)]
     `(knitty.javaimpl.KAwaiter/awaitArr
       ~ls
       (let [~df (knitty.javaimpl.KAwaiter/createArr ~n)]
         ~@(for [[i x] (map vector (range) xs)]
             `(knitty.javaimpl.KAwaiter/setArrItem ~df ~i ~x))
         ~df)))))

(definline kd-await-coll [ls ds]
  `(let [^java.lang.Iterable ds# ~ds] (KAwaiter/awaitIter ~ls (.iterator ds#))))

(defmethod print-method KDeferred [y ^java.io.Writer w]
  (.write w "#knitty/D[")
  (let [error (md/error-value y ::none)
        value (md/success-value y ::none)]

    (cond
      (not (identical? error ::none))
      (do
        (.write w ":err ")
        (print-method (class error) w)
        (.write w " ")
        (print-method (ex-message error) w))

      (not (identical? value ::none))
      (print-method value w)

      :else
      (.write w "…")))
  (.write w "]"))
