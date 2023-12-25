(import 'knitty.javaimpl.KnittyLoader)
(KnittyLoader/touch)

(ns knitty.javaimpl
  (:require [manifold.deferred :as md]
            [clojure.tools.logging :as log])
  (:import [knitty.javaimpl MDM KDeferred KAwaiter Yarn]))


(definline yarn-deps [y]
  `(let [^Yarn y# ~y] (.deps y#)))

(definline yarn-key [y]
  `(let [^Yarn y# ~y] (.key y#)))

(definline yarn-yank [y mdm dest]
  `(let [^Yarn y# ~y] (.yank y# ~mdm ~dest)))


(KDeferred/setExceptionLogFn
 (fn log-ex [e] (log/error e "error in deferred handler")))


(definline kd-success! [d x t]
  `(let [^KDeferred d# ~d] (.success d# ~x ~t)))

(definline kd-error! [d x t]
  `(let [^KDeferred d# ~d] (.error d# ~x ~t)))

(defmacro kd-create
  ([] `(KDeferred/create))
  ([token] `(KDeferred/create ~token)))

(definline kd-wrap [x]
  `(KDeferred/wrap ~x))

(definline kd-unwrap [kd]
  `(let [^KDeferred x# ~kd] (.unwrap x#)))

(definline kd-get [kd]
  `(let [^KDeferred x# ~kd] (.get x#)))

(definline kd-revoke [d c]
  `(KDeferred/revoke ~d ~c))

(definline kd-claim [kd token]
  `(let [^KDeferred x# ~kd] (.claim x# ~token)))

(definline kd-chain-from [kd d token]
  `(let [^KDeferred x# ~kd] (.chainFrom x# ~d ~token)))

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


;; MDM

(definline max-initd []
  `(MDM/maxid))

(definline keyword->intid [k]
  `(MDM/regkw ~k))

(definline mdm-freeze! [mdm]
  (list '.freeze (with-meta mdm {:tag "knitty.javaimpl.MDM"})))

(definline mdm-cancel! [mdm]
  (list '.cancel (with-meta mdm {:tag "knitty.javaimpl.MDM"})))

(definline mdm-fetch! [mdm kid]
  (list '.fetch (with-meta mdm {:tag "knitty.javaimpl.MDM"}) kid))
