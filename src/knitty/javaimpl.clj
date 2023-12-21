(import 'knitty.javaimpl.KnittyLoader)
(KnittyLoader/touch)

(ns knitty.javaimpl
  (:require [manifold.deferred :as md]
            [clojure.tools.logging :as log])
  (:import [knitty.javaimpl MDM KDeferred KDeferredAwaiter]))


;; KdDeferred

(KDeferred/setExceptionLogFn
 (fn log-ex [e] (log/error e "error in deferred handler")))

(defmacro create-kd
  ([] `(KDeferred.))
  ([token] `(KDeferred. ~token)))

(defmacro kd-await-all [ls ds]
  `(KDeferredAwaiter/awaitAll ~ls ~ds))

(defmacro kd-await [ls & ds]
  `(KDeferredAwaiter/await ~ls ~@ds))

(definline kd-set-revokee [kd revokee]
  (list '.setRevokee (with-meta kd {:tag "knitty.javaimpl.KDeferred"}) revokee))

(definline kd-claim [kd token]
  (list '.claim (with-meta kd {:tag "knitty.javaimpl.KDeferred"}) token))

(definline kd-chain-from [kd d]
  (list '.chainFrom (with-meta kd {:tag "knitty.javaimpl.KDeferred"}) d))

(definline kd-unwrap [kd]
  (list '.unwrap (with-meta kd {:tag "knitty.javaimpl.KDeferred"})))


(defmethod print-method KDeferred [y ^java.io.Writer w]
  (.write w "#knitty/Deferred[")
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


;; MDM

(definline max-initd []
  `(MDM/maxid))

(definline keyword->intid [k]
  `(MDM/regkw ~k))

(definline mdm-fetch! [mdm kw kid]
  (list '.fetch (with-meta mdm {:tag "knitty.javaimpl.MDM"}) kw kid))

(definline mdm-freeze! [mdm]
  (list '.freeze (with-meta mdm {:tag "knitty.javaimpl.MDM"})))

(definline mdm-cancel! [mdm token]
  (list '.cancel (with-meta mdm {:tag "knitty.javaimpl.MDM"}) token))

(definline mdm-get! [mdm kw kid]
  (list '.get (with-meta mdm {:tag "knitty.javaimpl.MDM"}) kw kid))

(definline none? [x]
  `(MDM/isNone ~x))

(defn create-mdm [init]
  (MDM. init))
