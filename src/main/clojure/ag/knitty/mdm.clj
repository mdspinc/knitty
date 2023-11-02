(ns ag.knitty.mdm
  (:require [ag.knitty.deferred :as kd]
            [manifold.deferred :as md])
  (:import [clojure.lang Associative]
           [ag.knitty MDM]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicReference AtomicReferenceArray]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(definline max-initd [] `(MDM/maxid))
(definline keyword->intid [k] `(MDM/regkw ~k))

(defmacro mdm-fetch! [mdm kid]
  (list '.fetch (with-meta mdm {:tag "ag.knitty.MDM"}) kid))

(defmacro mdm-freeze! [mdm]
  (list '.freeze (with-meta mdm {:tag "ag.knitty.MDM"})))

(defmacro mdm-cancel! [mdm]
  (list '.cancel (with-meta mdm {:tag "ag.knitty.MDM"})))

(defmacro mdm-get! [mdm kid]
  (list '.get (with-meta mdm {:tag "ag.knitty.MDM"}) kid))

(defmacro fetch-result-claimed? [r]
  (list '.-claimed (with-meta r {:tag "ag.knitty.MDM$Result"})))

(defmacro fetch-result-value [r]
  (list '.-value (with-meta r {:tag "ag.knitty.MDM$Result"})))

(defmacro none? [x]
  `(MDM/isNone ~x))

(defn create-mdm [init]
  (MDM. init))
