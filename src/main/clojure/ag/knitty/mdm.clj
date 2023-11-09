(ns ag.knitty.mdm
  (:import [ag.knitty MDM]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(definline max-initd []
  `(MDM/maxid))

(definline keyword->intid [k]
  `(MDM/regkw ~k))

(definline mdm-fetch! [mdm kid]
  (list '.fetch (with-meta mdm {:tag "ag.knitty.MDM"}) kid))

(definline mdm-freeze! [mdm]
  (list '.freeze (with-meta mdm {:tag "ag.knitty.MDM"})))

(definline mdm-cancel! [mdm]
  (list '.cancel (with-meta mdm {:tag "ag.knitty.MDM"})))

(definline mdm-get! [mdm kid]
  (list '.get (with-meta mdm {:tag "ag.knitty.MDM"}) kid))

(definline fetch-result-claimed? [r]
  (list '.-claimed (with-meta r {:tag "ag.knitty.MDM$Result"})))

(definline fetch-result-value [r]
  (list '.-value (with-meta r {:tag "ag.knitty.MDM$Result"})))

(definline none? [x]
  `(MDM/isNone ~x))

(defn create-mdm [init]
  (MDM. init))
