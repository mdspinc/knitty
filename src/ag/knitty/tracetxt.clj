(ns ag.knitty.tracetxt
  (:require [ag.knitty.trace :as t]))


(def ^:private inst-format
  (java.text.SimpleDateFormat.
   "yyyy-MM-dd HH:mm:ss.SSS"))


(defn- format-inst [t]
  (.format inst-format t))


(def ^:private format-dep-kind
  {:sync   ""
   :defer  "[defer]"
   :lazy   "[lazy]"
   :ref    "[ref]"
   :yankfn "[yankfn]"})


(defn- render-tracegraph-txt [ts]

  (doseq [{:keys [at yarns tracelog] :as t} (reverse ts)]
    (let [{:keys [nodes]} (t/parse-trace t)]

      (println "at" (format-inst at)
               "yank" yarns)

      (doseq [{:keys [yarn event]} (reverse tracelog)
              :let [n (nodes yarn)]]

        (case event

          ::t/trace-start
          (do
            (println "yanked"
                     yarn
                     (if-let [c (:caller n)]
                       (str "by " c)
                       ""))
            (doseq [[d k] (:all-deps n)]
              (println
               " "
               (if (when-let [x (nodes d)] (not= (:type x) :input)) "call" "read")
               d
               (format-dep-kind k k))))

          ::t/trace-call
          (if-let [e (:error n)]
            (println "fail" yarn ":" (ex-message e))
            (println "exec" yarn))

          ::t/trace-finish
          (println
           "done"
           yarn
           (if (:deferred n)
             "[defer]"
             ""))

          :no-match)))

    (println)))


(defn print-trace
  [traces]
  (when-let [traces (t/find-traces traces)]
    (render-tracegraph-txt traces)))
