(ns ag.knitty.traceviz 
  (:require [ag.knitty.trace 
             :refer [find-traces
                     merge-parsed-traces
                     parse-trace]]
            [clojure.java.browse :as browse]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [tangle.core :as tgl]))


(defn- graphviz-escape [s]
  (if s
    (str/escape
     s
     {\< "&lt;", \> "&gt;", \& "&amp;"})
    ""))

(defn- short-string [n s]
  (graphviz-escape
   (if (> (count s) n)
     (str (subs s 0 (- n 3)) "…")
     s)))

(defn- short-string-multiline [s]
  (binding  [*print-length* 8
             *print-level* 2
             *print-namespace-maps* true]
    (->>
     (pr-str s)
     (short-string #=(* 10 60))
     (partition-all 60)
     (map #(str/join "" %))
     (map graphviz-escape)
     (str/join "<br />"))))


(defn- safe-minus [a b]
  (when (and a b (not (zero? a)) (not (zero? b)))
    (- a b)))


(defn- nice-time [t]
  (when t
    (let [[f s] (condp > t
                  1e3  ["%.0fns" 1e-0]
                  1e4  ["%.2fus" 1e-3]
                  5e4  ["%.1fus" 1e-3]
                  1e6  ["%.0fus" 1e-3]
                  1e7  ["%.2fms" 1e-6]
                  3e7  ["%.1fms" 1e-6]
                  5e8  ["%.0fms" 1e-6]
                  1e10 ["%.2fs"  1e-9]
                  5e10 ["%.1fs"  1e-9]
                  ["%.0fs" 1e-9])]
      (format f (* s t)))))


(defn- render-tracegraph-dot [g]
  (tgl/graph->dot

   ;; nodes
   (for [[k v] (sort-by #(some-> % second :start-at -) (:nodes g))]
     (assoc v :id k))

   ;;regular links
   (sort-by
    (fn [[_ c]] (some-> c :time -))
    (for [[[a b] c] (:links g)]
      [(str (:yankid-dst c) "$" b)
       (str (:yankid-src c) "$" a)
       c]))

   ;; tangle.core options
   {:directed? true

    :node->id
    (fn [{:keys [id yankid]}]
      (str yankid "$" id))

    :graph {:dpi 120
            :rankdir :TB
            :ranksep 1.5
            :newrank true}

    :node->cluster
    (fn [{:keys [yankid]}]
      (str yankid))

    :cluster->descriptor
    (fn [c]
      (let [x (-> (parse-long c))
            sg (-> g :clusters (get x))]
        {:label (str
                 (nice-time (or (:shift sg) 0))
                 " ⊕ "
                 "Δ" (nice-time (:time sg))
                 " ⟹ "
                 (nice-time (safe-minus (:done-at sg) (:base-at g))))
         :style "dashed"
         :color "gray"
         :labelloc "b"
         :labeljust "r"
         :fontsize  10}))

    :node->descriptor
    (fn [{:keys [type deferred id value thread error
                 start-at deps-time func-time finish-at]}]
      {:shape :rect
       :color "silver"
       :style  (cond
                 (= :lazy-unused type) "dotted,filled"
                 (= :input type)       "dotted,filled"
                 (= :yanked type)      "bold,filled"
                 deferred              "rounded,filled"
                 :else                 "solid,filled")
       :fillcolor (cond
                    error                   "lightpink"
                    (= :lazy-unused type)   "lightcyan"
                    (= :leaked type)        "violet"
                    (= :input type)         "skyblue"
                    (= :changed-input type) "lightpink"
                    (= :yanked type)        "lightgreen"
                    deferred                "navajowhite"
                    :else                   "lemonchiffon")
       :label [:font
               {:face "monospace" :point-size 7, :color "darkslategray"}
               (cond-> [:table {:border 0}]
                 true
                 (conj
                  [:tr [:td {:colspan 2}
                        [:font {:face "monospace bold"
                                :point-size 10
                                :color "black"}
                         (str id)]]])

                 (not (#{:lazy-unused :leaked} type))
                 (conj
                  [:tr [:td {:colspan 2, :align "text"}
                        [:font {:point-size 9
                                :face "monospace",
                                :color "blue"}
                         (cond
                           (some-> error ex-data ::mdm-frozen)
                           ""

                           (and error (instance? clojure.lang.IExceptionInfo error))
                           [:font
                            (ex-message error)
                            [:br {:align :left}]
                            (some-> error ex-data short-string-multiline)]

                           error
                           [:font
                            "exception " (-> error class (.getName))
                            [:br {:align :left}]
                            (ex-message error)]

                           :else (short-string-multiline value))
                         [:br {:align :left}]]]])

                 (#{:lazy-unused} type)
                 (conj [:tr [:td {:colspan 2} "unused"]])

                 (#{:leaked} type)
                 (conj [:tr [:td {:colspan 2} "leaked"]])

                 (#{:yanked :interim :leaked} type)
                 (conj
                  [:tr [:td {:align :left}
                        (if deferred
                          "thread*"
                          "thread")]
                   [:td {:align :right} (short-string 40 thread)]]
                  [:tr [:td {:align :left} "time"]
                   [:td {:align :right}
                    (nice-time (safe-minus start-at (:base-at g)))
                    " ⊕ "
                    "Δ" (or (nice-time deps-time) "...")
                    " ⊎ "
                    "Δ" (or (nice-time func-time) "...")
                    (when finish-at " ⟹ ")
                    (when finish-at
                      [:font {:color "black"}
                       (nice-time (safe-minus finish-at (:base-at g)))])]]))]})

    :edge->descriptor
    (fn [_ _ {:keys [source type used cause timex]}]
      {:label     (or (when (and timex (pos? timex))
                        (str "+" (nice-time timex)))
                      "")
       :fontsize  8
       :dir       "both"
       :color     (if cause "black" "dimgrey")
       :arrowsize "0.7"
       :arrowtail (cond
                    (#{:sync} source)               "normal"
                    (#{:defer :input-defer} source) "empty"
                    (#{:input} source)              "vee"
                    (#{:changed-input} type)        "none"
                    :else                           "normal")
       :arrowhead (cond
                    (= :sync type)           "none"
                    (= :defer type)          "odot"
                    (= :lazy-sync type)      "diamond"
                    (= :lazy-defer type)     "odiamond"
                    (#{:changed-input} type) "none"
                    :else                    "normal")
       :constraint  (not= type :changed-input)
       :style (cond
                cause                   "bold"
                (not used)              "dotted"
                (= type :changed-input) "tapered"
                :else                   "solid")})}))

(defn- fix-xdot-escapes [d]
  ;; slow workaround for https://gitlab.com/graphviz/graphviz/-/issues/165
  (str/replace d "\\" "⧵"))


(defn view-trace*
  [traces type]
  (when-let [traces (find-traces traces)]
    (let [gs (map parse-trace traces)
          g (merge-parsed-traces gs)]
      (case type
        :raw g
        :dot (render-tracegraph-dot g)
        ::xdot (fix-xdot-escapes (render-tracegraph-dot g))
        :png (tgl/dot->image (render-tracegraph-dot g) "png")
        :svg (tgl/dot->svg (render-tracegraph-dot g))
        (tgl/dot->image (render-tracegraph-dot g) (name type))))))


(def xdot-available
  (delay
   (zero? (:exit (shell/sh "python" "-m" "xdot" "--help")))))


(def graphviz-available
  (delay
   (zero? (:exit (shell/sh "dot" "-V")))))


(defn view-trace [poy]
  (cond 
    
    (and (force xdot-available) (force graphviz-available))
    (let [t (view-trace* poy ::xdot)
          f (java.io.File/createTempFile "knitty_untangle_" ".xdot")]
      (when-not t
        (throw (ex-info "trace not found" {::poy poy})))
      (io/copy t f)
      (future (shell/sh "python" "-m" "xdot" (str f))))

    (force graphviz-available)
    (let [t (view-trace* poy :svg)
          f (java.io.File/createTempFile "knitty_untangle_" ".svg")]
      (when-not t
        (throw (ex-info "trace not found" {::poy poy})))
      (io/copy t f)
      (future (browse/browse-url f)))

    :else
    (throw (ex-info "graphviz (dot) not found, please install it" {}))))

