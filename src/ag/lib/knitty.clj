(ns ag.lib.knitty
  (:require [clojure.java.browse]
            [clojure.java.browse-ui]
            [clojure.java.io :as io]
            [clojure.java.shell]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [manifold.deferred :as md]
            [manifold.executor]
            [tangle.core :as tgl])
  (:import [java.util HashMap Map]))

;; >> API
(declare yank)        ;; func,  (yank <map> [yarn-keys]) => @<new-map>
(declare yarn)        ;; macro, (yarn keyword { (bind-symbol keyword)* } expr)
(declare defyarn)     ;; macro, (defyarn name doc? spec? { (bind-symbol keyword)* } <expr>)
(declare with-yarns)  ;; macro, (with-yarns [<yarns>+] <body ...>)
(declare untangle)    ;; func,  (untangle <map> or (deffered [_ <map>]))
;; << API

(set! *warn-on-reflection* true)

;; mapping {keyword => Yarn}
(def ^:dynamic *registry* {})

(def ^:dynamic *tracing* true)


(defprotocol IYarn
  (yarn-snatch* [_ mdm reg tracer] "get value or obligated deferred")
  (yarn-key* [_] "get yarn key"))

(defprotocol MutableDeferredMap
  (mdm-fetch! [_ k] "get value or obligate deferred")
  (mdm-freeze! [_] "freeze map, deref all completed deferreds"))

(deftype Yarn [yarn deps funcv yget]
  IYarn
  (yarn-snatch* [_ mdm reg tracer] (yget mdm reg tracer))
  (yarn-key* [_] yarn))

(defprotocol Tracer
  (trace-start [_])
  (trace-call [_])
  (trace-finish [_ value error async])
  (trace-dep [_ k])
  (trace-all-deps [_ yarns])
  (trace-build-sub-tracer [_ k]))


(defn register-yarn [yarn]
  (let [k (yarn-key* yarn)]
    (when-not (qualified-keyword? k)
      (throw (ex-info "yarn must be a qualified keyword" {::yarn k})))
    (alter-var-root #'*registry* assoc k yarn)))


;; MDM


(defn- unwrap-mdm-deferred
  [d]
  (let [d (md/unwrap' d)]
    (when-not (identical? d ::nil)
      (when (md/deferred? d)
        (alter-meta! d assoc ::leaked true :type ::leaked-deferred))
      d)))


(deftype LockedMapMDM
         [poy lock frozen ^Map hm]

  MutableDeferredMap

  (mdm-fetch!
    [_ k]
    (if-let [kv (find poy k)]
      (let [v (val kv)
            vv (md/unwrap' v)]
        (when-not (identical? vv v)
          (locking lock (.put hm k vv)))
        [false vv])
      (locking lock
        (if-let [v (.get hm k)]
          (if (and (md/deferred? v) (md/realized? v) (not @frozen))
            (let [vv (md/success-value v v)]
              (.put hm k (if (nil? vv) ::nil vv))
              [false vv])
            [false (when-not (identical? ::nil v) v)])
          (let [d (md/deferred)]
            (when @frozen
              (throw (ex-info "fetch from frozen mdm" {::yarn k, ::mdm-frozen true})))
            (.put hm k d)
            [true d])))))

  (mdm-freeze!
    [_]
    (locking lock
      (when @frozen
        (throw (ex-info "mdm is already frozen" {})))
      (vreset! frozen true))
    (if (.isEmpty hm)
      poy
      (into poy (map (fn [[k v]] [k (unwrap-mdm-deferred v)])) hm))))


(defmethod print-method ::leaked-deferred [y ^java.io.Writer w]
  (.write w "#knitty.LeakD[")
  (let [error (md/error-value y nil)]
    (cond
      error 
      (do
        (.write w ":error ")
        (print-method (class error) w)
        (.write w " ")
        (print-method (ex-message error) w))
      (md/realized? y)
      (do
        (.write w ":value ")
        (print-method (md/success-value y nil) w))
      :else
      (.write w "…")))
  (.write w "]"))


(defn- locked-hmap-mdm [poy hm-size]
  (LockedMapMDM. poy (Object.) (volatile! false) (HashMap. (int hm-size))))


;; TRACING

(defrecord TraceLog
           [yarn
            event
            value])

(defrecord Trace
           [at
            base-at
            done-at
            poy
            yarns
            tracelog])

;; PPRINT

(defmethod print-method Trace
  [x ^java.io.Writer w]
  (.write w "#knitty.Trace{")
  (.write w ":at ")
  (print-method (:at x) w)
  (.write w " :yarns ")
  (print-method (:yarns x) w)
  (.write w " :tracelogs-count ")
  (print-method (count (:tracelog x)) w) 
  (.write w " ...}"))


(deftype NilTracer []
  Tracer
  (trace-start [_])
  (trace-call [_])
  (trace-finish [_ _value _error _async])
  (trace-dep [_ _yarn])
  (trace-all-deps [_ _yarns])
  (trace-build-sub-tracer [this _yarn] this))

(def nil-tracer (NilTracer.))


(defmacro ^:private now [] `(System/nanoTime))

(defn- aconj-tlog [a yarn event value]
  (let [t (TraceLog. yarn event value)]
    (swap! a #(when % (cons t %)))))

(deftype TracerImpl
         [store
          this-yarn
          by-yarn]

  Tracer
  (trace-start [_]
    (aconj-tlog store this-yarn ::trace-start (now))
    (aconj-tlog store this-yarn ::trace-caller by-yarn))

  (trace-call [_]
    (aconj-tlog store this-yarn ::trace-call (now))
    (aconj-tlog store this-yarn ::trace-thread (.getName (Thread/currentThread))))

  (trace-dep [_ yarn]
    (aconj-tlog store this-yarn ::trace-dep [yarn (now)]))

  (trace-all-deps
    [_ yarns]
    (aconj-tlog store this-yarn ::trace-all-deps yarns))

  (trace-build-sub-tracer
    [_ k]
    (TracerImpl. store k this-yarn))

  (trace-finish
    [_ value error deferred]
    (when deferred
      (aconj-tlog store this-yarn ::trace-deferred true))
    (when value
      (aconj-tlog store this-yarn ::trace-value value))
    (when error
      (aconj-tlog store this-yarn ::trace-error error))
    (aconj-tlog store this-yarn ::trace-finish (now))))

(defn- empty-tracer [root]
  (let [a (atom ())]
    [a (TracerImpl. a root nil)]))


;; YARN CODEGEN

(defn coerce-deferred [v]
  (let [v (force v)]
    (md/unwrap' (md/->deferred v v))))


(defn- as-deferred [v]
  (if (md/deferrable? v)
    (md/->deferred v)
    (md/success-deferred v nil)))


(defn- bind-param-type [ds]
  (let [{:keys [defer lazy]} (meta ds)]
    (cond
      (and defer lazy) :lazy-defer
      lazy             :lazy-sync
      defer            :defer
      :else            :sync)))


(defrecord NotADeferred
           [deferred])


(defmacro unwrap-defer [w]
  `(let [w# ~w]
     (if (instance? NotADeferred w#)
       (.-deferred ^NotADeferred w#)
       w#)))


(defn yarn-key
  [k]
  (cond
    (keyword? k) k
    (instance? Yarn k) (yarn-key* k)
    :else (throw (ex-info "invalid yarn key" {::yarn k}))))


(defn yarn-snatch
  [mdm k registry tracer]
  (let [yd (cond
             (keyword? k) (registry k)
             (instance? Yarn k) k
             :else (throw (ex-info "invalid yarn" {::yarn k})))]
    (when-not yd
      (throw (ex-info "yarn is not registered" {::yarn k})))
    (trace-dep tracer k)
    (yarn-snatch* yd mdm registry (trace-build-sub-tracer tracer k))))


(defn yarn-snatch-defer [mdm k reg tracer]
  (NotADeferred. (as-deferred (yarn-snatch mdm k reg tracer))))


(defn yarn-snatch-lazy-defer [mdm k reg tracer]
  (NotADeferred.
   (delay
    (as-deferred
     (yarn-snatch mdm k reg tracer)))))


(defn yarn-snatch-lazy-sync [mdm k reg tracer]
  (NotADeferred.
   (delay
    (let [d (yarn-snatch mdm k reg tracer)]
      (if (md/deferred? d) @d d)))))


(defonce synclazy-executor-delay
  (delay
   (let [cnt (atom 0)]
     (manifold.executor/utilization-executor
      0.95 Integer/MAX_VALUE
      {:thread-factory (manifold.executor/thread-factory
                        #(str "knitty-synclazy-" (swap! cnt inc))
                        (deliver (promise) nil))
       ;; :stats-callback (fn [stats] )
       }))))


(defn get-synclazy-executor []
  @synclazy-executor-delay)


(defn resolve-executor-var [e]
  (when e
    (when-let [ee (var-get e)]
      (if (ifn? ee) (ee) ee))))


(defmacro maybe-future-with [executor-var & body]
  (if-not executor-var
    `(do ~@body)
    `(md/future-with (resolve-executor-var ~executor-var) ~@body)))


(defn- build-yank-fns
  [k bind expr]
  (let [mdm '_mdm
        reg '_reg
        tracer '_tracer
        the-fnv (gensym "fn")

        yank-all-deps
        (mapcat identity
                (for [[ds dk] bind]
                  [ds
                   (case (bind-param-type ds)
                     :sync        `(yarn-snatch ~mdm ~dk ~reg ~tracer)
                     :defer       `(yarn-snatch-defer ~mdm ~dk ~reg ~tracer)
                     :lazy-sync   `(yarn-snatch-lazy-sync ~mdm ~dk ~reg ~tracer)
                     :lazy-defer  `(yarn-snatch-lazy-defer ~mdm ~dk ~reg ~tracer))]))

        maybe-unwrap-defers
        (mapcat identity
                (for [[ds _dk] bind
                      :when (#{:lazy-sync :defer :lazy-defer} (bind-param-type ds))]
                  [ds `(unwrap-defer ~ds)]))

        deps (keys bind)
        fnname #(-> k name (str "--" %) symbol)
        any-lazy-sync (->> deps (map bind-param-type) (some #{:lazy-sync}) some?)
        executor-var (or (:executor (meta bind))
                         (when any-lazy-sync #'get-synclazy-executor))]

    `(let [dep-keys# ~(vec (for [[ds dk] bind] [dk (bind-param-type ds)]))

           ;; input - vector. unwraps 'defers', called by yget-fn#
           ~the-fnv
           (fn ~(fnname "vec")
             ([[~@deps] ~tracer]
              (trace-call ~tracer)
              (coerce-deferred (let [~@maybe-unwrap-defers] ~expr))))

           ;; input - mdm and registry, called by `yank-snatch
           yget-fn#
           (fn ~(fnname "yget") [~mdm ~reg ~tracer]
             (let [[new# d#] (mdm-fetch! ~mdm ~k)]
               (if new#
                 (maybe-future-with
                  ~executor-var
                  (trace-start ~tracer)
                  (try ;; d# is alsways deffered
                    (trace-all-deps ~tracer dep-keys#)
                    (let [~@yank-all-deps]
                      (let [x# (if ~(or
                                     (some? executor-var)
                                     (list*
                                      `or
                                      (for [d deps, :when (= (bind-param-type d) :sync)]
                                        `(md/deferred? ~d))))
                                 (md/chain' (md/zip' ~@deps) #(~the-fnv % ~tracer))
                                 (~the-fnv [~@deps] ~tracer))]
                        (if (md/deferred? x#)
                          (md/on-realized
                           x#
                           (fn [xv#]
                             (trace-finish ~tracer xv# nil true)
                             (md/success! d# xv#))
                           (fn [xv#]
                             (trace-finish ~tracer nil xv# true)
                             (md/error! d# xv#)))
                          (do
                            (trace-finish ~tracer x# nil false)
                            (md/success! d# x#)))

                        x#))
                    (catch Throwable e#
                      (trace-finish ~tracer nil e# false)
                      (md/error! d# e#)
                      d#)))
                 (do
                   d#))))]
       [~the-fnv
        yget-fn#])))


;; MACRO SPECS

(s/def
  ::pile-of-yarn
  (s/keys))


(s/def
  ::yarn-binding
  (s/map-of symbol? ident?))

(s/def
  ::defyarn
  (s/cat
   :name symbol?
   :doc (s/? string?)
   :bind-and-expr (s/? (s/cat :bind ::yarn-binding
                              :expr any?))))

(s/def
  ::yarn
  (s/cat
   :name qualified-keyword?
   :bind-and-expr (s/? (s/cat :bind ::yarn-binding
                              :expr any?))))


;; PUBLIC API IMPL

(defmacro yarn
  [k & exprs]
  (let [bd (cons k exprs)
        cf (s/conform ::yarn bd)]
    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::yarn bd))))
    (let [{{:keys [bind expr]} :bind-and-expr} cf
          bind (or bind {})
          expr (or expr `(throw (ex-info "missing input-only yarn" {::yarn ~k})))]
      `(let [[funcv# yget#] ~(build-yank-fns k bind expr)]
         (Yarn.
          ~k
          ~(vec (vals bind))
          funcv#
          yget#)))))


(defmacro defyarn
  [nm & body]
  (let [bd (cons nm body)
        cf (s/conform ::defyarn bd)]
    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::defyarn bd))))
    (let [k (keyword (-> *ns* ns-name name) (name nm))
          {doc :doc, {:keys [bind expr]} :bind-and-expr} cf
          bind (or bind {})
          m (merge (meta bind) (meta nm))
          doc (or doc (:doc m))
          m (if doc (assoc m :doc doc) m)
          spec (:spec m)
          expr (or expr `(throw (ex-info "missing input-only yarn" {::yarn ~k})))
          nm (with-meta nm m)]
      `(do
         ~@(when spec `((s/def ~k ~spec)))
         (register-yarn (yarn ~k ~bind ~expr))
         (def ~nm ~k)))))


(defn yank*
  [poy yarns registry tracer]
  (let [mdm (locked-hmap-mdm poy (max 8 (* 2 (count yarns))))
        ydefs (map #(yarn-snatch mdm % registry tracer) yarns)]
    (md/catch'
     (md/chain'
      (apply md/zip' ydefs)
      (fn [yvals] [yvals (mdm-freeze! mdm)]))
     (fn [e]
       (throw (ex-info "Failed to yank"
                       {::poy poy
                        ::yarns yarns
                        ::mdm (mdm-freeze! mdm)}
                       e))))))


(def ^:private yank-cnt (atom 0))

(defn yank
  [poy yarns]
  (assert (map? poy) "poy should be a map")
  (assert (sequential? yarns) "yarns should be vector/sequence")

  (if-not *tracing*
    (yank* poy yarns *registry* nil-tracer)
    (let [at (java.util.Date.)
          [a t] (empty-tracer ::yank)
          base (now)
          pick-trace!
          (fn []
            (let [aa @a]
              (reset! a nil)
              (map->Trace
               {:at at
                :yankid (swap! yank-cnt inc)
                :base-at base
                :done-at (now)
                :poy poy
                :yarns (mapv yarn-key yarns)
                :tracelog aa})))]
      (->
       (md/chain'
        (yank* poy yarns *registry* t)
        (fn [[vs poy']] [vs (vary-meta poy' update ::trace conj (pick-trace!))]))
       (md/catch' clojure.lang.IExceptionInfo
                  #(throw
                    (ex-info "Failed to yank"
                             (assoc (ex-data %) ::trace (conj (-> poy meta ::trace) (pick-trace!)))
                             (ex-cause %))))))))


;; ANALYZE TRACE

(defn- safe-minus [a b]
  (when (and a b (not (zero? a)) (not (zero? b)))
    (- a b)))


(defn- find-traces [poy]
  (cond
    (instance? Trace poy) [poy]
    (map? poy)         (-> poy meta ::trace)
    (vector? poy)      (-> second poy meta ::trace)
    (md/deferred? poy) @(md/catch
                         (md/chain poy find-traces)
                         #(-> % ex-data ::trace))))

(defn parse-trace [t]
  (let [{:keys [at base-at done-at poy tracelog yankid]} t
        ytlog (update-vals
               (group-by :yarn tracelog)
               (fn [ts] (into {} (map (juxt :event :value) ts))))
        ytdep (into {}
                    (for [{y :yarn, e :event, v :value} tracelog
                          :when (= e ::trace-dep)
                          :let [[dep time] v]]
                      [[y dep] time]))
        all-deps (set (for [t (vals ytlog), [d _] (::trace-all-deps t)] d))
        ex-deps (set/difference all-deps (set (keys ytlog)))]
    {:at at
     :time (safe-minus done-at base-at)
     :base-at base-at
     :done-at done-at
     :yankid yankid

     :nodes (into
             {}
             (concat
              (for [[y t] ytlog
                    :when (not= y ::yank)]
                [y (let [{value  ::trace-value
                          error  ::trace-error
                          thread ::trace-thread
                          finish-at ::trace-finish
                          start-at  ::trace-start
                          call-at   ::trace-call
                          deferred? ::trace-deferred
                          caller    ::trace-caller} t]
                     {:type
                      (cond
                        (= ::yank caller) :yanked
                        (nil? finish-at)  :leaked
                        :else             :interim)
                      :caller caller
                      :value value
                      :error error
                      :thread thread
                      :yankid yankid
                      :start-at start-at
                      :finish-at finish-at
                      :deps-time (safe-minus call-at start-at)
                      :func-time (safe-minus finish-at call-at)
                      :deferred (or deferred? (nil? finish-at))})])
              (for [pd ex-deps]
                [pd (if (contains? poy pd)
                      {:type :input, :value (get poy pd), :yankid yankid}
                      {:type :lazy-unused, :deferred true, :yankid yankid})])))
     :links (into
             {}
             (concat
              (for [[y t] ytlog
                    [dk dt] (::trace-all-deps t)
                    :let [time (get ytdep [y dk])]]
                [[y dk]
                 {:source (cond
                            (contains? poy dk) :input
                            (::trace-deferred (ytlog dk)) :defer
                            :else :sync)
                  :cause (= (::trace-caller (ytlog dk)) y)
                  :time (when-let [t (safe-minus (::trace-finish t)
                                                 (::trace-finish (ytlog dk)))]
                          (when (pos? t) t))
                  :used (not (nil? time))
                  :type dt
                  :yankid-src yankid
                  :yankid-dst yankid}])))}))


(defn- merge-two-parsed-traces [b a]
  (let [nodesa (into {} (:nodes a))
        nodesb (into {} (:nodes b))
        sameval #(= (:value (get nodesb %)) (:value (get nodesa %)))]
    {:clusters (assoc (:clusters b)
                      (:yankid a) {:at (:at a)
                                   :time (:time a)
                                   :base-at (:base-at a)
                                   :done-at (:done-at a)
                                   :shift (safe-minus (:base-at a) (:base-at b))})
     :base-at (or (:base-at b) (:base-at a))
     :done-at (or (:done-at b) (:done-at a))
     :at   (or (:at b) (:at a))
     :time   (+ (:time b 0) (:time a 0))
     :nodes (concat
             (:nodes b)
             (for [[y n :as yn] (:nodes a)
                   :let [inb (contains? nodesb y)
                         eq  (sameval y)]
                   :when (or (not inb) (not eq))]
               (if inb
                 [y (assoc n :type :changed-input)]
                 yn)))
     :links (concat
             (:links b)
             (for [[[_ y :as xy] c] (:links a)]
               (if (and (contains? nodesb y)
                        (= (:value (nodesb y))
                           (:value (nodesa y))))
                 [xy (assoc c :yankid-dst (:yankid (nodesb y)))]
                 [xy c]))
             (set
              (for [[[_ y] c] (:links a)
                    :when (contains? nodesb y)
                    :when (not= (:value (nodesb y)) (:value (nodesa y)))]
                [[y y] {:yankid-dst (:yankid (nodesb y)),
                        :yankid-src (:yankid (nodesa y)),
                        :type :changed-input}])))}))



(defn merge-parsed-traces [gs]
  (reduce merge-two-parsed-traces nil (reverse gs)))


;; GRAPHVIZ

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


(defn untangle*
  [traces type]
  (let [gs (map parse-trace traces)
        g (merge-parsed-traces gs)]
    (case type
      :raw g
      :dot (render-tracegraph-dot g)
      ::xdot (fix-xdot-escapes (render-tracegraph-dot g))
      :png (tgl/dot->image (render-tracegraph-dot g) "png")
      :svg (tgl/dot->svg (render-tracegraph-dot g))
      (tgl/dot->image (render-tracegraph-dot g) (name type)))))


(def xdot-available
  (delay
   (zero? (:exit (clojure.java.shell/sh "python" "-m" "xdot" "--help")))))


(def graphviz-available
  (delay
   (zero? (:exit (clojure.java.shell/sh "dot" "-V")))))


(defn untangle [poy]
  (let [t (find-traces poy)]
    (cond
      (nil? t)
      (println "no knitty trace found")

      (and (force xdot-available) (force graphviz-available))
      (let [f (java.io.File/createTempFile "knitty_untangle_" ".xdot")]
        (io/copy (untangle* t ::xdot) f)
        (future (clojure.java.shell/sh "python" "-m" "xdot" (str f))))

      (force graphviz-available)
      (let [f (java.io.File/createTempFile "knitty_untangle_" ".svg")]
        (io/copy (untangle* t :svg) f)
        (future (clojure.java.browse/browse-url f)))

      :else
      (print "graphviz (dot) was not found, please install it."))))


;; PUBLIC HELPERS

(defmacro with-yarns [yarns & body]
  `(binding [*registry*
             (into *registry*
                   (map #(vector (yarn-key %) %))
                   ~yarns)]
     ~@body))


(defmethod print-method Yarn [y ^java.io.Writer w]
  (.write w "#knitty/yarn ")
  (.write w (str (yarn-key* y))))


(defn select-ns-keys [m ns]
  (let [s (name ns)]
    (into
     (empty m)
     (comp
      (filter (comp keyword? key))
      (filter #(= s (namespace (key %)))))
     m)))


(defn assert-spec-keys [m]
  (s/assert ::pile-of-yarn m))


;;  #_{:clj-kondo/ignore [:knitty/defyarn-symref]}
(comment

  (do
    (defyarn zero  ;; define "yarn" - single slot/value
      {}           ;; no inputs
      0)           ;; value, use explicit `do when needed

    (defyarn one
      {_ zero}     ;; wait for zero, but don't use it
      1)           ;; any manifold-like deferred can be finished

    (defyarn one-slooow
      {}
      (future (Thread/sleep (rand-int 20)) 1))

    (defyarn two
      {^:defer x one
       ^:defer y one-slooow}       ;; don't unwrap deferred (all values coerced to manifold/deffered)
      (md/chain' (md/alt x y) inc))

    (defyarn three-fast {x one, y two}
      (future
        (Thread/sleep (rand-int 5)) (+ x y)))

    (defyarn three-slow {x one, y two}
      (future
        (Thread/sleep (rand-int 10)) (+ x y)))

    (defyarn three          ;; put deferred into delay, enables branching
      {^:lazy         f three-fast
       ^:lazy ^:defer s three-slow}
      (if (zero? (rand-int 2)) f s))

    (defyarn four
      {x ::one, y ::three}  ;; use raw keywords (not recommended)
      (future (+ x y)))

    (defyarn five
      {x ::two, y three}    ;; mixed approach 
      (+ x y))

    (defyarn six
      ^{:doc "doc string"}          ;; doc
      ^{:spec number?}               ;; spec
      {x ::two , y ::three}
      (do                   ;; explicit do
        (* x y))))

  ;; yank - ensure all keys are inside the map - finishs deferred
  @(yank {} [one])
  @(yank {} [six])
  @(yank {one 1000} [four six])
  @(yank {two 2000} [four five])

  ;; dynamically create 'yarn - not recommended
  @(yank {} [(yarn ::eight {f ::four} (* f 2))])

  ;; dynamically create 'yarn & capture locals
  (for [i (range 1 4)]
    @(yank {} [(yarn ::seven {s ::six} (* i s))]))

  ;; redefine (aka mock) yarns in registry
  (with-yarns [(yarn ::three {} (rand-int 1000))
               (yarn ::four {} (assert false))]
    @(yank {} [::six]))

  ;; or dynamically switch yarns
  (with-yarns [(rand-nth
                [(yarn ::seven {s ::six} (float (+ s 1)))
                 (yarn ::seven {s ::six} (long (+ s 1)))])]
    @(yank {} [::seven]))

   ;; recommended to insntall 'xdot' (via pkg manager or pip)
  (untangle @(yank {} [six]))

   ;; get raw format
  (untangle* @(md/chain (yank {} [six]) second) :raw)

   ;; or graphviz dot
  (untangle* @(md/chain (yank {} [six]) second) :dot)


  (identity
   (untangle*
    (->
     @(md/chain
       {}
       #(yank % [four])
       second
       #(yank % [six])
       second)
     meta
     ::trace)
    :raw))

  (untangle
   (md/chain
    {}
    #(yank % [two]) second
    #(yank % [five]) second
    #(assoc % ::two 222222)
    #(yank % [six]) second))

  (untangle (yank {} [six]))

  ;; get raw format
  (untangle* @(md/chain (yank {} [six]) second) :raw)

  ;; or graphviz dot
  (untangle* @(md/chain (yank {} [six]) second) :dot))

