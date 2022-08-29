(ns ag.knitty.impl
  (:require [ag.knitty.mdm :refer [mdm-fetch! mdm-freeze! locked-hmap-mdm]]
            [ag.knitty.trace :as t :refer [capture-trace!]]
            [manifold.deferred :as md]
            [manifold.executor]))


(defprotocol IYarn
  (yarn-get [_ mdm reg tracer] "get value or obligated deferred")
  (yarn-key [_] "get yarn key"))


(deftype Yarn [key deps funcv yget]
  IYarn
  (yarn-get
    [_ mdm reg tracer]
    (yget mdm reg tracer))
  (yarn-key [_] key))


(deftype YarnAlias [key orig-key transform]

  IYarn
  (yarn-key
   [_]
   (yarn-key key))

  (yarn-get
   [_ mdm reg tracer]
   (let [y (get reg orig-key)
         d (yarn-get y mdm reg tracer)]
     (cond
       (nil? transform) d
       (md/deferred? d) (md/chain' d transform)
       :else (transform d)))))


(extend-protocol IYarn
  clojure.lang.Keyword
  (yarn-key
    [k]
    k)
  (yarn-get
    [k mdm reg tracer]
    (let [y (reg k)]
      (when-not y
        (throw (ex-info "yarn is not registered" {::yarn k})))
      (yarn-get y mdm reg tracer))))


(defmethod print-method Yarn [y ^java.io.Writer w]
  (.write w "#knitty/yarn ")
  (.write w (str (yarn-key y))))


(defn coerce-deferred [v]
  (let [v (force v)]
    (md/->deferred v v)))


(defn as-deferred [v]
  (if (md/deferrable? v)
    (md/->deferred v)
    (md/success-deferred v nil)))


(defn bind-param-type [ds]
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


(definline yarn-get-sync [k mdm reg tracer]
  `(yarn-get (~reg ~k ~k) ~mdm ~reg ~tracer))


(definline yarn-get-defer [k mdm reg tracer]
  `(NotADeferred. (as-deferred (yarn-get (~reg ~k ~k) ~mdm ~reg ~tracer))))


(defn yarn-get-lazy-defer [k mdm reg tracer]
  (NotADeferred.
   (delay
    (as-deferred
     (yarn-get (reg k k) mdm reg tracer)))))


(defn yarn-get-lazy-sync [k mdm reg tracer]
  (NotADeferred.
   (delay
    (let [d (yarn-get (reg k k) mdm reg tracer)]
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


(defn- exception-java-cause [ex]
  (cond
    (nil? ex) nil
    (instance? clojure.lang.ExceptionInfo ex) (recur (ex-cause ex))
    (instance? java.util.concurrent.ExecutionException ex) (recur (ex-cause ex))
    :else ex))


(defn wrap-yarn-exception [k ex]
  (let [d (ex-data ex)]
    (if (:knitty/failed-yarn d)
      ex
      (let [jc (exception-java-cause ex)]
        (ex-info "yarn failure"
                 (assoc (ex-data ex)
                        :knitty/fail-at   (java.util.Date.)
                        :knitty/failed-yarn k
                        :knitty/java-cause jc)
                 ex)))))

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
                     :sync        `(yarn-get-sync       ~dk ~mdm ~reg ~tracer)
                     :defer       `(yarn-get-defer      ~dk ~mdm ~reg ~tracer)
                     :lazy-sync   `(yarn-get-lazy-sync  ~dk ~mdm ~reg ~tracer)
                     :lazy-defer  `(yarn-get-lazy-defer ~dk ~mdm ~reg ~tracer))]))

        maybe-unwrap-defers
        (mapcat identity
                (for [[ds _dk] bind
                      :when (#{:lazy-sync :defer :lazy-defer} (bind-param-type ds))]
                  [ds `(unwrap-defer ~ds)]))

        deps (keys bind)
        fnname #(-> k name (str "-" %) symbol)
        any-lazy-sync (->> deps (map bind-param-type) (some #{:lazy-sync}) some?)
        executor-var (or (:executor (meta bind))
                         (when any-lazy-sync #'get-synclazy-executor))]

    `(let [;; input - vector. unwraps 'defers', called by yget-fn#
           ~the-fnv
           (fn ~(fnname "expr")
             ([[~@deps] ~tracer]
              (when ~tracer (t/trace-call ~tracer))
              (coerce-deferred (let [~@maybe-unwrap-defers] ~expr))))

           ;; input - mdm and registry, called by `yank-snatch
           yget-fn#
           (fn ~(fnname "yarn") [~mdm ~reg ~tracer]
             (let [~tracer (when ~tracer (t/trace-build-sub-tracer ~tracer ~k))]
               (let [[claim# d#] (mdm-fetch! ~mdm ~k)]
                 (if-not claim#
                   ;; got item from mdm
                   d#

                   ;; calculate & provide to mdm
                   (maybe-future-with
                    ~executor-var
                    (when ~tracer (t/trace-start ~tracer))

                    (try ;; d# is alsways deffered
                      (when ~tracer
                        (t/trace-all-deps ~tracer
                                          ~(vec (for [[ds dk] bind] [dk (bind-param-type ds)]))))

                      (let [~@yank-all-deps]
                        (let [x# (if ~(or
                                       (some? executor-var)
                                       (list*
                                        `or
                                        (for [d deps, :when (= (bind-param-type d) :sync)]
                                          `(md/deferred? ~d))))
                                   (md/chain' (md/zip' ~@deps) (fn [z#] (~the-fnv z# ~tracer)))
                                   (~the-fnv [~@deps] ~tracer))]
                          (if (md/deferred? x#)
                            (do
                              (md/on-realized
                               x#
                               (fn [xv#]
                                 (when ~tracer (t/trace-finish ~tracer xv# nil true))
                                 (md/success! d# xv# claim#))
                               (fn [e#]
                                 (let [ew# (wrap-yarn-exception ~k e#)]
                                   (when ~tracer (t/trace-finish ~tracer nil ew# true))
                                     (md/error! d# ew# claim#))))
                              d#)
                            (do
                              (when ~tracer (t/trace-finish ~tracer x# nil false))
                              (md/success! d# x# claim#)
                              d#))))

                      (catch Throwable e#
                        (let [ew# (wrap-yarn-exception ~k e#)]
                          (when ~tracer (t/trace-finish ~tracer nil ew# false))
                            (md/error! d# ew# claim#)
                          d#))))))))]
       [~the-fnv
        yget-fn#])))


(defn gen-yarn
  [key bind expr]
  `(let [[funcv# yget#] ~(build-yank-fns key bind expr)]
     (Yarn.
      ~key
      ~(vec (vals bind))
      funcv#
      yget#)))


(defn gen-yarn-ref
  [key dest transform]
  `(YarnAlias.
    ~key
    ~dest
    ~transform))


(defn yank*
  [poy yarns registry tracer]
  (let [mdm (locked-hmap-mdm poy (max 8 (* 2 (count yarns))))
        ydefs (map #(yarn-get (registry % %) mdm registry tracer) yarns)]
    (md/catch'
     (md/chain'
      (apply md/zip' ydefs)
      (fn [yvals]
        [yvals
         (let [poy' (mdm-freeze! mdm)]
           (if tracer
             (vary-meta poy' update :ag.knitty/trace conj (capture-trace! tracer))
             poy'))]))
     (fn [e]
       (throw (ex-info "Failed to yank"
                       (assoc (ex-data e)
                              :ag.knitty/yanked-poy poy
                              :ag.knitty/yanked-yarns yarns
                              :ag.knitty/failed-poy (mdm-freeze! mdm)
                              :ag.knitty/trace (when tracer
                                                 (conj
                                                  (-> poy meta :ag.knitty/trace)
                                                  (capture-trace! tracer))))
                       e))))))
