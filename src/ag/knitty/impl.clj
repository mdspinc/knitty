(ns ag.knitty.impl
  (:require [ag.knitty.mdm :refer [mdm-fetch! mdm-freeze! locked-hmap-mdm]]
            [ag.knitty.trace :as t :refer [capture-trace!]]
            [manifold.deferred :as md]
            manifold.executor))


(defprotocol IYarn
  (yarn-snatch* [_ mdm reg tracer] "get value or obligated deferred")
  (yarn-key* [_] "get yarn key"))


(deftype Yarn [yarn deps funcv yget]
  IYarn
  (yarn-snatch* [_ mdm reg tracer] (yget mdm reg tracer))
  (yarn-key* [_] yarn))


(defmethod print-method Yarn [y ^java.io.Writer w]
  (.write w "#knitty/yarn ")
  (.write w (str (yarn-key* y))))


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
    (when tracer (t/trace-dep tracer k))
    (yarn-snatch* yd mdm registry (when tracer (t/trace-build-sub-tracer tracer k)))))


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

    `(let [;; input - vector. unwraps 'defers', called by yget-fn#
           ~the-fnv
           (fn ~(fnname "vec")
             ([[~@deps] ~tracer]
              (when ~tracer (t/trace-call ~tracer))
              (coerce-deferred (let [~@maybe-unwrap-defers] ~expr))))

           ;; input - mdm and registry, called by `yank-snatch
           yget-fn#
           (fn ~(fnname "yget") [~mdm ~reg ~tracer]
             (let [[claim# d#] (mdm-fetch! ~mdm ~k)]
               (if claim#
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
                                 (md/chain' (md/zip' ~@deps) #(~the-fnv % ~tracer))
                                 (~the-fnv [~@deps] ~tracer))]
                        (if (md/deferred? x#)
                          (md/on-realized
                           x#
                           (fn [xv#]
                             (when ~tracer (t/trace-finish ~tracer xv# nil true))
                             (md/success! d# xv# claim#))
                           (fn [e#]
                             (when ~tracer (t/trace-finish ~tracer nil e# true))
                             (md/error! d# e# claim#)))
                          (do
                            (when ~tracer (t/trace-finish ~tracer x# nil false))
                            (md/success! d# x# claim#)))

                        x#))
                    (catch Throwable e#
                      (when ~tracer (t/trace-finish ~tracer nil e# false))
                      (md/error! d# e# claim#)
                      d#)))
                 (do
                   d#))))]
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


(defn yank*
  [poy yarns registry tracer]
  (let [mdm (locked-hmap-mdm poy (max 8 (* 2 (count yarns))))
        ydefs (map #(yarn-snatch mdm % registry tracer) yarns)]
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
                       {:ag.knitty/poy poy
                        :ag.knitty/yarns yarns
                        :ag.knitty/mdm (mdm-freeze! mdm)
                        :ag.knitty/trace (when tracer (capture-trace! tracer))}
                       e))))))
