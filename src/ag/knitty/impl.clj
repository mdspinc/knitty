(ns ag.knitty.impl
  (:require [ag.knitty.deferred :as kd]
            [ag.knitty.mdm :refer [create-mdm mdm-cancel! mdm-fetch!
                                   mdm-freeze! MutableDeferredMap]]
            [ag.knitty.trace :as t :refer [capture-trace!]]
            [manifold.deferred :as md]
            [manifold.executor]
            [manifold.utils]))


(defprotocol IYarn
  (yarn-gtr [_] "get value or obligated deferred")
  (yarn-key [_] "get yarn ykey"))


(defprotocol IYankCtx
  (cancelled? [_])
  (resolve-yarn [_ k])
  (resolve-yarn-gtr [_ k])
  )


(deftype YankCtx
         [mdm
          registry
          ^:volatile-mutable ^boolean cancelled]

  MutableDeferredMap
  (mdm-fetch! [_ k] (mdm-fetch! mdm k))
  (mdm-freeze! [_] (mdm-freeze! mdm))
  (mdm-cancel! [_] (set! cancelled (boolean true)) (mdm-cancel! mdm))

  IYankCtx
  (cancelled? [_] cancelled)
  (resolve-yarn [_ k] (registry k k))
  (resolve-yarn-gtr [_ k] (yarn-gtr (registry k k)))
  )


(deftype Yarn [ykey deps yget]
  IYarn
  (yarn-gtr [_] yget)
  (yarn-key [_] ykey))


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
      lazy  :lazy
      defer :defer
      :else :sync)))


(defrecord NotADeferred [deferred])

(defmacro unwrap-not-a-deferred [w]
  `(let [^NotADeferred w# ~w]
     (.-deferred w#)))


(definline yarn-get-sync [yk ykey ctx tracer]
  `(do
     (when ~tracer (t/trace-dep ~tracer ~yk ~ykey))
     ((resolve-yarn-gtr ~ctx ~ykey) ~ctx ~tracer)))


(definline yarn-get-defer [yk  ykey ctx tracer]
  `(do
     (when ~tracer (t/trace-dep ~tracer ~yk ~ykey))
     (NotADeferred. (as-deferred
                     ((resolve-yarn-gtr ~ctx ~ykey) ~ctx ~tracer)))))

(defn yarn-get-lazy [yk ykey ctx tracer]
  (NotADeferred.
   (delay
    (when tracer (t/trace-dep tracer yk ykey))
    (as-deferred
     ((resolve-yarn-gtr ctx ykey) ctx tracer)))))


(defn resolve-executor-var [e]
  (when e
    (when-let [ee (var-get e)]
      (if (ifn? ee) (ee) ee))))


(defn run-future [executor-var thefn]
  (let [d (md/deferred)]
    (manifold.utils/future-with
     (resolve-executor-var executor-var)
     (try
       (let [v (md/unwrap' (thefn))]
         (if (md/deferred? v)
           (md/on-realized v
                           #(md/success! d %)
                           #(md/error! d %))
           (md/success! d v)))
       (catch Throwable e
         (md/error! d e))))
    d))


(defmacro maybe-future-with [executor-var & body]
  (if-not executor-var
    `(do ~@body)
    `(run-future ~executor-var (fn [] ~@body))))


(defn- exception-java-cause [ex]
  (cond
    (nil? ex) nil
    (instance? clojure.lang.ExceptionInfo ex) (recur (ex-cause ex))
    (instance? java.util.concurrent.ExecutionException ex) (recur (ex-cause ex))
    :else ex))


(defn wrap-yarn-exception [k ex]
  (if (kd/revokation-exception? ex)
    ex
    (let [d (ex-data ex)]
      (if (::inyank d)
        (ex-info "yarn failure"
                 (assoc d
                        :knitty/failed-yarn k
                        :knitty/failed-yarn-chain (conj (:knitty/failed-yarn-chain d) k))
                 (ex-cause ex))
        (ex-info "yarn failure"
                 (assoc d
                        ::inyank true
                        :knitty/fail-at   (java.util.Date.)
                        :knitty/failed-yarn k
                        :knitty/failed-yarn-chain [k]
                        :knitty/java-cause (exception-java-cause ex))
                 ex)))))


(defn connect-result-mdm [ykey result mdm-deferred tracer maybe-real-result]

  (if (md/deferred? result)
    (do

      ;; revokation mdm-deferred -> result
      (when maybe-real-result
        (md/on-realized
         mdm-deferred
         #(when-let [d @maybe-real-result]
            (when (instance? manifold.deferred.IMutableDeferred d)
              (md/success! d %)))
         #(when-let [d @maybe-real-result]
            (when (instance? manifold.deferred.IMutableDeferred d)
              (md/error! d %)))))

      ;; connect result -> mdm-deferred
      (md/on-realized
       result
       (fn [xv]
         (when-not (md/realized? mdm-deferred)
           (when tracer (t/trace-finish tracer ykey xv nil true))
           (md/success! mdm-deferred xv)))

       (fn [e]
         (when-not (md/realized? mdm-deferred)
           (let [ew (wrap-yarn-exception ykey e)]
             (when tracer (t/trace-finish tracer ykey nil ew true))
             (md/error! mdm-deferred ew)))))

      mdm-deferred)

    (do
      (when tracer (t/trace-finish tracer ykey result nil false))
      (md/success! mdm-deferred result)
      result)))


(defn connect-error-mdm [ykey error mdm-deferred tracer maybe-real-result]
  (let [ew (wrap-yarn-exception ykey error)]

    ;; try to revoke
    (when maybe-real-result
      (when-let [d @maybe-real-result]
        (when (instance? manifold.deferred.IMutableDeferred d)
          (md/error! d error))))

    (when tracer (t/trace-finish tracer ykey nil ew false))

    (md/error! mdm-deferred ew)
    mdm-deferred))


(defn- build-yank-fns
  [ykey bind expr]
  (let [ctx (with-meta '_yank_ctx {:tag (str `YankCtx)})
        tracer '_yank_tracer
        the-fnv (gensym "fn")
        reald  (gensym "real_deferred")

        yank-all-deps
        (mapcat identity
                (for [[ds dk] bind]
                  [ds
                   (case (bind-param-type ds)
                     :sync   `(yarn-get-sync  ~ykey ~dk ~ctx ~tracer)
                     :defer  `(yarn-get-defer ~ykey ~dk ~ctx ~tracer)
                     :lazy   `(yarn-get-lazy  ~ykey ~dk ~ctx ~tracer))]))

        maybe-unwrap-defers
        (mapcat identity
                (for [[ds _dk] bind
                      :when (#{:defer :lazy} (bind-param-type ds))]
                  [ds `(unwrap-not-a-deferred ~ds)]))

        deps (keys bind)
        [deps1 deps2] (split-at 16 deps)
        all-deps-tr (vec (for [[ds dk] bind] [dk (bind-param-type ds)]))
        executor-var (:executor (meta bind))]

    `(let [;; input - vector. unwraps 'defers', called by yget-fn#
           ~the-fnv
           (fn ~(-> ykey name symbol)
             ([~ctx ~tracer ~@deps1 ~@(when (seq deps2) ['& (vec deps2)])]
              (when-not (cancelled? ~ctx)
                (when ~tracer (t/trace-call ~tracer ~ykey))
                (coerce-deferred (let [~@maybe-unwrap-defers] ~expr)))))

           ;; input - mdm and registry, called by `yank-snatch
           yget-fn#
           (fn ~(-> ykey name (str "--yarn") symbol) [~ctx ~tracer]
             (let [[new# d#] (mdm-fetch! ~ctx ~ykey)]
               (if-not new#
                 ;; got item from mdm
                 d#
                 ;; calculate & provide to mdm
                 (maybe-future-with
                  ~executor-var

                  (when ~tracer (t/trace-start ~tracer ~ykey :yarn ~all-deps-tr))

                  (let [~reald (volatile! nil)]
                    (try ;; d# is alsways deffered
                      (let [~@yank-all-deps]
                        (let [x# (if ~(list*
                                       `or
                                       (for [d deps, :when (= (bind-param-type d) :sync)]
                                         `(md/deferred? ~d)))
                                   ~(if (<= (count deps) 3) ;; TOOD: check this number
                                      `(kd/realize' [~@deps] (vreset! ~reald (~the-fnv ~ctx ~tracer ~@deps)))
                                      `(md/chain' (md/zip' ~@deps) (fn [[~@deps]]
                                                                     (vreset! ~reald (~the-fnv ~ctx ~tracer ~@deps)))))
                                   (vreset! ~reald (~the-fnv ~ctx ~tracer ~@deps)))]

                          (connect-result-mdm ~ykey x# d# ~tracer ~reald)))

                      (catch Throwable e#
                        (connect-error-mdm ~ykey e# d# ~tracer ~reald))))))))]

       yget-fn#)))


(defn build-yarn-ref-gtr  [ykey orig-ykey]
  (fn [ctx tracer]
    (let [[new d] (mdm-fetch! ctx ykey)]
      (if-not new
        d
        (do
          (when tracer (t/trace-start tracer ykey :knot [[orig-ykey :ref]]))
          (try
            (when tracer (t/trace-call tracer ykey))
            (let [x (yarn-get-sync ykey orig-ykey ctx tracer)]
              (connect-result-mdm ykey x d tracer nil))
            (catch Throwable e
              (connect-error-mdm ykey e d tracer nil))))))))


(defn gen-yarn
  [ykey bind expr]
  `(Yarn.
    ~ykey
    ~(vec (vals bind))
    ~(build-yank-fns ykey bind expr)))


(defn gen-yarn-ref
  [ykey from]
  `(Yarn.
    ~ykey
    [~from]
    (build-yarn-ref-gtr ~ykey ~from)))


(defn- hide [d]
  (delay d))


(defn yank*
  [poy yarns registry tracer]
  (let [mdm (create-mdm poy (max 8 (* 2 (count yarns))))
        ctx (YankCtx. mdm registry false)
        errh (fn [e]
               (throw (ex-info "failed to yank"
                               (assoc (dissoc (ex-data e) ::inyank)
                                      :knitty/yanked-poy (hide poy)
                                      :knitty/failed-poy (hide (mdm-freeze! ctx))
                                      :knitty/yanked-yarns yarns
                                      :knitty/trace (when tracer
                                                      (conj
                                                       (-> poy meta :knitty/trace)
                                                       (capture-trace! tracer))))
                               e)))]
    (try
      (->
       (apply md/zip' (map #((resolve-yarn-gtr ctx %) ctx tracer) yarns))
       (md/chain'
        (fn [yvals]
          [yvals
           (let [poy' (mdm-freeze! ctx)]
             (if tracer
               (vary-meta poy' update :knitty/trace conj (capture-trace! tracer))
               poy'))]))
       (md/catch' errh)
       (kd/revoke' #(mdm-cancel! ctx)))
      (catch Throwable e (errh e)))))
