(ns knitty.impl
  (:require [clojure.set :as set]
            [knitty.javaimpl :as ji]
            [knitty.trace :as t]
            [manifold.executor]
            [manifold.utils])
  (:import [clojure.lang AFn]
           [knitty.javaimpl KDeferred YankCtx YarnProvider KwMapper]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- check-no-cycle
  [root n path yarns]
  (if (= root n)
    (throw (ex-info "detected yarns cycle"
                    {:knitty/yarns-cycle (vec (reverse path))
                     :knitty/yarn root}))
    (doseq [p (ji/yarn-deps (yarns n))]
      (check-no-cycle root p (cons p path) yarns))))


(deftype Registry [ycache asmap all-deps]

  YarnProvider
  (yarn [_ kkw] (get asmap kkw))
  (ycache [_] @ycache)

  clojure.lang.Seqable
  (seq [_] (seq asmap))

  clojure.lang.IPersistentCollection
  (count [_] (count asmap))
  (cons [t x] (.assoc t (ji/yarn-key x) x))
  (equiv [_ o] (and (instance? Registry o) (= asmap (.-asmap ^Registry o))))
  (empty [_] (Registry. (delay (make-array AFn 0)) {} {}))

  clojure.lang.ILookup
  (valAt [_ k] (asmap k))
  (valAt [_ k d] (asmap k d))

  clojure.lang.Associative
  (containsKey [_ k] (contains? asmap k))
  (entryAt [_ k] (find asmap k))
  (assoc [_ k v]

    (let [k' (ji/yarn-key v)]
      (when (not= k k')
        (throw (ex-info "yarn key mismatch" {:knitty/assoc-key k, :knitty/yarn k'}))))

    (doseq [p (ji/yarn-deps v)]
      (when-not (contains? asmap p)
        (throw (ex-info "yarn has unknown dependency" {:knitty/yarn k, :knitty/dependency p}))))

    (let [deps (ji/yarn-deps v)
          all-deps' (assoc all-deps k (apply set/union deps (map all-deps deps)))]

      (when (contains? (all-deps' k) k)
        ;; node depends on itself
        (doseq [d deps]
          (check-no-cycle k d [k] asmap)))

      (Registry.
       (delay (make-array AFn (inc (KwMapper/maxi))))
       (assoc asmap k v)
       all-deps'))))


(defn create-registry []
  (Registry. (delay (make-array AFn 0)) {} {}))


(defn bind-param-type [ds]
  (let [{:keys [defer lazy case maybe]} (meta ds)]
    (cond
      lazy   :lazy
      defer  :defer
      maybe  :maybe
      case   :case
      :else  :sync)))


(defmacro tracer-> [yctx f & args]
  `(t/if-tracing
    (when-some [^knitty.trace.Tracer t# (.-tracer ~yctx)]
      (~f t# ~@args))))


(defmacro yarn-get-impl
  ([yk ykey yctx]
   `(yarn-get-impl ~yk ~ykey ~(KwMapper/reg ykey) ~yctx))
  ([yk ykey ykeyi yctx]
   `(do
      (tracer-> ~yctx .traceDep ~yk ~ykey)
      (.fetch ~yctx ~ykeyi ~ykey))))


(defmacro yarn-get-maybe
  [yk ykey yctx]
  `(do
     (tracer-> ~yctx .traceDep ~yk ~ykey)
     (.pull ~yctx ~(KwMapper/reg ykey))))


(deftype Lazy
         [^YankCtx yctx
          ^clojure.lang.Keyword yk
          ^clojure.lang.Keyword ykey
          ^long ykeyi]

  clojure.lang.IDeref
  (deref [_] (yarn-get-impl yk ykey ykeyi yctx))

  clojure.lang.IFn
  (invoke [_] (yarn-get-impl yk ykey ykeyi yctx))

  Object
  (toString [_] (str "#knitty/Lazy[" ykey "]")))


(defmacro yarn-get-lazy [yk ykey yctx]
  `(Lazy.
    ~yctx
    ~yk
    ~ykey
    ~(KwMapper/reg ykey)))


(defmacro yarn-get-case [yk keys-map yctx]
  (let [keys-map (cond
                   (map? keys-map)
                   keys-map

                   (or (vector? keys-map) (set? keys-map))
                   (into {} (map vector keys-map keys-map))

                   :else
                   (throw (ex-info "invalid yank-fn args mapping"
                                   {:knitty/yankfn-yarn yk
                                    :knitty/yankfn-mapping keys-map})))]
    `(fn [k#]
       (case k#
         ~@(mapcat (fn [[k v]]
                     [(list k)
                      `(do
                         (tracer-> ~yctx .traceDep ~yk ~v)
                         (yarn-get-impl ~yk ~v ~yctx))])
                   keys-map)
         (throw (ex-info "invalid yank-fn arg" {:knitty/yankfn-arg k#
                                                :knytty/yankfn-known-args ~(set (keys keys-map))}))))))


(defmacro force-lazy-result [v]
  `(let [v# ~v]
     (if (instance? Lazy v#)
       @v#
       v#)))


(defn resolve-executor-var [e]
  (when-let [ee (var-get e)]
    (if (ifn? ee) (ee) ee)))


(defmacro maybe-future-with [executor-var & body]
  (if-not executor-var
    `(do ~@body)
    `(manifold.utils/future-with (resolve-executor-var ~executor-var) ~@body)))


(defmacro connect-result [yctx ykey result dest]
  `(if (instance? manifold.deferred.IDeferred ~result)
     (do
       (t/if-tracing
        (do
          (.chain ~dest ~result (.-token ~yctx))
          (when-some [^knitty.trace.Tracer t# (.-tracer ~yctx)]
            (ji/kd-await
             (fn
               ([] (.traceFinish t# ~ykey (ji/kd-get ~dest) nil true))
               ([e#] (.traceFinish t# ~ykey nil e# true)))
             ~dest)))
        (do
          (ji/kd-chain-from ~dest ~result (.token ~yctx)))))
     (do
       (tracer-> ~yctx .traceFinish ~ykey ~result nil false)
       (.success ~dest ~result (.-token ~yctx)))))


(defmacro connect-error [yctx ykey error dest]
  `(do
     (tracer-> ~yctx .traceFinish ~ykey nil ~error false)
     (.error ~dest ~error (.-token ~yctx))))


(defn emit-yarn-impl
  [the-fn-body ykey bind yarn-meta deps]
  (let [{:keys [executor]} yarn-meta
        yctx '__yank_ctx

        yank-deps
        (mapcat identity
                (for [[ds dk] bind]
                  [ds
                   (case (bind-param-type ds)
                     :sync   `(yarn-get-impl   ~ykey ~dk ~yctx)
                     :lazy   `(yarn-get-lazy   ~ykey ~dk ~yctx)
                     :defer  `(yarn-get-impl   ~ykey ~dk ~yctx)
                     :maybe  `(yarn-get-maybe  ~ykey ~dk ~yctx)
                     :case   `(yarn-get-case   ~ykey ~dk ~yctx))]))

        sync-deps
        (for [[ds _dk] bind
              :when (#{:sync} (bind-param-type ds))]
          ds)

        param-types (set (for [[ds _dk] bind] (bind-param-type ds)))

        coerce-deferred (if (param-types :lazy)
                          `force-lazy-result
                          `do)

        deref-syncs
        (mapcat identity
                (for [[ds _dk] bind
                      :when (#{:sync} (bind-param-type ds))]
                  [ds `(.get ~ds)]))

        all-deps-tr (into
                     []
                     (comp cat (distinct))
                     (for [[ds dk] bind
                           :let [pt (bind-param-type ds)]]
                       (if (= :case pt)
                         (for [[_ k] dk] [k :case])
                         [[dk pt]])))]

    `(ji/decl-yarn
      ~ykey
      ~(set deps)
      (fn [~yctx ^KDeferred d#]
        (maybe-future-with
         ~executor
         (tracer-> ~yctx .traceStart ~ykey :yarn ~all-deps-tr)
         (try
           (let [~@yank-deps]
             (ji/kd-await
              (fn
                ([]
                 (try
                   (let [~@deref-syncs]
                     (tracer-> ~yctx .traceCall ~ykey)
                     (let [z# (~coerce-deferred ~the-fn-body)]
                       (connect-result ~yctx ~ykey z# d#)))
                   (catch Throwable e#
                     (connect-error ~yctx ~ykey e# d#))))
                ([e#]
                 (connect-error ~yctx ~ykey e# d#)))
              ~@sync-deps))
           (catch Throwable e#
             (connect-error ~yctx ~ykey e# d#))))))))


(defn- grab-yarn-bindmap-deps [bm]
  (into
   #{}
   cat
   (for [[_ k] bm]
     (cond
       (keyword? k) [k]
       (map? k) (vals k)
       :else (throw (ex-info "invalid binding arg" {::param k}))))))


(defn gen-yarn
  [ykey bind expr opts]
  (KwMapper/reg ykey)
  (let [deps (grab-yarn-bindmap-deps bind)
        {:keys [keep-deps-order]} opts
        bind (if keep-deps-order
               bind
               (sort-by (comp #(when (keyword? %) (KwMapper/reg %)) second) bind))]
    (emit-yarn-impl expr ykey bind opts deps)))


(defn gen-yarn-ref
  [ykey from]
  `(ji/decl-yarn
    ~ykey #{~from}
    (fn [yctx# d#]
      (tracer-> yctx# .traceStart ~ykey :knot [[~from :ref]])
      (try
        (let [x# (ji/kd-unwrap (yarn-get-impl ~ykey ~from yctx#))]
          (connect-result yctx# ~ykey x# d#))
        (catch Throwable e#
          (connect-error yctx# ~ykey e# d#))))))


(defn yarn-multifn [yarn-name]
  (cond
    (symbol? yarn-name) `(yarn-multifn ~(eval yarn-name))
    (keyword? yarn-name) (symbol (namespace yarn-name) (str (name yarn-name) "--multifn"))
    :else (throw (ex-info "invalid yarn-name" {:yarn-name yarn-name}))))


(defn make-multiyarn-route-key-fn [ykey k]
  (let [i (long (KwMapper/reg k))]
    (fn yank-route-key [^YankCtx yctx ^KDeferred _]
      (tracer-> yctx .traceRouteBy ykey k)
      (ji/kd-get (.fetch yctx i k)))))


(defn yarn-multi-deps [multifn route-key]
  (into #{route-key}
        (comp (map val)
              (keep #(%))
              (mapcat ji/yarn-deps))
        (methods multifn)))


(defn gen-yarn-multi
  [ykey route-key mult-options]
  (KwMapper/reg ykey)
  `(do
     (defmulti ~(yarn-multifn ykey)
       (make-multiyarn-route-key-fn ~ykey ~route-key)
       ~@mult-options)
     (let [multifn# ~(yarn-multifn ykey)]
       (ji/decl-yarn
        ~ykey
        (yarn-multi-deps multifn# ~route-key)
        (fn [yctx# d#]
          (let [r# (yarn-get-impl ~ykey ~route-key yctx#)]
            (ji/kd-await
             (fn
               ([] (try
                     (multifn# yctx# d#)
                     (catch Throwable e#
                       (ji/kd-error! d# e# (.-token yctx#)))))
               ([e#] (ji/kd-error! d# e# (.-token yctx#))))
             r#)))))))


(defn gen-reg-yarn-method
  [yk yarn route-val]
  `(let [y# ~yarn
         rv# ~route-val]
     (defmethod
       ~(yarn-multifn yk)
       rv#
       ([] y#)
       ([yctx# d#] (ji/yarn-yank y# yctx# d#)))))


(defn fail-always-yarn [ykey msg]
  (ji/decl-yarn
   fail-always-yarn
   ykey
   #{}
   (fn [yctx d] (ji/kd-error! d (java.lang.UnsupportedOperationException. (str msg)) (.-token yctx)))))


(defn gen-yarn-input [ykey]
  `(fail-always-yarn ~ykey ~(str "input-only yarn " ykey)))


(definline yank' [poy yarns registry tracer]
  `(let [yctx# (YankCtx. ~poy ~registry ~tracer)]
     (.yank yctx# ~yarns)))

(definline yank1' [poy yarn registry tracer]
  `(let [yctx# (YankCtx. ~poy ~registry ~tracer)]
     (.yank1 yctx# ~yarn)))
