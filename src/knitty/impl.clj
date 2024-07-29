(ns knitty.impl
  (:require [clojure.set :as set]
            [knitty.deferred :as kd]
            [knitty.trace :as t]
            [manifold.executor]
            [manifold.utils])
  (:import [clojure.lang AFn]
           [java.util Arrays]
           [java.util.concurrent ForkJoinPool ForkJoinPool$ForkJoinWorkerThreadFactory TimeUnit]
           [knitty.javaimpl
            KDeferred
            KwMapper
            YankCtx
            YarnProvider]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defrecord YarnInfo
           [type key deps body-sexp])

(defmacro decl-yarn
  ([ykey deps bodyf]
   (assert (qualified-keyword? ykey))
   (KwMapper/registerKeyword ykey)
   `(decl-yarn ~(symbol (name ykey)) ~ykey ~deps ~bodyf))
  ([fnname ykey deps [_fn [ctx dst] & body]]
   `(fn
      ~fnname
      ([] ~(if (and (keyword? ykey)
                    (set? deps))
             (->YarnInfo
              :knitty/yarn-info
              ykey
              deps
              body)
             (list
              `->YarnInfo
              :knitty/yarn-info
              ykey
              deps
              (list `quote body))))
      ([~(vary-meta ctx assoc :tag "knitty.javaimpl.YankCtx")
        ~(vary-meta dst assoc :tag "knitty.javaimpl.KDeferred")]
       ~@body))))


(definline yarn-deps [y]
  `(:deps (~y)))

(definline yarn-key [y]
  `(:key (~y)))

(definline yarn-yank [y ctx d]
  `(~y ~ctx ~d))

(defn- check-no-cycle
  [root n path yarns]
  (if (= root n)
    (throw (ex-info "detected yarns cycle"
                    {:knitty/yarns-cycle (vec (reverse path))
                     :knitty/yarn root}))
    (doseq [p (yarn-deps (yarns n))]
      (check-no-cycle root p (cons p path) yarns))))


(defn- ensure-array-len ^objects [^objects arr ^long new-size]
  (if (>= (alength arr) new-size)
    arr
    (Arrays/copyOf arr (-> new-size (quot 128) (inc) (* 128)))))


(deftype Registry [ycache asmap all-deps]

  YarnProvider
  (yarn [_ kkw] (get asmap kkw))
  (ycache [_] ycache)

  clojure.lang.Seqable
  (seq [_] (seq asmap))

  clojure.lang.IPersistentCollection
  (count [_] (count asmap))
  (cons [t x] (.assoc t (yarn-key x) x))
  (equiv [_ o] (and (instance? Registry o) (= asmap (.-asmap ^Registry o))))
  (empty [_] (Registry. (make-array AFn 32) {} {}))

  clojure.lang.ILookup
  (valAt [_ k] (asmap k))
  (valAt [_ k d] (asmap k d))

  clojure.lang.Associative
  (containsKey [_ k] (contains? asmap k))
  (entryAt [_ k] (find asmap k))
  (assoc [_ k v]

    (let [k' (yarn-key v)]
      (when (not= k k')
        (throw (ex-info "yarn key mismatch" {:knitty/assoc-key k, :knitty/yarn k'}))))

    (doseq [p (yarn-deps v)]
      (when-not (contains? asmap p)
        (throw (ex-info "yarn has unknown dependency" {:knitty/yarn k, :knitty/dependency p}))))

    (let [i (KwMapper/registerKeyword k)
          max-idx (.maxIndex (KwMapper/getInstance))
          deps (yarn-deps v)
          all-deps' (assoc all-deps k (apply set/union deps (map all-deps deps)))]

      (when (contains? (all-deps' k) k)
        ;; node depends on itself
        (doseq [d deps]
          (check-no-cycle k d [k] asmap)))

      (let [^objects ycache' (ensure-array-len ycache (inc max-idx))]
        (aset ycache' i v)
        (Registry. ycache' (assoc asmap k v) all-deps')))))


(defn create-registry []
  (Registry. (make-array AFn 32) {} {}))


(defn bind-param-type [ds]
  ;; TODO: validate
  (let [{:keys [defer lazy case maybe fork]} (meta ds)]
    (cond
      (and fork defer) :fork-defer
      (and fork (not (or lazy case maybe defer))) :fork-sync
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
   `(yarn-get-impl ~yk ~ykey ~(KwMapper/registerKeyword ykey) ~yctx))
  ([yk ykey ykeyi yctx]
   `(do
      (tracer-> ~yctx .traceDep ~yk ~ykey)
      (.fetch ~yctx ~ykeyi ~ykey))))


(defmacro yarn-get-maybe
  [yk ykey yctx]
  `(do
     (tracer-> ~yctx .traceDep ~yk ~ykey)
     (.pull ~yctx ~(KwMapper/registerKeyword ykey))))


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
    ~(KwMapper/registerKeyword ykey)))


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


(defmacro do-pool-fork [ctx & body]
  `(.fork (.pool ~ctx) (fn* ^:once [] ~@body)))


(defmacro pool-run [ctx & body]
  `(.run (.pool ~ctx) (fn* ^:once [] ~@body)))


(defmacro yarn-get-fork [yk ykey yctx]
  `(let [d# (.pull ~yctx ~(KwMapper/registerKeyword ykey))]
     (when-not (.owned d#)
       (do-pool-fork ~yctx (yarn-get-impl ~yk ~ykey ~yctx)))
     d#))


(defmacro connect-result [yctx ykey result dest]
  `(if (instance? manifold.deferred.IDeferred ~result)
     (do
       (kd/on ~result
              (fn ~'on-val [x#]
                (tracer-> ~yctx .traceFinish ~ykey x# nil true)
                (pool-run ~yctx (.success ~dest x# (.-token ~yctx))))
              (fn ~'on-err [e#]
                (tracer-> ~yctx .traceFinish ~ykey nil e# true)
                (pool-run ~yctx (.error ~dest e# (.-token ~yctx))))))
     (do
       (tracer-> ~yctx .traceFinish ~ykey ~result nil false)
       (.success ~dest ~result (.-token ~yctx)))))


(defmacro connect-error [yctx ykey error dest]
  `(do
     (tracer-> ~yctx .traceFinish ~ykey nil ~error false)
     (.error ~dest ~error (.-token ~yctx))))


(defn emit-yarn-impl
  [the-fn-body ykey bind yarn-meta deps]
  (let [{:keys [fork]} yarn-meta
        yctx '__yank_ctx

        yank-deps
        (mapcat identity
                (for [[ds dk] bind]
                  [ds
                   (case (bind-param-type ds)
                     :sync       `(yarn-get-impl   ~ykey ~dk ~yctx)
                     :defer      `(yarn-get-impl   ~ykey ~dk ~yctx)
                     :fork-sync  `(yarn-get-fork   ~ykey ~dk ~yctx)
                     :fork-defer `(yarn-get-fork   ~ykey ~dk ~yctx)
                     :lazy       `(yarn-get-lazy   ~ykey ~dk ~yctx)
                     :maybe      `(yarn-get-maybe  ~ykey ~dk ~yctx)
                     :case       `(yarn-get-case   ~ykey ~dk ~yctx))]))

        sync-deps
        (for [[ds _dk] bind
              :when (#{:sync :fork-sync} (bind-param-type ds))]
          ds)

        param-types (set (for [[ds _dk] bind] (let [p (bind-param-type ds)]
                                                (get {:fork-defer :defer, :fork-sync :sync} p p))))

        coerce-deferred (if (param-types :lazy)
                          `force-lazy-result
                          `do)

        deref-syncs
        (mapcat identity
                (for [[ds _dk] bind
                      :when (#{:sync :fork-sync} (bind-param-type ds))]
                  [ds `(kd/kd-get ~ds)]))

        all-deps-tr (into
                     []
                     (comp cat (distinct))
                     (for [[ds dk] bind
                           :let [pt (bind-param-type ds)]]
                       (if (= :case pt)
                         (for [[_ k] dk] [k :case])
                         [[dk pt]])))

        do-maybe-fork (if fork `do-pool-fork `do)
        ;;
        ]

    `(decl-yarn
      ~ykey
      ~(set deps)
      (fn [~yctx ^KDeferred d#]
        (tracer-> ~yctx .traceStart ~ykey :yarn ~all-deps-tr)
        (~do-maybe-fork
         ~yctx
         (try
           (let [~@yank-deps]
             (kd/kd-await!
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
  (KwMapper/registerKeyword ykey)
  (let [deps (grab-yarn-bindmap-deps bind)
        {:keys [keep-deps-order]} opts
        bind (if keep-deps-order
               bind
               (sort-by (comp #(when (keyword? %) (KwMapper/registerKeyword %)) second) bind))]
    (emit-yarn-impl expr ykey bind opts deps)))


(defn gen-yarn-ref
  [ykey from]
  `(decl-yarn
    ~ykey #{~from}
    (fn [yctx# d#]
      (tracer-> yctx# .traceStart ~ykey :knot [[~from :ref]])
      (try
        (let [x# (.unwrap (yarn-get-impl ~ykey ~from yctx#))]
          (connect-result yctx# ~ykey x# d#))
        (catch Throwable e#
          (connect-error yctx# ~ykey e# d#))))))


(defn yarn-multifn [yarn-name]
  (cond
    (symbol? yarn-name) `(yarn-multifn ~(eval yarn-name))
    (keyword? yarn-name) (symbol (namespace yarn-name) (str (name yarn-name) "--multifn"))
    :else (throw (ex-info "invalid yarn-name" {:yarn-name yarn-name}))))


(defn make-multiyarn-route-key-fn [ykey k]
  (let [i (long (KwMapper/registerKeyword k))]
    (fn yank-route-key [^YankCtx yctx ^KDeferred _]
      (tracer-> yctx .traceRouteBy ykey k)
      (.get (.fetch yctx i k)))))


(defn yarn-multi-deps [multifn route-key]
  (into #{route-key}
        (comp (map val)
              (keep #(%))
              (mapcat yarn-deps))
        (methods multifn)))


(defn gen-yarn-multi
  [ykey route-key mult-options]
  (KwMapper/registerKeyword ykey)
  `(do
     (defmulti ~(yarn-multifn ykey)
       (make-multiyarn-route-key-fn ~ykey ~route-key)
       ~@mult-options)
     (let [multifn# ~(yarn-multifn ykey)]
       (decl-yarn
        ~ykey
        (yarn-multi-deps multifn# ~route-key)
        (fn [yctx# d#]
          (let [r# (yarn-get-impl ~ykey ~route-key yctx#)]
            (kd/kd-await!
             (fn
               ([] (try
                     (multifn# yctx# d#)
                     (catch Throwable e#
                       (.error d# e# (.-token yctx#)))))
               ([e#] (.error d# e# (.-token yctx#))))
             r#)))))))


(defn gen-reg-yarn-method
  [yk yarn route-val]
  `(let [y# ~yarn
         rv# ~route-val]
     (defmethod
       ~(yarn-multifn yk)
       rv#
       ([] y#)
       ([yctx# d#] (yarn-yank y# yctx# d#)))))


(defn fail-always-yarn [ykey msg]
  (decl-yarn
   fail-always-yarn
   ykey
   #{}
   (fn [yctx d] (.error d (java.lang.UnsupportedOperationException. (str msg)) (.-token yctx)))))


(defn gen-yarn-input [ykey]
  `(fail-always-yarn ~ykey ~(str "input-only yarn " ykey)))


(defn enumerate-fjp-factory [name-prefix]
  (let [c (atom 0)
        f ForkJoinPool/defaultForkJoinWorkerThreadFactory]
    (reify ForkJoinPool$ForkJoinWorkerThreadFactory
      (newThread [_ pool]
        (let [w (.newThread f pool)]
          (.setName w (str name-prefix "-" (swap! c inc)))
          w)))))


(defn create-fjp
  [{:keys [parallelism
           factory
           factory-prefix
           exception-handler
           max-size
           min-size
           saturate
           keep-alive-seconds
           min-runnable
           async-mode
           ]}]
  {:pre [(or (not factory) (factory-prefix))]}
  (let [parallelism (or parallelism (.availableProcessors (Runtime/getRuntime)))
        factory (or factory (enumerate-fjp-factory factory-prefix))
        saturate
        (when saturate
          (reify java.util.function.Predicate
            (test [_ pool]
              (boolean (saturate pool)))))
        exception-handler
        (when exception-handler
          (reify java.lang.Thread$UncaughtExceptionHandler
            (uncaughtException [_ thread exception] (exception-handler thread exception))))]
    (ForkJoinPool.
     parallelism
     factory
     exception-handler
     (boolean async-mode)
     (int (or min-size 0))
     (int (or max-size 32768))
     (int (or min-runnable 1))
     saturate
     (or keep-alive-seconds 60)
     TimeUnit/SECONDS)
    ))
