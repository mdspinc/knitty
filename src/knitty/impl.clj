(ns knitty.impl
  (:require [knitty.javaimpl :as ji]
            [knitty.trace :as t]
            [clojure.set :as set]
            [manifold.deferred :as md]
            [manifold.executor]
            [manifold.utils])
  (:import [knitty.javaimpl YankCtx KDeferred Yarn YarnProvider]))


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
  (empty [_] (Registry. (delay (make-array Yarn 0)) {} {}))

  clojure.lang.ILookup
  (valAt [_ k] (asmap k))
  (valAt [_ k d] (asmap k d))

  clojure.lang.Associative
  (containsKey [_ k] (contains? asmap k))
  (entryAt [_ k] (find asmap k))
  (assoc [_ k v]

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
       (delay (make-array Yarn (inc (ji/maxid))))
       (assoc asmap k v)
       all-deps'))))


(defn create-registry []
  (Registry. (delay (make-array Yarn 0)) {} {}))


(defn bind-param-type [ds]
  (let [{:keys [defer lazy yankfn maybe]} (meta ds)]
    (cond
      lazy   :lazy
      defer  :defer
      maybe  :maybe
      yankfn :yankfn
      :else  :sync)))


(defmacro tracer-> [yctx f & args]
  `(t/if-tracing
    (when-some [t# (.tracer ~yctx)]
      (~f t# ~@args))))


(defmacro yarn-get-impl
  ([yk ykey yctx]
   `(yarn-get-impl ~yk ~ykey ~(ji/regkw ykey) ~yctx))
  ([yk ykey ykeyi yctx]
   `(do
      (tracer-> ~yctx t/trace-dep ~yk ~ykey)
      (.fetch ~yctx ~ykeyi))))


(defmacro yarn-get-maybe
  [yk ykey yctx]
  `(do
     (tracer-> ~yctx t/trace-dep ~yk ~ykey)
     (.pull ~yctx ~(ji/regkw ykey))))


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
    ~(ji/regkw ykey)))


(defn make-yankfn
  [^YankCtx yctx
   yk
   yarns-map]
  (fn yankfn [y]
    (if-let [[i k] (yarns-map y)]
      (do
        (tracer-> yctx t/trace-dep yk k)
        (.fetch yctx i))
      (throw (ex-info "Invalid yank-fn arg" {:knitty/yankfn-arg y
                                             :knytty/yankfn-known-args (keys yarns-map)})))))


(defmacro yarn-get-yankfn [yk keys-map yctx]
  (let [args (into {} (map (fn [[k v]] [k [(ji/regkw v) v]])) keys-map)]
    `(make-yankfn ~yctx ~yk ~args)))


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


(t/if-tracing
 (deftype TraceListener [tracer ykey]
   manifold.deferred.IDeferredListener
   (onSuccess [_ x]
     (t/trace-finish tracer ykey x nil true))
   (onError [_ x]
     (t/trace-finish tracer ykey nil x true))))


(defmacro connect-result [yctx ykey result dest]
  `(if (instance? manifold.deferred.IDeferred ~result)
     (do
       (t/if-tracing
        (do
          (ji/kd-chain-from ~dest ~result (.token ~yctx))
          (when-some [t# (.-tracer ~yctx)]
            (md/add-listener! ~dest (TraceListener. t# ~ykey))))
        (do
          (ji/kd-chain-from ~dest ~result (.token ~yctx)))))
     (do
       (tracer-> ~yctx t/trace-finish ~ykey ~result nil false)
       (ji/kd-success! ~dest ~result (.token ~yctx)))))


(defmacro connect-error [yctx ykey error dest]
  `(do
     (tracer-> ~yctx t/trace-finish ~ykey nil ~error false)
     (ji/kd-error! ~dest ~error (.token ~yctx))))


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
                     :yankfn `(yarn-get-yankfn ~ykey ~dk ~yctx))]))

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
                  [ds `(ji/kd-get ~ds)]))

        all-deps-tr (into
                     []
                     (comp cat (distinct))
                     (for [[ds dk] bind
                           :let [pt (bind-param-type ds)]]
                       (if (= :yankfn pt)
                         (for [[_ k] dk] [k :yankfn])
                         [[dk pt]])))]

    `(reify Yarn
       (~'key [_] ~ykey)
       (~'deps [_] #{~@deps})
       (~'yank [_# ~yctx d#]
         (maybe-future-with
          ~executor
          (tracer-> ~yctx t/trace-start ~ykey :yarn ~all-deps-tr)
          (try
            (let [~@yank-deps]
              (ji/kd-await
               (reify manifold.deferred.IDeferredListener
                 (~'onSuccess
                   [_# _#]
                   (let [z# (let [~@deref-syncs]
                              (tracer-> ~yctx t/trace-call ~ykey)
                              (~coerce-deferred ~the-fn-body))]
                     (connect-result ~yctx ~ykey z# d#)))
                 (~'onError
                   [_ e#]
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
  [ykey bind expr]
  (ji/regkw ykey)
  (let [deps (grab-yarn-bindmap-deps bind)
        yarn-meta (meta bind)
        {:keys [keep-deps-order]} yarn-meta
        bind (if keep-deps-order
               bind
               (sort-by (comp #(when (keyword? %) (ji/regkw %)) second) bind))]
    (emit-yarn-impl expr ykey bind yarn-meta deps)))


(defn gen-yarn-ref
  [ykey from]
  (ji/regkw ykey)
  `(reify Yarn
     (~'deps [_#] #{~from})
     (~'key [_#] ~ykey)
     (~'yank [_# yctx# d#]
       (tracer-> yctx# t/trace-start ~ykey :knot [[~from :ref]])
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
  (let [i (long (ji/regkw k))]
    (fn yank-route-key [^YankCtx yctx ^KDeferred _]
      (tracer-> yctx t/trace-route-by ykey k)
      (ji/kd-get (.fetch yctx i)))))


(defn yarn-multi-deps [multifn route-key]
  (into #{route-key}
        (comp (map val)
              (keep #(%))
              (mapcat ji/yarn-deps))
        (methods multifn)))


(defn gen-yarn-multi
  [ykey route-key mult-options]
  (ji/regkw ykey)
  `(do
     (defmulti ~(yarn-multifn ykey)
       (make-multiyarn-route-key-fn ~ykey ~route-key)
       ~@mult-options)
     (let [multifn# ~(yarn-multifn ykey)]
       (reify Yarn
         (~'deps [_#]
           (yarn-multi-deps multifn# ~route-key))
         (~'key [_#]
           ~ykey)
         (~'yank [_ yctx# d#]
           (let [r# (yarn-get-impl ~ykey ~route-key yctx#)]
             (ji/kd-await
              (reify manifold.deferred.IDeferredListener
                (onSuccess [_# _#] (multifn# yctx# d#))
                (onError [_ e#] (ji/kd-error! r# e# (.token yctx#))))
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
  (ji/regkw ykey)
  (reify
    Yarn
    (key [_] ykey)
    (deps [_] #{})
    (yank [_ yctx d] (ji/kd-error! d (java.lang.UnsupportedOperationException. (str msg)) (.token yctx)))))


(defn gen-yarn-input [ykey]
  `(fail-always-yarn ~ykey ~(str "input-only yarn " ykey)))


(defn yank' [poy yarns registry tracer]
  (let [yctx (YankCtx. poy registry tracer)
        res (ji/kd-create)
        c (reify manifold.deferred.IDeferredListener

            (onSuccess
              [_ _]
              (when-not (ji/kd-realized? res)
                (ji/kd-success! res (.freezePoy yctx) nil)))

            (onError
              [_ e]
              (when-not (ji/kd-realized? res)
                (ji/kd-error! res
                              (ex-info "failed to yank"
                                       (assoc (ex-data e)
                                              :knitty/yank-error? true
                                              :knitty/yanked-poy poy
                                              :knitty/failed-poy (.freezePoy yctx)
                                              :knitty/yanked-yarns yarns)
                                       e)
                              nil))))]
    (md/add-listener! res (.canceller yctx))
    (.yank yctx yarns c)
    res))


(defn yank1' [poy yarn registry tracer]
  (let [yctx (YankCtx. poy registry tracer)
        r0 (ji/kd-create)
        c (reify manifold.deferred.IDeferredListener

            (onSuccess
              [_ x]
              (.freeze yctx)
              (ji/kd-success! r0 x nil))

            (onError
              [_ e]
              (when-not (ji/kd-realized? r0)
                (ji/kd-error! r0
                              (ex-info "failed to yank"
                                       (assoc (ex-data e)
                                              :knitty/yank-error? true
                                              :knitty/yanked-poy poy
                                              :knitty/failed-poy (.freezePoy yctx)
                                              :knitty/yanked-yarn yarn)
                                       e)
                              nil))))]
    (.addListener r0 (.canceller yctx))
    (.addListener (.yank1 yctx yarn) c)
    r0))
