(ns knitty.impl
  (:require [knitty.javaimpl :as ji]
            [knitty.trace :as t]
            [clojure.set :as set]
            [manifold.deferred :as md]
            [manifold.executor]
            [manifold.utils])
  (:import [knitty.javaimpl MDM KDeferred Yarn YarnProvider]))


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


(deftype Registry [ygets asmap all-deps]

  clojure.lang.Seqable
  (seq [_] (seq asmap))

  clojure.lang.ILookup
  (valAt [_ k] (asmap k))
  (valAt [_ k d] (asmap k d))

  clojure.lang.Associative
  (containsKey [_ k] (contains? asmap k))
  (entryAt [_ k] (find asmap k))
  (empty [_] (Registry. (delay (make-array Yarn 0)) {} {}))

  YarnProvider
  (yarn [_ kkw] (get asmap kkw))

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
       (delay (make-array Yarn (+ 10 (ji/max-initd))))
       (assoc asmap k v)
       all-deps'))))


(defn create-registry []
  (Registry. (delay (make-array Yarn 0)) {} {}))


(defn bind-param-type [ds]
  (let [{:keys [defer lazy yankfn]} (meta ds)]
    (cond
      lazy   :lazy
      defer  :defer
      yankfn :yankfn
      :else  :sync)))


(defmacro do-tracing [& body]
  (when-not t/elide-tracing
    `(do ~@body)))


(defmacro ctx-tracer-> [ctx fn & args]
  `(do-tracing
    (when-some [t# (.-tracer ~ctx)]
      (~fn t# ~@args))))


(defn yarn-get [^MDM ctx y]
  (if (keyword? y)
    (.fetchK ctx y)
    (.fetchY ctx y)))


(defmacro yarn-get-impl
  ([yk ykey ctx]
   `(yarn-get-impl ~yk ~ykey ~(ji/keyword->intid ykey) ~ctx))
  ([yk ykey ykeyi ctx]
   `(do
      (ctx-tracer-> ~ctx t/trace-dep ~yk ~ykey)
      (.fetch ~ctx ~ykeyi))))


(deftype Lazy
         [^MDM ctx
          ^clojure.lang.Keyword yk
          ^clojure.lang.Keyword ykey
          ^long ykeyi]

  clojure.lang.IDeref
  (deref [_] (yarn-get-impl yk ykey ykeyi ctx))

  Object
  (toString [_] (str "#knitty/Lazy[" ykey "]")))


(defmacro yarn-get-lazy [yk ykey ctx]
  `(Lazy.
    ~ctx
    ~yk
    ~ykey
    ~(ji/keyword->intid ykey)))


(defn make-yankfn
  [^MDM ctx
   yk
   yarns-map]
  (fn yankfn [y]
    (if-let [[i k] (yarns-map y)]
      (do
        (ctx-tracer-> ctx t/trace-dep yk k)
        (ji/mdm-fetch! ctx i))
      (throw (ex-info "Invalid yank-fn arg" {:knitty/yankfn-arg y
                                             :knytty/yankfn-known-args (keys yarns-map)})))))


(defmacro yarn-get-yankfn [yk keys-map ctx]
  (let [args (into {} (map (fn [[k v]] [k [(ji/keyword->intid v) v]])) keys-map)]
    `(make-yankfn ~ctx ~yk ~args)))


(defn force-lazy-result [v]
  (if (instance? Lazy v)
    @v
    v))


(defn resolve-executor-var [e]
  (when-let [ee (var-get e)]
    (if (ifn? ee) (ee) ee)))


(defmacro maybe-future-with [executor-var & body]
  (if-not executor-var
    `(do ~@body)
    `(manifold.utils/future-with (resolve-executor-var ~executor-var) ~@body)))


(do-tracing
 (deftype TraceListener [tracer ykey]
   manifold.deferred.IDeferredListener
   (onSuccess [_ x]
     (t/trace-finish tracer ykey x nil true))
   (onError [_ x]
     (t/trace-finish tracer ykey nil x true))))


(defmacro connect-result-mdm [ctx ykey result mdm-deferred]
  `(if (instance? manifold.deferred.IDeferred ~result)
     (do
       (ji/kd-chain-from ~mdm-deferred ~result (.-token ~ctx))
       (do-tracing
        (when-some [t# (.-tracer ~ctx)]
          (md/add-listener! ~mdm-deferred (TraceListener. t# ~ykey)))))
     (do
       (ctx-tracer-> ~ctx t/trace-finish ~ykey ~result nil false)
       (ji/kd-success! ~mdm-deferred ~result (.-token ~ctx))
       )))


(defmacro connect-error-mdm [ctx ykey error mdm-deferred]
  `(do
     (ctx-tracer-> ~ctx t/trace-finish ~ykey nil ~error false)
     (ji/kd-error! ~mdm-deferred ~error (.-token ~ctx))))


(defn emit-yarn-impl
  [the-fn-body ykey bind yarn-meta deps]
  (let [{:keys [executor]} yarn-meta
        ctx '_yank_ctx
        yank-deps
        (mapcat identity
                (for [[ds dk] bind]
                  [ds
                   (case (bind-param-type ds)
                     :sync   `(yarn-get-impl   ~ykey ~dk ~ctx)
                     :defer  `(yarn-get-impl   ~ykey ~dk ~ctx)
                     :lazy   `(yarn-get-lazy   ~ykey ~dk ~ctx)
                     :yankfn `(yarn-get-yankfn ~ykey ~dk ~ctx))]))

        sync-deps
        (for [[ds _dk] bind
              :when (#{:sync} (bind-param-type ds))]
          ds)

        param-types (set (for [[ds _dk] bind] (bind-param-type ds)))

        coerce-deferred (if (param-types :lazy)
                          `force-lazy-result
                          `identity)

        deref-syncs
        (mapcat identity
                (for [[ds _dk] bind
                      :when (#{:sync} (bind-param-type ds))]
                  [ds `(ji/kd-unwrap ~ds)]))

        all-deps-tr (into
                     []
                     (comp cat (distinct))
                     (for [[ds dk] bind
                           :let [pt (bind-param-type ds)]]
                       (if (= :yankfn pt)
                         (for [[_ k] dk] [k :yankfn])
                         [[dk pt]])))]

    `(reify Yarn
       (key [_] ~ykey)
       (deps [_] #{~@deps})
       (yank [_# ~ctx d#]
         (maybe-future-with
          ~executor
          (ctx-tracer-> ~ctx t/trace-start ~ykey :yarn ~all-deps-tr)
          (try
            (let [~@yank-deps]
              (ji/kd-await
               (reify manifold.deferred.IDeferredListener
                 (onSuccess
                   [_# _#]
                   (let [z# (let [~@deref-syncs]
                              (ctx-tracer-> ~ctx t/trace-call ~ykey)
                              (~coerce-deferred ~the-fn-body))]
                     (connect-result-mdm ~ctx ~ykey z# d#)))
                 (onError
                   [_ e#]
                   (connect-error-mdm ~ctx ~ykey e# d#)))
               ~@sync-deps))
            (catch Throwable e#
              (connect-error-mdm ~ctx ~ykey e# d#))))))))




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
  (MDM/regkw ykey)
  (let [deps (grab-yarn-bindmap-deps bind)
        yarn-meta (meta bind)
        {:keys [keep-deps-order]} yarn-meta
        bind (if keep-deps-order
               bind
               (sort-by (comp #(when (keyword? %) (ji/keyword->intid %)) second) bind))]
      (emit-yarn-impl expr ykey bind yarn-meta deps)))


(defn gen-yarn-ref
  [ykey from]
  (MDM/regkw ykey)
  `(reify Yarn
    (deps [_] #{~from})
    (key [_] ~ykey)
    (yank [_# ctx# d#]
      (ctx-tracer-> ctx# t/trace-start ~ykey :knot [[~from :ref]])
      (try
        (let [x# (ji/kd-unwrap (yarn-get-impl ~ykey ~from ctx#))]
           ;; (ctx-tracer-> ctx# t/trace-call ~ykey)
          (connect-result-mdm ctx# ~ykey x# d#))
        (catch Throwable e#
          (connect-error-mdm ctx# ~ykey e# d#))))))


(defn yarn-multifn [yarn-name]
  (cond
    (symbol? yarn-name) `(yarn-multifn ~(eval yarn-name))
    (keyword? yarn-name) (symbol (namespace yarn-name) (str (name yarn-name) "--multifn"))
    :else (throw (ex-info "invalid yarn-name" {:yarn-name yarn-name}))))


(defn make-multiyarn-route-key-fn [ykey k]
  (let [i (long (ji/keyword->intid k))]
    (fn yank-route-key [^MDM ctx ^KDeferred _]
      (ji/kd-unwrap (yarn-get-impl ykey k i ctx)))))


(defn yarn-multi-deps [multifn route-key]
  (into #{route-key}
        (comp (map val)
              (keep #(%))
              (mapcat ji/yarn-deps))
        (methods multifn)))


(defn gen-yarn-multi
  [ykey route-key mult-options]
  (MDM/regkw ykey)
  `(do
     (defmulti ~(yarn-multifn ykey)
       (make-multiyarn-route-key-fn ~ykey ~route-key)
       ~@mult-options)
     (let [multifn# ~(yarn-multifn ykey)]
       (reify Yarn
         (deps [_]
           (yarn-multi-deps multifn# ~route-key))
         (key [_]
           ~ykey)
         (yank [_ ctx# d#]
           (let [r# (yarn-get-impl ~ykey ~route-key ctx)]
             (ji/kd-await
              (reify manifold.deferred.IDeferredListener
                (onSuccess [_# _#] (multifn# ctx# d#))
                (onError [_ e#] (ji/kd-error! r# e# (.-token ctx#))))
              r#)))
         ~(yarn-multifn ykey) ~ykey ~route-key))))


(defn gen-reg-yarn-method
  [yk yarn route-val]
  (let [y (with-meta (gensym) {:tag (str Yarn)})]
    `(let [~y ~yarn
           rv# ~route-val]
       (defmethod
         ~(yarn-multifn yk)
         rv#
         ([] ~y)
         ([ctx# d#] ;;fixme
          (.yank ~y ctx# d#))))))


(defn fail-always-yarn [ykey msg]
  (MDM/regkw ykey)
  (reify
    Yarn
    (key [_] ykey)
    (deps [_] #{})
    (yank [_ ctx d] (ji/kd-error! d (java.lang.UnsupportedOperationException. (str msg)) (.-token ctx)))))


(defn gen-yarn-input [ykey]
  (MDM/regkw ykey)
  `(reify
     Yarn
     (key [_] ~ykey)
     (deps [_] #{})
     (yank [_# ctx# d#]
       (connect-error-mdm ctx# ~ykey (java.lang.UnsupportedOperationException. ~(str "input-only yarn " ykey)) d#))))


(defn yank0
  [poy yarns registry tracer]
  (let [ctx (MDM. poy registry @(.-ygets ^Registry registry) tracer)
        token (.-token ctx)
        result (ji/kd-create token)
        kds (map (fn [y] (yarn-get ctx y)) yarns)]
    (run! identity kds)
    (ji/kd-await-coll
     (reify manifold.deferred.IDeferredListener
       (onSuccess [_ _]
         (ji/kd-success! result
                         (let [poy' (ji/mdm-freeze! ctx)]
                           (if tracer
                             (with-meta poy'
                               (update (meta poy) :knitty/trace conj (t/capture-trace! tracer)))
                             poy'))
                         token))
       (onError [_ e]
         (ji/kd-error! result
                       (ex-info "failed to yank"
                                (cond->
                                 (assoc (dissoc (ex-data e) ::inyank)
                                        :knitty/yanked-poy poy
                                        :knitty/failed-poy (ji/mdm-freeze! ctx)
                                        :knitty/yanked-yarns yarns)
                                  tracer (assoc
                                          :knitty/trace
                                          (conj
                                           (-> poy meta :knitty/trace)
                                           (t/capture-trace! tracer))))
                                e)
                       token)))
     kds)
    (ji/kd-revoke result
                  (fn cancel-mdm [] (ji/mdm-cancel! ctx)))))
