(ns knitty.impl
  (:require [knitty.deferred :as kd]
            [knitty.javaimpl :as ji]
            [knitty.trace :as t :refer [capture-trace!]]
            [clojure.set :as set]
            [manifold.deferred :as md]
            [manifold.executor]
            [manifold.utils])
  (:import [java.util.concurrent.atomic AtomicReference]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol IYarn
  (yarn-yankfn [_] "get or build yank fn [IYankCtx => result]")
  (yarn-deps [_] "get yarn dependencies as set of keywords")
  (yarn-key [_] "get yarn qualified keyword id"))

(defprotocol IYarnMulti
  (yarn-multifn [_] "get yarn routing multifn"))


(deftype Yarn [yankfn key deps]
  IYarn
  (yarn-yankfn [_] yankfn)
  (yarn-deps [_] deps)
  (yarn-key [_] key))


(defn- check-no-cycle
  [root n path yarns]
  (if (= root n)
    (throw (ex-info "detected yarns cycle"
                    {:knitty/yarns-cycle (vec (reverse path))
                     :knitty/yarn root}))
    (doseq [p (yarn-deps (yarns n))]
      (check-no-cycle root p (cons p path) yarns))))


(deftype Registry [^objects ygets asmap all-deps]

  clojure.lang.Seqable
  (seq [_] (seq asmap))

  clojure.lang.ILookup
  (valAt [_ k] (asmap k))
  (valAt [_ k d] (asmap k d))

  clojure.lang.Associative
  (containsKey [_ k] (contains? asmap k))
  (entryAt [_ k] (find asmap k))
  (empty [_] (Registry. nil {} {}))

  (assoc [_ k v]

    (doseq [p (yarn-deps v)]
      (when-not (contains? asmap p)
        (throw (ex-info "yarn has unknown dependency" {:knitty/yarn k, :knitty/dependency p}))))

    (let [deps (yarn-deps v)
          all-deps' (assoc all-deps k (apply set/union deps (map all-deps deps)))]

      (when (contains? (all-deps' k) k)
        ;; node depends on itself
        (doseq [d deps]
          (check-no-cycle k d [k] asmap)))

      (Registry.
       (object-array (inc (ji/max-initd)))
       (assoc asmap k v)
       all-deps'))))


(defn create-registry []
  (Registry. nil {} {}))


(defn registry-yankfn
  [^Registry registry
   ^clojure.lang.Keyword kkw
   ^long kid]
  (let [^objects ygets (.-ygets registry)
        y (aget ygets kid)]
    (if (some? y)
      y
      (locking ygets
        (let [y (aget ygets kid)]
          (if (nil? y)
            (let [yarn ((.-asmap registry) kkw)
                  yd (yarn-yankfn yarn)]
              (aset ygets kid yd)
              yd)
            y))))))


(defmacro registry-yankfn' [registry kkw kid]
  `(let [^knitty.impl.Registry r# ~registry]
     (if-some [y# (aget ^"[Ljava.lang.Object;" (.-ygets r#) ~kid)]
       y#
       (registry-yankfn r# ~kkw ~kid))))


(deftype YankCtx
         [^knitty.javaimpl.MDM mdm
          ^knitty.impl.Registry registry
          ^knitty.trace.Tracer tracer])


(defn bind-param-type [ds]
  (let [{:keys [defer lazy yankfn]} (meta ds)]
    (cond
      lazy   :lazy
      defer  :defer
      yankfn :yankfn
      :else  :sync)))


(defmacro ctx-tracer-> [ctx fn & args]
  (when-not t/elide-tracing
   `(when-let [t# (.-tracer ~ctx)] (~fn t# ~@args))))


(defn yarn-get [^YankCtx ctx ^clojure.lang.Keyword ykey]
  (let [kid (ji/keyword->intid ykey)
        v (ji/mdm-get! (.mdm ctx) kid)]
    (if (ji/none? v)
      ((registry-yankfn' (.-registry ctx) ykey kid) ctx)
      v)))


(defmacro yarn-get-sync
  ([yk ykey ctx]
   `(yarn-get-sync ~yk ~ykey ~(ji/keyword->intid ykey) ~ctx))
  ([yk ykey ykeyi ctx]
   `(do
      (ctx-tracer-> ~ctx t/trace-dep ~yk ~ykey)
      (let [v# (ji/mdm-get! (.-mdm ~ctx) ~ykeyi)]
        (if (ji/none? v#)
          ((registry-yankfn' (.-registry ~ctx) ~ykey ~ykeyi) ~ctx)
          v#)))))


(defmacro yarn-get-defer [yk ykey ctx]
  `(do
     (ctx-tracer-> ~ctx t/trace-dep ~yk ~ykey)
     (kd/as-deferred
      (let [v# (ji/mdm-get! (.-mdm ~ctx) ~(ji/keyword->intid ykey))]
        (if (ji/none? v#)
          ((registry-yankfn' (.-registry ~ctx) ~ykey ~(ji/keyword->intid ykey)) ~ctx)
          v#)))))


(deftype Lazy
         [^AtomicReference value
          ^YankCtx ctx
          ^clojure.lang.Keyword yk
          ^clojure.lang.Keyword ykey
          ^long ykeyi]

  clojure.lang.IDeref
  (deref [_]
    (if-let [v (.get value)]
      v
      (let [v (ji/create-kd)]
        (if (.compareAndSet value nil v)
          (do
            (try
              (ctx-tracer-> ctx t/trace-dep yk ykey)
              (kd/connect-to-ka-deferred (let [r (ji/mdm-get! (.-mdm ctx) ykeyi)]
                                           (if (ji/none? r)
                                             ((registry-yankfn' (.-registry ctx) ykey ykeyi) ctx)
                                             r))
                                         v)
              (catch Throwable t (kd/error'! v t)))
            v)
          (.get value)))))

  Object
  (toString [_] (str "#knitty/Lazy[" ykey "]")))


(defmacro yarn-get-lazy [yk ykey ctx]
  `(Lazy.
    (AtomicReference.)
    ~ctx
    ~yk
    ~ykey
    ~(ji/keyword->intid ykey)))


(defn make-yankfn
  [^YankCtx ctx
   yk
   yarns-map]
  (fn yankfn [y]
    (if-let [[i k] (yarns-map y)]
      (do
        (ctx-tracer-> ctx t/trace-dep yk k)
        (kd/as-deferred
         (let [v (ji/mdm-get! (.-mdm ctx) i)]
           (if (ji/none? v)
             ((registry-yankfn' (.-registry ctx) k i) ctx)
             v))))
      (throw (ex-info "Invalid yank-fn arg" {:knitty/yankfn-arg y
                                             :knytty/yankfn-known-args (keys yarns-map)})))))


(defmacro yarn-get-yankfn [yk keys-map ctx]
  (let [args (into {} (map (fn [[k v]] [k [(ji/keyword->intid v) v]])) keys-map)]
    `(make-yankfn ~ctx ~yk ~args)))


(defn force-lazy-result [v]
  (if (instance? Lazy v)
    (kd/unwrap1' @v)
    v))


(defn resolve-executor-var [e]
  (when e
    (when-let [ee (var-get e)]
      (if (ifn? ee) (ee) ee))))


(defn run-future [executor-var thefn]
  (let [d (ji/create-kd)]
    (manifold.utils/future-with
     (resolve-executor-var executor-var)
     (try
       (let [v (thefn)]
         (kd/connect-to-ka-deferred v d))
       (catch Throwable e
         (kd/error'! d e))))
    d))


(defmacro maybe-future-with [executor-var & body]
  (if-not executor-var
    `(do ~@body)
    `(run-future ~executor-var (fn [] ~@body))))


(deftype TraceListener [^YankCtx ctx, ykey]
  manifold.deferred.IDeferredListener
  (onSuccess [_ x]
    (ctx-tracer-> ctx t/trace-finish ykey x nil true))
  (onError [_ x]
    (ctx-tracer-> ctx t/trace-finish ykey nil x true)))


(defmacro connect-result-mdm [ctx ykey result mdm-deferred]
  `(if (md/deferred? ~result)
     (do
       (kd/listen! ~result ~mdm-deferred)
       (when (.-tracer ~ctx)
         (kd/listen! ~mdm-deferred (TraceListener. ~ctx ~ykey)))
       ~mdm-deferred)
     (do
       (ctx-tracer-> ~ctx t/trace-finish ~ykey ~result nil false)
       (kd/success'! ~mdm-deferred ~result)
       (ji/mdm-put! (.-mdm ~ctx) ~(ji/keyword->intid ykey) ~result)
       ~result)))


(defmacro connect-error-mdm [ctx ykey error mdm-deferred]
  `(do
    (ctx-tracer-> ~ctx t/trace-finish ~ykey nil ~error false)
    (kd/error'! ~mdm-deferred ~error)
    ~mdm-deferred))


(defn emit-yank-fns-impl
  [the-fnv ykey bind deps yarn-meta]
  (let [{:keys [executor norevoke]} yarn-meta

        ctx (with-meta '_yank_ctx {:tag (str `YankCtx)})
        kad (with-meta '_yank_kad {:tag (str `manifold.deferred.IMutableDeferred)})
        kid (ji/keyword->intid ykey)
        fnn #(-> ykey name (str %) symbol)

        yank-deps
        (mapcat identity
                (for [[ds dk] bind]
                  [ds
                   (case (bind-param-type ds)
                     :sync   `(yarn-get-sync   ~ykey ~dk ~ctx)
                     :defer  `(yarn-get-defer  ~ykey ~dk ~ctx)
                     :lazy   `(yarn-get-lazy   ~ykey ~dk ~ctx)
                     :yankfn `(yarn-get-yankfn ~ykey ~dk ~ctx))]))

        sync-deps
        (for [[ds _dk] bind
              :when (#{:sync} (bind-param-type ds))]
          ds)

        param-types (set (for [[ds _dk] bind] (bind-param-type ds)))

        coerce-deferred (if (param-types :lazy)
                          `force-lazy-result
                          `do)

        some-syncs-unresolved (list* `or (for [d sync-deps] `(md/deferred? ~d)))

        deref-syncs
        (mapcat identity
                (for [[ds _dk] bind
                      :when (#{:sync} (bind-param-type ds))]
                  [ds `(md/unwrap' ~ds)]))

        fn-args (if (> (count deps) 18)
                  (let [[a b] (split-at 18 deps)]
                    (conj (vec a) (vec b)))
                  deps)

        revoke (if norevoke [`comment] [`ji/kd-set-revokee kad])

        all-deps-tr (into
                     []
                     (comp cat (distinct))
                     (for [[ds dk] bind
                           :let [pt (bind-param-type ds)]]
                       (if (= :yankfn pt)
                         (for [[_ k] dk] [k :yankfn])
                         [[dk pt]])))]

    `(fn ~(fnn "--yank") [~ctx]
       (let [kv# (ji/mdm-fetch! (.-mdm ~ctx) ~kid)
             d# (ji/fetch-result-value kv#)]
         (if-not (ji/fetch-result-claimed? kv#)

           ;; got item from mdm
           (kd/unwrap1' d#)

           ;; calculate & provide new value to mdm
           (maybe-future-with
            ~executor

            (ctx-tracer-> ~ctx t/trace-start  ~ykey :yarn ~all-deps-tr)
            (let [~kad d#]
              (try ;; d# is alsways deffered
                (let [~@yank-deps]
                  (if ~some-syncs-unresolved
                    (do
                      (kd/await-ary*

                       (reify
                         manifold.deferred.IDeferredListener
                         (onSuccess
                           [_# _#]
                           (let [x# (let [~@deref-syncs]
                                        (ctx-tracer-> ~ctx t/trace-call ~ykey)
                                        (~coerce-deferred (~the-fnv ~@fn-args)))]
                             (~@revoke x#)
                             (connect-result-mdm ~ctx ~ykey x# ~kad)))
                         (onError
                           [_ e#]
                           (connect-error-mdm ~ctx ~ykey e# ~kad)))

                       ~@sync-deps)
                      ~kad)

                    (let [x# (do
                               (ctx-tracer-> ~ctx t/trace-call ~ykey)
                               (~coerce-deferred (~the-fnv ~@fn-args)))]
                      (~@revoke x#)
                      (connect-result-mdm ~ctx ~ykey x# ~kad))))

                (catch Throwable e#
                  (connect-error-mdm ~ctx ~ykey e# ~kad))))))))))


(defn emit-yank-fns [thefn ykey bind]
  (let [yarn-meta (meta bind)
        deps (map first bind)
        {:keys [keep-deps-order]} yarn-meta
        bind (if keep-deps-order
               bind
               (sort-by (comp #(when (keyword? %) (ji/keyword->intid %)) second) bind))]
    (emit-yank-fns-impl thefn ykey bind deps yarn-meta)))


(defn emit-yarn-ref-gtr [ykey orig-ykey]
  (let [kid (ji/keyword->intid ykey)]
    `(fn [^YankCtx ctx#]
       (let [kv# (ji/mdm-fetch! (.-mdm ctx#) ~kid)
             d# (ji/fetch-result-value kv#)]
         (if-not (ji/fetch-result-claimed? kv#)
           (kd/unwrap1' d#)
           (do
             (ctx-tracer-> ctx# t/trace-start ~ykey :knot [[~orig-ykey :ref]])
             (try
               (ctx-tracer-> ctx# t/trace-call ~ykey)
               (let [x# (yarn-get-sync ~ykey ~orig-ykey ctx#)]
                 (connect-result-mdm ctx# ~ykey x# d#))
               (catch Throwable e#
                 (connect-error-mdm ctx# ~ykey e# d#)))))))))


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
  (let [deps (grab-yarn-bindmap-deps bind)
        ff (gensym)
        fargs (if (> (count bind) 18)
                (let [[args1 args2] (split-at 18 (map first bind))]
                  (vec (conj (vec args1) (vec args2))))
                (mapv first bind))]
    `(let [~ff (fn ~(-> ykey name symbol) [~@fargs] ~expr)
           gtr# ~(emit-yank-fns ff ykey bind)]
       (Yarn. gtr# ~ykey '~deps))))


(defn gen-yarn-ref
  [ykey from]
  `(let [d# ~(emit-yarn-ref-gtr ykey from)]
     (Yarn. d# ~ykey #{~from})))


(defn yarn-multifn [yarn-name]
  (cond
    (symbol? yarn-name) `(yarn-multifn ~(eval yarn-name))
    (keyword? yarn-name) (symbol (namespace yarn-name) (str (name yarn-name) "--multifn"))
    :else (throw (ex-info "invalid yarn-name" {:yarn-name yarn-name}))))


(deftype YarnMulti [^clojure.lang.MultiFn multifn key route-key]

  IYarn
  (yarn-yankfn [_] multifn)
  (yarn-deps [_]
    (into #{route-key}
          (comp (map val)
                (keep #(%))
                (mapcat yarn-deps))
          (methods multifn)))
  (yarn-key [_] key)

  IYarnMulti
  (yarn-multifn [_] multifn)
  )


(defn make-multiyarn-route-key-fn [ykey k]
  (let [i (long (ji/keyword->intid k))]
    (fn yank-route-key [^YankCtx ctx]
      (ctx-tracer-> ctx t/trace-route-by ykey k)
      (let [d (yarn-get-sync ykey k i ctx)]
        (if (md/deferred? d)
          ::deferred
          d)))))


(defn gen-yarn-multi
  [ykey route-key mult-options]
  `(do

     (defmulti ~(yarn-multifn ykey)
       (make-multiyarn-route-key-fn ~ykey ~route-key)
       ~@mult-options)

     (defmethod ~(yarn-multifn ykey) ::deferred
       ([] nil)
       ([^YankCtx ctx#]
        (md/chain' (yarn-get-sync ~ykey ~route-key ctx#)
                   (fn ~(symbol (str (name ykey) '--reyank)) [_#]
                     ((var ~(yarn-multifn ykey)) ctx#)) ;; call again
                   )))

     (YarnMulti. ~(yarn-multifn ykey) ~ykey ~route-key)))


(defn gen-reg-yarn-method
  [yk yarn route-val]
  (let [y (gensym)]
    `(let [~y ~yarn
           rv# ~route-val
           yf# (yarn-yankfn ~y)]
       (defmethod
         ~(yarn-multifn yk)
         rv#
         ([] ~y)
         ([ctx#]
          (yf# ctx#))))))


(defn fail-always-yarn [ykey msg]
  (Yarn. (fn [_] (throw (java.lang.UnsupportedOperationException. (str msg)))) ykey #{}))



(defn yank0
  [poy yarns ^Registry registry tracer]
  (let [mdm (ji/create-mdm poy)
        ctx (YankCtx. mdm registry tracer)
        errh (fn [e]
               (ex-info "failed to yank"
                        (cond->
                         (assoc (dissoc (ex-data e) ::inyank)
                                :knitty/yanked-poy poy
                                :knitty/failed-poy (ji/mdm-freeze! mdm)
                                :knitty/yanked-yarns yarns)
                          tracer (assoc
                                  :knitty/trace
                                  (conj
                                   (-> poy meta :knitty/trace)
                                   (capture-trace! tracer))))
                        e))
        n (count yarns)
        yks (java.util.ArrayList. n)]

    (try
      (doseq [y yarns]
        (.add yks
              (if (keyword? y)
                (yarn-get ctx y)
                (do
                  (when (contains? registry (yarn-key y))
                    (throw (ex-info "dynamic yarn is already in registry"
                                    {:knitty/yarn (yarn-key y)})))
                  ((yarn-yankfn y) ctx)))))
      (let [r (ji/create-kd)]
        (kd/await-all!
         (reify manifold.deferred.IDeferredListener
           (onSuccess [_ _]
             (kd/success'! r
                           (try
                             (let [poy' (ji/mdm-freeze! mdm)]
                               (if tracer
                                 (vary-meta poy' update :knitty/trace conj (capture-trace! tracer))
                                 poy'))
                             (catch Throwable e
                               (kd/error'! r (errh e))))))
           (onError [_ e]
             (kd/error'! r (errh e))))
         (.toArray yks))
        (kd/revoke' r (fn cancel-mdm [] (ji/mdm-cancel! mdm))))
      (catch Throwable e
        (md/error-deferred (errh e))))))
