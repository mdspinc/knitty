(ns knitty.impl
  (:require [knitty.javaimpl :as ji]
            [knitty.trace :as t]
            [clojure.set :as set]
            [manifold.deferred :as md]
            [manifold.executor]
            [manifold.utils]))


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
          ^knitty.trace.Tracer tracer
          ^java.lang.Object token])


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


(defn yarn-get [^YankCtx ctx ^clojure.lang.Keyword ykey]
  (let [kid (ji/keyword->intid ykey)
        v (ji/mdm-get! (.mdm ctx) ykey kid)]
    (if v
      v
      ((registry-yankfn' (.-registry ctx) ykey kid) ctx))))


(defmacro yarn-get-impl
  ([yk ykey ctx]
   `(yarn-get-impl ~yk ~ykey ~(ji/keyword->intid ykey) ~ctx))
  ([yk ykey ykeyi ctx]
   `(do
      (ctx-tracer-> ~ctx t/trace-dep ~yk ~ykey)
      (let [v# (ji/mdm-get! (.-mdm ~ctx) ~ykey ~ykeyi)]
        (if v#
          v#
          ((registry-yankfn' (.-registry ~ctx) ~ykey ~ykeyi) ~ctx))))))


(deftype Lazy
         [^knitty.javaimpl.KDeferred value
          ^YankCtx ctx
          ^clojure.lang.Keyword yk
          ^clojure.lang.Keyword ykey
          ^long ykeyi]

  clojure.lang.IDeref
  (deref [_]
    (when (ji/kd-claim value (.-token ctx))
      (try
        (ctx-tracer-> ctx t/trace-dep yk ykey)
        (ji/kd-chain-from
         value
         (let [r (ji/mdm-get! (.-mdm ctx) yk ykeyi)]
           (if r
             r
             ((registry-yankfn' (.-registry ctx) ykey ykeyi) ctx)))
         (.-token ctx))
        (catch Throwable t (ji/kd-error! value t (.-token ctx)))))
    value)

  Object
  (toString [_] (str "#knitty/Lazy[" ykey "]")))


(defmacro yarn-get-lazy [yk ykey ctx]
  `(Lazy.
    (ji/kd-create)
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
        (let [v (ji/mdm-get! (.-mdm ctx) yk i)]
          (if v
            v
            ((registry-yankfn' (.-registry ctx) k i) ctx))))
      (throw (ex-info "Invalid yank-fn arg" {:knitty/yankfn-arg y
                                             :knytty/yankfn-known-args (keys yarns-map)})))))


(defmacro yarn-get-yankfn [yk keys-map ctx]
  (let [args (into {} (map (fn [[k v]] [k [(ji/keyword->intid v) v]])) keys-map)]
    `(make-yankfn ~ctx ~yk ~args)))


(defn force-lazy-result [v]
  (if (instance? Lazy v)
    (md/unwrap' @v)
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


(defn emit-yank-fns-impl
  [the-fn-body ykey bind yarn-meta]
  (let [{:keys [executor norevoke]} yarn-meta

        ctx (with-meta '_yank_ctx {:tag (str `YankCtx)})
        kid (ji/keyword->intid ykey)

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

        revoke (if norevoke `comment `ji/kd-set-revokee)

        all-deps-tr (into
                     []
                     (comp cat (distinct))
                     (for [[ds dk] bind
                           :let [pt (bind-param-type ds)]]
                       (if (= :yankfn pt)
                         (for [[_ k] dk] [k :yankfn])
                         [[dk pt]])))]

    `(fn ~(-> ykey name (str '--yank) symbol) [~ctx]
       (let [d# (ji/mdm-fetch! (.-mdm ~ctx) ~ykey ~kid)]
         (when (ji/kd-claim d# (.-token ~ctx))
           (maybe-future-with
            ~executor
            (ctx-tracer-> ~ctx t/trace-start  ~ykey :yarn ~all-deps-tr)
            (try
              (let [~@yank-deps]
                (ji/kd-await
                 (reify
                   manifold.deferred.IDeferredListener
                   (onSuccess
                     [_# _#]
                     (let [z# (let [~@deref-syncs]
                                (ctx-tracer-> ~ctx t/trace-call ~ykey)
                                (~coerce-deferred ~the-fn-body))]
                       (~revoke d# z#)
                       (connect-result-mdm ~ctx ~ykey z# d#)))
                   (onError
                     [_ e#]
                     (connect-error-mdm ~ctx ~ykey e# d#)))
                 ~@sync-deps))
              (catch Throwable e#
                (connect-error-mdm ~ctx ~ykey e# d#)))))
         d#))))


(defn emit-yank-fns [thefn-body ykey bind]
  (let [yarn-meta (meta bind)
        {:keys [keep-deps-order]} yarn-meta
        bind (if keep-deps-order
               bind
               (sort-by (comp #(when (keyword? %) (ji/keyword->intid %)) second) bind))]
    (emit-yank-fns-impl thefn-body ykey bind yarn-meta)))


(defn emit-yarn-ref-gtr [ykey orig-ykey]
  (let [kid (ji/keyword->intid ykey)]
    `(fn ~(-> ykey name (str '--ref) symbol) [^YankCtx ctx#]
       (let [d# (ji/mdm-fetch! (.-mdm ctx#) ~ykey ~kid)]
         (if-not (ji/kd-claim d# (.-token ctx#))
           (ji/kd-unwrap d#)
           (do
             (ctx-tracer-> ctx# t/trace-start ~ykey :knot [[~orig-ykey :ref]])
             (try
               (let [x# (ji/kd-unwrap (yarn-get-impl ~ykey ~orig-ykey ctx#))]
                 ;; (ctx-tracer-> ctx# t/trace-call ~ykey)
                 (connect-result-mdm ctx# ~ykey x# d#))
               (catch Throwable e#
                 (connect-error-mdm ctx# ~ykey e# d#)))
             d#))))))


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
  (let [deps (grab-yarn-bindmap-deps bind)]
    `(let [gtr# ~(emit-yank-fns expr ykey bind)]
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
  (yarn-multifn [_] multifn))


(defn make-multiyarn-route-key-fn [ykey k]
  (let [i (long (ji/keyword->intid k))]
    (fn yank-route-key [^YankCtx ctx]
      (ctx-tracer-> ctx t/trace-route-by ykey k)
      (let [d (ji/kd-unwrap (yarn-get-impl ykey k i ctx))]
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
        (md/chain' (yarn-get-impl ~ykey ~route-key ctx#)
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


(defn emit-yarn-input-gtr [ykey]
  (let [kid (ji/keyword->intid ykey)]
    `(fn ~(-> ykey name (str "--yget") symbol) [^YankCtx ctx#]
       (let [d# (ji/mdm-fetch! (.-mdm ctx#) ~ykey ~kid)]
         (if (ji/kd-claim d# (.-token ctx#))
           (connect-error-mdm ctx# ~ykey (java.lang.UnsupportedOperationException. ~(str "input-only yarn " ykey)) d#)
           d#)))))


(defn gen-yarn-input
  [ykey]
  `(let [d# ~(emit-yarn-input-gtr ykey)]
     (Yarn. d# ~ykey #{})))


(defn yank0
  [poy yarns registry tracer]
  (let [token (Object.)
        mdm (ji/create-mdm poy)
        result (ji/kd-create token)
        ctx (YankCtx. mdm registry tracer token)
        yss (java.util.ArrayList. 16)]

    (doseq [y yarns]
      (.add
       yss
       (if (keyword? y)
         (yarn-get ctx y)
         (do
           (when (contains? registry (yarn-key y))
             (throw (ex-info "dynamic yarn is already in registry"
                             {:knitty/yarn (yarn-key y)})))
           ((yarn-yankfn y) ctx)))))

    (ji/kd-await-coll
     (reify manifold.deferred.IDeferredListener
       (onSuccess [_ _]
         (ji/kd-success! result
                         (let [poy' (ji/mdm-freeze! mdm)]
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
                                        :knitty/failed-poy (ji/mdm-freeze! mdm)
                                        :knitty/yanked-yarns yarns)
                                  tracer (assoc
                                          :knitty/trace
                                          (conj
                                           (-> poy meta :knitty/trace)
                                           (t/capture-trace! tracer))))
                                e)
                       token)))
     yss)

    (ji/kd-revoke result
                  (fn cancel-mdm [] (ji/mdm-cancel! mdm token)))))
