(ns ag.knitty.impl
  (:require [ag.knitty.deferred :as kd]
            [ag.knitty.mdm :as mdm]
            [ag.knitty.trace :as t :refer [capture-trace!]]
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
       (object-array (inc (mdm/max-initd)))
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
  `(let [^ag.knitty.impl.Registry r# ~registry]
     (if-some [y# (aget ^"[Ljava.lang.Object;" (.-ygets r#) ~kid)]
       y#
       (registry-yankfn r# ~kkw ~kid))))


(deftype YankCtx
         [^ag.knitty.mdm.IMutableDeferredMap mdm
          ^ag.knitty.impl.Registry registry
          ^ag.knitty.trace.Tracer tracer])


(defn bind-param-type [ds]
  (let [{:keys [defer lazy]} (meta ds)]
    (cond
      lazy  :lazy
      defer :defer
      :else :sync)))


(defmacro get-yank-fn [ctx ykey]
  (let [k (mdm/keyword->intid ykey)]
    `(registry-yankfn' (.-registry ~ctx) ~ykey ~k)))


(defmacro ctx-tracer-> [ctx fn & args]
  `(when-let [t# (.-tracer ~ctx)] (~fn t# ~@args)))


(defn yarn-get [^YankCtx ctx ^clojure.lang.Keyword ykey]
  (let [kid (mdm/keyword->intid ykey)
        v (mdm/mdm-get! (.mdm ctx) ykey kid)]
    (if (mdm/none? v)
      ((registry-yankfn' (.-registry ctx) ykey kid) ctx)
      v)))


(defmacro yarn-get-sync [yk ykey ctx]
  `(do
     (ctx-tracer-> ~ctx t/trace-dep ~yk ~ykey)
     (let [v# (mdm/mdm-get! (.-mdm ~ctx) ~ykey ~(mdm/keyword->intid ykey))]
       (if (mdm/none? v#)
         ((get-yank-fn ~ctx ~ykey) ~ctx)
         v#)
      )))


(defmacro yarn-get-defer [yk ykey ctx]
  `(do
     (ctx-tracer-> ~ctx t/trace-dep ~yk ~ykey)
     (kd/as-deferred
      (let [v# (mdm/mdm-get! (.-mdm ~ctx) ~ykey ~(mdm/keyword->intid ykey))]
        (if (mdm/none? v#)
          ((get-yank-fn ~ctx ~ykey) ~ctx)
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
      (let [v (kd/ka-deferred)]
        (if (.compareAndSet value nil v)
          (do
            (try
              (ctx-tracer-> ctx t/trace-dep yk ykey)
              (kd/connect-to-ka-deferred (let [r (mdm/mdm-get! (.-mdm ctx) ykey ykeyi)]
                                           (if (mdm/none? r)
                                             ((registry-yankfn' (.-registry ctx) ykey ykeyi) ctx)
                                             r))
                            v)
              (catch Throwable t (kd/error'! v t)))
            v)
          (.get value)))))
  
  Object
  (toString [_] (str "#ag.knitty/Lazy[" ykey "]"))
  )


(defmacro yarn-get-lazy [yk ykey ctx]
  `(Lazy.
    (AtomicReference.)
    ~ctx
    ~yk
    ~ykey
    ~(mdm/keyword->intid ykey)))


(defn force-lazy-result [v]
  (if (instance? Lazy v)
    (kd/unwrap1' @v)
    v))


(defn resolve-executor-var [e]
  (when e
    (when-let [ee (var-get e)]
      (if (ifn? ee) (ee) ee))))


(defn run-future [executor-var thefn]
  (let [d (kd/ka-deferred)]
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


(deftype RevokationListener [^clojure.lang.Volatile dref]
  manifold.deferred.IDeferredListener
  (onSuccess [_ x]
    (let [d @dref]
      (when (instance? manifold.deferred.IMutableDeferred d)
        (kd/success'! d x))))
  (onError [_ e]
    (let [d @dref]
      (when (instance? manifold.deferred.IMutableDeferred d)
        (kd/error'! d e)))))


(deftype ConnectListener [^YankCtx ctx
                          ykey
                          ^manifold.deferred.IMutableDeferred mdmd]
  manifold.deferred.IDeferredListener
  (onSuccess [_ x]
    (ctx-tracer-> ctx t/trace-finish ykey x nil true)
    (kd/success'! mdmd x))
  (onError [_ x]
    (let [ew (wrap-yarn-exception ykey x)]
      (ctx-tracer-> ctx t/trace-finish ykey nil ew true)
      (kd/error'! mdmd ew))))


(defn connect-result-mdm [^YankCtx ctx ykey result mdm-deferred maybe-real-result]

  (if (md/deferred? result)
    (do

      ;; revokation mdm-deferred -> result
      (when maybe-real-result
        (let [mrr @maybe-real-result]
          (if (identical? ::none mrr)
            ;; not yet ready
            (kd/listen! mdm-deferred (RevokationListener. maybe-real-result))
            ;; ready - revoke only if result if revokable deferred
            (when (instance? manifold.deferred.IMutableDeferred mrr)
              (kd/connect-two-deferreds mdm-deferred mrr)))))

      ;; connect result -> mdm-deferred
      (kd/listen! result (ConnectListener. ctx ykey mdm-deferred))

      mdm-deferred)

    (do
      ;; no revokation for sync results
      (ctx-tracer-> ctx t/trace-finish ykey result nil false)
      (kd/success'! mdm-deferred result)
      result)))


(defn connect-error-mdm [^YankCtx ctx ykey error mdm-deferred maybe-real-result]
  (let [ew (wrap-yarn-exception ykey error)]

    ;; try to revoke
    (when maybe-real-result
      (when-let [d @maybe-real-result]
        (when (instance? manifold.deferred.IMutableDeferred d)
          (kd/error'! d error))))

    (ctx-tracer-> ctx t/trace-finish ykey nil ew false)

    (kd/error'! mdm-deferred ew)
    mdm-deferred))


(defn emit-yank-fns-impl
  [the-fnv ykey bind deps yarn-meta]
  (let [{:keys [executor norevoke]} yarn-meta

        ctx (with-meta '_yank_ctx {:tag (str `YankCtx)})
        reald  (gensym "rd")
        df-array (with-meta (gensym "dfs") {:tag "[Ljava.lang.Object;"})
        kid (mdm/keyword->intid ykey)
        fnn #(-> ykey name (str %) symbol)

        yank-deps
        (mapcat identity
                (for [[ds dk] bind]
                  [ds
                   (case (bind-param-type ds)
                     :sync   `(yarn-get-sync  ~ykey ~dk ~ctx)
                     :defer  `(yarn-get-defer ~ykey ~dk ~ctx)
                     :lazy   `(yarn-get-lazy  ~ykey ~dk ~ctx))]))

        sync-deps
        (for [[ds _dk] bind
              :when (#{:sync} (bind-param-type ds))]
          ds)

        param-types (set (for [[ds _dk] bind] (bind-param-type ds)))

        coerce-deferred (if (param-types :lazy)
                          force-lazy-result
                          'do)

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

        all-deps-tr (vec (for [[ds dk] bind] [dk (bind-param-type ds)]))]

    `(fn ~(fnn "--yank") [~ctx]
       (let [kv# (mdm/mdm-fetch! (.-mdm ~ctx) ~ykey ~kid)
             d# (mdm/fetch-result-value kv#)]
         (if-not (mdm/fetch-result-claimed? kv#)

           ;; got item from mdm
           (kd/unwrap1' d#)

           ;; calculate & provide new value to mdm
           (maybe-future-with
            ~executor

            (ctx-tracer-> ~ctx t/trace-start  ~ykey :yarn ~all-deps-tr)
            (let [~@(if norevoke [] [reald `(volatile! ::none)])]
              (try ;; d# is alsways deffered
                (let [~@yank-deps]
                  (let [x# (if ~some-syncs-unresolved

                             (kd/unwrap1'
                              (kd/await*

                               (let [~df-array (object-array ~(count sync-deps))]
                                 ~@(for [[i d] (map vector (range) sync-deps)]
                                     `(aset ~df-array ~i ~d))
                                 ~df-array)

                               (fn ~(fnn "--async") []
                                 (let [~@deref-syncs]
                                   (~@(if norevoke `[do] [`vreset! reald])
                                    (do
                                      (ctx-tracer-> ~ctx t/trace-call ~ykey)
                                      (~coerce-deferred (~the-fnv ~@fn-args))))))))

                             (~@(if norevoke `[do] [`vreset! reald])
                              (do
                                (ctx-tracer-> ~ctx t/trace-call ~ykey)
                                (~coerce-deferred (~the-fnv ~@fn-args)))))]

                    (connect-result-mdm ~ctx ~ykey x# d# ~reald)))
                (catch Throwable e#
                  (connect-error-mdm ~ctx ~ykey e# d# ~reald))))))))))


(defn emit-yank-fns [thefn ykey bind]
  (let [yarn-meta (meta bind)
        deps (map first bind)
        {:keys [keep-deps-order]} yarn-meta
        bind (if keep-deps-order
               bind
               (sort-by (comp mdm/keyword->intid second) bind))]
    (emit-yank-fns-impl thefn ykey bind deps yarn-meta)))


(defn emit-yarn-ref-gtr [ykey orig-ykey]
  (let [kid (mdm/keyword->intid ykey)]
    `(fn [ctx#]
       (let [kv# (mdm/mdm-fetch! (.-mdm ctx#) ~ykey ~kid)
             d# (mdm/fetch-result-value kv#)]
         (if-not (mdm/fetch-result-claimed? kv#)
           (kd/unwrap1' d#)
           (do
             (ctx-tracer-> ctx# t/trace-start ~ykey :knot [[~orig-ykey :ref]])
             (try
               (ctx-tracer-> ctx# t/trace-call ~ykey)
               (let [x# (yarn-get-sync ~ykey ~orig-ykey ctx#)]
                 (connect-result-mdm ctx# ~ykey x# d# nil))
               (catch Throwable e#
                 (connect-error-mdm ctx# ~ykey e# d# nil)))))))))

(defn gen-yarn
  [ykey bind expr]
  (let [deps (set (map second bind))
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


(defn fail-always-yarn [ykey msg]
  (Yarn. (fn [_] (throw (java.lang.UnsupportedOperationException. (str msg)))) ykey #{}))


(defn yank0
  [poy yarns ^Registry registry tracer]
  (let [mdm (mdm/create-mdm poy)
        ctx (YankCtx. mdm registry tracer)
        errh (fn [e]
               (throw (ex-info "failed to yank"
                               (cond->
                                (assoc (dissoc (ex-data e) ::inyank)
                                       :knitty/yanked-poy poy
                                       :knitty/failed-poy (mdm/mdm-freeze! mdm)
                                       :knitty/yanked-yarns yarns)
                                 tracer (assoc
                                         :knitty/trace
                                         (conj
                                          (-> poy meta :knitty/trace)
                                          (capture-trace! tracer))))
                               e)))
        n (count yarns)
        yks (java.util.ArrayList. n)
        ]

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
      (->
       (kd/await* (.toArray yks)
                   (fn fisnih-yarn []
                     (let [poy' (mdm/mdm-freeze! mdm)]
                       (if tracer
                         (vary-meta poy' update :knitty/trace conj (capture-trace! tracer))
                         poy'))))
       (md/catch' errh)
       (kd/revoke' (fn cancel-mdm [] (mdm/mdm-cancel! mdm))))
      (catch Throwable e
        (errh e)))))
