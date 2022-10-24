(ns ag.knitty.impl
  (:require [ag.knitty.deferred :as kd]
            [ag.knitty.mdm :as mdm :refer [create-mdm mdm-cancel! mdm-fetch! mdm-freeze! mdm-get!]]
            [ag.knitty.trace :as t :refer [capture-trace!]]
            [clojure.string :as str]
            [com.stuartsierra.dependency :as dependency]
            [manifold.deferred :as md]
            [manifold.executor]
            [manifold.utils])
  (:import [ag.knitty.mdm FetchResult MutableDeferredMap]))


(defprotocol IYarn
  (yarn-gtr [_] "get value or obligated deferred")
  (yarn-deps [_] "get dependencies")
  (yarn-key* [_] "get yarn ykey"))


(defprotocol IYankCtx
  (get-registry [_])
  (cancelled? [_])
  )


(deftype YankCtx [^MutableDeferredMap mdm
                  registry
                  ^:volatile-mutable ^boolean cancelled]

  MutableDeferredMap
  (mdmFetch [_ k i] (.mdmFetch mdm k i))
  (mdmGet [_ k i] (.mdmGet mdm k i))
  (mdmFreeze [_] (.mdmFreeze mdm))
  (mdmCancel [_] (set! cancelled (boolean true)) (.mdmCancel mdm))

  IYankCtx
  (cancelled? [_] cancelled)
  (get-registry [_] registry)
  )


(deftype Yarn [ykey deps yget]
  IYarn
  (yarn-gtr [_] yget)
  (yarn-key* [_] ykey)
  (yarn-deps [_] deps))


(defn yarn-key [k]
  (if (keyword? k)
    k
    (yarn-key* k)))


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


(defn unns-keyword [k]
  (keyword nil (str (namespace k) "/" (name k))))


(defn ns-keyword [k]
  (if (namespace k)
    k
    (apply keyword (str/split (name k) #"/" 2))))


(defmacro get-yank-fn [ctx ykey]
  `(yarn-gtr (~ykey (get-registry ~ctx))))


(defmacro yarn-get-sync [yk ykey ctx tracer]
  `(do
     (when ~tracer (t/trace-dep ~tracer ~yk ~ykey))
     (or
      (mdm-get! ~ctx ~ykey ~(mdm/keyword->intid ykey))
      ((get-yank-fn ~ctx ~ykey) ~ctx ~tracer))))


(defmacro yarn-get-defer [yk  ykey ctx tracer]
  `(do
     (when ~tracer (t/trace-dep ~tracer ~yk ~ykey))
     (or
      (mdm-get! ~ctx ~ykey ~(mdm/keyword->intid ykey))
      (as-deferred
       ((get-yank-fn ~ctx ~ykey) ~ctx ~tracer)))))


(defmacro yarn-get-lazy [yk ykey ctx tracer]
  `(delay
     (when ~tracer (t/trace-dep ~tracer ~yk ~ykey))
     (or
      (mdm-get! ~ctx ~ykey ~(mdm/keyword->intid ykey))
      (as-deferred
       ((get-yank-fn ~ctx ~ykey) ~ctx ~tracer)))))


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
         (kd/connect'' v d))
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


(deftype TracedConnectListener [tracer ykey mdmd]
  manifold.deferred.IDeferredListener
  (onSuccess [_ x]
    (when tracer (t/trace-finish tracer ykey x nil true))
    (kd/success'! mdmd x))
  (onError [_ x]
    (let [ew (wrap-yarn-exception ykey x)]
      (when tracer (t/trace-finish tracer ykey nil ew true))
      (kd/error'! mdmd ew))))


(defn connect-result-mdm [ykey result mdm-deferred tracer maybe-real-result]

  (if (md/deferred? result)
    (do

      ;; revokation mdm-deferred -> result
      (when maybe-real-result
        (let [mrr @maybe-real-result]
          (if (identical? ::none mrr)
            ;; not yet ready
            (kd/listen! mdm-deferred (RevokationListener. maybe-real-result))
            ;; ready - revoke only if it is IMutableDeferred
            (when (instance? manifold.deferred.IMutableDeferred mrr)
              (kd/connect'' mdm-deferred mrr)))))

      ;; connect result -> mdm-deferred
      (if tracer
        (kd/listen! result (TracedConnectListener. tracer ykey mdm-deferred))
        (kd/connect'' result mdm-deferred))

      mdm-deferred)

    (do
      (when tracer (t/trace-finish tracer ykey result nil false))
      (kd/success'! mdm-deferred result)
      result)))


(defn connect-error-mdm [ykey error mdm-deferred tracer maybe-real-result]
  (let [ew (wrap-yarn-exception ykey error)]

    ;; try to revoke
    (when maybe-real-result
      (when-let [d @maybe-real-result]
        (when (instance? manifold.deferred.IMutableDeferred d)
          (kd/error'! d error))))

    (when tracer (t/trace-finish tracer ykey nil ew false))

    (kd/error'! mdm-deferred ew)
    mdm-deferred))


(defn- build-yank-fns
  [ykey bind expr]
  (let [ctx (with-meta '_yank_ctx {:tag (str `YankCtx)})
        tracer '_yank_tracer
        the-fnv (gensym "fn")
        reald  (gensym "real_deferred")
        df-array (with-meta (gensym "arr_of_deferreds") {:tag "[Ljava.lang.Object;"})
        kid (mdm/keyword->intid ykey)
        fnn #(-> ykey name (str %) symbol)

        bind (if (:no-reorder-deps (meta bind) false)
               bind
               (sort-by #(mdm/keyword->intid (second %)) bind))

        yank-deps
        (mapcat identity
                (for [[ds dk] bind]
                  [ds
                   (case (bind-param-type ds)
                     :sync   `(yarn-get-sync  ~ykey ~dk ~ctx ~tracer)
                     :defer  `(yarn-get-defer ~ykey ~dk ~ctx ~tracer)
                     :lazy   `(yarn-get-lazy  ~ykey ~dk ~ctx ~tracer))]))

        sync-deps
        (for [[ds _dk] bind
              :when (#{:sync} (bind-param-type ds))]
          ds)

        some-syncs-unresolved (list* `or (for [d sync-deps] `(md/deferred? ~d)))

        try-deref-syncs
        (mapcat identity
                (for [[ds _dk] bind
                      :when (#{:sync} (bind-param-type ds))]
                  [ds `(kd/unwrap1' ~ds)]))

        deref-syncs
        (mapcat identity
                (for [[ds _dk] bind
                      :when (#{:sync} (bind-param-type ds))]
                  [ds `(md/unwrap' ~ds)]))


        deps (keys bind)
        [deps1 deps2] (split-at 16 deps)
        all-deps-tr (vec (for [[ds dk] bind] [dk (bind-param-type ds)]))
        executor-var (:executor (meta bind))]

    `(let [;; input - vector. unwraps 'defers', called by yget-fn#
           ~the-fnv
           (fn ~(fnn "--body")
             ([~ctx ~tracer ~@deps1 ~@(when (seq deps2) ['& (vec deps2)])]
              (when-not (cancelled? ~ctx)
                (when ~tracer (t/trace-call ~tracer ~ykey))
                (coerce-deferred ~expr))))

           yget-fn#
           (fn ~(fnn "--yank") [~ctx ~tracer]
             (let [kv# ^FetchResult (mdm-fetch! ~ctx ~ykey ~kid)
                   d# (.mdmResult kv#)]
               (if-not (.mdmClaimed kv#)

                 ;; got item from mdm
                 (kd/unwrap1' d#)

                 ;; calculate & provide new value to mdm
                 (maybe-future-with
                  ~executor-var

                  (when ~tracer (t/trace-start ~tracer ~ykey :yarn ~all-deps-tr))
                  (let [~reald (volatile! ::none)]
                    (try ;; d# is alsways deffered
                      (let [~@yank-deps
                            ~@try-deref-syncs]
                      (let [x# (if ~some-syncs-unresolved
                                   (kd/await*
                                    (let [~df-array (object-array ~(count sync-deps))]
                                      ~@(for [[i d] (map vector (range) (reverse sync-deps))]
                                          `(aset ~df-array ~i ~d))
                                      ~df-array)
                                    (fn ~(fnn "--async") []
                                      (let [~@deref-syncs]
                                        (vreset! ~reald (~the-fnv ~ctx ~tracer ~@deps)))))
                                   (vreset! ~reald (~the-fnv ~ctx ~tracer ~@deps)))]
                          (connect-result-mdm ~ykey x# d# ~tracer ~reald)))
                      (catch Throwable e#
                        (connect-error-mdm ~ykey e# d# ~tracer ~reald))))))))]

       yget-fn#)))


(defn build-yarn-ref-gtr [ykey orig-ykey]
  (let [kid (mdm/keyword->intid ykey)]
    `(fn [ctx# tracer#]
       (let [kv# ^FetchResult (mdm-fetch! ctx# ~ykey ~kid)
             d# (.mdmResult kv#)]
         (if-not (.mdmClaimed kv#)
           d#
           (do
             (when tracer# (t/trace-start tracer# ~ykey :knot [[~orig-ykey :ref]]))
             (try
               (when tracer# (t/trace-call tracer# ~ykey))
               (let [x# (yarn-get-sync ~ykey ~orig-ykey ctx# tracer#)]
                 (connect-result-mdm ~ykey x# d# tracer# nil))
               (catch Throwable e#
                 (connect-error-mdm ~ykey e# d# tracer# nil)))))))))

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
    ~(build-yarn-ref-gtr ykey from)))


(defn- hide [d]
  (delay d))


(defn yank0
  [poy yarns registry tracer]
  (let [mdm (create-mdm poy (max 8 (* 2 (count yarns))))
        ctx (YankCtx. mdm registry false)
        errh (fn [e]
               (throw (ex-info "failed to yank"
                               (cond->
                                (assoc (dissoc (ex-data e) ::inyank)
                                       :knitty/yanked-poy (hide poy)
                                       :knitty/failed-poy (hide (mdm-freeze! ctx))
                                       :knitty/yanked-yarns yarns)
                                 tracer (assoc
                                         :knitty/trace
                                         (when tracer
                                           (conj
                                            (-> poy meta :knitty/trace)
                                            (capture-trace! tracer)))))
                               e)))
        n (count yarns)
        yks (java.util.ArrayList. n)
        ]

    (try
      (doseq [y yarns]
        (.add yks
              (if (keyword? y)
                ((get-yank-fn ctx y) ctx tracer)
                ((yarn-gtr y) ctx tracer))))
      (->
       (kd/await* (.toArray yks)
                   (fn []
                     (let [poy' (mdm-freeze! ctx)]
                       (if tracer
                         (vary-meta poy' update :knitty/trace conj (capture-trace! tracer))
                         poy'))))
       (md/catch' errh)
       (kd/revoke' #(mdm-cancel! ctx)))
      (catch Throwable e (errh e)))))


(deftype Registry [asmap dgraph]

  clojure.lang.Seqable
  (seq [_] (seq asmap))

  clojure.lang.ILookup
  (valAt [_ k] (asmap k))
  (valAt [_ k d] (asmap k d))

  clojure.lang.IKeywordLookup
  (getLookupThunk
    [fr k]  ;; FIXME: implement custom resolve for thunks
    (let [v (asmap (ns-keyword k))]
      (reify clojure.lang.ILookupThunk
        (get [t fr']
          (if (identical? fr fr')
            v
            t)))))

  clojure.lang.Associative
  (containsKey [_ k] (contains? asmap k))
  (entryAt [_ k] (find asmap k))
  (empty [_] (Registry. {} (dependency/graph)))
  (assoc [_ k v] (Registry.
                  (assoc asmap k v)
                  nil #_(reduce #(dependency/depend %1 k %2) dgraph (yarn-deps v))
                  )))


(defn create-registry []
  (Registry. {} (dependency/graph)))
