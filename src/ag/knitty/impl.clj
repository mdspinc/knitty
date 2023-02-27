(ns ag.knitty.impl
  (:require [ag.knitty.deferred :as kd]
            [ag.knitty.mdm :as mdm :refer [create-mdm mdm-cancel! mdm-fetch!
                                           mdm-freeze! mdm-get!]]
            [ag.knitty.trace :as t :refer [capture-trace! Tracer]]
            [com.stuartsierra.dependency :as dependency]
            [manifold.deferred :as md]
            [manifold.executor]
            [manifold.utils])
  (:import [ag.knitty.mdm FetchResult MutableDeferredMap]
           [java.util.concurrent.atomic AtomicReference]))


(set! *warn-on-reflection* true)

(defprotocol IYarn
  (yarn-gtr [_ registry] "get or build yarn getter")
  (yarn-deps [_] "get dependencies")
  (yarn-key [_] "get yarn ykey"))


(definterface IRegistry
  (^clojure.lang.IFn resolveYarnGtr [^clojure.lang.Keyword kkw ^int kid])
  (yarnsComparator []))


(definterface IYankCtx
  (^ag.knitty.impl.IRegistry getRegistry [])
  (^boolean isCancelled [])
  (^boolean isTracingEnabled [])
  (getTracer [])
  (^clojure.lang.IFn resolveYarnGtr [^clojure.lang.Keyword kkw ^int kid])
  )


(deftype YankCtx [^MutableDeferredMap mdm
                  ^IRegistry registry
                  ^:volatile-mutable ^boolean cancelled
                  tracer]

  MutableDeferredMap
  (mdmFetch [_ k i] (.mdmFetch mdm k i))
  (mdmGet [_ k i] (.mdmGet mdm k i))
  (mdmFreeze [_] (.mdmFreeze mdm))
  (mdmCancel [_] (set! cancelled (boolean true)) (.mdmCancel mdm))

  IYankCtx
  (isCancelled [_] (boolean cancelled))
  (getRegistry [_] registry)
  (getTracer [_] (when-not cancelled tracer))
  (resolveYarnGtr [_ kkw kid] (.resolveYarnGtr registry kkw kid))
  )


(defn as-deferred [v]
  (if (md/deferred? v)
    v
    (md/success-deferred v nil)))


(defn bind-param-type [ds]
  (let [{:keys [defer lazy]} (meta ds)]
    (cond
      lazy  :lazy
      defer :defer
      :else :sync)))


(defmacro get-yank-fn [ctx ykey]
  (let [k (mdm/keyword->intid ykey)]
    `(.resolveYarnGtr ~ctx ~ykey ~k)))


(defmacro ctx-tracer-> [ctx fn & args]
  `(when-let [t# (.getTracer ~ctx)] (~fn t# ~@args)))


(defmacro yarn-get-sync [yk ykey ctx]
  `(do
     (ctx-tracer-> ~ctx t/trace-dep ~yk ~ykey)
     (or
      (mdm-get! ~ctx ~ykey ~(mdm/keyword->intid ykey))
      ((get-yank-fn ~ctx ~ykey) ~ctx))))


(defmacro yarn-get-defer [yk ykey ctx]
  `(do
     (ctx-tracer-> ~ctx t/trace-dep ~yk ~ykey)
     (or
      (mdm-get! ~ctx ~ykey ~(mdm/keyword->intid ykey))
      (as-deferred
       ((get-yank-fn ~ctx ~ykey) ~ctx)))))


(deftype Lazy
         [^AtomicReference value
          ^IYankCtx ctx
          yk 
          ykey
          ^int ykeyi
          yank-fn]
  
  clojure.lang.IDeref
  (deref [_]
    (if-let [v (.get value)]
      v
      (let [v (kd/ka-deferred)]
        (if (.compareAndSet value nil v)
          (do
            (try
              (ctx-tracer-> ctx t/trace-dep yk ykey)
              (kd/connect'' (or
                             (mdm-get! ctx ykey ykeyi)
                             (as-deferred (yank-fn ctx)))
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
    ~(mdm/keyword->intid ykey)
    (get-yank-fn ~ctx ~ykey)))


(defn force-lazy-result [v]
  (if (instance? Lazy v)
    @v
    v))


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


(deftype ConnectListener [^IYankCtx ctx
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


(defn connect-result-mdm [^IYankCtx ctx ykey result mdm-deferred maybe-real-result]

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
              (kd/connect-deferreds'' mdm-deferred mrr)))))

      ;; connect result -> mdm-deferred
      (kd/listen! result (ConnectListener. ctx ykey mdm-deferred))

      mdm-deferred)

    (do
      ;; no revokation for sync results
      (ctx-tracer-> ctx t/trace-finish ykey result nil false)
      (kd/success'! mdm-deferred result)
      result)))


(defn connect-error-mdm [^IYankCtx ctx ykey error mdm-deferred maybe-real-result]
  (let [ew (wrap-yarn-exception ykey error)]

    ;; try to revoke
    (when maybe-real-result
      (when-let [d @maybe-real-result]
        (when (instance? manifold.deferred.IMutableDeferred d)
          (kd/error'! d error))))

    (ctx-tracer-> ctx t/trace-finish ykey nil ew false)

    (kd/error'! mdm-deferred ew)
    mdm-deferred))


(defn build-yank-fns
  [registry ykey bind body-fn]
  (let [{:keys [keep-deps-order executor norevoke]} (meta bind)
        deps (keys bind)
        bind (if (or keep-deps-order (nil? registry))
               bind
               (sort-by second (.yarnsComparator ^IRegistry registry) bind))

        ctx (with-meta '_yank_ctx {:tag (str `YankCtx)})
        the-fnv (gensym "fn")
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
                          identity)

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

        all-deps-tr (vec (for [[ds dk] bind] [dk (bind-param-type ds)]))
        ]

    `(let [~the-fnv ~body-fn]
       (fn ~(fnn "--yank") [~ctx]
         (let [kv# ^FetchResult (mdm-fetch! ~ctx ~ykey ~kid)
               d# (.mdmResult kv#)]
           (if-not (.mdmClaimed kv#)

                 ;; got item from mdm
             (kd/unwrap1' d#)

                 ;; calculate & provide new value to mdm
             (maybe-future-with
              ~executor

              (ctx-tracer-> ~ctx t/trace-start  ~ykey :yarn ~all-deps-tr)
              (let [~@(if norevoke [] [reald `(volatile! ::none)])]
                (try ;; d# is alsways deffered
                  (let [~@yank-deps
                        ~@try-deref-syncs]
                    (let [x# (if ~some-syncs-unresolved

                               (kd/unwrap1'
                                (kd/await*

                                 (let [~df-array (object-array ~(count sync-deps))]
                                   ~@(for [[i d] (map vector (range) (reverse sync-deps))]
                                       `(aset ~df-array ~i ~d))
                                   ~df-array)

                                 (fn ~(fnn "--async") []
                                   (let [~@deref-syncs]
                                     (~@(if norevoke `[do] [`vreset! reald])
                                      (do
                                        (ctx-tracer-> ~ctx t/trace-call ~ykey)
                                        (~coerce-deferred (~the-fnv ~@deps))))))))

                               (~@(if norevoke `[do] [`vreset! reald])
                                (do
                                  (ctx-tracer-> ~ctx t/trace-call ~ykey)
                                  (~coerce-deferred (~the-fnv ~@deps)))))]

                      (connect-result-mdm ~ctx ~ykey x# d# ~reald)))
                  (catch Throwable e#
                    (connect-error-mdm ~ctx ~ykey e# d# ~reald)))))))))))


(defn build-yarn-ref-gtr [ykey orig-ykey]
  (let [kid (mdm/keyword->intid ykey)]
    `(fn [ctx#]
       (let [kv# ^FetchResult (mdm-fetch! ctx# ~ykey ~kid)
             d# (.mdmResult kv#)]
         (if-not (.mdmClaimed kv#)
           d#
           (do
             (ctx-tracer-> ctx# t/trace-start ~ykey :knot [[~orig-ykey :ref]])
             (try
               (ctx-tracer-> ctx# t/trace-call ~ykey)
               (let [x# (yarn-get-sync ~ykey ~orig-ykey ctx#)]
                 (connect-result-mdm ctx# ~ykey x# d# nil))
               (catch Throwable e#
                 (connect-error-mdm ctx# ~ykey e# d# nil)))))))))

;; variable used to pass current yarn-body-fn into `eval
(def ^:dynamic *evaluated-yarn-fn*)

(defn gen-yarn
  [ykey bind expr]
  (let [[deps1 deps2] (split-at 16 (keys bind))]
    `(let [b# '~bind
           f# (fn ~(-> ykey name symbol) [~@deps1 & [~@deps2]] ~expr)
           d# (delay
                (binding [*evaluated-yarn-fn* f#]
                  (eval
                   (build-yank-fns nil ~ykey b# `*evaluated-yarn-fn*))))]
       (reify IYarn
         (yarn-gtr [_ r#]
           ;; lazily eval 'gtr' side function when registry is avaliable
           (if (nil? r#)
             (force d#)
             (binding [*evaluated-yarn-fn* f#]
               (eval
                (build-yank-fns r# ~ykey b# `*evaluated-yarn-fn*)))))
         (yarn-key [_] ~ykey)
         (yarn-deps [_] ~(vec (vals bind)))))))


(defn gen-yarn-ref
  [ykey from]
  `(let [d# ~(build-yarn-ref-gtr ykey from)]
     (reify IYarn
       (yarn-gtr [_ r#] d#)
       (yarn-key [_] ~ykey)
       (yarn-deps [_] [~from]))))


(defn yank0
  [poy yarns registry tracer]
  (let [mdm (create-mdm poy (max 8 (* 2 (count yarns))))
        ctx (YankCtx. mdm registry false tracer)
        errh (fn [e]
               (throw (ex-info "failed to yank"
                               (cond->
                                (assoc (dissoc (ex-data e) ::inyank)
                                       :knitty/yanked-poy poy
                                       :knitty/failed-poy (mdm-freeze! ctx)
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
                ((.resolveYarnGtr ctx y (mdm/keyword->intid y)) ctx)
                ((yarn-gtr y nil) ctx))))
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


(deftype Registry [^objects ygets asmap dgraph]

  IRegistry
  (resolveYarnGtr
   [this kkw kid]
   (or (aget ygets kid)
       (locking ygets
         (aset ygets kid (yarn-gtr (get asmap kkw) this)))))

  (yarnsComparator
   [_]
   (dependency/topo-comparator dgraph))

  clojure.lang.Seqable
  (seq [_] (seq asmap))

  clojure.lang.ILookup
  (valAt [_ k] (asmap k))
  (valAt [_ k d] (asmap k d))

  clojure.lang.Associative
  (containsKey [_ k] (contains? asmap k))
  (entryAt [_ k] (find asmap k))
  (empty [_] (Registry. nil {} (dependency/graph)))
  (assoc [_ k v]
         (mdm/keyword->intid k)
         (Registry.
          (object-array (inc (mdm/max-initd)))
          (assoc asmap k v)
          (reduce #(dependency/depend %1 k %2) dgraph (yarn-deps v)))))


(defn create-registry []
  (Registry. nil {} (dependency/graph)))
