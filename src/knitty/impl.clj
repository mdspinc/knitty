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


(deftype Registry [ycache asmap all-deps]

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
  (ycache [_] @ycache)

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
       (delay (make-array Yarn (inc (MDM/maxid))))
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


(defmacro tracer-> [mdm fn & args]
  `(do-tracing
    (when-some [t# (.-tracer ~mdm)]
      (~fn t# ~@args))))


(defmacro yarn-get-impl
  ([yk ykey mdm]
   `(yarn-get-impl ~yk ~ykey ~(ji/regkw ykey) ~mdm))
  ([yk ykey ykeyi mdm]
   `(do
      (tracer-> ~mdm t/trace-dep ~yk ~ykey)
      (.fetch ~mdm ~ykeyi))))


(deftype Lazy
         [^MDM mdm
          ^clojure.lang.Keyword yk
          ^clojure.lang.Keyword ykey
          ^long ykeyi]

  clojure.lang.IDeref
  (deref [_] (yarn-get-impl yk ykey ykeyi mdm))

  Object
  (toString [_] (str "#knitty/Lazy[" ykey "]")))


(defmacro yarn-get-lazy [yk ykey mdm]
  `(Lazy.
    ~mdm
    ~yk
    ~ykey
    ~(ji/regkw ykey)))


(defn make-yankfn
  [^MDM mdm
   yk
   yarns-map]
  (fn yankfn [y]
    (if-let [[i k] (yarns-map y)]
      (do
        (tracer-> mdm t/trace-dep yk k)
        (.fetch mdm i))
      (throw (ex-info "Invalid yank-fn arg" {:knitty/yankfn-arg y
                                             :knytty/yankfn-known-args (keys yarns-map)})))))


(defmacro yarn-get-yankfn [yk keys-map mdm]
  (let [args (into {} (map (fn [[k v]] [k [(ji/regkw v) v]])) keys-map)]
    `(make-yankfn ~mdm ~yk ~args)))


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


(defmacro connect-result-mdm [mdm ykey result mdm-deferred]
  `(if (instance? manifold.deferred.IDeferred ~result)
     (do
       (ji/kd-chain-from ~mdm-deferred ~result (.-token ~mdm))
       (do-tracing
        (when-some [t# (.-tracer ~mdm)]
          (md/add-listener! ~mdm-deferred (TraceListener. t# ~ykey)))))
     (do
       (tracer-> ~mdm t/trace-finish ~ykey ~result nil false)
       (ji/kd-success! ~mdm-deferred ~result (.-token ~mdm))
       )))


(defmacro connect-error-mdm [mdm ykey error mdm-deferred]
  `(do
     (tracer-> ~mdm t/trace-finish ~ykey nil ~error false)
     (ji/kd-error! ~mdm-deferred ~error (.-token ~mdm))))


(defn emit-yarn-impl
  [the-fn-body ykey bind yarn-meta deps]
  (let [{:keys [executor]} yarn-meta
        mdm '__knitty_mdm

        yank-deps
        (mapcat identity
                (for [[ds dk] bind]
                  [ds
                   (case (bind-param-type ds)
                     :sync   `(yarn-get-impl   ~ykey ~dk ~mdm)
                     :defer  `(yarn-get-impl   ~ykey ~dk ~mdm)
                     :lazy   `(yarn-get-lazy   ~ykey ~dk ~mdm)
                     :yankfn `(yarn-get-yankfn ~ykey ~dk ~mdm))]))

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
       (~'yank [_# ~mdm d#]
         (maybe-future-with
          ~executor
          (tracer-> ~mdm t/trace-start ~ykey :yarn ~all-deps-tr)
          (try
            (let [~@yank-deps]
              (ji/kd-await
               (reify manifold.deferred.IDeferredListener
                 (~'onSuccess
                   [_# _#]
                   (let [z# (let [~@deref-syncs]
                              (tracer-> ~mdm t/trace-call ~ykey)
                              (~coerce-deferred ~the-fn-body))]
                     (connect-result-mdm ~mdm ~ykey z# d#)))
                 (~'onError
                   [_ e#]
                   (connect-error-mdm ~mdm ~ykey e# d#)))
               ~@sync-deps))
            (catch Throwable e#
              (connect-error-mdm ~mdm ~ykey e# d#))))))))


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
               (sort-by (comp #(when (keyword? %) (ji/regkw %)) second) bind))]
      (emit-yarn-impl expr ykey bind yarn-meta deps)))


(defn gen-yarn-ref
  [ykey from]
  (MDM/regkw ykey)
  `(reify Yarn
    (~'deps [_#] #{~from})
    (~'key [_#] ~ykey)
    (~'yank [_# mdm# d#]
      (tracer-> mdm# t/trace-start ~ykey :knot [[~from :ref]])
      (try
        (let [x# (ji/kd-unwrap (yarn-get-impl ~ykey ~from mdm#))]
          (connect-result-mdm mdm# ~ykey x# d#))
        (catch Throwable e#
          (connect-error-mdm mdm# ~ykey e# d#))))))


(defn yarn-multifn [yarn-name]
  (cond
    (symbol? yarn-name) `(yarn-multifn ~(eval yarn-name))
    (keyword? yarn-name) (symbol (namespace yarn-name) (str (name yarn-name) "--multifn"))
    :else (throw (ex-info "invalid yarn-name" {:yarn-name yarn-name}))))


(defn make-multiyarn-route-key-fn [ykey k]
  (let [i (long (ji/regkw k))]
    (fn yank-route-key [^MDM mdm ^KDeferred _]
      (tracer-> mdm t/trace-route-by ykey k)
      (ji/kd-get (.fetch mdm i)))))


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
         (~'deps [_#]
           (yarn-multi-deps multifn# ~route-key))
         (~'key [_#]
           ~ykey)
         (~'yank [_ mdm# d#]
           (let [r# (yarn-get-impl ~ykey ~route-key mdm#)]
             (ji/kd-await
              (reify manifold.deferred.IDeferredListener
                (onSuccess [_# _#] (multifn# mdm# d#))
                (onError [_ e#] (ji/kd-error! r# e# (.-token mdm#))))
              r#)))))))


(defn gen-reg-yarn-method
  [yk yarn route-val]
    `(let [y# ~yarn
           rv# ~route-val]
       (defmethod
         ~(yarn-multifn yk)
         rv#
         ([] y#)
         ([mdm# d#] (ji/yarn-yank y# mdm# d#)))))


(defn fail-always-yarn [ykey msg]
  (MDM/regkw ykey)
  (reify
    Yarn
    (key [_] ykey)
    (deps [_] #{})
    (yank [_ mdm d] (ji/kd-error! d (java.lang.UnsupportedOperationException. (str msg)) (.-token mdm)))))


(defn gen-yarn-input [ykey]
  `(fail-always-yarn ~ykey ~(str "input-only yarn " ykey)))


(defn yank0
  [poy yarns registry tracer]
  (let [mdm (MDM. poy registry tracer)
        token (.-token mdm)
        result (ji/kd-create token)
        ry (fn yank-root [y] (.fetchRoot mdm y))
        kds (map ry yarns)]
    (run! identity kds)
    (ji/kd-await-coll
     (reify manifold.deferred.IDeferredListener
       (onSuccess [_ _]
         (ji/kd-success! result
                         (let [poy' (.freeze mdm)]
                           (if tracer
                             (with-meta poy' (update (meta poy) :knitty/trace conj (t/capture-trace! tracer)))
                             poy'))
                         token))
       (onError [_ e]
         (ji/kd-error! result
                       (ex-info "failed to yank"
                                (if tracer
                                  (assoc (ex-data e)
                                         :knitty/yanked-poy poy
                                         :knitty/failed-poy (.freeze mdm)
                                         :knitty/yanked-yarns yarns
                                         :knitty/trace (conj (-> poy meta :knitty/trace) (t/capture-trace! tracer)))
                                  (assoc (ex-data e)
                                         :knitty/yanked-poy poy
                                         :knitty/failed-poy (.freeze mdm)
                                         :knitty/yanked-yarns yarns))
                                e)
                       token)))
     kds)
    (ji/kd-revoke result
                  (fn cancel-mdm [] (.cancel mdm)))))
