(ns ag.lib.knitty
  (:require [clojure.java.browse]
            [clojure.java.browse-ui]
            [clojure.java.shell]
            [clojure.spec.alpha :as s]
            [manifold.deferred :as md]
            [manifold.executor])
  (:import [java.util HashMap Map]))

;; >> API
(declare yank)        ;; func,  (yank <map> [yarn-keys]) => @<new-map>
(declare yarn)        ;; macro, (yarn keyword { (bind-symbol keyword)* } expr)
(declare defyarn)     ;; macro, (defyarn name doc? spec? { (bind-symbol keyword)* } <expr>)
(declare with-yarns)  ;; macro, (with-yarns [<yarns>+] <body ...>)
;; << API

(set! *warn-on-reflection* true)

;; mapping {keyword => Yarn}
(def ^:dynamic *registry* {})


(defprotocol IYarn
  (yarn-snatch* [_ mdm reg tracer] "get value or obligated deferred")
  (yarn-key* [_] "get yarn key"))

(defprotocol MutableDeferredMap
  (mdm-fetch! [_ k] "get value or obligate deferred")
  (mdm-freeze! [_] "freeze map, deref all completed deferreds"))

(deftype Yarn [yarn deps funcv yget]
  IYarn
  (yarn-snatch* [_ mdm reg tracer] (yget mdm reg tracer))
  (yarn-key* [_] yarn))


(defn register-yarn [yarn]
  (let [k (yarn-key* yarn)]
    (when-not (qualified-keyword? k)
      (throw (ex-info "yarn must be a qualified keyword" {::yarn k})))
    (alter-var-root #'*registry* assoc k yarn)))


;; MDM


(defn- unwrap-mdm-deferred
  [d]
  (let [d (md/unwrap' d)]
    (when (md/deferred? d)
      (alter-meta! d assoc ::leaked true :type ::leaked-deferred))
    d))


(deftype LockedMapMDM
         [poy lock frozen ^Map hm]

  MutableDeferredMap

  (mdm-fetch!
    [_ k]
    (if-let [kv (find poy k)]
      (let [v (val kv)
            vv (md/unwrap' v)]
        (when-not (identical? vv v)
          (locking lock (.put hm k vv)))
        [false vv])
      (locking lock
        (if-let [v (.get hm k)]
          (if (and (md/deferred? v) (md/realized? v) (not @frozen))
            (let [vv (md/success-value v v)]
              (.put hm k (if (nil? vv) ::nil vv))
              [false vv])
            [false (when-not (identical? ::nil v) v)])
          (let [d (md/deferred)]
            (when @frozen
              (throw (ex-info "fetch from frozen mdm" {::yarn k, ::mdm-frozen true})))
            (.put hm k d)
            [true d])))))

  (mdm-freeze!
    [_]
    (locking lock
      (when @frozen
        (throw (ex-info "mdm is already frozen" {})))
      (vreset! frozen true))
    (if (.isEmpty hm)
      poy
      (into poy (map (fn [[k v]] [k (unwrap-mdm-deferred v)])) hm))))




(defn- locked-hmap-mdm [poy hm-size]
  (LockedMapMDM. poy (Object.) (volatile! false) (HashMap. (int hm-size))))


(defn coerce-deferred [v]
  (let [v (force v)]
    (md/unwrap' (md/->deferred v v))))


(defn- as-deferred [v]
  (if (md/deferrable? v)
    (md/->deferred v)
    (md/success-deferred v nil)))


(defn- bind-param-type [ds]
  (let [{:keys [defer lazy]} (meta ds)]
    (cond
      (and defer lazy) :lazy-defer
      lazy             :lazy-sync
      defer            :defer
      :else            :sync)))


(defrecord NotADeferred
           [deferred])


(defmacro unwrap-defer [w]
  `(let [w# ~w]
     (if (instance? NotADeferred w#)
       (.-deferred ^NotADeferred w#)
       w#)))


(defn yarn-key
  [k]
  (cond
    (keyword? k) k
    (instance? Yarn k) (yarn-key* k)
    :else (throw (ex-info "invalid yarn key" {::yarn k}))))


(defn yarn-snatch
  [mdm k registry tracer]
  (let [yd (cond
             (keyword? k) (registry k)
             (instance? Yarn k) k
             :else (throw (ex-info "invalid yarn" {::yarn k})))]
    (when-not yd
      (throw (ex-info "yarn is not registered" {::yarn k})))
    (yarn-snatch* yd mdm registry tracer)))


(defn yarn-snatch-defer [mdm k reg tracer]
  (NotADeferred. (as-deferred (yarn-snatch mdm k reg tracer))))


(defn yarn-snatch-lazy-defer [mdm k reg tracer]
  (NotADeferred.
   (delay
    (as-deferred
     (yarn-snatch mdm k reg tracer)))))


(defn yarn-snatch-lazy-sync [mdm k reg tracer]
  (NotADeferred.
   (delay
    (let [d (yarn-snatch mdm k reg tracer)]
      (if (md/deferred? d) @d d)))))


(defonce synclazy-executor-delay
  (delay
   (let [cnt (atom 0)]
     (manifold.executor/utilization-executor
      0.95 Integer/MAX_VALUE
      {:thread-factory (manifold.executor/thread-factory
                        #(str "knitty-synclazy-" (swap! cnt inc))
                        (deliver (promise) nil))
       ;; :stats-callback (fn [stats] )
       }))))


(defn get-synclazy-executor []
  @synclazy-executor-delay)


(defn resolve-executor-var [e]
  (when e
    (when-let [ee (var-get e)]
      (if (ifn? ee) (ee) ee))))


(defmacro maybe-future-with [executor-var & body]
  (if-not executor-var
    `(do ~@body)
    `(md/future-with (resolve-executor-var ~executor-var) ~@body)))


(defn- build-yank-fns
  [k bind expr]
  (let [mdm '_mdm
        reg '_reg
        tracer '_tracer
        the-fnv (gensym "fn")

        yank-all-deps
        (mapcat identity
                (for [[ds dk] bind]
                  [ds
                   (case (bind-param-type ds)
                     :sync        `(yarn-snatch ~mdm ~dk ~reg ~tracer)
                     :defer       `(yarn-snatch-defer ~mdm ~dk ~reg ~tracer)
                     :lazy-sync   `(yarn-snatch-lazy-sync ~mdm ~dk ~reg ~tracer)
                     :lazy-defer  `(yarn-snatch-lazy-defer ~mdm ~dk ~reg ~tracer))]))

        maybe-unwrap-defers
        (mapcat identity
                (for [[ds _dk] bind
                      :when (#{:lazy-sync :defer :lazy-defer} (bind-param-type ds))]
                  [ds `(unwrap-defer ~ds)]))

        deps (keys bind)
        fnname #(-> k name (str "--" %) symbol)
        any-lazy-sync (->> deps (map bind-param-type) (some #{:lazy-sync}) some?)
        executor-var (or (:executor (meta bind))
                         (when any-lazy-sync #'get-synclazy-executor))]

    `(let [dep-keys# ~(vec (for [[ds dk] bind] [dk (bind-param-type ds)]))

           ;; input - vector. unwraps 'defers', called by yget-fn#
           ~the-fnv
           (fn ~(fnname "vec")
             ([[~@deps] ~tracer]
              (coerce-deferred (let [~@maybe-unwrap-defers] ~expr))))

           ;; input - mdm and registry, called by `yank-snatch
           yget-fn#
           (fn ~(fnname "yget") [~mdm ~reg ~tracer]
             (let [[new# d#] (mdm-fetch! ~mdm ~k)]
               (if new#
                 (maybe-future-with
                  ~executor-var
                  (try ;; d# is alsways deffered
                    (let [~@yank-all-deps]
                      (let [x# (if ~(or
                                     (some? executor-var)
                                     (list*
                                      `or
                                      (for [d deps, :when (= (bind-param-type d) :sync)]
                                        `(md/deferred? ~d))))
                                 (md/chain' (md/zip' ~@deps) #(~the-fnv % ~tracer))
                                 (~the-fnv [~@deps] ~tracer))]
                        (md/connect x# d#)
                        x#))
                    (catch Throwable e#
                      (md/error! d# e#)
                      d#)))
                 (do
                   d#))))]
       [~the-fnv
        yget-fn#])))


;; MACRO SPECS

(s/def
  ::pile-of-yarn
  (s/keys))


(s/def
  ::yarn-binding
  (s/map-of symbol? ident?))

(s/def
  ::defyarn
  (s/cat
   :name symbol?
   :doc (s/? string?)
   :bind-and-expr (s/? (s/cat :bind ::yarn-binding
                              :expr any?))))

(s/def
  ::yarn
  (s/cat
   :name qualified-keyword?
   :bind-and-expr (s/? (s/cat :bind ::yarn-binding
                              :expr any?))))


;; PUBLIC API IMPL

(defmacro yarn
  [k & exprs]
  (let [bd (cons k exprs)
        cf (s/conform ::yarn bd)]
    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::yarn bd))))
    (let [{{:keys [bind expr]} :bind-and-expr} cf
          bind (or bind {})
          expr (or expr `(throw (ex-info "missing input-only yarn" {::yarn ~k})))]
      `(let [[funcv# yget#] ~(build-yank-fns k bind expr)]
         (Yarn.
          ~k
          ~(vec (vals bind))
          funcv#
          yget#)))))


(defmacro defyarn
  [nm & body]
  (let [bd (cons nm body)
        cf (s/conform ::defyarn bd)]
    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::defyarn bd))))
    (let [k (keyword (-> *ns* ns-name name) (name nm))
          {doc :doc, {:keys [bind expr]} :bind-and-expr} cf
          bind (or bind {})
          m (merge (meta bind) (meta nm))
          doc (or doc (:doc m))
          m (if doc (assoc m :doc doc) m)
          spec (:spec m)
          expr (or expr `(throw (ex-info "missing input-only yarn" {::yarn ~k})))
          nm (with-meta nm m)]
      `(do
         ~@(when spec `((s/def ~k ~spec)))
         (register-yarn (yarn ~k ~bind ~expr))
         (def ~nm ~k)))))


(defn yank*
  [poy yarns registry]
  (let [mdm (locked-hmap-mdm poy (max 8 (* 2 (count yarns))))
        ydefs (map #(yarn-snatch mdm % registry nil) yarns)]
    (md/catch'
     (md/chain'
      (apply md/zip' ydefs)
      (fn [yvals] [yvals (mdm-freeze! mdm)]))
     (fn [e]
       (throw (ex-info "Failed to yank"
                       {::poy poy
                        ::yarns yarns
                        ::mdm (mdm-freeze! mdm)}
                       e))))))


(defn yank
  [poy yarns]
  (assert (map? poy) "poy should be a map")
  (assert (sequential? yarns) "yarns should be vector/sequence")
  (yank* poy yarns *registry*))


(defmacro with-yarns [yarns & body]
  `(binding [*registry*
             (into *registry*
                   (map #(vector (yarn-key %) %))
                   ~yarns)]
     ~@body))


(defn select-ns-keys [m ns]
  (let [s (name ns)]
    (into
     (empty m)
     (comp
      (filter (comp keyword? key))
      (filter #(= s (namespace (key %)))))
     m)))

(defn assert-spec-keys [m]
  (s/assert ::pile-of-yarn m))


(comment

  (do
    (defyarn zero  ;; define "yarn" - single slot/value
      {}           ;; no inputs
      0)           ;; value, use explicit `do when needed

    (defyarn one
      {_ zero}     ;; wait for zero, but don't use it
      1)           ;; any manifold-like deferred can be finished

    (defyarn one-slooow
      {}
      (future (Thread/sleep (rand-int 20)) 1))

    (defyarn two
      {^:defer x one
       ^:defer y one-slooow}       ;; don't unwrap deferred (all values coerced to manifold/deffered)
      (md/chain' (md/alt x y) inc))

    (defyarn three-fast {x one, y two}
      (future
        (Thread/sleep (rand-int 5)) (+ x y)))

    (defyarn three-slow {x one, y two}
      (future
        (Thread/sleep (rand-int 10)) (+ x y)))

    (defyarn three          ;; put deferred into delay, enables branching
      {^:lazy         f three-fast
       ^:lazy ^:defer s three-slow}
      (if (zero? (rand-int 2)) f s))

    (defyarn four
      {x ::one, y ::three}  ;; use raw keywords (not recommended)
      (future (+ x y)))

    (defyarn five
      {x ::two, y three}    ;; mixed approach
      (+ x y))

    (defyarn six
      ^{:doc "doc string"}          ;; doc
      ^{:spec number?}               ;; spec
      {x ::two , y ::three}
      (do                   ;; explicit do
        (* x y))))

  ;; yank - ensure all keys are inside the map - finishs deferred
  @(yank {} [one])
  @(yank {} [six])
  @(yank {one 1000} [four six])
  @(yank {two 2000} [four five])

  ;; dynamically create 'yarn - not recommended
  @(yank {} [(yarn ::eight {f ::four} (* f 2))])

  ;; dynamically create 'yarn & capture locals
  (for [i (range 1 4)]
    @(yank {} [(yarn ::seven {s ::six} (* i s))]))

  ;; redefine (aka mock) yarns in registry
  (with-yarns [(yarn ::three {} (rand-int 1000))
               (yarn ::four {} (assert false))]
    @(yank {} [::six]))

  ;; or dynamically switch yarns
  (with-yarns [(rand-nth
                [(yarn ::seven {s ::six} (float (+ s 1)))
                 (yarn ::seven {s ::six} (long (+ s 1)))])]
    @(yank {} [::seven]))
)

