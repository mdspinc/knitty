(ns ag.lib.knitty
  (:require
   [clojure.spec.alpha :as s]
   [manifold.deferred :as md]
   [taoensso.timbre :refer [debugf]])
  (:import
   [java.util HashMap Map]))


(set! *warn-on-reflection* true)

(defprotocol YarnDescripitor
  (yarn-snatch* [_ mdm] "get-or-create value from `poy and `mdm, optionally use tracer `tcr")
  (yarn-key [_] "get yarn key")
  )

(defprotocol MutableDeferredMap
  (mdm-fetch! [_ k] "get value or obligated deferred")
  (mdm-freeze! [_] "freeze map, return single deferred")
  )

(defrecord Yarn [key func funcv deps spec yget]
  YarnDescripitor
  (yarn-snatch* [_ mdm] (yget mdm))
  (yarn-key [_] key)
  )


;; mapping {keyword => Yarn}
(defonce yarn-registry {})


(defn register-yarn [{k :key, s :spec, :as sd}]
  (if s
    (debugf "tie yarn %s as '%s" k s)
    (debugf "tie yarn %s" k))
  (when-not (and (keyword? k) (namespace k))
    (throw (ex-info "yarn key must be a namespaced keyword" {::key k})))
  (alter-var-root #'yarn-registry assoc k sd)
  sd)


(deftype LockedMapMDM
         [poy lock frozen ^Map hm]
  MutableDeferredMap
  (mdm-fetch!
    [_ k]
    (if-let [kv (find poy k)]
      [false (val kv)]
      (locking lock
        (when @frozen
          (throw (ex-info "attemt to fetch from frozen MDM" {::key k})))
        (if (.containsKey hm k)
          (let [v (.get hm k)]
            (if (and (md/deferred? v) (md/realized? v))
              (let [vv (md/success-value v v)]
                (.put hm k vv)
                [false vv])
              [false v]))
          (let [d (md/deferred)]
            (.put hm k d)
            [true d])))))
  (mdm-freeze!
    [_]
    (locking lock
      (when @frozen
        (throw (ex-info "attemt to freeze already frozen MDM" {})))
      (vreset! frozen true))
    (if (.isEmpty hm)
      (md/success-deferred poy)
      (let [d? #(-> % val md/deferred?)
            [dfs vls] [(filter d? hm) (remove d? hm)]]
        (md/chain
         (apply md/zip (map val dfs))
         (fn [dfs-vals]
           (-> poy
               (into vls)
               (into (zipmap (map key dfs) dfs-vals))))))))
  )


(defn- locked-hmap-mdm [poy]
  (LockedMapMDM. poy (Object.) (volatile! false) (HashMap.)))


(defn coerce-deferred [v]
  (let [v (force v)]
    (md/->deferred v v)))


(defn- as-deferred [v]
  (if (md/deferrable? v)
    (md/->deferred v)
    (md/success-deferred v)))


(defn- bmap-param-type [ds]
  (let [m (meta ds)]
    (cond
      (:defer m) ::defer
      (:lazy m)  ::lazy-defer
      :else      ::sync)))


(defrecord WrpdDefer [deferred])

(defmacro unwrap-defer [w]
  `(let [w# ~w]
     (if (instance? WrpdDefer w#) 
       (:deferred w#)
       w#)))


(defn yarn-snatch
  ([mdm k]
   (let [yd (yarn-registry k)]
     (when-not yd
       (throw (ex-info (str "yarn " k " is not registered") {::key k})))
     (yarn-snatch* yd mdm))))

(defn yarn-snatch-defer [mdm k]
  (->WrpdDefer (as-deferred (yarn-snatch mdm k))))

(defn yarn-snatch-lazy [mdm k]
  (delay (as-deferred (yarn-snatch mdm k))))


(defmacro build-yank-fns
  [k bmap expr]
  (let [mdm '__mdm
        yank-all-deps
        (mapcat identity
                (for [[ds dk] bmap]
                  [ds
                   (case (bmap-param-type ds)
                     ::sync       `(yarn-snatch ~mdm ~dk)
                     ::defer      `(yarn-snatch-defer ~mdm ~dk)
                     ::lazy-defer `(yarn-snatch-lazy ~mdm ~dk))]))

        maybe-unwrap-defers
        (mapcat identity
                (for [[ds _dk] bmap
                      :when (= ::defer (bmap-param-type ds))]
                  [ds `(unwrap-defer ~ds)]))

        deps (keys bmap)
        fnname #(-> k name (str "--" %) symbol)]

    `(let [;; input - map (no defer unwrapping), suitable for testing
           the-fnm#
           (fn ~(fnname "map") [~bmap]
             (coerce-deferred
              ~expr))

           ;; input - vector. unwraps 'defers', called by yget-fn#
           the-fnv#
           (fn ~(fnname "vec")
             [[~@deps]]
             (coerce-deferred
              (let [~@maybe-unwrap-defers]
                ~expr)))

           ;; input - poy mdm and caller, called by `yank-snatch
           yget-fn#
           (fn ~(fnname "yget") [~mdm]
             (let [[new# d#] (mdm-fetch! ~mdm ~k)]
               (if new#
                 (try ;; d# is alsways deffered
                   (let [~@yank-all-deps]
                     (let [x# (if (or
                                   ~@(for [d deps
                                           :when (= (bmap-param-type d) ::sync)]
                                       `(md/deferred? ~d)))
                                (md/chain' (md/zip' ~@deps) the-fnv#)
                                (the-fnv# [~@deps]))]
                       (md/connect x# d#)
                       x#))
                   (catch Throwable e#
                     (md/error! d# e#)))
                 d#)))]

       {:yget yget-fn#
        :func the-fnm#
        :funcv the-fnv#})))


(defmacro defyarn*
  [nm k doc spec bmap expr]
  `(do
     ~@(when spec `((s/def ~k ~spec)))
     (register-yarn
      (map->Yarn
       (merge
        (build-yank-fns ~k ~bmap ~expr)
        {:key ~k
         :deps ~(vec (vals bmap))
         :spec '~spec})))
     (def ~(vary-meta nm assoc :doc doc) ~k)))


(s/def
  ::pile-of-yarn
  (s/keys))

(s/def
  ::bmap
  (s/alt
   :vec (s/tuple (s/map-of symbol? (some-fn keyword? symbol?)))
   :map (s/map-of symbol? (some-fn keyword? symbol?))))

(s/def
  ::defyarn
  (s/cat
   :name symbol?
   :doc (s/? string?)
   :spec (s/? any?)
   :bmap-expr (s/?
               (s/cat :bmap ::bmap
                      :expr any?))))

(defn- parse-bmap [bm]
  (let [[bmap-type bmap-body] bm]
    (case bmap-type
      :vec (first bmap-body)
      :map bmap-body)))


(defmacro defyarn
  [nm & body]
  (let [bd (cons nm body)
        cf (s/conform ::defyarn bd)]
    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::defyarn bd))))
    (let [{:keys [doc spec] {:keys [bmap expr]} :bmap-expr} cf
          doc (or doc "")
          bmap (or bmap [:map {}])
          expr (or expr `(throw (IllegalStateException. ~(str "missing yarn " nm))))
          k (keyword (-> *ns* ns-name name) (name nm))]
      `(defyarn* ~nm ~k ~doc ~spec ~(parse-bmap bmap) ~expr))))


(defn yank
  [poy yarns]
  (let [mdm (locked-hmap-mdm poy)
        ys (mapv #(yarn-snatch mdm %) yarns)]
    (md/chain
     (mdm-freeze! mdm)
     (fn [poy']
       [(map #(if (md/deferred? %) @% %) ys) poy']))))


;; helpers

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

  ;; define "yarn" - single slot/value
  (defyarn zero
    [{}]  ;; no inputs
    0)    ;; value, use explicit `do when needed

  ;; true, symbols just resolves into keyword
  (keyword? zero)

  (defyarn one
    [{_ zero}]     ;; wait for zero, but don't use it
    (future
      1))  ;; any manifold-like deferred can be returned

  (defyarn two
    [{^:defer x one}] ;; don't unwrap deferred (all values coerced to manifold/deffered)
    (md/chain' x inc))

  (defyarn three-fast
    [{x one y two}]
    (do
      (+ x y)))

  (defyarn three-slow
    [{x one y two}]
    (future
      (+ x y)))

  (defyarn three
    [{^:lazy t1 three-fast
      ^:lazy t2 three-slow}]
    (if (zero? (rand-int 2))
      t1 t2))

  (defyarn four
    [{y two}]
    (future (* y y)))

  (defyarn five
    [{x two, y three}]
    (future (+ x y)))

  (defyarn six
    [{x two
      y three}]
    (+ x y))

  ;; yank - ensure all keys are inside the map - returns deferred
  @(yank {} [one])
  @(yank {} [six])
  @(yank {one 10} [four])
  @(yank {} [zero])
  @(yank {} [five six])
  @(yank {two 2000} [four five])
  )