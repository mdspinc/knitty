(ns ag.knitty.core
  (:require [ag.knitty.deferred :as kd]
            [ag.knitty.impl :as impl]
            [ag.knitty.trace :refer [create-tracer]]
            [clojure.java.browse]
            [clojure.java.browse-ui]
            [clojure.java.shell]
            [clojure.spec.alpha :as s]
            [manifold.deferred :as md]))


;; mapping {keyword => Yarn}
(def ^:dynamic *registry* (impl/create-registry))
(def ^:dynamic *tracing* (not impl/elide-tracing))


(defn register-yarn
  "Registers Yarn into the global registry, do nothing when
   yarn is already registed and no-override is true."
  ([yarn]
   (register-yarn yarn false))
  ([yarn no-override]
   (let [k (impl/yarn-key yarn)]
     (when-not (qualified-keyword? k)
       (throw (ex-info "yarn must be a qualified keyword" {::yarn k})))
     (if no-override
       (alter-var-root #'*registry* #(if (contains? % k) % (assoc % k yarn)))
       (alter-var-root #'*registry* assoc k yarn)))))


(defn- valid-bind-type? [bsym]
  (let [{:keys [defer lazy yankfn]} (meta bsym)
        n (count (filter identity [defer lazy yankfn]))]
    (<= n 1)))


(s/def ::yarn-binding-yankfn-map
  (s/map-of any? (some-fn qualified-keyword? symbol?)))

(s/def ::yarn-bindind-var
  (s/and symbol? valid-bind-type?))

(s/def ::yarn-binding
  (s/map-of
   ::yarn-bindind-var
   (s/or :ident ident?
         :yankfn-map ::yarn-binding-yankfn-map)))

(s/def ::bind-and-body
  (s/? (s/cat :bind ::yarn-binding :body (s/+ any?))))

(s/def ::yarn
  (s/cat
   :name qualified-keyword?
   :bind-and-body ::bind-and-body))

(s/def ::defyarn
  (s/cat
   :name symbol?
   :doc (s/? string?)
   :bind-and-body ::bind-and-body))


(defmacro declare-yarn
  "Defines abstract yarn without an implementation,
   useful for making forward declarations."
  [nm]
  {:pre [(ident? nm)]}
  (let [k (keyword (or (namespace nm)
                       (-> *ns* ns-name name))
                   (name nm))]
     `(do (register-yarn (impl/fail-always-yarn ~k ~(str "declared-only yarn " k)) true)
          ~k)))


(defn link-yarn
  "Redeclares yarn as symlink to yarn-target."
  [yarn yarn-target]
  {:pre [(qualified-keyword? yarn)
         (qualified-keyword? yarn-target)]}
  (register-yarn (eval (impl/gen-yarn-ref yarn yarn-target))))


(defn- resolve-sym-or-kw
  ([env]
    (partial resolve-sym-or-kw env))
  ([env k]
   (if (keyword? k)
     k
     (let [v @(resolve env k)]
       (when-not (qualified-keyword? v)
         (throw (ex-info "yarn bindings must be qualified keyword" {::binding k})))
       v))))


(defmacro yarn
  "Returns yarn object (without registering into a registry).
   May capture variables from outer scope."
  [k & exprs]
  (if (empty? exprs)
    `(impl/fail-always-yarn ~k ~(str "input-only yarn " k))
    (let [bd (cons k exprs)
          cf (s/conform ::yarn bd)]
      (when (s/invalid? cf)
        (throw (Exception. (s/explain-str ::yarn bd))))
      (let [{{:keys [bind body]} :bind-and-body} cf
            bind (update-vals bind (fn [[t k]]
                                     (case t
                                       :ident (resolve-sym-or-kw &env k)
                                       :yankfn-map (update-vals k (resolve-sym-or-kw &env))
                                       )))]
        (impl/gen-yarn k bind `(do ~@body))))))


(defn- pick-yarn-meta [obj ex-meta doc]
  (let [m (merge ex-meta (meta obj))
        doc (or doc (:doc m))
        m (if doc (assoc m :doc doc) m)]
    [(with-meta obj m)
     m]))


(defmacro defyarn
  "Defines yarn - computation node. Uses current *ns* to build qualified keyword as yarn id.
  Examples:

  ```clojure

  ;; declare ::yarn-1 without a body
  (defyarn yarn-1)

  ;; declare ::yarn-2 with docstring
  (defyarn yarn-2 \"documentation\")

  ;; define ::yarn-3 without any inputs
  (defyarn yarn-3 {} (rand-int 10))

  ;; define ::yarn-4 with inputs
  (defyarn yarn-4 {x yarn-3} (str \"Random is\" x))
  ```
  "

  [name & doc-binds-body]
  (let [bd (cons name doc-binds-body)
        cf (s/conform ::defyarn bd)
        k (keyword (-> *ns* ns-name clojure.core/name)
                   (clojure.core/name name))]

    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::defyarn bd))))

    (let [{doc :doc, {:keys [bind body]} :bind-and-body} cf
          [nm m] (pick-yarn-meta name (meta bind) doc)
          bind (update-vals bind second)
          spec (:spec m)
          bind (when bind (with-meta bind m))
          y (if (empty? body)
              `(impl/fail-always-yarn ~k ~(str "input-only yarn " k))
              `(yarn ~k ~bind ~@body))]
      (list
       `do
       (when spec `(s/def ~k ~spec))
       `(register-yarn ~y)
       `(def ~nm ~k)))))


(defn yank
  "Computes and adds missing nodes into 'poy' map. Always returns deferred."
  [poy yarns]
  (assert (map? poy) "poy should be a map")
  (assert (sequential? yarns) "yarns should be vector/sequence")
  (impl/yank0 poy
              yarns
              *registry*
              (when *tracing*
                (create-tracer poy yarns))))


(defn yank-error?
  "Returns true when exception is rethrown by 'yank'."
  [ex]
  (boolean (:knitty/yanked-yarns (ex-data ex))))


(defmacro doyank
  "Runs anonymous yarn, returns deferred with [map-of-yarns yarn-value].
   Example:

   (declare-yarn ::yarn1)
   (defyarn yarn2 {x ::yarn1} (inc x))

   @(doyank
      {::yarn1 10}
      {x ::yarn2}
      (inc x))
   ;; => [{::yarn1 10, ::yarn2 11, ::xxeayaes 12}, 12]
   "
  [poy binds & body]
  (let [k (keyword (-> *ns* ns-name name) (name (gensym "doyank")))]
    `(let [r# (yank ~poy [(yarn ~k ~binds (do ~@body))])]
       (kd/revoke'
        (md/chain' r# (juxt ~k identity))
        #(kd/cancel! r#)))))


(defmacro doyank!
  "Similar to `doyank, but ruturn only deferred with yarns-map."
  [poy binds & body]
  (let [k (keyword (-> *ns* ns-name name) (name (gensym "doyank")))]
    `(yank ~poy [(yarn ~k ~binds (do ~@body))])))
