(ns knitty.core
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [knitty.deferred :as kd]
            [knitty.impl :as impl]
            [knitty.trace :as trace])
  (:import [java.util.concurrent Executor ExecutorService]))


(def ^:dynamic *registry*
  (impl/create-registry))

(def ^:dynamic *tracing*
  false)

(def ^:dynamic ^Executor *executor*
  (delay
    (impl/create-fjp
     {:parallelism (.availableProcessors (Runtime/getRuntime))
      :factory-prefix "knitty-fjp"
      :exception-handler (fn [t e] (log/errorf e "uncaught exception in %s" t))
      :async-mode true})))


(defn enable-tracing!
  "Globally enable knitty tracing."
  ([]
   (enable-tracing! true))
  ([enable]
   (alter-var-root #'*tracing* (constantly (boolean enable)))))


(defn set-executor!
  "Globally set knitty executor."
  ([executor]
   (set-executor! executor true))
  ([executor shutdown-current]
   (alter-var-root #'*executor*
                   (fn [e]
                     (when (and shutdown-current
                                (or (not (delay? e)) (realized? e))
                                (instance? ExecutorService (force e)))
                       (.shutdown ^ExecutorService (force e)))
                     executor))))


(defn register-yarn
  "Registers Yarn into the global registry, do nothing when
   yarn is already registed and no-override flag is true."
  ([yarn]
   (register-yarn yarn false))
  ([yarn no-override]
   (let [k (impl/yarn-key yarn)]
     (when-not (qualified-keyword? k)
       (throw (ex-info "yarn must be a qualified keyword" {::yarn k, ::class (type k)})))
     (if no-override
       (alter-var-root #'*registry* #(if (contains? % k) % (assoc % k yarn)))
       (alter-var-root #'*registry* assoc k yarn)))))


(defn- valid-bind-type? [bsym]
  (let [{:keys [defer lazy case]} (meta bsym)
        n (count (filter identity [defer lazy case]))]
    (<= n 1)))


(s/def ::yarn-binding-case-map
  (s/or
   :map (s/map-of any? (some-fn qualified-keyword? symbol?))
   :seq (s/coll-of (some-fn qualified-keyword? symbol?))
  ))

(s/def ::yarn-bindind-var
  (s/and symbol? valid-bind-type?))

(s/def ::yarn-binding
  (s/map-of
   ::yarn-bindind-var
   (s/or :ident ident?
         :case-map ::yarn-binding-case-map)))

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
  (let [spec (:spec (meta nm))
        k (keyword (or (namespace nm)
                       (-> *ns* ns-name name))
                   (name nm))]
    `(do (register-yarn (impl/fail-always-yarn ~k ~(str "declared-only yarn " k)) true)
         ~(when spec `(spec/def ~k ~spec))
         ~(when (simple-ident? nm) `(def ~nm ~k))
         ~k)))


(defn bind-yarn
  "Redeclares yarn as symlink to yarn-target."
  [yarn yarn-target]
  {:pre [(qualified-keyword? yarn)
         (qualified-keyword? yarn-target)]}
  (register-yarn (eval (impl/gen-yarn-ref yarn yarn-target))))


(defn- resolve-sym-or-kw
  [env k]
  (if (keyword? k)
    k
    (let [v (resolve env k)]
      (when-not v
        (throw (ex-info "unable to resovlve yarn binding variable" {::binding-var v})))
      (let [k @v]
        (when-not (qualified-keyword? k)
          (throw (ex-info "yarn binding must be a qualified keyword" {::binding k})))
        k))))


(defn- parse-case-params-map
  [env vv]
  (let [[vv-t vv-val] vv]
    (case vv-t
      :map
      (into {} (for [[x y] vv-val]
                 [x (resolve-sym-or-kw env y)]))
      :seq
      (into {} (map (comp (juxt identity identity)
                          (partial resolve-sym-or-kw env))) vv-val))))

(defn ^:private emit-yarn
  [env k cf mt]
  (let [{{:keys [bind body]} :bind-and-body} cf
        mt (merge mt (meta bind))
        bind (into {}
                   (for [[k [vt vv]] bind]
                     [k
                      (case vt
                        :ident (resolve-sym-or-kw env vv)
                        :case-map (parse-case-params-map env vv))]))]
    (impl/gen-yarn k bind `(do ~@body) mt)))


(defmacro yarn
  "Returns yarn object (without registering into a registry).
   May capture variables from outer scope."
  [k & exprs]
  (if (empty? exprs)
    (impl/gen-yarn-input k)
    (let [bd (cons k exprs)
          cf (s/conform ::yarn bd)]
      (when (s/invalid? cf)
        (throw (Exception. (str (s/explain-str ::yarn bd)))))
      (emit-yarn &env k cf nil))))


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
  {:arglists '([name doc-string?]
               [name doc-string? [dependencies*] & body])}
  [name & doc-binds-body]
  (let [bd (cons name doc-binds-body)
        cf (s/conform ::defyarn bd)
        k (keyword (-> *ns* ns-name clojure.core/name)
                   (clojure.core/name name))]

    (when (s/invalid? cf)
      (throw (Exception. (str (s/explain-str ::defyarn bd)))))

    (let [{doc :doc, {:keys [bind body]} :bind-and-body} cf
          [nm m] (pick-yarn-meta name (meta bind) doc)
          spec (:spec m)
          y (if (empty? body)
              (impl/gen-yarn-input k)
              (emit-yarn &env k cf m))]

      (list
       `do
       (when spec `(s/def ~k ~spec))
       `(register-yarn ~y)
       `(def ~nm ~k)))))


(defmacro defyarn-multi
  ([name route-by]
   `(defyarn-multi ~name nil ~route-by))
  ([name docstring route-by]
   (let [k (keyword (-> *ns* ns-name clojure.core/name)
                    (clojure.core/name name))
         my (impl/gen-yarn-multi k (resolve-sym-or-kw &env route-by) {})
         [name m] (pick-yarn-meta name {} docstring)
         spec (:spec m)]
     (list
      `do
      (when spec `(s/def ~k ~spec))
      `(register-yarn ~my)
      `(def ~name ~k)))))


(defmacro defyarn-method [name route-value bvec & body]
  (let [k (resolve-sym-or-kw &env name)
        y (gensym)]
    `(let [~y (yarn ~k ~bvec ~@body)]
       ~(impl/gen-reg-yarn-method k y route-value)
       (register-yarn (get *registry* ~k) false)  ;; reregister to trigger cycle-check
       )))


(defn yarn-prefer-method
  "Causes the multiyarn to prefer matches of dispatch-val-x over dispatch-val-y"
  [yarn dispatch-val-x dispatch-val-y]
  (let [y (if (keyword? yarn) (get *registry* yarn) yarn)
        m (impl/yarn-multifn y)]
    (prefer-method m dispatch-val-x dispatch-val-y)))


(defn yank
  "Computes and adds missing nodes into 'poy' map. Always returns deferred."
  [poy yarns]
  (let [yarns (condp instance? yarns
                java.lang.Iterable   yarns
                clojure.lang.Keyword [yarns]
                clojure.lang.IFn [yarns]
                (vec yarns))
        t (trace/if-tracing (when *tracing* (trace/create-tracer poy yarns)))
        executor (force *executor*)
        r (impl/yank' poy yarns *registry* t (knitty.javaimpl.ExecutionPool/adapt executor))]
    (->
     (trace/if-tracing
      (if t
        (let [td (delay (trace/capture-trace! t))
              r' (kd/bind
                  r
                  (fn [x]
                    (vary-meta x update :knitty/trace conj @td))
                  (fn [e]
                    (throw
                     (ex-info
                      (ex-message e)
                      (assoc (ex-data e) :knitty/trace (conj (:knitty/trace poy) @td))
                      (ex-cause e)))))]
          (let [f (fn [_] (conj (:knitty/trace poy) @td))]
            (reset-meta! r' {:knitty/trace (kd/bind r f f)}))
          r')
        r)
      r)
     (kd/kd-revoke-to r))))


(defn yank1
  "Computes and returns a single node. Logically similar to

   ```clojure
   (chain (yank poy [::yarn-key]) ::yarn-key)
   ```
   "
  [poy yarn]
  (let [t (trace/if-tracing (when *tracing* (trace/create-tracer poy [yarn])))
        executor *executor*
        r (impl/yank1' poy yarn *registry* t (knitty.javaimpl.ExecutionPool/adapt executor))]
    (->
     (trace/if-tracing
      (if t
        (let [td (delay (trace/capture-trace! t))
              r' (kd/bind
                  r
                  (fn [x]
                    x)
                  (fn [e]
                    (throw
                     (ex-info
                      (ex-message e)
                      (assoc (ex-data e) :knitty/trace (conj (:knitty/trace poy) @td))
                      (ex-cause e)))))]
          (let [f (fn [_] (conj (:knitty/trace poy) @td))]
            (reset-meta! r' {:knitty/trace (kd/bind r f f)}))
          r')
        r)
      r)
     (kd/kd-revoke-to r))))


(defn yank-error?
  "Returns true when exception is rethrown by 'yank'."
  [ex]
  (:knitty/yank-error? (ex-data ex) false))
