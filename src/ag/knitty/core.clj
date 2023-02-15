(ns ag.knitty.core
  (:require [ag.knitty.deferred :as kd]
            [ag.knitty.impl :as impl]
            [ag.knitty.trace :refer [create-tracer]]
            [clojure.java.browse]
            [clojure.java.browse-ui]
            [clojure.java.shell]
            [clojure.spec.alpha :as s]))

;; >> API
(declare yank)        ;; func,  (yank <map> [yarn-keys]) => @<new-map>
(declare yarn)        ;; macro, (yarn keyword { (bind-symbol keyword)* } expr)
(declare defyarn)     ;; macro, (defyarn name doc? spec? { (bind-symbol keyword)* } <expr>)
(declare doyank)      ;; macro, (doyank <map> { (bind-symbol keyword)* } <expr>)
(declare doyank!)     ;; macro, (doyank <map> { (bind-symbol keyword)* } <expr>)
;; << API

;; mapping {keyword => Yarn}
(def ^:dynamic *registry* (impl/create-registry))
(def ^:dynamic *tracing* false)


(defn register-yarn
  ([yarn]
   (register-yarn yarn false))
  ([yarn no-override]
  (let [k (impl/yarn-key yarn)]
    (when-not (qualified-keyword? k)
      (throw (ex-info "yarn must be a qualified keyword" {::yarn k})))
    (if no-override
      (alter-var-root #'*registry* #(if (contains? % k) % (assoc % k yarn)))
      (alter-var-root #'*registry* assoc k yarn)))))


(s/def ::yarn-binding (s/map-of symbol? ident?))
(s/def ::bind-and-body (s/? (s/cat :bind ::yarn-binding :body (s/+ any?))))

(s/def ::yarn (s/cat
               :name qualified-keyword?
               :bind-and-body ::bind-and-body))


(defmacro declare-yarn [nm]
  {:pre [(ident? nm)]}
  (let [k (keyword (or (namespace nm)
                       (-> *ns* ns-name name))
                   (name nm))]
     `(do (register-yarn (impl/fail-always-yarn ~k ~(str "declared-only yarn " k)) true)
          ~k)))


(defmacro yarn
  [k & exprs]
  (if (empty? exprs)
    `(impl/fail-always-yarn ~k ~(str "input-only yarn " k))
    (let [bd (cons k exprs)
          cf (s/conform ::yarn bd)]
      (when (s/invalid? cf)
        (throw (Exception. (s/explain-str ::yarn bd))))
      (let [{{:keys [bind body]} :bind-and-body} cf
            bind (or bind {})
            bind (update-vals bind #(if (keyword? %) % @(resolve &env %)))]
        (when-not (every? qualified-keyword? (vals bind))
          (throw (Exception. "yarn bindings must be qualified keywords")))
        (impl/gen-yarn k bind `(do ~@body))))))


(defn- pick-yarn-meta [obj ex-meta doc]
  (let [m (merge ex-meta (meta obj))
        doc (or doc (:doc m))
        m (if doc (assoc m :doc doc) m)]
    [(with-meta obj m)
     m]))


(s/def ::defyarn (s/cat
                  :name symbol?
                  :doc (s/? string?)
                  :bind-and-body ::bind-and-body))
(defmacro defyarn
  [nm & body]
  (let [bd (cons nm body)
        cf (s/conform ::defyarn bd)
        k (keyword (-> *ns* ns-name name) (name nm))]

    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::defyarn bd))))

    (let [{doc :doc, {:keys [bind body]} :bind-and-body} cf
          bind (or bind {})
          [nm m] (pick-yarn-meta nm (meta bind) doc)
          spec (:spec m)
          bind (with-meta bind m)
          y (if (empty? body)
              `(impl/fail-always-yarn ~k ~(str "input-only yarn " k))
              `(yarn ~k ~bind ~@body))]
      (list
       `do
       (when spec `(s/def ~k ~spec))
       `(register-yarn ~y)
       `(def ~nm ~k)))))


(defn yank
  [poy yarns]
  (assert (map? poy) "poy should be a map")
  (assert (sequential? yarns) "yarns should be vector/sequence")
  (impl/yank0 poy yarns *registry* (when *tracing* (create-tracer poy yarns))))


(defmacro doyank
  [poy binds & body]
  (let [k (keyword (-> *ns* ns-name name) (name (gensym "doyank")))]
    `(kd/chain-revoke'
      (yank ~poy [(yarn ~k ~binds (do ~@body))])
      (juxt ~k identity))))


(defn yank-error? [ex]
  (boolean (:knitty/yanked-yarns (ex-data ex))))


(defmacro doyank!
  [poy binds & body] 
  (let [k (keyword (-> *ns* ns-name name) (name (gensym "doyank")))]
    `(yank ~poy [(yarn ~k ~binds (do ~@body))])))


(defn tieknot [from dst]
  (register-yarn (eval (impl/gen-yarn-ref dst from)))
  from)
