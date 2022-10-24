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
(declare with-yarns)  ;; macro, (with-yarns [<yarns>+] <body ...>)
;; << API

;; mapping {keyword => Yarn}
(def ^:dynamic *registry* (atom (impl/create-registry)))
(def ^:dynamic *tracing* true)


(defn register-yarn [yarn]
  (let [k (impl/yarn-key yarn)]
    (when-not (qualified-keyword? k)
      (throw (ex-info "yarn must be a qualified keyword" {::yarn k})))
    (swap! *registry* assoc k yarn)))


(s/def ::yarn-binding (s/map-of symbol? ident?))
(s/def ::bind-and-body (s/? (s/cat :bind ::yarn-binding :body (s/* any?))))

(s/def ::yarn (s/cat
               :name qualified-keyword?
               :bind-and-body ::bind-and-body))

(defmacro yarn
  [k & exprs]
  (let [bd (cons k exprs)
        cf (s/conform ::yarn bd)]
    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::yarn bd))))
    (let [{{:keys [bind body]} :bind-and-body} cf
          bind (or bind {})
          bind (update-vals bind #(if (keyword? %) % @(resolve &env %)))
          body (if (seq body) body `((throw (java.lang.UnsupportedOperationException. ~(str "input-only yarn " k)))))]
      (when-not (every? qualified-keyword? (vals bind))
        (throw (Exception. "yarn bindings must be qualified keywords")))
      (impl/gen-yarn k bind `(do ~@body)))))


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
        cf (s/conform ::defyarn bd)]

    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::defyarn bd))))

    (let [k (keyword (-> *ns* ns-name name) (name nm))
          {doc :doc, {:keys [bind body]} :bind-and-body} cf
          bind (or bind {})
          [nm m] (pick-yarn-meta nm (meta bind) doc)
          spec (:spec m)
          bind (with-meta bind m)]

      (list
       `do
       (when spec `(s/def ~k ~spec))
       `(register-yarn (yarn ~k ~bind ~@body))
       `(def ~nm ~k)))))


(defn yank
  [poy yarns]
  (assert (map? poy) "poy should be a map")
  (assert (sequential? yarns) "yarns should be vector/sequence")
  (impl/yank0 poy yarns @*registry* (when *tracing* (create-tracer poy yarns))))


(defn yank*
  [poy yarns]
  (kd/chain-revoke'
   (yank poy yarns)
   (fn [poy'] [(map (comp poy' impl/yarn-key) yarns) poy'])))



(defmacro with-yarns [yarns & body]
  `(binding [*registry* (atom (reduce #(assoc %1 (impl/yarn-key %2) %2) @*registry* ~yarns))]
     ~@body))


(defmacro doyank!
  [poy binds & body]
  `(kd/chain-revoke'
    (yank ~poy ~(vec (vals binds)))
    (fn [[[~@(keys binds)] ctx#]]
      (kd/chain-revoke' (do ~@body) (constantly ctx#)))))


(defn tieknot [from dst]
  (register-yarn (eval (impl/gen-yarn-ref dst from)))
  from)
