(ns ag.knitty.core
  (:require [ag.knitty.impl :as impl :refer [yank* yarn-key]]
            [ag.knitty.trace :refer [create-tracer]]
            [clojure.java.browse]
            [clojure.java.browse-ui]
            [clojure.java.shell]
            [clojure.spec.alpha :as s]
            [manifold.deferred :as md]
            [manifold.executor]))

;; >> API
(declare yank)        ;; func,  (yank <map> [yarn-keys]) => @<new-map>
(declare yarn)        ;; macro, (yarn keyword { (bind-symbol keyword)* } expr)
(declare defyarn)     ;; macro, (defyarn name doc? spec? { (bind-symbol keyword)* } <expr>)
(declare with-yarns)  ;; macro, (with-yarns [<yarns>+] <body ...>)
;; << API

;; mapping {keyword => Yarn}
(def ^:dynamic *registry* (atom {}))
(def ^:dynamic *tracing* true)


(defn register-yarn [yarn]
  (let [k (yarn-key yarn)]
    (when-not (qualified-keyword? k)
      (throw (ex-info "yarn must be a qualified keyword" {::yarn k})))
    (swap! *registry* assoc k yarn)))


(s/def ::yarn-binding (s/map-of symbol? ident?))
(s/def ::bind-and-expr (s/? (s/cat :bind ::yarn-binding :expr any?)))

(s/def ::yarn (s/cat
               :name qualified-keyword?
               :bind-and-expr ::bind-and-expr))

(defmacro yarn
  [k & exprs]
  (let [bd (cons k exprs)
        cf (s/conform ::yarn bd)]
    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::yarn bd))))
    (let [{{:keys [bind expr]} :bind-and-expr} cf
          bind (or bind {})
          expr (or expr `(throw (ex-info "missing input-only yarn" {::yarn ~k})))]
      (impl/gen-yarn k bind expr))))


(defn- pick-yarn-meta [obj ex-meta doc]
  (let [m (merge ex-meta (meta obj))
        doc (or doc (:doc m))
        m (if doc (assoc m :doc doc) m)]
    [(with-meta obj m)
     m]))


(s/def ::defyarn (s/cat
                  :name symbol?
                  :doc (s/? string?)
                  :bind-and-expr ::bind-and-expr))
(defmacro defyarn
  [nm & body]
  (let [bd (cons nm body)
        cf (s/conform ::defyarn bd)]

    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::defyarn bd))))

    (let [k (keyword (-> *ns* ns-name name) (name nm))
          {doc :doc, {:keys [bind expr]} :bind-and-expr} cf
          bind (or bind {})
          [nm m] (pick-yarn-meta nm (meta bind) doc)
          expr (or expr `(throw (java.lang.UnsupportedOperationException. "input-only yarn")))
          spec (:spec m)
          bind (with-meta bind m)]

      (list
       `do
       (when spec `(s/def ~k ~spec))
       `(register-yarn (yarn ~k ~bind ~expr))
       `(def ~nm ~k)))))


(defn yank
  [poy yarns]
  (assert (map? poy) "poy should be a map")
  (assert (sequential? yarns) "yarns should be vector/sequence")
  (yank* poy yarns @*registry* (when *tracing* (create-tracer poy yarns))))


(defmacro with-yarns [yarns & body]
  `(binding [*registry* (atom
                         (into @*registry*
                               (map #(vector (yarn-key %) %))
                               ~yarns))]
     ~@body))


(defmacro doyank
  [poy binds & body]
  `(md/chain'
    (yank ~poy ~(vec (vals binds)))
    (fn [[[~@(keys binds)] ctx#]]
      ~@body
      ctx#)))


;; TODO: name?
(defn tieknot [from dst]
     ;; TODO: check (abstract? to)
  (register-yarn #=(impl/gen-yarn-ref dst from))
  from)
