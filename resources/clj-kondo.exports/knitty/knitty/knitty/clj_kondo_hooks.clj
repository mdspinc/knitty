(ns knitty.clj-kondo-hooks
  (:require [clj-kondo.hooks-api :as api]))


(defn map-evens [f c]
  (map-indexed
   #(if (odd? %1) (f %2) %2) c))


(defn- eval-node-meta [n]
  (into {}
        (comp
         (map api/sexpr)
         (map #(if (map? %) % {% true})))
        (:meta n)))


(defn infer-param-type [bind-node]
  (let [{:keys [sync lazy defer case]} (eval-node-meta bind-node)
        n (count (filter identity [sync lazy defer case]))]
    (when (> n 1)
      (api/reg-finding!
       (assoc (meta bind-node)
              :message (str "yarn dependency may be marked with one of :sync, :defer, :lazy or :case")
              :type :knitty/invalid-yarn-binding)))
    (cond
      lazy  :lazy
      defer :defer
      case  :case
      :else :sync)))


(defn check-yankfn-arg [v0]
  (when-not (or
             (and (api/token-node? v0) (ident? (:value v0)))
             (and (api/keyword-node? v0) (or (:namespaced? v0) (qualified-keyword? (:k v0)))))
    (api/reg-finding!
     (assoc (meta v0)
            :message "case argument val must be a symbol or qualified keyword"
            :type :knitty/invalid-yarn-binding))))


(defn yarn* [[key bmap & body]]

  (cond

    (not (api/keyword-node? key))
    (api/reg-finding!
     (assoc (meta key)
            :message "yarn key must be a ::keyword"
            :type :knitty/invalid-yarn-key))

    (not (or (:namespaced? key) (qualified-keyword? (:k key))))
    (api/reg-finding!
     (assoc (meta key)
            :message "yarn key must be a qualified ::keyword"
            :type :knitty/invalid-yarn-key))

    (not (:namespaced? key))
    (api/reg-finding!
     (assoc (meta key)
            :message "yarn key should be an auto-resolved ::keyword"
            :type :knitty/explicit-ns-in-yarn-key)))

  (if-not (api/map-node? bmap)

    (api/reg-finding!
     (assoc (meta bmap)
            :message "yarn bindings must be a map"
            :type :knitty/invlid-yarn-binding))

    (doseq [[s v] (partition-all 2 (:children bmap))
            :let [btype (infer-param-type s)]]

      (when-not (and (api/token-node? s)
                     (simple-symbol? (:value s)))
        (api/reg-finding!
         (assoc (meta s)
                :message "binding must be an unqualified symbol"
                :type :knitty/invalid-yarn-binding)))

      (when (= :case btype)
        (cond
          (or (api/set-node? v) (api/vector-node? v))
          (doseq [v0 (:children v)]
            (check-yankfn-arg v0))

          (api/map-node? v)
          (doseq [[_s0 v0] (partition-all 2 (:children v))]
            (check-yankfn-arg v0))

          :else
          (api/reg-finding!
           (assoc (meta v)
                  :message "yarn dependency must be a map or set"
                  :type :knitty/invalid-yarn-binding))))

      (when (and
             (not= :case btype)
             (not (or
                   (and (api/token-node? v) (ident? (:value v)))
                   (and (api/keyword-node? v) (or (:namespaced? v) (qualified-keyword? (:k v)))))))
        (api/reg-finding!
         (assoc (meta v)
                :message (if (api/map-node? v)
                           "yarn dependency should not be a map or should be marked with ^:case"
                           "yarn dependency should be a symbol or qualified keyword")
                :type :knitty/invalid-yarn-binding)))))

  (api/list-node
   (list* ;; (let
    (api/token-node `let)
     ;; [ ~@bmap ]
    (api/vector-node
     (concat
      (map-evens
      ; [~s (do ~v (Object.))] - force type to be :any
       #(api/list-node [(api/token-node `do) % (api/list-node (list (api/token-node 'java.lang.Object.)))])
       (:children bmap))))
    ;; ~key
    key
    ;; ~expr
    body
     ;; )
    )))


(defn yarn [{:keys [node]}]
  (let [[_ key bmap & body] (:children node)]
    {:node (yarn* (list* key bmap body))}))


(defn- skip-second [[x _ & xs]]
  (cons x xs))


(defn defyarn [{:keys [node]}]

  (let [node-child (rest (:children node))
        [name bmap & body] (if (-> node-child second api/string-node?)
                             (skip-second node-child)
                             node-child)

        almeta (concat (:meta name) (:meta bmap))
        spec (first (for [m almeta
                          :when (api/map-node? m)
                          [k v] (partition 2 (:children m))
                          :when (api/keyword-node? k)
                          :when (= :spec (:k k))]
                      v))
        name-kv (api/keyword-node (keyword (:string-value name "INVALID")) [true])]

    {:node
     (api/list-node
      [(api/token-node 'do)
       ;; (do

       ;; (spec/def ~name ~spec)
       (when spec
         (api/list-node
          [(api/token-node 'clojure.spec.alpha/def)
           name-kv
           spec]))

       ;; (yarn ::~name ~bmap ~body)
       (yarn* (list* (api/reg-keyword!
                      (with-meta name-kv (-> (meta name)
                                             (assoc :end-col (-> name meta :col))  ;; zero-width - workaround highligting issues
                                             (assoc :clj-kondo/ignore [:clojure-lsp/unused-public-var])))
                      'knitty.core/defyarn)
                     bmap
                     body))
       ;; (def ~name ::~name)
       (if (and (api/token-node? name) (-> name :value symbol?))
         (api/list-node [(api/token-node 'def) name name-kv])
         (api/reg-finding!
          (assoc (meta name)
                 :message "name must be a symbol"
                 :type :knitty/invalid-defyarn)))

       ;; )
       ])}))


(defn declare-yarn [{:keys [node]}]

  (let [node-child (rest (:children node))
        [name] node-child]

    (when-not (or
               (and (api/keyword-node? name)
                    (or
                     (:namespaced? name)
                     (-> name :k qualified-keyword?)))
               (and (api/token-node? name)
                    (-> name :value symbol?)))
      (api/reg-finding!
       (assoc (meta name)
              :message "name must be a symbol or or qualified keyword"
              :type :knitty/invalid-defyarn)))

    (when (and (api/token-node? name)
               (-> name :value symbol?))
      {:node
       ;; (declare ~name)
       (api/list-node
        (list* (api/token-node 'clojure.core/declare)
               node-child))})))


(defn defyarn-multi [{:keys [node]}]
  (let [node-child (rest (:children node))
        [name route-key & opts-raw] (if (-> node-child second api/string-node?)
                                      (skip-second node-child)
                                      node-child)
        opts (apply array-map (map api/sexpr opts-raw))]

    (when-let [p (seq (dissoc opts :hierarchy :default))]
      (api/reg-finding!
       (assoc (meta (first p))
              :message "unexpected extra parameter"
              :type :knitty/invalid-defyarn)))

    {:node
     (api/list-node
      (list
       (api/token-node `do)

       (vary-meta
        (api/list-node (list* (api/token-node `array-map) opts-raw))
        assoc :clj-kondo/ignore [:unused-value])

       (:node
        (defyarn {:node
                  (api/list-node
                   (list
                    (api/token-node 'knitty.core/defyarn)
                    name
                    (api/map-node [(api/token-node '_) route-key])
                    (api/token-node nil)))}))))}))


(defn defyarn-method [{:keys [node]}]
  (let [node-child (rest (:children node))
        [name route-val bvec & body] node-child
        name' (if (and (api/token-node? name)
                       (-> name :value symbol?))
                (api/keyword-node (keyword (:string-value name "INVALID")) [true])
                name)]
    {:node
     (api/list-node
      [(api/token-node 'do)
       ;; (do
       route-val
       name
       ;; (yarn :name ...)
       (yarn* (list* name' bvec body))])}))

