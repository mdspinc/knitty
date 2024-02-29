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
  (let [{:keys [sync lazy defer yankfn]} (eval-node-meta bind-node)
        n (count (filter identity [sync lazy defer yankfn]))]
    (when (> n 1)
      (api/reg-finding!
       (assoc (meta bind-node)
              :message (str "yarn dependency may be marked with one of :sync, :defer, :lazy or :yankfn")
              :type :knitty/invalid-yarn-binding)))
    (cond
      lazy :lazy
      defer :defer
      yankfn :yankfn
      :else :sync)))


(defn check-yankfn-arg [v0]
  (when-not (or
             (and (api/token-node? v0) (ident? (:value v0)))
             (and (api/keyword-node? v0) (or (:namespaced? v0) (qualified-keyword? (:k v0)))))
    (api/reg-finding!
     (assoc (meta v0)
            :message "yankfn argument val must be a symbol or qualified keyword"
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

      (when (= :yankfn btype)
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
             (not= :yankfn btype)
             (not (or
                   (and (api/token-node? v) (ident? (:value v)))
                   (and (api/keyword-node? v) (or (:namespaced? v) (qualified-keyword? (:k v)))))))
        (api/reg-finding!
         (assoc (meta v)
                :message (if (api/map-node? v)
                           "yarn dependency should not be a map or should be marked with ^:yankfn"
                           "yarn dependency should be a symbol or qualified keyword")
                :type :knitty/invalid-yarn-binding)))))

  (api/list-node
   (list* ;; (let
    (api/token-node `let)
     ;; [ ~@bmap ]
    (api/vector-node
     (map-evens
      #(api/list-node [(api/token-node `deref) %])
      (:children bmap)))
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

        name-kv (api/keyword-node (keyword (:string-value name "INVALID")) true)]

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

       ;; (def ~name ::~name)
       (if (and (api/token-node? name) (-> name :value symbol?))
         (api/list-node [(api/token-node 'def) name name-kv])
         (api/reg-finding!
          (assoc (meta name)
                 :message "name must be a symbol"
                 :type :knitty/invalid-defyarn)))

       ;; (yarn ::~name ~bmap ~body)
       (yarn* (list* name-kv bmap body))

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
        [name route-key & exx] (if (-> node-child second api/string-node?)
                                 (skip-second node-child)
                                 node-child)]
    (when (seq exx)
      (api/reg-finding!
       (assoc (meta (first exx))
              :message "unexpected extra parameter"
              :type :knitty/invalid-defyarn)))
     (defyarn
       {:node
        (api/list-node
         (list*
          (api/token-node 'defyarn)
          name
          (api/map-node [(api/token-node '_) route-key])
          (api/token-node nil)))})))


(defn defyarn-method [{:keys [node]}]
  (let [node-child (rest (:children node))
        [name route-val bvec & body] node-child
        name' (if (and (api/token-node? name)
                       (-> name :value symbol?))
                (api/token-node ::SYMBOL)
                name)]
    {:node
     (api/list-node
      [(api/token-node 'do)
       ;; (do
       route-val
       name
       ;; (yarn :name ...)
       (yarn* [name' bvec body])])}))

