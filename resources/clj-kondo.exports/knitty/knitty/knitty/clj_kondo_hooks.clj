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
        (if (not (api/map-node? v))
          (api/reg-finding!
           (assoc (meta v)
                  :message "yarn dependency must be a map"
                  :type :knitty/invalid-yarn-binding))
          (doseq [[_s0 v0] (partition-all 2 (:children v))]
            (when-not (or
                       (api/token-node? v0) (ident? (:value v0))
                       (api/keyword-node? v0) (or (:namespaced? v0) (qualified-keyword? (:k v0))))
              (api/reg-finding!
               (assoc (meta v0)
                      :message "yankfn argument val must be a symbol or qualified keyword"
                      :type :knitty/invalid-yarn-binding))))))

      (when (and
             (not= :yankfn btype)
             (not (or
                   (api/token-node? v) (ident? (:value v))
                   (api/keyword-node? v) (or (:namespaced? v) (qualified-keyword? (:k v))))))
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
        [name bmap & body] (if (-> node-child second string?)
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
                 :type :knitty/invalid-defyarn-name)))

       ;; (yarn ::~name ~bmap ~body)
       (yarn* (list* name-kv bmap body))

       ;; )
       ])}))


(defn doyank* [{:keys [node]}]
  (let [[_ poy bmap & body] (:children node)]
    ;; (doyank x {} body) => (do (yarn ::fake {} body) x)
    {:node
     (api/list-node
      [(api/token-node `do)
       (yarn*
        [::fake
         bmap
         (api/list-node
          (list* (api/token-node `do) body))])
       poy])}))


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
              :type :knitty/invalid-defyarn-name)))

    (when (and (api/token-node? name)
               (-> name :value symbol?))
      {:node
       ;; (declare ~name)
       (api/list-node
        (list* (api/token-node 'clojure.core/declare)
               node-child))}
      )))

