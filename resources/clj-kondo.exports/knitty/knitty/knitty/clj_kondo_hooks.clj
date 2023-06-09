(ns knitty.clj-kondo-hooks
  (:require [clj-kondo.hooks-api :as api]))


(defn map-evens [f c]
  (map-indexed
   #(if (odd? %1) (f %2) %2) c))


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

    (doseq [[s v] (partition-all 2 (:children bmap))]

      (when-not (and (api/token-node? s) (simple-symbol? (:value s)))
        (api/reg-finding!
         (assoc (meta s)
                :message "binding must be an unqualified symbol"
                :type :knitty/invalid-yarn-binding)))

      (when-not 
       (or 
        (and (api/token-node? v) (ident? (:value v)))
        (and (api/keyword-node? v) (or (:namespaced? v) (qualified-keyword? (:k v)))))
        (api/reg-finding!
         (assoc (meta v)
                :message "yarn dependency must be a symbol or qualified keyword"
                :type :knitty/invalid-yarn-binding))))
    )

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
       ])
     }))


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
