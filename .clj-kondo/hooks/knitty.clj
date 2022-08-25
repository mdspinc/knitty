(ns hooks.knitty
  (:require [clj-kondo.hooks-api :as api]))


(defn map-evens [f c]
  (map-indexed
   #(if (odd? %1) (f %2) %2) c))


(defn yarn* [[key bmap expr]]

  (cond

    (not (api/keyword-node? key))
    (api/reg-finding!
     (assoc (meta key)
            :message "yarn key must be a ::keyword"
            :type :knitty/yarn-key))

    (not (or (:namespaced? key) (qualified-keyword? (:k key))))
    (api/reg-finding!
     (assoc (meta key)
            :message "yarn key must be a qualified :ns/keyword"
            :type :knitty/yarn-key))

    (not (:namespaced? key))
    (api/reg-finding!
     (assoc (meta key)
            :message "yarn key should be an auto-resolved ::keyword"
            :type :knitty/yarn-key-autons)))

  (if-not (api/map-node? bmap)
    (api/reg-finding!
     (assoc (meta bmap)
            :message "yarn bindings must be a map"
            :type :knitty/yarn-binding))
    (doseq [[s _v] (partition-all 2 (:children bmap))]
      (when-not (api/token-node? s)
        (api/reg-finding!
         (assoc (meta s)
                :message "destructuring is not allowed in yarn bindings"
                :type :knitty/yarn-binding)))))

  (api/list-node
    [;; (let
     (api/token-node `let)
     ;; [ ~@bmap ]
     (api/vector-node
      (map-evens
       #(api/list-node [(api/token-node `deref) %])
       (:children bmap)))
     ;; ~expr
     expr
     ;; )
     ]))


(defn yarn [{:keys [node]}]
  (let [[_ key bmap expr] (:children node)]
    {:node (yarn* [key bmap expr])}))


(defn- skip-second [[x _ & xs]]
  (cons x xs))


(defn defyarn [{:keys [node]}]

  (let [node-child (rest (:children node))
        [name bmap expr] (if (-> node-child second string?)
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
                 :type :knitty/defyarn-name)))

       ;; (yarn ::~name ~bmap ~expr)
       (yarn* [name-kv bmap expr])

       ;; )
       ])
     }))


(defn doyank [{:keys [node]}]
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
