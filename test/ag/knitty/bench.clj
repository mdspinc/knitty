(ns ag.knitty.bench
  (:require [ag.knitty.core :refer [defyarn yank]]
            [manifold.deferred :as md]
            [criterium.core :as cc]
            [clj-async-profiler.core :as prof]))


(defn sample-few-deps [c]
  (let [r (java.util.Random. (hash-ordered-coll c))
        p (/ 10 (max 1 (count c)))]
    (filter (fn [_] (< (.nextDouble r) p)) c)))


(defn node-symbol [i]
  (symbol (format "node-%04d" i)))


(defn node-keyword [i]
  (keyword (name (ns-name *ns*)) (format "node-%04d" i)))


(defn mod-or-future [x y]
  (let [m (mod x y)]
    (if (zero? m) 
      m #_(future 0)
      m)))


(eval
 (list*
  `do
  (for [i (range 0 500)]
    (let [node-xxxx (node-symbol i)
          deps (map node-symbol (sample-few-deps (range 0 i)))
          ]
      `(defyarn ~node-xxxx
         ~(zipmap deps deps)
         (mod-or-future (+ 1 ~@deps) 25))))))


(eval
 (list*
  `do
  (for [i (range 500 1000)]
    (let [node-xxxx (node-symbol i)
          deps (map node-symbol (sample-few-deps (range (- i 100) i)))
          ]
      `(defyarn ~node-xxxx
         ~(zipmap deps deps)
         (mod-or-future (+ 1 ~@deps) 25))))))


(def target-keys (mapv node-keyword (range 950 1000)))
(time @(md/chain (yank {} target-keys) second count))


(defn dobench  []

  (print "Tracing off")
  (binding [ag.knitty.core/*tracing* false]
    (cc/quick-bench
     @(md/chain (yank {} target-keys) second count)))

  #_(println "Tracing on")
  #_(binding [ag.knitty.core/*tracing* true]
    (cc/quick-bench
     @(md/chain (yank {} target-keys) second count))))


(comment
  
  (time
   (dotimes [_ 100]
     @(md/chain (yank {} target-keys) second count)))

  (dobench)
  (dobench)

  (prof/profile (dobench))
  (prof/serve-files 8080)
  
  )