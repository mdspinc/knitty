(ns ag.knitty.bench1
  (:require
   [clojure.test :as t :refer [deftest is testing]]
   [ag.knitty.core :refer [defyarn yank]]
   [manifold.deferred :as md]
   [criterium.core :as cc]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


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
      (md/future m)
      m)))


(defonce init-graph1k
  (delay
    (println "compile 1k nodes...")
    (eval
     (list* `do
            (concat
             (for [i (range 0 200)]
               (let [node-xxxx (node-symbol i)
                     deps (map node-symbol (sample-few-deps (range 0 i)))]
                 `(defyarn ~node-xxxx
                    ~(zipmap deps deps)
                    (mod-or-future (+ 1 ~@deps) 5))))
             (for [i (range 200 400)]
               (let [node-xxxx (node-symbol i)
                     deps (map node-symbol (sample-few-deps (range (- i 50) i)))]
                 `(defyarn ~node-xxxx
                    ~(zipmap deps deps)
                    (mod-or-future (+ 1 ~@deps) 5)))))))
    (println "1k nodes had been compiled")))


(defmacro do-bench [& body]
  `(do
     (println (t/testing-contexts-str) " - run bench...")
     (cc/quick-bench
      ~@body)))


(deftest ^:benchmark graph-test-1k

  @init-graph1k
  (println "run benchmarks...")

  (let [target-keys (mapv node-keyword (range 1 90))]

    (testing "tracing disabled"
      (binding [ag.knitty.core/*tracing* false]
        (do-bench @(md/chain (yank {} target-keys) second count))))

    #_(testing "tracing enabled"
      (binding [ag.knitty.core/*tracing* false]
        (cc/quick-bench
         @(md/chain (yank {} target-keys) second count))))))



(deftest ^:stress check-big-graph

  @init-graph1k
  (dotimes [i 400]
    (println ">>" i)
    (let [target-keys (mapv node-keyword (range 1 i))]
      (binding [ag.knitty.core/*tracing* false]
        @(md/chain (yank {} target-keys) second count)))))
