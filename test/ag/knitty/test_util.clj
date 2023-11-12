(ns ag.knitty.test-util
  (:require [manifold.deferred :as md]))


(defn sample-few-deps [c]
  (let [r (java.util.Random. (hash-ordered-coll c))
        p (/ 10 (max 1 (count c)))]
    (filter (fn [_] (< (.nextDouble r) p)) c)))


(defn node-symbol [prefix i]
  (symbol (format "%s-%04d" prefix i)))


(defn node-keyword [prefix i]
  (keyword (name (ns-name *ns*)) (format "%s-%04d" prefix i)))


(defn mod-or-future [x y]
  (let [m (mod x y)]
    (if (zero? m)
      (md/future m)
      m)))


(defmacro yarn-graph [prefix size deps-fn body-fn]
  (eval
   (list*
    `do
    (for [i (range 0 size)]
      (let [node-xxxx (node-symbol prefix i)
            deps (map #(node-symbol prefix %) (deps-fn i))]
        `(defyarn ~node-xxxx
           ~(zipmap deps deps)
           (~body-fn ~i ~@deps)))))))
