(ns knitty.pl-bench
  (:require [clj-async-profiler.core :as prof]
            [clojure.test :as t :refer [deftest]]
            [knitty.core :as kt :refer [defyarn yank yank1 yarn]]
            [knitty.test-util :as tu :refer [bench]]
            [plumbing.core :as pc]
            [plumbing.graph :as pg]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


(t/use-fixtures :once
  (t/join-fixtures
   [(tu/report-benchmark-fixture)]))


;; plain Fn

(defn stats-fn
  [xs]
  (let [n  (count xs)
        m  (/ (pc/sum identity xs) n)
        m2 (/ (pc/sum #(* % %) xs) n)
        v  (- m2 (* m m))]
    {:n n   ; count
     :m m   ; mean
     :m2 m2 ; mean-square
     :v v   ; variance
     }))


;; Plumbing Graph

#_{:clj-kondo/ignore [:unresolved-symbol]}
(def stats-pg-g
  (pg/compile
   {:n  (pc/fnk [xs]   (count xs))
    :m  (pc/fnk [xs n] (/ (pc/sum identity xs) n))
    :m2 (pc/fnk [xs n] (/ (pc/sum #(* % %) xs) n))
    :v  (pc/fnk [m m2] (- m2 (* m m)))}))

(defn stats-pg [x]
  (stats-pg-g {:xs x}))


;; Memoized Map

(defmacro assoc-when-miss [ctx key dep-fns expr]
  {:pre [(symbol? ctx), (keyword? key)]}
  `(if (contains? ~ctx ~key)
     ~ctx
     (-> ~ctx
         ~@dep-fns
         (as-> ~ctx (assoc ~ctx ~key ~expr)))))

(defn stats-mm-n [ctx]
  (assoc-when-miss ctx ::n [] (count (::xs ctx))))

(defn stats-mm-m [ctx]
  (assoc-when-miss ctx ::m [stats-mm-n] (/ (pc/sum identity (::xs ctx)) (::n ctx))))

(defn stats-mm-m2 [ctx]
  (assoc-when-miss ctx ::m2 [stats-mm-n] (/ (pc/sum #(* % %) (::xs ctx)) (::n ctx))))

(defn stats-mm-v [ctx]
  (assoc-when-miss ctx ::v [stats-mm-m stats-mm-m2] (- (::m2 ctx) (let [m (::m ctx)] (* m m)))))

(defn stats-mm [x]
  (-> {::xs x}
      (stats-mm-m)
      (stats-mm-m2)
      (stats-mm-v)))


;; KniTty

(defyarn xs)

(defyarn n {xs xs}
  (count xs))

(defyarn m {xs xs, n n}
  (/ (pc/sum identity xs) n))

(defyarn m2 {xs xs, n n}
  (/ (pc/sum #(* % %) xs) n))

(defyarn v {m m, m2 m2}
  (- m2 (* m m)))

(defn stats-kt [x]
  @(yank {::xs x} [m m2 v]))

(defn stats-kt1 [x]
  @(yank1 {::xs x} (yarn ::stats {m m, m2 m2, v v} {:m m, :m2 m2, :v v})))


;; bench

(defn testem [x]
  (kt/set-executor! nil)
  (kt/enable-tracing! false)
  (bench :stats-fn    (stats-fn x))
  (bench :stats-pg    (stats-pg x))
  (bench :stats-kt    (stats-kt x))
  (bench :stats-kt1   (stats-kt1 x))
  (bench :stats-mm    (stats-mm x)))


(deftest ^:benchmark singletone-list
  (testem [1]))

(deftest ^:benchmark short-list
  (testem [1 2 3 6]))

(deftest ^:benchmark long-list
  (testem (vec (range 100))))


(comment

  (require '[clj-async-profiler.core :as prof])

  (kt/set-executor! nil)
  (kt/enable-tracing! false)
  (prof/serve-ui 8888)

  (testem [1 2 3 4 5])

  (binding [kt/*executor* nil
            kt/*tracing* false]
    (dotimes [_ 1000000]
      (let [x (vec (take (inc (rand-int 10))
                         (repeatedly #(rand-int 100))))]
        (stats-mm x)
        (stats-kt1 x))))

  (do
     (dotimes [_ 10000000]
       (stats-kt1 [3]))

    (prof/profile
     (dotimes [_ 10000000]
       (stats-kt1 [3]))))

  )