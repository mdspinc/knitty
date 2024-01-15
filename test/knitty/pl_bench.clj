(ns knitty.pl-bench
  (:require [clj-async-profiler.core :as prof]
            [clojure.test :as t :refer [deftest]]
            [knitty.core :refer [defyarn yank]]
            [knitty.test-util :refer [bench tracing-enabled-fixture]]
            [plumbing.core :as pc]
            [plumbing.graph :as pg]))


(t/use-fixtures :once
  (t/join-fixtures
   [(tracing-enabled-fixture false)]))


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

(defn stats-fn-ex
  [xs]
  (let [s (stats-fn xs)]
    (assoc s
           :sd (Math/sqrt (:v s)))))


(def stats-graph
  {:n  (pc/fnk [xs]   (count xs))
   :m  (pc/fnk [xs n] (/ (pc/sum identity xs) n))
   :m2 (pc/fnk [xs n] (/ (pc/sum #(* % %) xs) n))
   :v  (pc/fnk [m m2] (- m2 (* m m)))})


(def stats-pg
  (comp
   (pg/compile stats-graph)
   (fn [x] {:xs x})))

(def stats-pg-ex
  (comp
   (pg/compile
    (assoc stats-graph
           :sd (pc/fnk [^double v] (Math/sqrt v))))
   (fn [x] {:xs x})))

(do
  (defyarn xs)
  (defyarn n {xs xs} (count xs))
  (defyarn m {xs xs, ^long n n} (/ (pc/sum identity xs) n))
  (defyarn m2 {xs xs, ^long n n} (/ (pc/sum #(* % %) xs) n))
  (defyarn v {^double m m, ^double m2 m2} (- m2 (* m m)))
  (defyarn sd {^double v v} (Math/sqrt v))

  (defn stats-kt [x]
    @(yank {xs x} [m m2 v]))

  (defn stats-kt-ex [x]
    @(yank {xs x} [m m2 v sd])))


(defn testem [x]
  (bench :stats-fn    (stats-fn x))
  (bench :stats-pg    (stats-pg x))
  (bench :stats-kt    (stats-kt x))
  (bench :stats-fn-ex (stats-fn-ex x))
  (bench :stats-pg-ex (stats-pg-ex x))
  (bench :stats-kt-ex (stats-kt-ex x)))


(deftest ^:benchmark short-list
  (testem [1 2 3 6]))


(deftest ^:benchmark long-list
  (testem (vec (range 100))))


(comment

  (stats-kt [1 2 3 6])

  (dotimes [_ 10000]
    (stats-kt [1 2 3 6]))

  (prof/profile
   (dotimes [_ 10000000]
     (stats-kt [1 2 3 6])))

  (prof/profile
   (dotimes [_ 10000000]
     (stats-pg [1 2 3 6])))

  (prof/serve-ui 8080)
  )