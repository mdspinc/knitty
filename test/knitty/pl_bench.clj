(ns knitty.pl-bench
  (:require [clojure.test :as t :refer [deftest]]
            [knitty.core :refer [defyarn yank yank1 yarn]]
            [knitty.test-util :refer [bench do-defs tracing-enabled-fixture]]
            [plumbing.core :as pc]
            [plumbing.graph :as pg]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

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


 #_{:clj-kondo/ignore [:unresolved-symbol]}
(def stats-graph
  {:n  (pc/fnk [xs]   (count xs))
   :m  (pc/fnk [xs n] (/ (pc/sum identity xs) n))
   :m2 (pc/fnk [xs n] (/ (pc/sum #(* % %) xs) n))
   :v  (pc/fnk [m m2] (- m2 (* m m)))})

(def stats-pg
  (comp
   (pg/compile stats-graph)
   (fn [x] {:xs x})))


(declare stats-kt)
(declare stats-kt1)


(defn testem [x]

  #_{:clj-kondo/ignore [:inline-def]}
  (do-defs

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
     @(yank {xs x} [m m2 v]))

   (defn stats-kt1 [x]
     @(yank1 {xs x} (yarn ::stats {m m, m2 m2, v v} {:m m, :m2 m2, :v v})))
   )

  (bench :stats-fn    (stats-fn x))
  (bench :stats-pg    (stats-pg x))
  (bench :stats-kt    (stats-kt x))
  (bench :stats-kt1   (stats-kt1 x))
  )


(deftest ^:benchmark singletone-list
  (testem [1]))

(deftest ^:benchmark short-list
  (testem [1 2 3 6]))

(deftest ^:benchmark long-list
  (testem (vec (range 100))))

