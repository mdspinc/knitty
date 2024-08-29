(ns knitty.yank-test
  {:clj-kondo/ignore [:inline-def]}
  (:require
   [clojure.test :as t :refer [deftest is testing]]
   [knitty.core :as knitty :refer [defyarn yank*]]
   [knitty.test-util :refer [do-defs]]))


(deftest yank-result-test

  (do-defs
   (defyarn y1 {} 1)
   (defyarn y2 {y1 y1} (* y1 2))
   (defyarn y3 {y2 y2} (* y2 2))

   (testing "pick yarns from result"
     (let [yr @(yank* {y1 10} [y2])]

       (is (== 10 (get yr y1)))
       (is (== 20 (get yr y2)))
       (is (nil? (get yr y3)))

       (is (== 20 (get yr y2 ::nope)))
       (is (= ::nope (get yr y3 ::nope)))

       (is (== (y1 yr) 10))
       (is (== (y2 yr) 20))
       (is (= ::nope (y3 yr ::nope)))))

   (testing "yank result resolves into map"
     (let [yr @(yank* {y1 10} [y2])]
       (is (= {y1 10, y2 20} @yr))))

   (testing "yank result is reducible"
     (let [yr @(yank* {y1 10} [y2])]
       (is (== 30 (reduce (fn [a [_ v]] (+ a v)) 0 yr)))))

   (testing "yank result is kwreducible"
     (let [yr @(yank* {y1 10} [y2])]
       (is (== 30 (reduce-kv (fn [a _ v] (+ a v)) 0 yr)))))
   ))


(comment
  (clojure.test/test-ns *ns*)
  )
