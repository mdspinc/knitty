(ns knitty.yank-test
  {:clj-kondo/ignore [:inline-def]}
  (:require [clojure.template :refer [do-template]]
            [clojure.test :as t :refer [deftest is testing]]
            [knitty.core :as knitty :refer [defyarn yank yank* yank1]]
            [knitty.deferred :as kd]
            [knitty.test-util :as tu :refer [do-defs]]
            [manifold.deferred :as md]))


(t/use-fixtures :each
  (t/join-fixtures
   [(tu/reset-registry-fixture)]))


(deftest yank-simple-test

  (do-defs
   (defyarn y1 {} 1)
   (defyarn y2 {y1 y1} (* y1 2))
   (defyarn y3 {y2 y2} (* y2 2))

   (testing "result of yank"
     (is (kd/deferred? (yank {} [y2])))
     (is (map? @(yank {y1 10} [y2])))
     (is (= {y1 10, y2 20} @(yank {y1 10} [y2]))))

   (testing "result of yank"
     (is (kd/deferred? (yank1 {} y2)))
     (is (= 20 @(yank1 {y1 10} y2))))))


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

   (testing "yank* result resolves into map"
     (let [yr @(yank* {y1 10} [y2])]
       (is (= {y1 10, y2 20} @yr))
       (is (= {y1 10, y2 20} (knitty/yr->map yr)))))

   (testing "yank* result is reducible"
     (let [yr @(yank* {y1 10} [y2])]
       (is (== 30 (reduce (fn [a [_ v]] (+ a v)) 0 yr)))))

   (testing "yank* result is kwreducible"
     (let [yr @(yank* {y1 10} [y2])]
       (is (== 30 (reduce-kv (fn [a _ v] (+ a v)) 0 yr)))))

   (testing "yank* result is sequable"
     (let [yr @(yank* {y1 10} [y2])]
       (is (= (list [y2 20] [y1 10]) (seq yr)))))

   (testing "yank* result is an iobj"
     (let [yr @(yank* {y1 10} [y2])]
       (is (= nil (meta yr)))
       (is (= {::x 1} (meta (with-meta yr {::x 1}))))
       (is (= @yr @(with-meta yr {::x 1})))))

   ))


(deftest yank-error-test

  (do-defs

   (defyarn fail {} (throw (IllegalStateException. "my error")))

   (testing "yank propagates error"
     (is (thrown? clojure.lang.ExceptionInfo @(yank {} [fail]))))

   (testing "yank1 propagates error"
     (is (thrown? clojure.lang.ExceptionInfo @(yank1 {} fail))))

   (do-template
    [yank-expr]

    (testing (str (first 'yank-expr) " failed with an error")
      (let [yrd yank-expr]
        (let [x (try @yrd (catch clojure.lang.ExceptionInfo e e))
              e (md/error-value yrd nil)]
          (is (identical? x e))
          (is (some? e))
          (is (knitty/yank-error? e))
          (is (instance? IllegalStateException (ex-cause e)))
          (is (= "my error" (ex-message (ex-cause e))))
          (testing "ex-info data contains debug info"
            (is (= {:knitty/inputs {}, :knitty/yank-error? true, :knitty/yarns [fail]}
                   (select-keys (ex-data e) [:knitty/inputs :knitty/yank-error? :knitty/yarns])))
            (let [yr (:knitty/result (ex-data e))]
              (is (some? yr))
              (is (= [fail] (keys @yr)))
              (is (kd/deferred? (get yr fail)))
              (is (thrown? IllegalStateException @(get yr fail)))
              (is (identical? (ex-cause e)
                              (md/error-value (get yr fail) nil)))
             ;;
              )))))

    (yank* {} [fail])
    (yank {} [fail])
    (yank1 {} fail))

   ))


(comment
  (clojure.test/test-ns *ns*))
