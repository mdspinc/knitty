(ns knitty.multi-test
  {:clj-kondo/ignore [:inline-def]}
  (:require
   [clojure.test :as t :refer [deftest is testing]]
   [knitty.deferred :as kd]
   [knitty.core :as knitty :refer [defyarn defyarn-multi defyarn-method yank yank1]]
   [knitty.test-util :as tu :refer [do-defs]]))


(t/use-fixtures :each
  (t/join-fixtures
   [(tu/reset-registry-fixture)]))


(deftest defyarn-test

  (do-defs
   (defyarn rv)
   (defyarn-multi my rv)
   (defyarn-method my "one" {} 1)
   (defyarn-method my "two" {} 2)
   (defyarn-method my "three" {} 3)
   (defyarn-method my :default {} 0)

   (testing "route by sync string"
     (is (= {rv "one", my 1}   @(yank {rv "one"} [my])))
     (is (= {rv "two", my 2}   @(yank {rv "two"} [my])))
     (is (= {rv "three", my 3} @(yank {rv "three"} [my])))
     (is (= {rv "nil", my 0}   @(yank {rv "nil"} [my]))))

   (testing "route by async string"
     (is (= 1 @(yank1 {rv (kd/wrap-val "one")} my)))
     (is (= 2 @(yank1 {rv (tu/slow-future 10 "two")} my)))
     (is (= 3 @(yank1 {rv (tu/slow-future 10 (tu/slow-future 10 "three"))} my))))))


(deftest defyarn-hierarchical-test

  (do-defs
   (defyarn rv)
   (defyarn-multi my rv)
   (defyarn-method my ::root {} "root")
   (derive ::br1 ::root)
   (derive ::br2 ::root)
   (derive ::br3 ::root)
   (derive ::br4 ::br3)

   (testing "hierarchical route by keywords"

     (is (= {rv ::root, my "root"} @(yank {rv ::root} [my])))
     (is (= {rv ::br1, my "root"}  @(yank {rv ::br1} [my])))
     (is (= {rv ::br2, my "root"}  @(yank {rv ::br2} [my])))
     (is (= {rv ::br3, my "root"}  @(yank {rv ::br3} [my])))
     (is (= {rv ::br4, my "root"}  @(yank {rv ::br4} [my])))

     (defyarn-method my ::br3 {} "br3")
     (is (= {rv ::root, my "root"} @(yank {rv ::root} [my])))
     (is (= {rv ::br1, my "root"}  @(yank {rv ::br1} [my])))
     (is (= {rv ::br2, my "root"}  @(yank {rv ::br2} [my])))
     (is (= {rv ::br3, my "br3"}   @(yank {rv ::br3} [my])))
     (is (= {rv ::br4, my "br3"}   @(yank {rv ::br4} [my])))

     (defyarn-method my ::br1 {} "br1")
     (is (= {rv ::root, my "root"} @(yank {rv ::root} [my])))
     (is (= {rv ::br1, my "br1"}   @(yank {rv ::br1} [my])))
     (is (= {rv ::br2, my "root"}  @(yank {rv ::br2} [my])))
     (is (= {rv ::br3, my "br3"}   @(yank {rv ::br3} [my])))
     (is (= {rv ::br4, my "br3"}   @(yank {rv ::br4} [my]))))))


(deftest defyarn-vector-hierarchy-test

  (do-defs

   (defyarn rv)
   (defyarn-multi my rv)
   (defyarn-method my [1 1] {} 11)
   (defyarn-method my [2 2] {} 22)
   (defyarn-method my [1 2] {} 12)
   (defyarn-method my [2 1] {} 21)

   (testing "simple route by vector of consts"
     (is (= 11 @(yank1 {rv [1 1]} my)))
     (is (= 12 @(yank1 {rv [1 2]} my)))
     (is (= 21 @(yank1 {rv [2 1]} my)))
     (is (= 22 @(yank1 {rv [2 2]} my))))))


(deftest prefer-multi-yarn-test

  (do-defs
   (defyarn rv)
   (defyarn-multi my rv)

   (derive ::osx ::unix)
   (derive ::osx ::bsd)
   (defyarn-method my ::unix {} "unix")
   (defyarn-method my ::bsd {} "bsd")

   (testing "failure due to ambigous routing"
     (is (thrown? Exception @(yank1 {rv ::osx} my))))
   (testing "prefer one multi-yarn"
     (knitty.core/yarn-prefer-method my ::unix ::bsd)
     (is (= "unix" @(yank1 {rv ::osx} my)))))
  )

(deftest defyarn-custom-hierarchy-test
    (do-defs

     (defyarn rv)
     (defyarn-multi my rv :hierarchy (-> (make-hierarchy)
                                         (derive ::foo ::bar)
                                         (atom)))
     (defyarn-method my :default {} "default")
     (defyarn-method my ::bar {} "bar")

     (testing "simple route by keyword with custom hierarchy"
       (is (= "default" @(yank1 {rv ::unknown} my)))
       (is (= "bar" @(yank1 {rv ::foo} my)))
       (is (= "bar" @(yank1 {rv ::bar} my))))))


(deftest defyarn-with-default-test

  (do-defs
   (defyarn rv)
   (defyarn-multi my rv :default "default")
   (defyarn-method my "default" {} 0)
   (defyarn-method my "one" {} 1)

   (testing "route with custom default"
     (is (= 0 @(yank1 {rv "unknown"} my)))
     (is (= 0 @(yank1 {rv "default"} my)))
     (is (= 1 @(yank1 {rv "one"} my))))))


#_ ;; dont mimic clojure defmethod redefine strategy (for now?)
(deftest redeclare-test

  (do-defs
   (defyarn rv)
   (defyarn-multi my rv)
   (defyarn-method my "one" {} 111)
   (defyarn-method my "one" {} 1)

   (testing "redefine method actually redefines yarn"
     (is (= 1 @(yank1 {rv "one"} my))))

   (testing "redefine mutli does not reset methods"
     (defyarn-multi my rv)
     (is (= 1 @(yank1 {rv "one"} my))))))


(comment
  (clojure.test/test-ns *ns*))
