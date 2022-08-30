(ns ag.knitty.knitty-test 
  (:require [ag.knitty.core :as knitty :refer [defyarn with-yarns yank yarn]]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest is testing]]
            [manifold.deferred :as md]
            [manifold.executor :as executor]))


(defn fix-knitty-registry [body]
  (with-redefs [knitty/*registry* {}]
    (body)))

(t/use-fixtures :each #'fix-knitty-registry)


(deftest smoke-test

  (defyarn zero {} 0)
  (defyarn one {_ zero} 1)
  (defyarn one-slooow {} (future (Thread/sleep 10) 1))
  (defyarn two {^:defer x one, ^:defer y one-slooow} (md/chain' (md/alt x y) inc))
  (defyarn three-fast {x one, y two} (future (Thread/sleep 1) (+ x y)))
  (defyarn three-slow {x one, y two} (future (Thread/sleep 10) (+ x y)))
  (defyarn three {^:lazy f three-fast, ^:lazy ^:defer s three-slow} (if (zero? (rand-int 2)) f s))
  (defyarn four {x one, y three} (future (+ x y)))
  (defyarn six {x two , y three} (* x y))

  (testing "trace enabled"
    (binding [ag.knitty.core/*tracing* true]
      (is (= [4 6] @(md/chain (yank {} [four six]) first)))))

  (testing "trace disabled"
    (binding [ag.knitty.core/*tracing* false]
      (is (= [4 6] @(md/chain (yank {} [four six]) first))))))
  

(deftest defyarn-test

  (testing "define yarn without args"
    (defyarn test-yarn)
    (is (= test-yarn ::test-yarn))
    (is (some? (get ag.knitty.core/*registry* test-yarn))))

  (testing "define yarn without args but meta"

    (s/def ::the-spec string?)
    (defyarn
      ^{:doc "some doc"
        :spec ::the-spec}
      test-yarn)

    (is (some? (get ag.knitty.core/*registry* test-yarn)))
    (is (= "some doc" (-> #'test-yarn meta :doc)))
    (is (= `string? (s/form (get (s/registry) test-yarn)))))

  (testing "define yarn with args"
    (defyarn y1 {} 1)
    (defyarn y2 {x1 y1} (+ x1 x1))
    (defyarn y3 {x1 y1, x2 y2} (+ x1 x2))
    (is (every? keyword? [y1 y2 y3])))
  )


(deftest types-of-bindings-test

  (testing "const yarn"
    (defyarn y {} 1)
    (is (= [[1] {::y 1}] @(yank {} [y]))))

  (testing "sync binding"
    (defyarn y1 {} 1)
    (defyarn y2 {x1 y1} (+ x1 1))
    (is (= [[2] {::y1 1, ::y2 2}] @(yank {} [y2]))))

  (testing "defer binding"
    (defyarn y1 {} 1)
    (defyarn y2 {^:defer x1 y1} (md/chain' x1 inc))
    (is (= [[2] {::y1 1, ::y2 2}] @(yank {} [y2]))))

  (testing "lazy binding"
    (defyarn y1 {} 1)
    (defyarn y2 {^:lazy x1 y1} (+ @x1 1))
    (is (= [[2] {::y1 1, ::y2 2}] @(yank {} [y2]))))

  (testing "lazy defer binding"
    (defyarn y1 {} 1)
    (defyarn y2 {^:lazy ^:defer x1 y1} (md/chain' @x1 inc))
    (is (= [[2] {::y1 1, ::y2 2}] @(yank {} [y2]))))

  (testing "lazy unused binding"
    (defyarn y1 {} 1)
    (defyarn y2 {} (throw (AssertionError.)))
    (defyarn y3 {^:lazy x1 y1
                 ^:lazy _x2 y2}
      (+ @x1 10))
    (is (= [[11] {::y1 1, ::y3 11}] @(yank {} [y3]))))
  )

  
(deftest yank-deferreds-coercing-test

  (testing "coerce future to deferred"
    (defyarn y {} (future 1))
    (is (= [[1] {::y 1}] @(yank {} [y]))))

  (testing "autoforce delays"
    (defyarn y {} (delay 1))
    (is (= [[1] {::y 1}] @(yank {} [y]))))

  (testing "coerce promises"
    (defyarn y {} (let [p (promise)]
                    (future (deliver p 1))
                    p))
    (is (= [[1] {::y 1}] @(yank {} [y]))))
  )


(deftest input-deferreds-test
  (defyarn y1)
  (defyarn y2 {x1 y1} (inc x1))
  (is (= [11] @(md/chain (yank {y1 (md/future 10)} [y2]) first)))
  )


(deftest inline-yarn-test

  (defyarn y1 {} 1) 
  (defyarn ^:inline y2 {x1 y1} (inc x1))
  (defyarn y3 {x1 y1, x2 y2} (+ x1 x2))

  (is (= [[3] {::y1 1, ::y3 3}] @(yank {} [y3]))))


(deftest long-chain-of-yanks-test
  (eval
   (list*
    `do
    `(defyarn ~'chain-0)
    (for [i (range 1 100)]
      `(defyarn
         ~(symbol (str "chain-" i))
         {x# ~(symbol (str "chain-" (dec i)))}
         (+ x# 1)))))

  (is (= 99 @(md/chain (yank {::chain-0 0} [::chain-99]) ffirst)))
  (is (= 100 @(md/chain (yank {::chain-0 0} [::chain-99]) second count)))

  (is (= 9 @(md/chain (yank {::chain-90 0} [::chain-99]) ffirst)))
  (is (= 10 @(md/chain (yank {::chain-90 0} [::chain-99]) second count))))

  
(deftest everytying-is-memoized-test

  #_{:clj-kondo/ignore [:inline-def]}
  (def everything-memoized-counter (atom 0))

  (eval
   (list*
    `do
    `(defyarn ~'net-0 {} (swap! everything-memoized-counter inc))
    (for [i (range 1 100)]
      `(defyarn
         ~(symbol (str "net-" i))
         {_2# ~(symbol (str "net-" (dec i)))
          _1# ~(symbol (str "net-" (quot i 2)))}
         (swap! everything-memoized-counter inc)))))

  (is (= 100 @(md/chain (yank {} [::net-99]) ffirst)))
  (is (= 100 @everything-memoized-counter)))


(deftest use-executor-test

  (let [nodsym #(symbol (str "node-" %))
        nodkey #(keyword (name (ns-name *ns*)) (str "node-" %))]

    (eval
     (list*
      `do
      '(defyarn node-0 {} 0)
      (for [i (range 1 100)]
        (let [x (with-meta
                  (nodsym i)
                  {:executor #'executor/execute-pool})
              deps [(max (dec i) 0) (quot i 2)]]
          `(defyarn ~x
             ~(zipmap (map nodsym deps) (map nodkey deps))
             (+ 1 (max ~@(map nodsym deps))))))))

    (is (= 99 @(md/chain (yank {} [::node-99]) ffirst))))
  )

  
(deftest hundred-of-inputs-test
  
  (eval
   (list*
    `do
    (for [i (range 100)]
      `(defyarn ~(symbol (str "pass-" i)) {} ~i))))

  (eval
   `(defyarn ~'sum1k
      ~(zipmap
        (for [i (range 100)] (symbol (str "x" i)))
        (for [i (range 100)] (keyword (name (ns-name *ns*)) (str "pass-" i))))
      (reduce + 0 ~(vec (for [i (range 100)] (symbol (str "x" i)))))))

  (is (= 4950 @(md/chain (yank {} [::sum1k]) ffirst))))


(deftest registry-test

  (defyarn y1 {} 1)
  (defyarn y2 {x1 y1} (+ x1 x1))
  (defyarn y3 {x1 y1, x2 y2} (+ x1 x2))

  (testing "yank adhoc yarns"
    (is (= 6 @(md/chain (yank {} [(yarn ::six {x2 y2, x3 y3} (* x2 x3))]) ffirst))))

  (testing "yank adhoc yarns with capturing"
    (is (= [6 12 18]
           (for [i [1 2 3]]
             @(md/chain (yank {} [(yarn ::six {x2 y2, x3 y3} (* x2 x3 i))]) ffirst)))))

  (testing "override yanks from registry"
    (with-yarns [(yarn ::y1 {} 101)]
      (is (= 303 @(md/chain (yank {} [y3]) ffirst))))
      ))


(deftest errhandle-test
  )


(deftest tracing-test
  )

  
(comment
  (clojure.test/test-ns *ns*)
  )
