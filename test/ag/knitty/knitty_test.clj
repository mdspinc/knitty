(ns ag.knitty.knitty-test 
  (:require [ag.knitty.core :as knitty 
             :refer [defyarn doyank! with-yarns yank yank* yarn]]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest is testing]]
            [manifold.deferred :as md]
            [manifold.executor :as executor]))


(defmacro do-defs 
  "eval forms one by one - allows to intermix defs"
  [& body]
  (list*
   `do
   (for [b body]
     (list `eval (list `quote b)))))


(deftest smoke-test

  (do-defs
   (defyarn zero {} 0)
   (defyarn one {_ zero} 1)
   (defyarn one-slooow {} (future (Thread/sleep 10) 1))
   (defyarn two {^:defer x one, ^:defer y one-slooow} (md/chain' (md/alt x y) inc))
   (defyarn three-fast {x one, y two} (future (Thread/sleep 1) (+ x y)))
   (defyarn three-slow {x one, y two} (future (Thread/sleep 10) (+ x y)))
   (defyarn three {^:lazy f three-fast, ^:lazy s three-slow} (if (zero? (rand-int 2)) f @s))
   (defyarn four {x one, y three} (future (+ x y)))
   (defyarn six {x two , y three} (* x y))

   (testing "trace enabled"
     (binding [knitty/*tracing* true]
       (is (= [4 6] @(md/chain (yank* {} [four six]) first)))))

   (testing "trace disabled"
     (binding [knitty/*tracing* false]
       (is (= [4 6] @(md/chain (yank* {} [four six]) first))))))
  )
  

(deftest defyarn-test
   (testing "define yarn without args"
     (do-defs
      (defyarn test-yarn)
      (is (= test-yarn ::test-yarn))
      (is (some? (get @knitty/*registry* test-yarn)))))

   (testing "define yarn without args but meta"
     (do-defs
      (s/def ::the-spec string?)
      (defyarn
        ^{:doc "some doc"
          :spec ::the-spec}
        test-yarn)

      (is (some? (get @knitty/*registry* test-yarn)))
      (is (= "some doc" (-> #'test-yarn meta :doc)))
      (is (= `string? (s/form (get (s/registry) test-yarn))))))

   (testing "define yarn with args"
     (do-defs
      (defyarn y1 {} 1)
      (defyarn y2 {x1 y1} (+ x1 x1))
      (defyarn y3 {x1 y1, x2 y2} (+ x1 x2))
      (is (every? keyword? [y1 y2 y3]))))
   )
  

(deftest types-of-bindings-test

  (testing "const yarn"
    (do-defs
     (defyarn y {} 1)
     (is (= {::y 1} @(yank {} [y])))))

  (testing "sync binding"
    (do-defs
     (defyarn y1 {} 1)
     (defyarn y2 {x1 y1} (+ x1 1))
     (is (= {::y1 1, ::y2 2} @(yank {} [y2])))))

  (testing "defer binding"
    (do-defs
     (defyarn y1 {} 1)
     (defyarn y2 {^:defer x1 y1} (md/chain' x1 inc))
     (is (= {::y1 1, ::y2 2} @(yank {} [y2])))))

  (testing "lazy defer binding"
    (do-defs
     (defyarn y1 {} 1)
     (defyarn y2 {^:lazy x1 y1} (md/chain' @x1 inc))
     (is (= {::y1 1, ::y2 2} @(yank {} [y2])))))

  (testing "lazy unused binding"
    (do-defs
     (defyarn y1 {} 1)
     (defyarn y2 {} (throw (AssertionError.)))
     (defyarn y3 {^:lazy x1 y1
                  ^:lazy _x2 y2}
       (+ @@x1 10))
     (is (= {::y1 1, ::y3 11} @(yank {} [y3]))))))

  
(deftest yank-deferreds-coercing-test

  (testing "coerce future to deferred"
    (do-defs
     (defyarn y {} (future 1))
     (is (= {::y 1} @(yank {} [y])))))

  (testing "autoforce delays"
    (do-defs
     (defyarn y {} (delay 1))
     (is (= {::y 1} @(yank {} [y])))))

  (testing "coerce promises"
    (do-defs
     (defyarn y {} (let [p (promise)]
                     (future (deliver p 1))
                     p))
     (is (= {::y 1} @(yank {} [y])))))
  )


(deftest input-deferreds-test
  (do-defs
   (defyarn y1)
   (defyarn y2 {x1 y1} (inc x1))
   (is (= 11 @(md/chain (yank {y1 (md/future 10)} [y2]) y2))))
  )


(defmacro do-eval [& body]
  `(do-defs ~@(eval (cons `do body))))


(deftest long-chain-of-yanks-test
  
  (defyarn chain-0)

  (do-eval
   (for [i (range 1 100)]
     `(defyarn
        ~(symbol (str "chain-" i))
        {x# ~(symbol (str "chain-" (dec i)))}
        (+ x# 1))))

  (is (= 99 @(md/chain (yank {::chain-0 0} [::chain-99]) ::chain-99)))
  (is (= 100 @(md/chain (yank {::chain-0 0} [::chain-99]) count)))

  (is (= 9 @(md/chain (yank {::chain-90 0} [::chain-99]) ::chain-99)))
  (is (= 10 @(md/chain (yank {::chain-90 0} [::chain-99]) count))))

  
(deftest everytying-is-memoized-test

  #_{:clj-kondo/ignore [:inline-def]}
  (def everything-memoized-counter (atom 0))
  (defyarn net-0 {} (swap! everything-memoized-counter inc))

  (do-eval
   (for [i (range 1 100)]
     `(defyarn
        ~(symbol (str "net-" i))
        {_2# ~(symbol (str "net-" (dec i)))
         _1# ~(symbol (str "net-" (quot i 2)))}
        (swap! everything-memoized-counter inc))))

  (is (= 100 @(md/chain (yank {} [::net-99]) ::net-99)))
  (is (= 100 @everything-memoized-counter)))


(deftest use-executor-test

  (defyarn node-0 {} 0)
  (do-eval
   (for [i (range 1 100)]
     (let [x (with-meta
               (symbol (str "node-" i))
               {:executor #'executor/execute-pool})
           deps [(max (dec i) 0) (quot i 2)]]
       `(defyarn ~x
          ~(zipmap (map #(symbol (str "node-" %)) deps)
                   (map #(keyword (name (ns-name *ns*)) (str "node-" %)) deps))
          (+ 1 (max ~@(map #(symbol (str "node-" %)) deps)))))))

  (is (= 99 @(md/chain (yank {} [::node-99]) ::node-99)))
  )

  
(deftest hundred-of-inputs-test
  
  (do-eval
   (for [i (range 100)]
     `(defyarn ~(symbol (str "pass-" i)) {} ~i)))

  (do-eval
   (list
    `(defyarn ~'sum1k
       ~(zipmap
         (for [i (range 100)] (symbol (str "x" i)))
         (for [i (range 100)] (keyword (name (ns-name *ns*)) (str "pass-" i))))
       (reduce + 0 ~(vec (for [i (range 100)] (symbol (str "x" i))))))))

  (is (= 4950 @(md/chain (yank {} [::sum1k]) ::sum1k))))


(deftest registry-test
  (do-defs
   (defyarn y1 {} 1)
   (defyarn y2 {x1 y1} (+ x1 x1))
   (defyarn y3 {x1 y1, x2 y2} (+ x1 x2))

   (testing "yank adhoc yarns"
     (is (= 6 @(md/chain (yank {} [(yarn ::six {x2 y2, x3 y3} (* x2 x3))]) ::six))))

   (testing "yank adhoc yarns with capturing"
     (is (= [6 12 18]
            (for [i [1 2 3]]
              @(md/chain (yank {} [(yarn ::six {x2 y2, x3 y3} (* x2 x3 i))]) ::six)))))
   
   (testing "override yanks from registry"
     (with-yarns [(yarn ::y1 {} 101)]
       (is (= 303 @(md/chain (yank {} [y3]) y3)))))))


(deftest cancellation-test
  (do-defs
   (defyarn cnt)

   (defyarn count1 {c cnt}
     (md/future
       (Thread/sleep 10)
       (swap! c inc)))

   (defyarn count2 {c cnt, _ count1}
     (md/future
       (Thread/sleep 10)
       (swap! c inc)))

   (defyarn count3 {c cnt, _ count2}
     (md/future
       (Thread/sleep 10)
       (swap! c inc)))

   (let [a (atom 0)]
     (is (= 3 (-> (yank {cnt a} [count3]) deref cnt deref))))

   (let [a (atom 0)]
     (is (= ::t @(-> (doyank! {cnt a} {x count3} x) (md/timeout! 15 ::t))))
     (is (= 1 @a))))

  )

  
(comment
  (clojure.test/test-ns *ns*)
  )
