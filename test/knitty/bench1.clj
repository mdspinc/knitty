(ns knitty.bench1
  (:require [clojure.test :as t :refer [deftest  testing]]
            [knitty.core :refer [yank]]
            [knitty.test-util :refer :all]
            [manifold.deferred :as md]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

(t/use-fixtures :once
  (t/join-fixtures
   [(tracing-enabled-fixture false)
    (report-benchmark-fixture)]))


(t/use-fixtures :each
  (t/join-fixtures
   [(clear-known-yarns-fixture)]))

#_
(deftest ^:benchmark bench-deferred

  (binding [debug/*dropped-error-logging-enabled?* false]
    (doseq [[t create-d] [[:manifold #(md/deferred nil)]
                          [:knitty #(ji/create-kd)]]]
      (testing t
        (bench :create
               (create-d))
        (bench :listener
               (let [d (create-d)]
                 (md/add-listener! d (md/listener (fn [_]) nil))
                 (md/success! d 1)))
        (bench :add-listener-3
               (let [d (create-d)]
                 (md/add-listener! d (md/listener (fn [_]) nil))
                 (md/add-listener! d (md/listener (fn [_]) nil))
                 (md/add-listener! d (md/listener (fn [_]) nil))
                 (md/success! d 1)))
        (bench :add-listener-10
               (let [d (create-d)]
                 (dotimes [_ 10]
                   (md/add-listener! d (md/listener (fn [_]) nil)))
                 (md/success! d 1)))
        (bench :add-listener-33
               (let [d (create-d)]
                 (dotimes [_ 33]
                   (md/add-listener! d (md/listener (fn [_]) nil)))
                 (md/success! d 1)))
        (bench :suc-add-listener
               (let [d (create-d)]
                 (md/success! d 1)
                 (md/add-listener! d (md/listener (fn [_]) nil))))
        (bench :success-get
               (let [d (create-d)]
                 (md/success! d 1)
                 (md/success-value d 2)))
        (bench :success-deref
               (let [d (create-d)]
                 (md/success! d 1)
                 @d)))))
  )


(defn pyank [ids]
  #(yank % ids))


(defn linear-sync-deps [c]
  (take-while
   nat-int?
   (nnext (reductions (fn [a x] (- a x)) c (range)))))


(defn exp-sync-deps [c]
  (map #(- c %)
       (butlast
        (take-while
         nat-int?
         (iterate #(min (dec %) (int (* % 0.71))) c)))))


(defn- run-benchs [nodes]
  (let [ps (map #(nth nodes %) (range 0 (count nodes) 20))]

    (bench :yank-last
           @(yank {} [(last nodes)]))
    (bench :yank-all
           @(yank {} nodes))
    (bench :seq-yank
           @(md/chain'
             (reduce #(md/chain' %1 (pyank [%2])) {} ps)
             (pyank [(first nodes) (last nodes)])))))


(deftest ^:benchmark sync-futures-50
  (build-yarns-graph
   :ids (range 50)
   :prefix :node
   :deps linear-sync-deps
   :emit-body (fn [i & xs] `(mfut (reduce unchecked-add ~i [~@xs]) 3)))
  (run-benchs (nodes-range :node 50)))


(deftest ^:benchmark sync-futures-50-exp
  (build-yarns-graph
   :ids (range 50)
   :prefix :node
   :deps exp-sync-deps
   :emit-body (fn [i & xs] `(mfut (reduce unchecked-add ~i [~@xs]) 3)))
  (run-benchs (nodes-range :node 50)))


(deftest ^:benchmark sync-futures-200
  (build-yarns-graph
   :ids (range 200)
   :prefix :node
   :deps linear-sync-deps
   :emit-body (fn [i & xs] `(mfut (reduce unchecked-add ~i [~@xs]) 20)))
  (run-benchs (nodes-range :node 200)))


(deftest ^:benchmark g100-by-deptype
  (doseq [[nf f] [[:syn `do]
                  [:fut `md/future]]]
    (testing nf
      (doseq [tt [:sync :defer :lazy]]
        (clear-known-yarns!)
        (build-yarns-graph
         :ids (range 100)
         :prefix :node
         :deps #(map vector (repeat tt) (exp-sync-deps %))
         :emit-body (fn [i & xs]
                      `(~f
                        (reduce
                         (fn [a# ~'x]
                           (md/chain'
                            a#
                            (fn [aa#]
                              (md/chain'
                               ~(if (= :lazy tt) `(deref ~'x) 'x)
                               (fn [xx#] (unchecked-add aa# xx#))))))
                         ~i
                         [~@xs]))))
        (bench tt (::node99 @(yank {} [::node99])))))))


(deftest ^:benchmark sync-nofutures-200
  (build-yarns-graph
   :ids (range 200)
   :prefix :node
   :deps linear-sync-deps
   :emit-body (fn [i & xs] `(reduce unchecked-add ~i [~@xs])))
  (run-benchs (nodes-range :node 200)))


(deftest ^:stress check-big-graph

  (build-yarns-graph
   :ids (range 1000)
   :deps linear-sync-deps
   :emit-body (fn [i & xs] `(mfut (reduce unchecked-add ~i [~@xs]) 10)))

  (dotimes [i 100]
    (println ".. " i " / 100")
    (dotimes [_ 1000]
      (binding [knitty.core/*tracing* false]
        @(yank {} (random-sample 0.01 (nodes-range :node 0 500)))))))
