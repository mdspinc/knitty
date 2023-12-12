(ns knitty.bench1
  (:require [knitty.core :refer [yank]]
            [knitty.deferred :as kd]
            [knitty.test-util :refer :all]
            [clojure.test :as t :refer [deftest testing use-fixtures]]
            [manifold.debug :as debug]
            [manifold.deferred :as md]
            [manifold.deferred :as d]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


(use-fixtures :once (report-benchmark-fixture))
(use-fixtures :each (clear-known-yarns-fixture))


(deftest ^:benchmark bench-deferred

  (binding [debug/*dropped-error-logging-enabled?* false]
    (doseq [[t create-d] [;;
                          ;; [:manifold #(d/deferred nil)]
                          [:knitty kd/ka-deferred]]]
      (testing t
        (bench :create
               (create-d))
        (bench :listener
               (let [d (create-d)]
                 (d/add-listener! d (d/listener (fn [_]) nil))
                 (d/success! d 1)))
        (bench :add-listener-3
               (let [d (create-d)]
                 (d/add-listener! d (d/listener (fn [_]) nil))
                 (d/add-listener! d (d/listener (fn [_]) nil))
                 (d/add-listener! d (d/listener (fn [_]) nil))
                 (d/success! d 1)))
        (bench :add-listener-10
               (let [d (create-d)]
                 (dotimes [_ 10]
                   (d/add-listener! d (d/listener (fn [_]) nil)))
                 (d/success! d 1)))
        (bench :add-listener-33
               (let [d (create-d)]
                 (dotimes [_ 33]
                   (d/add-listener! d (d/listener (fn [_]) nil)))
                 (d/success! d 1)))
        (bench :suc-add-listener
               (let [d (create-d)]
                 (d/success! d 1)
                 (d/add-listener! d (d/listener (fn [_]) nil))))
        (bench :success-get
               (let [d (create-d)]
                 (d/success! d 1)
                 (d/success-value d 2)))
        (bench :success-deref
               (let [d (create-d)]
                 (d/success! d 1)
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
                           (d/chain'
                            a#
                            (fn [aa#]
                              (d/chain'
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
