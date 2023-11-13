(ns ag.knitty.test-util
  (:require [ag.knitty.core :refer [*registry* defyarn]]
            [ag.knitty.impl :as impl]
            [clojure.pprint :as pp]
            [clojure.test :as t]
            [criterium.core :as cc]
            [manifold.deferred :as md]
            [clojure.string :as str]))


(defn mfut [x y]
  (if (zero? (mod x y)) (md/future x) x))


(defn compile-yarn-graph*
  [ns prefix ids deps-fn emit-body-fn]
  (println "compiling yarn-graph" ns)
  (let [n (create-ns ns)]
    (binding [*ns* n]
      (mapv var-get
            (for [i ids]
              (eval
               (let [nsym #(if (vector? %)
                             (let [[t i] %]
                               (with-meta
                                 (symbol (str prefix i))
                                 {t true}))
                             (symbol (str prefix %)))
                     node-xxxx (nsym i)
                     deps (map nsym (deps-fn i))]
                 `(defyarn ~node-xxxx
                    ~(zipmap deps (map (fn [s] (keyword (name ns) (name s))) deps))
                    ~(apply emit-body-fn i deps)))))))))


(defmacro build-yarns-graph
  [& {:keys [prefix ids deps emit-body]
      :or {prefix "node"
           emit-body (fn [i & _] i)}}]
  (let [g (ns-name *ns*)]
    `(compile-yarn-graph* '~g (name ~prefix) ~ids ~deps ~emit-body)))


(defmacro nodes-range
  ([prefix & range-args]
   (mapv #(keyword (name (ns-name *ns*)) (str (name prefix) %))
         (apply range range-args))))


(defn clear-known-yarns! []
  (alter-var-root #'*registry* (fn [_] (impl/create-registry)))
  (ag.knitty.MDM/resetKeywordsPoolForTests))


(defn clear-known-yarns-fixture
  []
  (fn [t]
    (try
      (clear-known-yarns!)
      (t)
      (finally (clear-known-yarns!)))))


(def benchmark-opts
  (->
   (if (some? (System/getenv "CRITERIM_QUICKBENCH"))
     cc/*default-quick-bench-opts*
     cc/*default-benchmark-opts*)))


(def ^:dynamic *bench-results*
  (atom []))


(defn current-test-id []
  (str/join "/"
   (concat
    (reverse (map #(:name (meta %)) t/*testing-vars*))
    (reverse t/*testing-contexts*))))


(defn track-results [id r]
  (swap! *bench-results* conj [id r])
  (cc/report-result r)
  (println))


(defn fmt-time-value
  [mean]
  (let [[factor unit] (cc/scale-time mean)]
    (cc/format-value mean factor unit)))


(defn report-benchmark-results [rs]
  (let [idlen (reduce max 10 (map (comp count first) rs))
        idf (str "%-" idlen "s")]
    (pp/print-table
     (for [[k r] rs]
       {:id (format idf k)
        :mean (-> r :mean first fmt-time-value)}))))


(defn report-benchmark-fixture
  []
  (fn [t]
    (binding [*bench-results* (atom [])]
      (t)
      (report-benchmark-results @*bench-results*))))


(defmacro bench
  ([id expr]
   `(t/testing ~id
      (println "= bench" (current-test-id) "...")
      (track-results (current-test-id) (cc/benchmark ~expr benchmark-opts))))
  ([id expr1 & exprs]
   `(t/testing ~id
      (println "= bench" (current-test-id) "...")
      (track-results (current-test-id) (cc/benchmark-round-robin [~expr1 ~@exprs] benchmark-opts)))))
