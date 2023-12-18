(ns knitty.test-util
  (:require [knitty.core :refer [*registry* defyarn]]
            [knitty.impl :as impl]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.test :as t]
            [criterium.core :as cc]
            [manifold.deferred :as md])
  (:import [java.time LocalDateTime]
           [java.time.format DateTimeFormatter]))


(def bench-results-dir ".bench-results")

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
  (knitty.javaimpl.MDM/resetKeywordsPoolForTests))


(defn clear-known-yarns-fixture
  []
  (fn [t]
    (try
      (clear-known-yarns!)
      (t)
      (finally (clear-known-yarns!)))))


(def benchmark-opts
  (->
   (if (some? (System/getenv "knitty_qb"))
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


(defn fmt-ratio-diff [r]
  (cond
    (< r 1) (format "-%.2f%%" (* 100 (- 1 r)))
    (> r 1) (format "+%.2f%%" (* 100 (- r 1)))))


(defn tests-run-id []
  (or (System/getenv "knitty_tid")
      (.format (LocalDateTime/now) (DateTimeFormatter/ofPattern "MMddHHmmss"))))


(defn report-benchmark-results [testid rs]
  (let [res-file (io/file (str bench-results-dir "/" testid ".edn"))
        ]
    (io/make-parents res-file)
    (let [oldr (->>
                (file-seq (io/file bench-results-dir))
                (next)
                (map (comp read-string slurp))
                (sort-by #(let [^java.util.Date w (:when %)] (.getTime w))))
          oids (map :id oldr)
          oldm (into {}
                     (for [ors oldr
                           [k r] (:results ors)]
                       [[(:id ors) k] r]))]

      (spit res-file (pr-str {:id testid
                              :when (java.util.Date.)
                              :results rs}))
      (let [idlen (reduce max 10 (map (comp count first) rs))
            idf (str "%-" idlen "s")]
        (pp/print-table
         (for [[k r] rs]
           (let [v (-> r :mean first)]
             (into
              {:id (format idf k)
               :time (fmt-time-value v)}
              (for [oid oids
                    :let [or (get oldm [oid k])]]
                [oid (some-> or :mean first (/ v) fmt-ratio-diff)]
                )))))))))


(defn report-benchmark-fixture
  []
  (fn [t]
    (let [x (tests-run-id)]
      (binding [*bench-results* (atom [])]
        (t)
        (report-benchmark-results x @*bench-results*)))))


(defmacro bench
  ([id expr]
   `(t/testing ~id
      (println "=== bench" (current-test-id))
      ;; (println "res>" ~expr)
      (track-results (current-test-id) (cc/benchmark (do ~expr nil) benchmark-opts))))
  ([id expr1 & exprs]
   `(t/testing ~id
      (println "=== bench" (current-test-id))
      (track-results (current-test-id)
                     (cc/benchmark-round-robin ~(mapv #(list `do %) (list* expr1 exprs))
                                               benchmark-opts)))))
