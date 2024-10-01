(ns knitty.test-util
  (:require [knitty.core :refer [*registry* defyarn]]
            [knitty.impl :as impl]
            [knitty.deferred :as kd]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.test :as t]
            [criterium.core :as cc]
            [manifold.deferred :as md]
            [manifold.debug :as md-debug]
            [manifold.executor :as ex])
  (:import [java.time LocalDateTime]
           [java.time.format DateTimeFormatter]))


(def bench-results-dir ".bench-results")

(defn mfut [x y]
  (if (zero? (mod x y)) (md/future x) x))


(defn compile-yarn-graph*
  [ns prefix ids deps-fn emit-body-fn fork?]
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
                     node-xxxx (cond-> (nsym i)
                                 (fork? i) (vary-meta assoc :fork true))
                     deps (map nsym (deps-fn i))]
                 `(defyarn ~node-xxxx
                    ~(zipmap deps (map (fn [s] (keyword (name ns) (name s))) deps))
                    ~(apply emit-body-fn i deps)))))))))


(defmacro build-yarns-graph
  [& {:keys [prefix ids deps emit-body fork?]
      :or {prefix "node"
           emit-body (fn [i & _] i)
           fork? `(constantly false)}}]
  (let [g (ns-name *ns*)]
    `(compile-yarn-graph* '~g (name ~prefix) ~ids ~deps ~emit-body ~fork?)))


(defmacro nodes-range
  ([prefix & range-args]
   (mapv #(keyword (name (ns-name *ns*)) (str (name prefix) %))
         (apply range range-args))))


(defn tracing-enabled-fixture
  [tracing-enabled]
  (fn [t]
    (binding [knitty.core/*tracing* (boolean tracing-enabled)]
      (t))))


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
  (let [res-file (io/file (str bench-results-dir "/" testid ".edn"))]
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
                [oid (some-> or :mean first (/ v) fmt-ratio-diff)])))))))))


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

(defmacro do-defs
  "eval forms one by one - allows to intermix defs"
  [& body]
  (list*
   `do
   (for [b body]
     `(binding [*ns* ~*ns*]
        (eval '~b))))) 34


(defmacro slow-future [delay & body]
  `(kd/future
     (Thread/sleep (long ~delay))
     ~@body))


(defn reset-registry-fixture
  []
  (fn [t]
    (let [r knitty.core/*registry*]
      (try
        (t)
        (finally
          (alter-var-root #'knitty.core/*registry* (constantly r)))))))


(defn disable-manifold-leak-detection-fixture
  []
  (fn [t]
    (let [e md-debug/*dropped-error-logging-enabled?*]
      (md-debug/disable-dropped-error-logging!)
      (try (t)
           (finally
             (.bindRoot #'md-debug/*dropped-error-logging-enabled?* e))))))



(def defer-callbacks (java.util.concurrent.atomic.AtomicReference.))
(def defer-random (java.util.Random.))


(defmacro with-defer [body]
  `(let [^java.util.concurrent.atomic.AtomicReference dcs# defer-callbacks]
     (try
       (.set dcs# (java.util.ArrayList.))
       ~body
       (finally
         (loop []
           (let [^java.util.ArrayList xxs# (.get dcs#)]
             (when (not (.isEmpty xxs#))
               (.set dcs# (java.util.ArrayList.))
               (dotimes [i# (.size xxs#)] ((.get xxs# i#)))
               (recur))))))))


(defmacro defer! [callback]
  `(let [^java.util.concurrent.atomic.AtomicReference dcs# defer-callbacks
         ^java.util.ArrayList xxs# (.get dcs#)
         ^java.util.Random rnd# defer-random
         n# (.size xxs#)]
     (.add xxs# (fn [] ~callback))
     (java.util.Collections/swap xxs#
                                 (.nextInt rnd# (unchecked-inc-int n#))
                                 n#)))


(def always-false false)

(defmacro ninl [x]
  `(if always-false
     (for [x# (range 10)] (for [x# (range x#)] (for [x# (range x#)] (for [x# (range x#)] (for [x# (range x#)] x#)))))
     ~x))

(defn ninl-inc
  ([]
   (ninl 0))
  ([^long x]
   (ninl (unchecked-inc x))))

(defmacro eval-template [emit-body vss-expr]
  (let [emit-body (eval emit-body)
        vss-expr (eval vss-expr)]
    (list*
     `do
     (map #(apply emit-body %) vss-expr))))

(defmacro with-md-executor [& body]
  `(let [~'__knitty__test_util__md_executor (ex/execute-pool)]
     ~@body))

(defmacro md-future
  "Equivalent to 'manifold.deferred/future', but use didicated executor"
  [& body]
  `(md/future-with ~'__knitty__test_util__md_executor ~@body))
