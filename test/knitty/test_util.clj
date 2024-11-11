(ns knitty.test-util
  (:require
   [clojure.java.io :as io]
   [clojure.math :as math]
   [clojure.pprint :as pp]
   [clojure.string :as str]
   [clojure.test :as t]
   [criterium.core :as cc]
   [knitty.core :refer [defyarn]]
   [knitty.deferred :as kd]
   [manifold.debug :as md-debug]
   [manifold.deferred :as md]
   [manifold.executor :as ex])
  (:import
   [java.time LocalDateTime]
   [java.time.format DateTimeFormatter]))


(defmethod t/report :begin-test-var [m]
  (t/with-test-out
    (println "-"
             (-> m :var meta :name)
             (or (some-> t/*testing-contexts* seq vec) ""))))

(defmethod t/report :end-test-var [_m]
  )

(defonce -override-report-error
  (let [f (get-method t/report :error)]
    (defmethod t/report :error [m]
      (f m)
      (t/with-test-out
        (println)))))


(def bench-results-dir ".bench-results")

(defn mfut [x y]
  (if (zero? (mod x y)) (md/future x) x))


(defn compile-yarn-graph*
  [ns prefix ids deps-fn emit-body-fn fork?]
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


(def smoke-benchmarks-opts
  (merge
   cc/*default-quick-bench-opts*
   {:max-gc-attempts 2
    :samples 5
    :target-execution-time (* cc/s-to-ns 0.1)
    :warmup-jit-period (* cc/s-to-ns 0.05)
    :bootstrap-size 100}))

(def benchmark-opts
  (cond
    (some? (System/getenv "knitty_qb_smoke")) smoke-benchmarks-opts
    (some? (System/getenv "knitty_qb")) cc/*default-quick-bench-opts*
    :else cc/*default-benchmark-opts*))


(def ^:dynamic *bench-results*
  (atom []))


(defn current-test-id []
  (str/join "/"
            (concat
             (reverse (map #(:name (meta %)) t/*testing-vars*))
             (reverse t/*testing-contexts*))))


(defn fmt-time-value
  [mean]
  (let [[factor unit] (cc/scale-time mean)]
    (cc/format-value mean factor unit)))


(defn fmt-ratio-diff [r]
  (cond
    (< r 1) (format "-%.2f%%" (* 100 (- 1 r)))
    (> r 1) (format "+%.2f%%" (* 100 (- r 1)))))


(defn track-results [results]
  (swap! *bench-results* conj [(current-test-id) results])
  (print "\t ⟶ ")
  (print "time" (fmt-time-value (first (:mean results))))
  (let [[v] (:variance results), m (math/sqrt v)]
    (print " ±" (fmt-time-value m)))
  (println))


(defn tests-run-id []
  (or (System/getenv "knitty_tid")
      (.format (LocalDateTime/now) (DateTimeFormatter/ofPattern "YYMMdd-HH:mm:ss"))))


(defn report-benchmark-results []
  (let [testid (tests-run-id)
        rs @*bench-results*
        res-file (io/file (str bench-results-dir "/" testid ".edn"))]
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
        (println "\nbench results:")
        (pp/print-table
         (for [[k r] rs]
           (let [v (-> r :mean first)]
             (into
              {:id (format idf k)
               :time (fmt-time-value v)}
              (for [oid oids
                    :let [or (get oldm [oid k])]]
                [oid (some-> or :mean first (/ v) fmt-ratio-diff)])))))
        (println)))))

(def report-benchmark-hook-installed (atom false))

(defn- add-report-benchmark-results-hook []
  (when (compare-and-set! report-benchmark-hook-installed false true)
    (.. (Runtime/getRuntime) (addShutdownHook (Thread. #'report-benchmark-results)))))

(defn report-benchmark-fixture
  []
  (fn [t]
    (add-report-benchmark-results-hook)
    (t)))

(defmacro bench
  ([id expr]
   `(t/testing ~id
      (print (format "  %-32s" (str/join " " (reverse t/*testing-contexts*))))
      (flush)
      (track-results (cc/benchmark (do ~expr nil) benchmark-opts))))
  ([id expr1 & exprs]
   `(t/testing ~id
      (print (format "  %-32s" (str/join " " (reverse t/*testing-contexts*))))
      (flush)
      (track-results (cc/benchmark-round-robin ~(mapv #(list `do %) (list* expr1 exprs))
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


(defn dotimes-prn* [n body-fn]
  (let [s (quot n 100)
        t (System/currentTimeMillis)]
    (dotimes [x n]
      (when (zero? (mod x s))
        (print ".")
        (flush))
      (body-fn n))
    (let [t1 (System/currentTimeMillis)]
      (println " done in" (- t1 t) "ms"))))


(defmacro dotimes-prn [xn & body]
  (let [[x n] (cond
                (and (vector? xn) (== 2 (count xn))) xn
                (and (vector? xn) (== 1 (count xn))) ['_ (first xn)]
                :else ['_ xn])]
    `(dotimes-prn* ~n (fn [~x] ~@body))))
