(ns ag.knitty.sstats
  (:require [ag.knitty.trace :as trace]
            [manifold.deferred :as md])
  (:import [org.HdrHistogram ConcurrentHistogram Histogram Recorder]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


(defrecord Stats 
  [^long count
   ^long mean
   ^long stdv
   ^long min
   ^long max
   ^{:doc "seq of pairs"} pcts
])


(defn- simple-stats-tracker
  [& {:keys [precision percentiles]}]
  (let [h (ConcurrentHistogram. (int precision))]
     (fn
       ([]
        (->Stats
         (.getTotalCount h)
         (Math/round (.getMean h))
         (Math/round (.getStdDeviation h))
         (.getMinValue h)
         (.getMaxValue h)
         (doall
          (for [p percentiles]
            [p (.getValueAtPercentile h p)]))))
       ([x]
        (.recordValue h (long x))
        x))))


(definline ^:private now-ms []
  `(System/currentTimeMillis))


(defn- windowed-stats-tracker
  [& {:keys [window window-chunk precision percentiles]}] 
  (let [;;
        window (long window)
        window-chunk (long window-chunk)

        r (Recorder. (int precision))
        hss (atom clojure.lang.PersistentQueue/EMPTY)
        lc (atom (quot (now-ms) window))

        cleanup!
        (fn []
          (let [mint (- (now-ms) window)]
            (while (some-> @hss ffirst long (< mint))
              (swap! hss
                     (fn [[[t _h] :as hs]]
                       (if (< t mint) (pop hs) hs))))))
        flush!
        (fn [force]
          (let [t (now-ms)
                c (quot t window-chunk)]
            (when (or force (not= c @lc))
              (let [[c' _] (swap-vals! lc (constantly c))]
                (when (or force (not= c c'))
                  (let [h (.getIntervalHistogram r)]
                    (when-not (zero? (.getTotalCount h))
                      (swap! hss conj [t h])))))
              (cleanup!))))]
 
    (fn

      ([]
       (flush! true)
       (let [hs @hss
             t (Histogram. (int precision))]
         (doseq [[_c ^Histogram h] hs]
           (.add t h))
         (->Stats
          (.getTotalCount t)
          (Math/round (.getMean t))
          (Math/round (.getStdDeviation t))
          (.getMinValue t)
          (.getMaxValue t)
          (doall
           (for [p percentiles]
             [p (.getValueAtPercentile t p)])))))

      ([x]
       (.recordValue r (long x))
       (flush! false)
       x)
      ;;
      )))


(defn- grouped-stats-tracker [create-tracker]
  (let [ss (atom {})]
    (fn
      ([]
       (update-vals @ss apply))
      ([m]
       (run!
        (fn [[k v]]
          (if-let [s (@ss k)]
            (s v)
            (let [s (create-tracker)]
              (swap! ss assoc k s)
              ((@ss k) v))))
        m))
      )))


(defn- safe-minus [a b]
  (when (and (some? a) (some? b))
    (let [^long a a
          ^long b b]
      (when (and (not (zero? a)) (not (zero? b)))
        (- a b)))))


(defn yarn-timings
  ([poy]
   (yarn-timings poy (constantly true)))
  ([poy yarns]
   (yarn-timings poy yarns (constantly true)))
  ([poy yarns events]
   (when-let [ts (trace/find-traces poy)]
     (let [att (- 0 (long (reduce min (map :base-at ts))))
           h (java.util.HashMap.)]
       (doseq [t ts]
         (doseq [log (:tracelog t)]
           (let [y (:yarn log)]
             (when (yarns y)
               (let [e (:event log)]
                 (when-let [i (cond
                                (identical? ::trace/trace-yank e) 0
                                (identical? ::trace/trace-call e) 1
                                (identical? ::trace/trace-done e) 2)]
                   (let [y (:yarn log)
                         v (long (:value log))
                         ^longs c (.get h y)]
                     (if c
                       (aset c i (+ v att))
                       (let [c (long-array 3)]
                         (.put h y c)
                         (aset c i (+ v att)))))))))))
       (let [emit-yank (events :yank)
             emit-call (events :call)
             emit-done (events :done)
             emit-wait (events :wait)
             emit-time (events :time)]
         (eduction
          (comp
           (mapcat (fn [kv]
                     (let [k (key kv)
                           ^longs v (val kv)
                           yank (aget v 0)
                           call (aget v 1)
                           done (aget v 2)]
                       [(when emit-yank [[k :yank] yank])
                        (when emit-call [[k :call] call])
                        (when emit-done [[k :done] done])
                        (when emit-wait [[k :wait] (safe-minus call yank)])
                        (when emit-time [[k :time] (safe-minus done call)])
                       ;;
                        ])))
           (remove nil?))
          (filter #(some? (nth % 1)))
          (.entrySet h)))))))


(defn timings-collector 
  [& {:keys [yarns events window window-chunk precision percentiles]
      :or {yarns (constantly true)  ;; all
           events #{:yank :call :done :wait :time}
           percentiles [0.50 0.90 0.95 0.99]
           precision 3
           window nil
           }}]
  (let [yarns (memoize yarns)
        tracker (grouped-stats-tracker
                 (if (some? window)
                   (partial windowed-stats-tracker
                            {:precision precision
                             :window window
                             :window-chunk (or window-chunk (long (* 0.05 window)))
                             :percentiles percentiles})
                   (partial simple-stats-tracker
                            {:precision precision
                             :percentiles percentiles})))]
    (fn
      ([]
       (->> (tracker)
            (group-by ffirst)
            (mapcat (fn [[y row]]
                      (->>
                       row
                       (map (fn [[[_ e] s]] (into {:event e, :yarn y} s))))))))
      ([poy]
       (md/chain'
        poy
        (fn [poy]
          (let [s (trace/find-traces poy)]
            (when s
              (tracker (yarn-timings s yarns events))))))
       poy)))
  )
