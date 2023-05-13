(ns ag.knitty.tstats
  (:require [ag.knitty.trace :as trace]
            [manifold.deferred :as md])
  (:import [org.HdrHistogram
            ConcurrentHistogram 
            Histogram
            Recorder]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


(defrecord Stats 
  [^long total-count
   ^long total-sum
   ^long count
   ^long mean
   ^long stdv
   ^long min
   ^long max
   ^{:doc "seq of pairs"} pcts])


(defn- simple-stats-tracker
  [& {:keys [precision percentiles]}]
  (let [h (ConcurrentHistogram. (int precision))
        total-count (atom 0)
        total-sum (atom 0)]
     (fn
       ([]
        (->Stats
         @total-count
         @total-sum
         (.getTotalCount h)
         (Math/round (.getMean h))
         (Math/round (.getStdDeviation h))
         (.getMinValue h)
         (.getMaxValue h)
         (doall
          (for [p percentiles]
            [p (.getValueAtPercentile h p)]))))
       ([x]
        (swap! total-count inc)
        (swap! total-sum + x)
        (.recordValue h (long x))
        x))))


(definline ^:private now-ms []
  `(System/currentTimeMillis))


(defn- windowed-stats-tracker
  [& {:keys [window window-chunk precision percentiles]}] 
  (let [;;
        window (long window)
        window-chunk (long window-chunk)

        total-count (atom 0)
        total-sum (atom 0)
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
          @total-count
          @total-sum
          (.getTotalCount t)
          (Math/round (.getMean t))
          (Math/round (.getStdDeviation t))
          (.getMinValue t)
          (.getMaxValue t)
          (doall
           (for [p percentiles]
             [p (.getValueAtPercentile t p)])))))

      ([x]
       (swap! total-count inc)
       (swap! total-sum + x)
       (.recordValue r (long x))
       (flush! false)
       x)
      ;;
      )))


(defn- grouped-stats-tracker [create-tracker]
  (let [ss (atom {})]
    (fn
      ([]
       (into {} (map (fn [[k v]] [k (v)])) @ss))
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
                 (when-let [i (case e
                                ::trace/trace-start 0
                                ::trace/trace-call 1
                                ::trace/trace-finish 2
                                ::trace/trace-deferred 3
                                nil)]
                   (let [y (:yarn log)
                         v (if (== i 3) 1 (long (:value log)))
                         ^longs c (.get h y)]
                     (if c
                       (aset c i (+ v att))
                       (let [c (long-array 4)]
                         (.put h y c)
                         (aset c i (+ v att)))))))))))
       (let [emit-yank (events :yank-at)
             emit-call (events :call-at)
             emit-done (events :done-at)
             emit-deps-time (events :deps-time)
             emit-sync-time (events :sync-time)
             emit-async-time (events :async-time)
             ]
         (eduction
          (mapcat (fn [kv]
                    (let [k (key kv)
                          ^longs v (val kv)
                          yank (aget v 0)
                          call (aget v 1)
                          done (aget v 2)
                          sync (== 0 (aget v 3))]
                      [(when emit-yank [[k :yank-at] yank])
                       (when emit-call [[k :call-at] call])
                       (when emit-done [[k :done-at] done])
                       (when emit-deps-time [[k :deps-time] (safe-minus call yank)])
                       (when (and emit-sync-time sync) [[k :sync-time] (safe-minus done call)])
                       (when (and emit-async-time (not sync)) [[k :async-time] (safe-minus done call)])
                       ;;
                       ])))
          (filter #(and (some? %) (some? (nth % 1))))
          (.entrySet h)))))))


(defn timings-collector 
  [& {:keys [yarns 
             events
             window
             window-chunk
             precision
             percentiles
             ]
      :or {yarns (constantly true)  ;; all
           events #{:yank-at :call-at :done-at :deps-time :sync-time :async-time}
           percentiles [50 90 95 99]
           precision 3
           window 60
           }}]
  (let [
        yarns (memoize yarns)
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
                       (map (fn [[[_ e] s]] 
                              (into {:event e, :yarn y} s)) 
                            row)))))
       ([poy]
        (md/chain'
         poy
         (fn [poy]
           (let [s (trace/find-traces poy)]
             (when s
               (tracker (yarn-timings s yarns events))))))
        poy))))
  
