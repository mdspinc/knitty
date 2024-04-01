(ns knitty.trace
  (:require
   [clojure.set :as set]
   [manifold.deferred :as md])
  (:import [java.util.concurrent.atomic AtomicReference]))


(set! *warn-on-reflection* true)


(def elide-tracing
  (#{"1" "true" "yes"} (System/getProperty "knitty.elide-tracing")))


(defmacro if-tracing
  ([trace-body]
   (when-not elide-tracing trace-body))
  ([trace-body notrace-body]
   (if elide-tracing notrace-body trace-body)))


(definterface Tracer
  (traceStart [yk kind deps])
  (traceCall [yk])
  (traceFinish [yk value error async])
  (traceDep [yk dep])
  (traceRouteBy [yk rk-dep])
  (captureTrace []))

(deftype TraceLogCons [yarn event value next])
(defrecord TraceLog [yarn event value])
(defrecord Trace [at base-at done-at poy yarns tracelog])


(defmethod print-method Trace
  [x ^java.io.Writer w]
  (.write w "#knitty.Trace{")
  (.write w ":at ")
  (print-method (:at x) w)
  (.write w " :yarns ")
  (print-method (:yarns x) w)
  (.write w " :tracelogs-count ")
  (print-method (count (:tracelog x)) w)
  (.write w " ...}"))


(defmacro now []
  `(System/nanoTime))


(defmacro ^:private aconj-tlog [a yarn event value]
  `(let [v# ~value]
     (loop [g# (.get ~a)]
       (when-not (.compareAndSet ~a g# (TraceLogCons. ~yarn ~event v# g#))
         (recur (.get ~a))))))


(defn tracelog->seq [^TraceLogCons t]
  (lazy-seq
   (when t
     (cons (TraceLog. (.-yarn t) (.-event t) (.-value t))
           (tracelog->seq (.-next t))))))


(deftype TracerImpl [^AtomicReference store extra]

  Tracer
  (traceStart
    [_ yk kind deps]
    (aconj-tlog store yk ::trace-start (now))
    (aconj-tlog store yk ::trace-kind kind)
    (aconj-tlog store yk ::trace-all-deps deps))

  (traceCall
    [_ yk]
    (aconj-tlog store yk ::trace-call (now))
    (aconj-tlog store yk ::trace-thread (.getName (Thread/currentThread))))

  (traceDep [_ yk dep]
    (aconj-tlog store yk ::trace-dep [dep (now)]))

  (traceRouteBy [_ yk rk-dep]
    (aconj-tlog store yk ::trace-route-by rk-dep))

  (traceFinish
    [_ yk value error deferred]
    (when deferred
      (aconj-tlog store yk ::trace-deferred true))
    (when value
      (aconj-tlog store yk ::trace-value value))
    (when error
      (aconj-tlog store yk ::trace-error error))
    (aconj-tlog store yk ::trace-finish (now)))

  (captureTrace
    [_]
    (map->Trace
     (assoc extra
            :done-at (now)
            :tracelog (tracelog->seq (.getAndSet store nil))))))


(def ^:private yank-cnt (atom 0))

(defn create-tracer [poy yarns]
  (when-not elide-tracing
    (let [store (AtomicReference.)
          extra {:at (java.util.Date.)
                 :yankid (swap! yank-cnt inc)
                 :base-at (now)
                 :poy poy
                 :yarns yarns}]
      (TracerImpl. store extra))))


(defn capture-trace! [t]
  (.captureTrace ^Tracer t))

(defn- safe-minus [a b]
  (when (and a b (not (zero? a)) (not (zero? b)))
    (- a b)))


(defn parse-trace [t]
  (let [{:keys [at base-at done-at poy tracelog yankid yarns]} t

        yanked? (set yarns)
        ytlog (into
               {}
               (map (fn [[k ts]] [k (into {} (map (juxt :event :value) ts))]))
               (group-by :yarn tracelog))

        ytdep-time (into {}
                         (for [{y :yarn, e :event, v :value} tracelog
                               :when (= e ::trace-dep)
                               :let [[dep time] v]]
                           [[y dep] time]))

        ytcause (into
                 {}
                 (comp
                  (filter #(= ::trace-dep (:event %)))
                  (filter #(-> (get ytlog (first (:value %))) ::trace-call some?))
                  (map (fn [{c :yarn, [y _] :value}] [y c])))
                 tracelog)

        all-deps (set
                  (concat
                   (keep ::trace-route-by (vals ytlog))
                   (mapcat #(map first (::trace-all-deps %)) (vals ytlog))))

        ex-deps (set/difference all-deps (set (keys ytlog)))

        knot-ref (fn [t]
                   (and
                    (-> t ::trace-all-deps count (= 1))
                    (-> t ::trace-all-deps first second (= :ref))
                    (-> t ::trace-all-deps first first)))
        resolve-knots (fn [t]
                        (if-let [k (knot-ref t)]
                          (recur (ytlog k))
                          t))]
    {:at at
     :time (safe-minus done-at base-at)
     :base-at base-at
     :done-at done-at
     :yankid yankid

     :nodes (into
             {}
             (concat
              (for [[y t] ytlog
                    :when (not= y ::yank)]
                [y (let [{value     ::trace-value
                          error     ::trace-error
                          thread    ::trace-thread
                          finish-at ::trace-finish
                          start-at  ::trace-start
                          call-at   ::trace-call
                          kind      ::trace-kind
                          deferred? ::trace-deferred
                          } t
                         caller (ytcause y)]
                     {:type
                      (cond
                        (yanked? y)      :yanked
                        (= :knot kind)   :knot
                        (nil? finish-at) :leaked
                        :else            :interim)
                      :caller caller
                      :value value
                      :error error
                      :thread thread
                      :yankid yankid
                      :start-at start-at
                      :finish-at finish-at
                      :deps-time (safe-minus call-at start-at)
                      :func-time (safe-minus finish-at call-at)
                      :deferred (or deferred? (nil? finish-at))})])
              (for [pd ex-deps]
                [pd (if (contains? poy pd)
                      {:type :input, :value (get poy pd), :yankid yankid}
                      {:type :lazy-unused, :value ::unused, :deferred true, :yankid yankid})])))
     :links (into
             {}
             (concat
              (for [[y t] ytlog
                    [dk dt] (concat
                             (::trace-all-deps t)
                             (when-let [a (::trace-route-by t)] [[a :route]]))
                    :let [time (get ytdep-time [y dk])]]
                [[y dk]
                 {:source (cond
                            (contains? poy dk) :input
                            (::trace-deferred (ytlog dk)) :defer
                            :else :sync)
                  :cause (= (ytcause dk) y)
                  :timex (when-let [t (safe-minus (::trace-finish t)
                                                  (::trace-finish (resolve-knots (ytlog dk))))]
                           (when (pos? t) t))
                  :used (not (nil? time))
                  :type dt
                  :yankid-src yankid
                  :yankid-dst yankid}])))}))


(defn- merge-two-parsed-traces [b a]
  (let [nodes-a (into {} (:nodes a))
        nodes-b (into {} (:nodes b))
        ]
    {:clusters (assoc (:clusters b)
                      (:yankid a) {:at (:at a)
                                   :time (:time a)
                                   :base-at (:base-at a)
                                   :done-at (:done-at a)
                                   :shift (safe-minus (:base-at a) (:base-at b))})

     :base-at (or (:base-at b) (:base-at a))
     :done-at (or (:done-at b) (:done-at a))
     :at      (or (:at b) (:at a))
     :time    ((fnil + 0 0) (:time b 0) (:time a 0))

     :nodes (concat
             (:nodes b)
             (for [[y n] (:nodes a)
                   :let [n1 (nodes-b y)
                         n2 (nodes-a y)
                         eq (= (:value n1) (:value n2))
                         changed (and
                                  n1 n2
                                  (not eq)
                                  (not= :lazy-unused (:type n1))
                                  (not= :lazy-unused (:type n2)))]
                   :when (or (nil? n1) (not eq))]
               (if changed
                 [y (assoc n :type :changed-input)]
                 [y n])))

     :links (concat
             (:links b)

             (for [[[_ y :as xy] c] (:links a)]
               (if (and (contains? nodes-b y)
                        (= (:value (nodes-b y))
                           (:value (nodes-a y))))
                 [xy (assoc c :yankid-dst (:yankid (nodes-b y)))]
                 [xy c]))

             (set
              (for [[[_ y] _] (:links a)
                    :let [n1 (nodes-b y)
                          n2 (nodes-a y)]
                    :when (and
                           n1 n2
                           (not= (:value n1) (:value n2)))]
                [[y y] {:yankid-dst (:yankid (nodes-b y)),
                        :yankid-src (:yankid (nodes-a y)),
                        :type :changed-input}])))
     }))


(defn merge-parsed-traces [gs]
  (reduce merge-two-parsed-traces nil (reverse gs)))


(defn find-traces* [poy]
  (cond
    (instance? Trace poy)
    [poy]

    (and (seqable? poy)
         (every? #(instance? Trace %) poy))
    poy

    (map? poy)
    (or (-> poy meta :knitty/trace)
        (-> poy :knitty/trace))

    (instance? clojure.lang.IExceptionInfo poy)
    (:knitty/trace (ex-data poy))

    (and (md/deferred? poy)
         (:knitty/trace (meta poy)))
    (:knitty/trace (meta poy))

    (md/deferred? poy)
    (md/catch (md/chain poy find-traces*) find-traces*)))


(defn find-traces [poy]
  (when-let [t (find-traces* poy)]
    (if (md/deferred? t) @t t)))
