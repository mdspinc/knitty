(ns ag.knitty.trace 
  (:require
   [clojure.set :as set]
   [manifold.deferred :as md]))


(set! *warn-on-reflection* true)


(defprotocol Tracer 
  (trace-start [_])
  (trace-call [_])
  (trace-finish [_ value error async])
  (trace-dep [_ k])
  (trace-all-deps [_ yarns])
  (trace-build-sub-tracer [_ k])
  (capture-trace! [_]))

(defrecord TraceLog
           [yarn
            event
            value])

(defrecord Trace
           [at
            base-at
            done-at
            poy
            yarns
            tracelog])

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


(defn- aconj-tlog [a yarn event value]
  (let [t (TraceLog. yarn event value)]
    (swap! a #(when % (cons t %)))))

(deftype TracerImpl
         [store
          this-yarn
          by-yarn
          extra]

  Tracer
  (trace-start
   [_]
   (aconj-tlog store this-yarn ::trace-start (now))
   (aconj-tlog store this-yarn ::trace-caller by-yarn))

  (trace-call
   [_]
   (aconj-tlog store this-yarn ::trace-call (now))
   (aconj-tlog store this-yarn ::trace-thread (.getName (Thread/currentThread))))


  (trace-dep [_ yarn]
    (aconj-tlog store this-yarn ::trace-dep [yarn (now)]))

  (trace-all-deps
   [_ yarns]
   (aconj-tlog store this-yarn ::trace-all-deps yarns))

  (trace-build-sub-tracer
   [_ k]
   (TracerImpl. store k this-yarn nil))

  (trace-finish
   [_ value error deferred]
   (when deferred
     (aconj-tlog store this-yarn ::trace-deferred true))
   (when value
     (aconj-tlog store this-yarn ::trace-value value))
   (when error
     (aconj-tlog store this-yarn ::trace-error error))
   (aconj-tlog store this-yarn ::trace-finish (now)))
  
  (capture-trace!
   [_]
   (let [s @store]
     (reset! store nil)
     (map->Trace
      (assoc extra
             :done-at (now)
             :tracelog s)))))


(def ^:private yank-cnt (atom 0))

(defn create-tracer [poy yarns]
  (let [store (atom ())
        extra {:at (java.util.Date.)
               :yankid (swap! yank-cnt inc)
               :base-at (now)
               :poy poy
               :yarns yarns}]
    (TracerImpl. store ::yank nil extra)))


(defn- safe-minus [a b]
  (when (and a b (not (zero? a)) (not (zero? b)))
    (- a b)))


(defn parse-trace [t]
  (let [{:keys [at base-at done-at poy tracelog yankid]} t
        ytlog (update-vals
               (group-by :yarn tracelog)
               (fn [ts] (into {} (map (juxt :event :value) ts))))
        ytdep (into {}
                    (for [{y :yarn, e :event, v :value} tracelog
                          :when (= e ::trace-dep)
                          :let [[dep time] v]]
                      [[y dep] time]))
        all-deps (set (for [t (vals ytlog), [d _] (::trace-all-deps t)] d))
        ex-deps (set/difference all-deps (set (keys ytlog)))]
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
                [y (let [{value  ::trace-value
                          error  ::trace-error
                          thread ::trace-thread
                          finish-at ::trace-finish
                          start-at  ::trace-start
                          call-at   ::trace-call
                          deferred? ::trace-deferred
                          caller    ::trace-caller} t]
                     {:type
                      (cond
                        (= ::yank caller) :yanked
                        (nil? finish-at)  :leaked
                        :else             :interim)
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
                    [dk dt] (::trace-all-deps t)
                    :let [time (get ytdep [y dk])]]
                [[y dk]
                 {:source (cond
                            (contains? poy dk) :input
                            (::trace-deferred (ytlog dk)) :defer
                            :else :sync)
                  :cause (= (::trace-caller (ytlog dk)) y)
                  :timex (when-let [t (safe-minus (::trace-finish t)
                                                  (::trace-finish (ytlog dk)))]
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


(defn find-traces [poy]
  (cond
    (instance? Trace poy)
    [poy]

    (and (seqable? poy)
         (every? #(instance? Trace %) poy))
    poy

    (map? poy)
    (-> poy meta :ag.knitty/trace)

    (vector? poy)
    (-> poy second meta :ag.knitty/trace)

    (instance? clojure.lang.IExceptionInfo poy)
    (:ag.knitty/trace (ex-data poy))

    (md/deferred? poy)
    @(md/catch (md/chain poy find-traces) find-traces)))
