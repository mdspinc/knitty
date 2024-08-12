(ns knitty.deferred-test
  {:clj-kondo/ignore [:inline-def]}
  (:require [clojure.test :as t :refer [are deftest is testing]]
            [clojure.tools.logging :as log]
            [knitty.deferred :as kd]))


(defmacro future' [& body]
  `(kd/future
     (Thread/sleep 10)
     (try
       ~@body
       (catch Exception e#
         (.printStackTrace e#)))))


(defn future-error
  [ex]
  (kd/future
    (Thread/sleep 10)
    (throw ex)))


(defn capture-success
  ([result]
   (capture-success result true))
  ([result expected-return-value]
   (let [p (promise)]
     (kd/on result
                    #(do (deliver p %) expected-return-value)
                    (fn [_] (throw (Exception. "ERROR"))))
     p)))


(defn capture-error
  ([result]
   (capture-error result true))
  ([result expected-return-value]
   (let [p (promise)]
     (kd/on result
                    (fn [_] (throw (Exception. "SUCCESS")))
                    #(do (deliver p %) expected-return-value))
     p)))


(deftest test-catch
  (is (thrown? ArithmeticException
               @(-> 0
                    (kd/bind #(/ 1 %))
                    (kd/bind-err IllegalStateException (constantly :foo)))))

  (is (thrown? ArithmeticException
               @(-> 0
                    kd/future
                    (kd/bind #(/ 1 %))
                    (kd/bind-err IllegalStateException (constantly :foo)))))

  (is (= :foo
         @(-> 0
              (kd/bind #(/ 1 %))
              (kd/bind-err ArithmeticException (constantly :foo)))))

  (let [d (kd/create)]
    (kd/future (Thread/sleep 100) (kd/error! d :bar))
    (is (= :foo @(kd/bind-err d (constantly :foo)))))

  (is (= :foo
         @(-> (kd/wrap-err :bar)
              (kd/bind-err (constantly :foo)))))

  (is (= :foo
         @(-> 0
              kd/future
              (kd/bind #(/ 1 %))
              (kd/bind-err ArithmeticException (constantly :foo))))))



(def ^:dynamic *test-dynamic-var*)

(deftest test-letm

  (let [flag (atom false)]
    @(let [z (clojure.core/future 1)]
       (kd/letm [z (kd/coerce z)
                 x (kd/future z)
                 _ (kd/future (Thread/sleep 1000) (reset! flag true))
                 y (kd/future (+ z x))]
                (kd/future (+ x x y z))))
    (is (= true @flag)))

  (is (= 5
         @(let [z (clojure.core/future 1)]
            (kd/letm [z (kd/coerce z)
                      x (kd/future (kd/coerce (clojure.core/future z)))
                      y (kd/future (+ z x))]
                     (kd/future (+ x x y z))))))

  (is (= 2
         @(let [d (kd/create)]
            (kd/letm [[x] (future' [1])]
                     (kd/letm [[x'] (future' [(inc x)])
                               y (future' true)]
                              (when y x')))))))


(deftest test-chain-errors
  (let [boom (fn [n] (throw (ex-info "" {:n n})))]
    (doseq [b [boom (fn [n] (kd/future (boom n)))]]
      (dorun
       (for [i (range 10)
             j (range 10)]
         (let [fs (concat (repeat i inc) [boom] (repeat j inc))]
           (is (= i
                  @(-> (reduce kd/bind 0 fs)
                       (kd/bind-err (fn [e] (:n (ex-data e)))))
                  ))))))))


(deftest test-chain
  (dorun
   (for [i (range 10)
         j (range i)]
     (let [fs  (take i (cycle [inc #(* % 2)]))
           fs' (-> fs
                   vec
                   (update-in [j] (fn [f] #(kd/future (f %)))))]
       (is
        (= (reduce #(%2 %1) 0 fs)
           @(reduce kd/bind 0 fs')
           ))))))


(deftest test-deferred
  ;; success!
  (let [d (kd/create)]
    (is (= true (kd/success! d 1)))
    (is (= 1 @(capture-success d)))
    (is (= 1 @d)))

  ;; claim and success!
  (let [d     (kd/create)
        token (kd/claim! d)]
    (is token)
    (is (= false (kd/success! d 1)))
    (is (= true (kd/success! d 1 token)))
    (is (= 1 @(capture-success d)))
    (is (= 1 @d)))

  ;; error!
  (let [d  (kd/create)
        ex (IllegalStateException. "boom")]
    (is (= true (kd/error! d ex)))
    (is (= ex @(capture-error d ::return)))
    (is (thrown? IllegalStateException @d)))

  ;; claim and error!
  (let [d     (kd/create)
        ex    (IllegalStateException. "boom")
        token (kd/claim! d)]
    (is token)
    (is (= false (kd/error! d ex)))
    (is (= true (kd/error! d ex token)))
    (is (= ex @(capture-error d ::return)))
    (is (thrown? IllegalStateException (deref d 1000 ::timeout))))

  ;; test deref with delayed result
  (let [d (kd/create)]
    (future' (kd/success! d 1))
    (is (= 1 (deref d 1000 ::timeout))))

  ;; test deref with delayed error result
  (let [d (kd/create)]
    (future' (kd/error! d (IllegalStateException. "boom")))
    (is (thrown? IllegalStateException (deref d 1000 ::timeout))))

  ;; test deref with non-Throwable error result
  (are [d timeout]
       (= :bar
          (-> (is (thrown? clojure.lang.ExceptionInfo
                           (if timeout (deref d 1000 ::timeout) @d)))
              ex-data
              :error))

    (doto (kd/create)
      (kd/error! :bar)) true

    (doto (kd/create) (as-> d (future' (kd/error! d :bar)))) true

    (kd/wrap-err :bar) true

    (kd/wrap-err :bar) false)

  ;; multiple callbacks w/ success
  (let [n               50
        d               (kd/create)
        callback-values (->> (range n)
                             (map (fn [_] (kd/future (capture-success d))))
                             (map deref)
                             doall)]
    (is (= true (kd/success! d 1)))
    (is (= 1 (deref d 1000 ::timeout)))
    (is (= (repeat n 1) (map deref callback-values))))

  ;; multiple callbacks w/ error
  (let [n               50
        d               (kd/create)
        callback-values (->> (range n)
                             (map (fn [_] (kd/future (capture-error d))))
                             (map deref)
                             doall)
        ex              (Exception.)]
    (is (= true (kd/error! d ex)))
    (is (thrown? Exception (deref d 1000 ::timeout)))
    (is (= (repeat n ex) (map deref callback-values))))

  (comment
  ;; cancel listeners
    (let [l (kd/listener (constantly :foo) nil)
          d (kd/create)]
      (is (= false (kd/cancel-listener! d l)))
      (is (= true (kd/add-listener! d l)))
      (is (= true (kd/cancel-listener! d l)))
      (is (= true (kd/success! d :foo)))
      (is (= :foo @(capture-success d)))
      (is (= false (kd/cancel-listener! d l))))
    )

  ;; deref
  (let [d (kd/create)]
    (is (= :foo (deref d 10 :foo)))
    (kd/success! d 1)
    (is (= 1 @d))
    (is (= 1 (deref d 10 :foo)))))


(deftest test-loop
  ;; body produces a non-deferred value
  (is @(capture-success
        (kd/loop [] true)))

  ;; body raises exception
  (let [ex (Exception.)]
    (is (= ex @(capture-error
                (kd/loop [] (throw ex))))))

  ;; body produces a realized result
  (is @(capture-success
        (kd/loop [] (kd/wrap-val true))))

  ;; body produces a realized error result
  (let [ex (Exception.)]
    (is (= ex @(capture-error
                (kd/loop [] (kd/wrap-err ex))))))

  ;; body produces a delayed result
  (is @(capture-success
        (kd/loop [] (future' true))))

  ;; body produces a delayed error result
  (let [ex (Exception.)]
    (is (= ex @(capture-error
                (kd/loop [] (future-error ex))))))

  ;; destructuring works for loop parameters
  (is (= 1 @(capture-success
             (kd/loop [{:keys [a]} {:a 1}] a))))
  (is @(capture-success
        (kd/loop [[x & xs] [1 2 3]] (or (= x 3) (kd/recur xs))))))


(deftest test-finally
  (let [target-d (kd/create)
        d        (kd/create)
        fd       (kd/bind-fin
                   d
                   (fn []
                     (kd/success! target-d ::delivered)))]
    (kd/on fd identity identity)  ;; clear ELD
    (kd/error! d (Exception.))
    (is (= ::delivered (deref target-d 0 ::not-delivered)))))



(deftest test-alt
  (is (#{1 2 3} @(kd/alt 1 2 3)))
  (is (= 2 @(kd/alt (kd/future (Thread/sleep 10) 1) 2)))

  (is (= 2 @(kd/alt (doto
                     (kd/future (Thread/sleep 10)
                                (throw (Exception. "boom")))
                      (kd/on *))
                    2)))

  (is (thrown-with-msg? Exception #"boom"
                        @(kd/alt (kd/future (throw (Exception. "boom"))) (kd/future (Thread/sleep 10)))))

  (testing "uniformly distributed"
    (let [results (atom {})
          ;; within 10%
          n       1e4, r 10, eps (* n 0.15)
          f       #(/ (% n eps) r)]
      (dotimes [_ n]
        @(kd/bind (apply kd/alt (range r))
                  #(swap! results update % (fnil inc 0))))
      (doseq [[i times] @results]
        (is (<= (f -) times (f +)))))))


(deftest ^:stress test-error-leak-detection
  (System/gc)
  (let [logs (atom [])]
    (with-redefs [log/log* (fn [& lm] (swap! logs conj lm))]
      (dotimes [_ 100]
        (kd/error! (kd/create) (Throwable.)))
      (System/gc)
      (Thread/sleep 1000)
      (is (== 100 (count @logs)))
      )))


(deftest ^:stress test-deferred-chain
  (dotimes [_ 1000]
    (let [d      (kd/create)
          result (kd/future
                   (last
                    (take 1000
                          (iterate
                           #(let [d' (kd/create)]
                              (kd/connect % d')
                              d')
                           d))))]
      (Thread/sleep (long (rand-int 10)))
      (kd/success! d 1)
      (is (= 1 @result)))))



(comment
  (clojure.test/test-ns *ns*))
