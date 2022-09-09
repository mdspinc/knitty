(ns ag.knitty.impl
  (:require [ag.knitty.mdm :refer [create-mdm mdm-freeze! mdm-fetch!]]
            [ag.knitty.trace :as t :refer [capture-trace!]]
            [manifold.deferred :as md]
            [manifold.executor]
            [manifold.utils]
            [taoensso.timbre :refer [info]])
  (:import [ag.knitty.mdm MutableDeferredMap]))


(defprotocol IYarn
  (yarn-get [_ mdm reg tracer] "get value or obligated deferred")
  (yarn-key [_] "get yarn key"))


(deftype Yarn [key deps funcv yget]
  IYarn
  (yarn-get
    [_ mdm reg tracer]
    (yget mdm reg tracer))
  (yarn-key [_] key))


(extend-protocol IYarn
  clojure.lang.Keyword
  (yarn-key
    [k]
    k)
  (yarn-get
    [k mdm reg tracer]
    (let [y (reg k)]
      (when-not y
        (throw (ex-info "yarn is not registered" {:knitty/yarn k})))
      (yarn-get y mdm reg tracer))))


(defmethod print-method Yarn [y ^java.io.Writer w]
  (.write w "#knitty/yarn ")
  (.write w (str (yarn-key y))))


(defn coerce-deferred [v]
  (let [v (force v)]
    (md/->deferred v v)))


(defn as-deferred [v]
  (if (md/deferrable? v)
    (md/->deferred v)
    (md/success-deferred v nil)))


(defn bind-param-type [ds]
  (let [{:keys [defer lazy]} (meta ds)]
    (cond
      lazy  :lazy
      defer :defer
      :else :sync)))


(defrecord NotADeferred [deferred])

(defmacro unwrap-not-a-deferred [w]
  `(let [w# ~w]
     (if (instance? NotADeferred w#)
       (.-deferred ^NotADeferred w#)
       w#)))


(definline yarn-get-sync [k mdm reg tracer]
  `(do
     (when ~tracer (t/trace-dep ~tracer ~k))
     (yarn-get (~reg ~k ~k) ~mdm ~reg ~tracer)))


(definline yarn-get-defer [k mdm reg tracer]
  `(do
     (when ~tracer (t/trace-dep ~tracer ~k))
     (NotADeferred. (as-deferred (yarn-get (~reg ~k ~k) ~mdm ~reg ~tracer)))))


(defn yarn-get-lazy [k mdm reg tracer]
  (delay
   (when tracer (t/trace-dep tracer k))
   (as-deferred
    (yarn-get (reg k k) mdm reg tracer))))


(defn resolve-executor-var [e]
  (when e
    (when-let [ee (var-get e)]
      (if (ifn? ee) (ee) ee))))


(defn run-future [executor-var thefn]
  (let [d (md/deferred)
        c (md/claim! d)]
    (manifold.utils/future-with
     (resolve-executor-var executor-var)
     (try
       (let [v (md/unwrap' (thefn))]
         (if (md/deferred? v)
           (md/on-realized v
                           #(md/success! d % c)
                           #(md/error! d % c))
           (md/success! d v c)))
       (catch Throwable e
         (md/error! d e c))))
    d))


(defmacro maybe-future-with [executor-var & body]
  (if-not executor-var
    `(do ~@body)
    `(run-future ~executor-var (fn [] ~@body))))


(defn- exception-java-cause [ex]
  (cond
    (nil? ex) nil
    (instance? clojure.lang.ExceptionInfo ex) (recur (ex-cause ex))
    (instance? java.util.concurrent.ExecutionException ex) (recur (ex-cause ex))
    :else ex))


(defn wrap-yarn-exception [k ex]
  (let [d (ex-data ex)]
    (if (::inyank d)
      (ex-info "yarn failure"
               (assoc d
                      :knitty/failed-yarn k
                      :knitty/failed-yarn-chain (conj (:knitty/failed-yarn-chain d) k))
               (ex-cause ex))
      (ex-info "yarn failure"
               (assoc d
                      ::inyank true
                      :knitty/fail-at   (java.util.Date.)
                      :knitty/failed-yarn k
                      :knitty/failed-yarn-chain [k]
                      :knitty/java-cause (exception-java-cause ex))
               ex))))


(defn- build-yank-fns
  [k bind expr]
  (let [mdm (with-meta '_mdm {:tag `MutableDeferredMap})
        reg '_reg
        tracer '_tracer
        the-fnv (gensym "fn")

        yank-all-deps
        (mapcat identity
                (for [[ds dk] bind]
                  [ds
                   (case (bind-param-type ds)
                     :sync   `(yarn-get-sync  ~dk ~mdm ~reg ~tracer)
                     :defer  `(yarn-get-defer ~dk ~mdm ~reg ~tracer)
                     :lazy   `(yarn-get-lazy  ~dk ~mdm ~reg ~tracer))]))

        maybe-unwrap-defers
        (mapcat identity
                (for [[ds _dk] bind
                      :when (#{:defer :lazy} (bind-param-type ds))]
                  [ds `(unwrap-not-a-deferred ~ds)]))

        deps (keys bind)
        executor-var (:executor (meta bind))]

    `(let [;; input - vector. unwraps 'defers', called by yget-fn#
           ~the-fnv
           (fn ~(-> k name symbol)
             ([[~@deps] ~tracer]
              (when ~tracer (t/trace-call ~tracer))
              (coerce-deferred (let [~@maybe-unwrap-defers] ~expr))))

           ;; input - mdm and registry, called by `yank-snatch
           yget-fn#
           (fn ~(-> k name (str "--yarn") symbol) [~mdm ~reg ~tracer]
             (let [~tracer (when ~tracer (t/trace-build-sub-tracer ~tracer ~k))]
               (let [[claim# d#] (mdm-fetch! ~mdm ~k)]
                 (if-not claim#
                   ;; got item from mdm
                   d#

                   ;; calculate & provide to mdm
                   (maybe-future-with
                    ~executor-var
                    (when ~tracer (t/trace-start ~tracer :yarn))

                    (try ;; d# is alsways deffered
                      (when ~tracer
                        (t/trace-all-deps ~tracer
                                          ~(vec (for [[ds dk] bind] [dk (bind-param-type ds)]))))

                      (let [~@yank-all-deps]
                        (let [x# (if ~(or
                                       (some? executor-var)
                                       (list*
                                        `or
                                        (for [d deps, :when (= (bind-param-type d) :sync)]
                                          `(md/deferred? ~d))))
                                   (md/chain' (md/zip' ~@deps) (fn [z#] (~the-fnv z# ~tracer)))
                                   (~the-fnv [~@deps] ~tracer))]
                          (if (md/deferred? x#)
                            (do
                              (md/on-realized
                               x#
                               (fn [xv#]
                                 (when ~tracer (t/trace-finish ~tracer xv# nil true))
                                 (md/success! d# xv# claim#))
                               (fn [e#]
                                 (let [ew# (wrap-yarn-exception ~k e#)]
                                   (when ~tracer (t/trace-finish ~tracer nil ew# true))
                                   (md/error! d# ew# claim#))))
                              d#)
                            (do
                              (when ~tracer (t/trace-finish ~tracer x# nil false))
                              (md/success! d# x# claim#)
                              x#))))

                      (catch Throwable e#
                        (let [ew# (wrap-yarn-exception ~k e#)]
                          (when ~tracer (t/trace-finish ~tracer nil ew# false))
                          (md/error! d# ew# claim#)
                          d#))))))))]
       [~the-fnv
        yget-fn#])))


(deftype YarnRef [key orig-key]

  IYarn
  (yarn-key
    [_]
    key)

  (yarn-get
   [_ mdm reg tracer]
   (let [tracer (when tracer (t/trace-build-sub-tracer tracer key))
         [claim d] (mdm-fetch! ^MutableDeferredMap mdm key)]
     (if-not claim
       d
       (do
         (when tracer (t/trace-start tracer :knot))
         (try ;; d# is alsways deffered
           (when tracer (t/trace-all-deps tracer [[orig-key :ref]]))
           (let [x (yarn-get-sync orig-key mdm reg tracer)]
             (if (md/deferred? x)
               (do
                 (md/on-realized
                  x
                  (fn [xv]
                    (when tracer (t/trace-finish tracer xv nil true))
                    (md/success! d xv claim))
                  (fn [e]
                    (let [ew (wrap-yarn-exception key e)]
                      (when tracer (t/trace-finish tracer nil ew true))
                      (md/error! d ew claim))))
                 d)
               (do
                 (when tracer (t/trace-finish tracer x nil false))
                 (md/success! d x claim)
                 x)))
           (catch Throwable e
             (let [ew (wrap-yarn-exception key e)]
               (when tracer (t/trace-finish tracer nil ew false))
               (md/error! d ew claim)
               d))))))))


(defmethod print-method YarnRef [^YarnRef y ^java.io.Writer w]
  (.write w "#knitty/yarn ")
  (.write w (str [(.-key y) (.-orig-key y)])))


(defn gen-yarn
  [key bind expr]
  `(let [[funcv# yget#] ~(build-yank-fns key bind expr)]
     (Yarn.
      ~key
      ~(vec (vals bind))
      funcv#
      yget#)))



(defn gen-yarn-ref
  [key from]
  (list `YarnRef. key from))


(defn- hide [d]
  (delay d))


(defn yank*
  [poy yarns registry tracer]
  (let [mdm (create-mdm poy (max 8 (* 2 (count yarns))))
        errh (fn [e]
               (throw (ex-info "failed to yank"
                               (assoc (dissoc (ex-data e) ::inyank)
                                      :knitty/yanked-poy (hide poy)
                                      :knitty/failed-poy (hide (mdm-freeze! mdm))
                                      :knitty/yanked-yarns yarns
                                      :knitty/trace (when tracer
                                                         (conj
                                                          (-> poy meta :knitty/trace)
                                                          (capture-trace! tracer))))
                               e)))]
    (try
      (md/catch'
       (md/chain'
        (apply md/zip' (map #(yarn-get (registry % %) mdm registry tracer) yarns))
        (fn [yvals]
          [yvals
           (let [poy' (mdm-freeze! mdm)]
             (if tracer
               (vary-meta poy' update :knitty/trace conj (capture-trace! tracer))
               poy'))]))
       errh)
      (catch Throwable e (errh e)))))
