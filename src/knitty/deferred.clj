(import 'knitty.javaimpl.KnittyLoader)
(KnittyLoader/touch)

(ns knitty.deferred
  (:refer-clojure :exclude [future future-call run! while reduce] )
  (:require [clojure.algo.monads :as m]
            [clojure.core :as c]
            [clojure.tools.macro :refer [with-symbol-macros]]
            [clojure.tools.logging :as log]
            [manifold.deferred :as md])
  (:import  [knitty.javaimpl KDeferred KAwaiter]
            [manifold.deferred IDeferred IMutableDeferred]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

;; ==

(defn create
  {:inline
   (fn
     ([] `(KDeferred/create))
     ([token] `(KDeferred/create ~token)))
   :inline-arities #{1 2}}
  (^KDeferred []
   (KDeferred/create))
  (^KDeferred [token]
   (KDeferred/create token)))

(definline wrap-err [e]
  `(KDeferred/wrapErr ~e))

(definline wrap-val [v]
  `(KDeferred/wrapVal ~v))

(definline wrap [x]
  `(KDeferred/wrap ~x))

(definline wrap* [x]
  `(let* [x# ~x] (wrap (manifold.deferred/->deferred x# x#))))

(defn future-call [f]
  (let [frame (clojure.lang.Var/cloneThreadBindingFrame)
        d (create)]
    (.execute
     clojure.lang.Agent/soloExecutor
     (fn []
       (clojure.lang.Var/resetThreadBindingFrame frame)
       (try
         (when-not (.realized d)
           (.chain d (f) nil))
         (catch Throwable e (.error d e nil)))))
    d))

(defmacro future [& body]
  `(future-call (^{:once true} fn* [] ~@body)))

;; ==

(defn kd-claim!
  ([^KDeferred d] (.claim d))
  ([^KDeferred d token] (.claim d token)))

(definline kd-get [kd]
  `(let [^KDeferred x# ~kd] (.get x#)))

(definline kd-chain-from [kd d token]
  `(let [^KDeferred x# ~kd] (.chain x# ~d ~token)))

(definline kd-revoke-to [kd d]
  `(let [^KDeferred x# ~kd] (.revokeTo x# ~d)))

;; ==

(defmacro success!
  ([d x] `(success! ~d ~x nil))
  ([d x token] `(let* [^IMutableDeferred d# ~d] (.success d# ~x ~token))))

(defmacro error!
  ([d x] `(error! ~d ~x nil))
  ([d x token] `(let* [^IMutableDeferred d# ~d] (.error d# ~x ~token))))

(definline deferred? [x]
  `(instance? IDeferred ~x))

(definline unwrap1 [x]
  `(let [x# ~x]
     (if (instance? IDeferred x#)
       (.successValue ^IDeferred x# x#)
       x#)))

;; ==

(defn bind

  ([d val-fn]
   (let [d' (unwrap1 d)]
     (if (deferred? d')
       (.bind (KDeferred/wrapDeferred d') val-fn nil nil)
       (try (KDeferred/wrap (val-fn d')) (catch Throwable e (wrap-err e))))))

  ([d val-fn err-fn]
   (let [d' (unwrap1 d)]
     (if (deferred? d')
       (.bind (KDeferred/wrapDeferred d') val-fn err-fn nil)
       (try (KDeferred/wrap (val-fn d')) (catch Throwable e (wrap-err e))))))

  ([d val-fn err-fn token]
   (let [d' (unwrap1 d)]
     (if (deferred? d')
       (.bind (KDeferred/wrapDeferred d') val-fn err-fn token)
       (try (KDeferred/wrap (val-fn d')) (catch Throwable e (wrap-err e)))))))


(defn bind*
  ([d val-fn executor]
   (.bind (wrap d) val-fn nil nil executor))
  ([d val-fn err-fn executor]
   (.bind (wrap d) val-fn err-fn nil executor))
  ([d val-fn err-fn token executor]
   (.bind (wrap d) val-fn err-fn token executor)))

(defn bind-err
  ([mv f] (bind mv identity f))
  ([mv exc f] (bind-err mv (fn [e] (if (instance? exc e) (f e) (wrap-err e))))))

(defn bind-after
  ([d f0]
   (bind
    d
    (fn [x] (f0) x)
    (fn [e] (f0) (wrap-err e)))))

(defmacro bind-> [expr & forms]
  (list*
   `->
   expr
   (map (comp (partial list `bind)
              #(if (seq? %) `(fn [x#] (-> x# ~%)) %))
        forms)))

(defn revoke
  ([d cancel-fn] (revoke d cancel-fn nil))
  ([d cancel-fn err-handler] (KDeferred/revoke d cancel-fn err-handler)))


(def anil (wrap-val nil))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(m/defmonad deferred-m
  [m-zero anil
   m-bind bind
   m-result wrap-val])


(defmacro with-monad-deferred
  [& exprs]
  `(let [~'m-bind   bind
         ~'m-result wrap-val
         ~'m-zero   anil]
     (with-symbol-macros ~@exprs)))


(defmacro letm [binds & body]
  `(with-monad-deferred
     (m/domonad ~binds (do ~@body))))

;; ==

(defn- iterator ^java.util.Iterator [xs]
  (let [^java.lang.Iterable c (if (instance? java.lang.Iterable xs) xs (seq xs))]
    (.iterator c)))

(definline kd-await!* [ls ds]
  `(KAwaiter/awaitIter ~ls (iterator ~ds)))

(defmacro kd-await!
  ([ls]
   `(let [ls# ~ls] (when (KAwaiter/await ls#) (ls#))))
  ([ls x1]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1) (ls#))))
  ([ls x1 x2]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2) (ls#))))
  ([ls x1 x2 x3]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3) (ls#))))
  ([ls x1 x2 x3 x4]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4) (ls#))))
  ([ls x1 x2 x3 x4 x5]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5 ~x6) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6 x7]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13 ~x14) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13 ~x14 ~x15) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16]
   `(let [ls# ~ls] (when (KAwaiter/await ls# ~x1 ~x2 ~x3 ~x4 ~x5 ~x6 ~x7 ~x8 ~x9 ~x10 ~x11 ~x12 ~x13 ~x14 ~x15 ~x16) (ls#))))
  ([ls x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 & xs]
   (let [xs (list* x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 xs)
         n (count xs)
         df (gensym)]
     `(let [ls# ~ls]
        (when (KAwaiter/awaitArr
               ls#
               (let [~df (KAwaiter/createArr ~n)]
                 ~@(for [[i x] (map vector (range) xs)]
                     `(KAwaiter/setArrItem ~df ~i ~x))
                 ~df))
          (ls#))))))

;; ==

(defn- call-after-all'
  [ds f]
  (let [d (create)]
    (KAwaiter/awaitIter
     (fn
       ([] (success! d (f) nil))
       ([e] (error! d e nil)))
     (iterator ds))
    d))

(defn zip*
  ([vs] (call-after-all'
         vs
         (fn []
           (let [it (iterator vs)]
             (iterator-seq
              (reify java.util.Iterator
                (hasNext [_] (.hasNext it))
                (next [_] (unwrap1 (.next it)))))))))
  ([a vs] (zip* (list* a vs)))
  ([a b vs] (zip* (list* a b vs)))
  ([a b c vs] (zip* (list* a b c vs)))
  ([a b c d vs] (zip* (list* a b c d vs)))
  ([a b c d e & vs] (zip* (concat [a b c d e] vs))))

(defmacro ^:private zip-inline [& xs]
  `(let [~@(mapcat identity (for [x xs] [x `(wrap ~x)]))]
         (let [res# (create)]
               (kd-await!
                (fn
                  ([] (success! res# ~(vec (for [x xs] `(kd-get ~x))) nil))
                  ([e#] (error! res# e# nil)))
                ~@xs)
               res#)))
(defn zip
  ([] (wrap-val []))
  ([a] (bind a vector))
  ([a b] (zip-inline a b))
  ([a b c] (zip-inline a b c))
  ([a b c d] (zip-inline a b c d))
  ([a b c d e] (zip-inline a b c d e))
  ([a b c d e f] (zip-inline a b c d e f))
  ([a b c d e f g] (zip-inline a b c d e f g))
  ([a b c d e f g h] (zip-inline a b c d e f g h))
  ([a b c d e f g h i] (zip-inline a b c d e f g h i))
  ([a b c d e f g h i j] (zip-inline a b c d e f g h i j))
  ([a b c d e f g h i j k] (zip-inline a b c d e f g h i j k))
  ([a b c d e f g h i j k l] (zip-inline a b c d e f g h i j k l))
  ([a b c d e f g h i j k l m] (zip-inline a b c d e f g h i j k l m))
  ([a b c d e f g h i j k l m n] (zip-inline a b c d e f g h i j k l m n))
  ([a b c d e f g h i j k l m n o] (zip-inline a b c d e f g h i j k l m n o))
  ([a b c d e f g h i j k l m n o p] (zip-inline a b c d e f g h i j k l m n o p))
  ([a b c d e f g h i j k l m n o p & z]
   (bind
    (zip a b c d e f g h i j k l m n o p)
    (fn [xg] (call-after-all' z #(into xg (map unwrap1) z))))))


(def ^:private ^java.util.Random alt-rnd (java.util.Random.))

(defn- alt-in
  ([^KDeferred res a b]
   (if (== 0 (.nextInt alt-rnd 2))
     (do (.chain res a nil) (when-not (.realized res) (.chain res b nil)))
     (do (.chain res b nil) (when-not (.realized res) (.chain res a nil)))))
  ([^KDeferred res a b c]
   (case (.nextInt alt-rnd 3)
     0 (do (.chain res a nil) (when-not (.realized res) (alt-in res b c)))
     1 (do (.chain res b nil) (when-not (.realized res) (alt-in res a c)))
     2 (do (.chain res c nil) (when-not (.realized res) (alt-in res a b)))))
  ([^KDeferred res a b c d]
   (case (.nextInt alt-rnd 4)
     0 (do (.chain res a nil) (when-not (.realized res) (alt-in res b c d)))
     1 (do (.chain res b nil) (when-not (.realized res) (alt-in res a c d)))
     2 (do (.chain res c nil) (when-not (.realized res) (alt-in res a b d)))
     3 (do (.chain res d nil) (when-not (.realized res) (alt-in res a b c))))))

(defn alt
  ([a]
   (wrap a))
  ([a b]
   (doto (create) (alt-in a b)))
  ([a b c]
   (doto (create) (alt-in a b c)))
  ([a b c d]
   (doto (create) (alt-in a b c d)))
  ([a b c d & vs]
   (let [^KDeferred res (create)]
     (clojure.core/reduce
      (fn [_ x] (.chain res x nil) (when (.realized res) (reduced nil)))
      (doto (java.util.ArrayList. (+ (count vs) 4))
        (.addAll ^clojure.lang.PersistentList vs)
        (.add a)
        (.add b)
        (.add c)
        (.add d)
        (java.util.Collections/shuffle)))
     res)))

;; ==

(defn iterate-while
  [f p x]
  (let [d (create)
        ef (fn [e] (error! d e))]
    (bind
     x
     (fn g [x]
       (cond

         (deferred? x)
         (let [y (wrap x)]
           (if (.listen0 y (knitty.javaimpl.AListener/fromFn g ef))
             d
             (recur (try (.get y) (catch Throwable t (ef t))))))

         (p x)
         (let [y (unwrap1 (f x))]
           (if (deferred? y)
             (let [y (wrap y)]
               (if (.listen0 y (knitty.javaimpl.AListener/fromFn g ef))
                 d
                 (recur (try (.get y) (catch Throwable t (ef t))))))
             (recur y)))

         :else
         (success! d x))))
    d))

(defmacro while
  ([body]
   `(iterate-while
     (fn [x#] (when x# ~body))
     identity
     true))
  ([pred & body]
   `(while (bind ~pred (fn [c#] (when c# ~@body))))))

(defn reduce [f initd xs]
  (bind
   (let [it (iterator xs)]
     (iterate-while
      (fn [a] (bind (.next it) #(f a %)))
      (fn [a] (and (not (reduced? a)) (.hasNext it)))
      initd))
   unreduced))

(defn run! [f xs]
  (let [it (iterator xs)]
    (iterate-while
     (fn [_] (bind (.next it) f))
     (fn [_] (.hasNext it))
     nil)))

;; ==

(KDeferred/setExceptionLogFn
 (fn log-ex
   [error? e msg]
   (if error?
     (log/error e msg)
     (log/warn e msg))))

(defmethod print-method KDeferred [y ^java.io.Writer w]
  (.write w "#knitty/D[")
  (let [error (md/error-value y ::none)
        value (md/success-value y ::none)]

    (cond
      (not (identical? error ::none))
      (do
        (.write w ":err ")
        (print-method (class error) w)
        (.write w " ")
        (print-method (ex-message error) w))

      (not (identical? value ::none))
      (print-method value w)

      :else
      (.write w "…")))
  (.write w "]"))
