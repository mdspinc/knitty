(ns ag.knitty.deferred
  (:refer-clojure :exclude [await])
  (:require [clojure.tools.logging :as log]
            [manifold.deferred :as md]
            [ag.knitty.deferred :as kd])
  (:import [java.util.concurrent CancellationException TimeoutException CountDownLatch]
           [java.util.concurrent.atomic AtomicReference]
           [manifold.deferred IDeferred IMutableDeferred IDeferredListener]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare connect-to-ka-deferred)
(declare ka-deferred)
(declare connect-two-deferreds)


(definline success'! [d x]
  `(let [^IMutableDeferred d# ~d] (.success d# ~x)))

(definline error'! [d x]
  `(let [^IMutableDeferred d# ~d] (.error d# ~x)))

(definline unwrap1' [x]
  `(let [x# ~x]
     (if (md/deferred? x#)
       (md/success-value x# x#)
       x#)))

(definline listen'!
  [d ls]
  `(let [^IMutableDeferred d# ~d] (.addListener d# ~ls)))

(definline listen!
  [d ls]
  `(let [d# ~d
         ^IDeferredListener ls# ~ls]
     (if (instance? IMutableDeferred d#)
       (.addListener ^IMutableDeferred d# ls#)
       (md/on-realized d#
                       (fn ~'on-okk [x#] (.onSuccess ls# x#))
                       (fn ~'on-err [e#] (.onError ls# e#))))))


(defmacro connect-to-ka-deferred [v ka-deferred]
  `(let [d1# ~v
         d2# ~ka-deferred]
     (if (md/deferred? d1#)
       (listen'! d1# d2#)
       (success'! d2# d1#))))



;; sequentially attaches itself as a listener
(deftype AwaiterListener [^:volatile-mutable ^int i
                          ^objects da
                          ^IMutableDeferred r
                          ^clojure.lang.IFn callback]
  
  manifold.deferred.IDeferredListener
  (onSuccess
   [this _]
   (loop [ii (int i)]  ;; desc order on purpose
     (if (== ii 0)
       (try
         (connect-to-ka-deferred (callback) r)
         (catch Throwable e (error'! r e)))
       (let [ii' (unchecked-dec-int ii)
             v (md/unwrap' (aget da ii'))]
         (if (md/deferred? v)
           (do
             (aset da ii' v)
             (set! i ii')
             (listen! v this))
           (do
             (aset da ii' nil)
             (recur ii')))))))
  (onError
   [_ e]
   (error'! r e)))


(defn await*
  [^objects ds f]
  (let [r (ka-deferred)
        a (AwaiterListener. (alength ds) ds r f)]
    (.onSuccess a nil)
    r))


(def ^:private cancellation-exception
  (doto (CancellationException. (str ::cancel!))
    (.setStackTrace (make-array StackTraceElement 0))))


(defn cancel! [d]
  (try
    (md/error! d cancellation-exception)
    (catch Exception e (log/error e "failure while cancelling"))))


(defn revokation-exception? [e]
  (or (instance? CancellationException e)
      (instance? TimeoutException e)))


(deftype RevokeListener 
  [^IDeferred deferred
   ^clojure.lang.IFn canceller]
  
  manifold.deferred.IDeferredListener
  (onSuccess [_ _] (when-not (md/realized? deferred) (canceller)))
  (onError [_ _] (when-not (md/realized? deferred) (canceller)))
)


(defn revoke' [^IDeferred d c]
  (let [d' (ka-deferred)]
    (connect-to-ka-deferred d d')
    (md/add-listener! d' (RevokeListener. d c))
    d'))


;; >> Knitty-aware deferred
;; slimmer version of manifold.deferred.IMutableDeferred
;; produces smaller stacktraces (just a nice touch) & works a tiny bit faster
;; also custom deferred class allows us to introduce any custom field for MDM

(deftype ListenerCons [listener next])
(def ^:const empty-listener-cons (ListenerCons. nil nil))

(deftype CountDownListener [^CountDownLatch cdl]
  manifold.deferred.IDeferredListener
  (onSuccess [_ _] (.countDown cdl))
  (onError [_ _] (.countDown cdl)))

(deftype KaListener [on-success on-error]
  IDeferredListener
  (onSuccess [_ x] (on-success x))
  (onError [_ e] (on-error e)))


(defmacro ^:private kd-deferred-deref
  [this await-form]
  `(let [s# ~'state]
     (if (== s# 1)
       ~'val
       (if (== s# 2)
         (throw ~'val)
         (let [cdl# (CountDownLatch. 1)]
           (.addListener ~this (CountDownListener. cdl#))
           (-> cdl# ~await-form)
           (if (== ~'state 1)
             ~'val
             (throw ~'val)))))))

(defmacro ^:private kd-deferred-onrealized
  [_this on-okk on-err listener]
  `(when-not
    (loop [s# (.get ~'callbacks)]
      (when s#
        (or
         (.compareAndSet ~'callbacks s# (ListenerCons. ~listener s#))
         (recur (.get ~'callbacks)))))
       (loop [] 
         (let [s# ~'state] 
           (if (== s# 1) 
             (->> ~'val ~on-okk)
             (if (== s# 2)
               (->> ~'val ~on-err)
               (recur)))))))

(defmacro ^:private kd-deferred-succerr
  [_this x ondo state]
  `(when-let [css# (.getAndSet ~'callbacks nil)]
     (set! ~'val ~x)
     (set! ~'state (int ~state))
     (loop [cs# css#]
       (let [^IDeferredListener c# (.-listener ^ListenerCons cs#)]
         (when-not (nil? c#)
           (try
             (-> c# ~ondo)
             (catch Throwable e# (log/error e# "error in deferred handler")))
           (recur (.-next ^ListenerCons cs#)))))
     true))

(deftype KaDeferred
         [^AtomicReference callbacks       ;; nil - deferred is semi-realized
          ^:volatile-mutable ^int state    ;; 1 ok, 2 err, 0 unrealized
          ^:unsynchronized-mutable val
          ^:volatile-mutable mta]

  clojure.lang.IDeref
  (deref [this] (kd-deferred-deref this (.await)))

  clojure.lang.IBlockingDeref
  (deref [this time timeout] (kd-deferred-deref this (.await time timeout)))

  clojure.lang.IPending
  (isRealized [_] (not (== 0 state)))

  clojure.lang.IFn
  (invoke [this x] (when (if (instance? Throwable x) (.onError this x) (.onSuccess this x)) this))

  clojure.lang.IReference
  (meta [_] mta)
  (resetMeta [_ m] (locking (set! mta m)))
  (alterMeta [_ f args] (locking callbacks (set! mta (apply f mta args))))

  manifold.deferred.IDeferred
  (realized [_] (not (== 0 state)))
  (onRealized [this on-okk on-err] (kd-deferred-onrealized this on-okk on-err (KaListener. on-okk on-err)))
  (successValue [_ default] (if (== state 1) val default))
  (errorValue [_ default] (if (== state 2) val default))

  manifold.deferred.IDeferredListener
  (onSuccess [this x] (.success this x))
  (onError [this x]  (.error this x))

  manifold.deferred.IMutableDeferred
  (addListener [this t] (let [^IDeferredListener t t] (kd-deferred-onrealized this (.onSuccess t) (.onError t) t)))
  (success [this x] (kd-deferred-succerr this x (.onSuccess x) 1))
  (error [this x]   (kd-deferred-succerr this x (.onError x) 2))
  (claim [_] nil)
  (cancelListener [_ _l] (throw (UnsupportedOperationException.)))
  (success [_ _x _token] (throw (UnsupportedOperationException.)))
  (error [_ _x _token] (throw (UnsupportedOperationException.)))

  ;;
  )

(definline ka-deferred
  "fast lock-free deferred (no executor/claim support)"
  []
  `(KaDeferred. (AtomicReference. empty-listener-cons) 0 nil nil))


(defmethod print-method KaDeferred [y ^java.io.Writer w]
  (.write w "#ag.knitty/Deferred[")
  (let [error (md/error-value y ::none)
        value (md/success-value y ::none)]
    (cond
      (not (identical? error ::none))
      (do
        (.write w ":error ")
        (print-method (class error) w)
        (.write w " ")
        (print-method (ex-message error) w))
      (not (identical? value ::none))
      (do
        (.write w ":value ")
        (print-method value w))
      :else
      (.write w "…")))
  (.write w "]"))

;; >> Knitty aware deferred

(deftype DeferredHookListener [^IMutableDeferred d]
  manifold.deferred.IDeferredListener
  (onSuccess [_ x] (success'! d x))
  (onError [_ e] (error'! d e)))


(defn connect-two-deferreds [^IDeferred d1 ^IDeferred d2]
  (if (instance? KaDeferred d2)
    (listen'! d1 d2)
    (listen'! d1 (DeferredHookListener. d2))))

