(ns ag.lib.knitty
  (:require [clojure.spec.alpha :as s]
            [clojure.walk :as w]
            [manifold.deferred :as md :refer [chain]]
            [taoensso.timbre :refer [debugf]]))


(defrecord Yarn
  [key func deps])

(defonce yarn-registry {})

(defn register-yarn [{k :key, :as sd}]
  (debugf "tie yarn %s" k)
  (alter-var-root #'yarn-registry assoc k sd))


(defn- yank* [ss k]
  (if (contains? ss k)
    ss
    (let [sd (yarn-registry k)
          _ (assert sd (str "Yarn " k " is not registered"))
          deps (:deps sd)
          func (:func sd)
          ss' (reduce yank* ss deps)
          vals (map ss' deps)
          v (if (some md/deferrable? vals)
              (chain (apply md/zip vals)
                        #(func (zipmap deps %)))
              (func ss'))
          d? (md/deferrable? v)]
      (cond-> ss'
        d? (assoc! ::deferreds (cons k (ss' ::deferreds)))
        true (assoc! k v)))))


(defn- yank-result-deferred
  [ss]
  (if-let [ds (ss ::deferreds)]
    (chain
     (apply md/zip (map ss ds))
     #(into
       (dissoc ss ::deferreds)
       (zipmap ds %)))
    (md/success-deferred ss)))


(defn yank
  ([k]
   (fn yank-partial [ss]
     (yank ss k)))
  ([ss k]
   (if (contains? ss k)
     (md/success-deferred ss)
     (yank-result-deferred
      (persistent! (yank* (transient ss) k))))))


(defn yank-all
  ([ks]
   (fn yank-all-partial [ss]
     (yank-all ss ks)))
  ([ss ks]
   (yank-result-deferred
    (persistent!
     (reduce yank* (transient ss) ks)))))


(defn select-ns-keys [m ns]
  (let [s (name ns)]
    (into
     (empty m)
     (comp
      (filter (comp keyword? key))
      (filter #(= s (namespace (key %)))))
     m)))


(defmacro defyarn*
  [nm k doc spec bmap expr]
  `(do
     (s/def ~k ~spec)
     (register-yarn
      (->Yarn
       ~k
       (fn ~nm [~bmap] ~expr)
       ~(vec (vals bmap))))
     (def ~(vary-meta nm assoc :doc doc) ~k)))


(s/def
  ::bmap
  (s/alt
   :vec (s/tuple (s/map-of symbol? (some-fn keyword? symbol?)))
   :map (s/map-of symbol? (some-fn keyword? symbol?))))


(s/def
  ::defyarn
  (s/cat
   :name symbol?
   :doc (s/? string?)
   :spec (s/? s/spec?)
   :bmap ::bmap
   :expr (s/? any?)))


(defn- parse-bmap [bm]
  (let [[bmap-type bmap-body] bm]
    (case bmap-type
      :vec (first bmap-body)
      :map bmap-body)))


(defmacro defyarn
  [nm & body]
  (let [bd (cons nm body)
        cf (s/conform ::defyarn bd)]
    (when (s/invalid? cf)
      (throw (Exception. (s/explain-str ::defyarn bd))))
    (let [{:keys [doc spec bmap expr]
           :or {doc "", spec `any?, bmap [:map {}]}} cf
          expr (or expr `(throw (IllegalStateException. (str "Missing defyarn for " '~nm))))
          k (keyword (-> *ns* ns-name name) (name nm))]
      `(defyarn* ~nm ~k ~doc ~spec ~(parse-bmap bmap) ~expr))))


(defmacro <y
  [k]
  (assert (contains? &env '__yarn__) "Should be used only insdie do-yank")
  `(do
     (assert (contains? ~'__yarn_ays__ ~k) "Error - yank-do> didn't find all <y calls")
     (~k ~'__yarn__)))


(defn- find-yy
  [ast]
  (let [a (atom #{})]
    (w/postwalk
     (fn [x]
       (when (list? x)
         (when-let [[f r] x]
           (when (#{'<y `<y} f)
             (swap! a conj r))))
       x)
     ast)
    (set @a)))


(defmacro yank-it>
  [ss & expr]
  (let [ays (find-yy expr)]
    `(chain
      (yank-all ~ss ~ays)
      (fn ~'yankit [~'__yarn__]
        (let [~'__yarn_ays__ ~ays]
          [~'__yarn__
           ~@expr])))))


(defmacro yank-do>
  [ss & body]
  `(chain
    (yank-it> ~ss (do ~@body nil))
    first))


(comment

  ;; define "yarn" - single slot/value
  (defyarn zero
    [{}]  ;; no inputs
    0)    ;; value, use explicit `do when needed

  ;; true, symbols just resolves into keyword
  (keyword? zero)

  (defyarn one
    [{_ zero}]   ;; wait for zero, but don't use it
    (future 1))  ;; any manifold-like deferred can be returned

  (defyarn two
    [{x ::one}]  ;; deps - vector of a single map of 'symbol -> keyword'
    (inc x))

  (defyarn three
    [{x one y two}]
    (+ x y))

  (defyarn four
    [{y two}]
    (future (* y y)))

  (defyarn five
    [{x two, y three}]
    (future (+ x y)))

  (defyarn six
    [{x two, y three}]
    (* x y))

  ;; yank - ensure one key is inside map - return deferred
  @(yank {} one)
  @(yank {} two)
  @(yank {one 10} four)

  ;; yank-all - ensure multiple keys at once (faster) - usually prefer this function
  @(yank-all {one 10} four)
  ;; 'also' - helper function (FIXME: is there something in stdlib?)

  ;; experimental ;)f
  @(yank-it>
    {}
    {:first (<y six)})

  @(chain
    (yank {} ::one)
    #(yank-do> % (println "four = " (<y five)))
    #(yank-do> % (println "five = " (<y five)))
    #(yank-do> % (println "six = " (<y six))))

  (require '[criterium.core :as cc])
  (cc/quick-bench @(yank-all {} [five six]))

  )