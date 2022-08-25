(ns ag.knitty.example
  (:require [ag.knitty.core :refer [defyarn yarn with-yarns doyank yank]]
            [ag.knitty.traceviz :refer [view-trace render-trace]]
            [manifold.deferred :as md]))

(defyarn zero;; define "yarn" - single slot/value
  {}         ;; no inputs
  0)         ;; value, use explicit `do when needed

(defyarn one
  {_ zero}   ;; wait for zero, but don't use it
  1)         ;; any manifold-like deferred can be finished

(defyarn one-slooow
  {}
  (future (Thread/sleep (rand-int 20)) 1))

(defyarn two
  {^:defer x one
   ^:defer y one-slooow}     ;; don't unwrap deferred (all values coerced to manifold/deffered)
  (md/chain' (md/alt x y) inc))

(defyarn three-fast {x one, y two}
  (future
    (Thread/sleep (rand-int 5)) (+ x y)))

(defyarn three-slow {x one, y two}
  (future
    (Thread/sleep (rand-int 10)) (+ x y)))

(defyarn three        ;; put deferred into delay, enables branching
  {^:lazy         f three-fast
   ^:lazy ^:defer s three-slow}
  (if (zero? (rand-int 2)) f s))

(defyarn four
  {x ::one, y ::three};; use raw keywords (not recommended)
  (future (+ x y)))

(defyarn five
  {x ::two, y three}  ;; mixed approach 
  (+ x y))

(defyarn six
  ^{:doc "doc string"}       ;; doc
  ^{:spec number?}           ;; spec
  {x ::two , y ::three}
  (do                        ;; explicit do
    (println "debug print")
    (* x y)))

;; yank - ensure all keys are inside the map - finishs deferred
@(yank {} [one])
@(yank {} [six])
@(yank {one 1000} [four six])
@(yank {two 2000} [four five])

;; dynamically create 'yarn - not recommended
@(yank {} [(yarn ::eight {f ::four} (* f 2))])

;; dynamically create 'yarn & capture locals
(for [i (range 1 4)]
  @(yank {} [(yarn ::seven {s ::six} (* i s))]))

;; redefine (aka mock) yarns in registry
(with-yarns [(yarn ::three {} (rand-int 1000))
             (yarn ::four {} (assert false))]
  @(yank {} [::six]))

;; or dynamically switch yarns
(with-yarns [(rand-nth
              [(yarn ::seven {s ::six} (float (+ s 1)))
               (yarn ::seven {s ::six} (long (+ s 1)))])]
  @(yank {} [::seven]))

;; recommended to insntall 'xdot' (via pkg manager or pip)
(view-trace @(yank {} [six]))

;; view all traces at once
(view-trace
 (md/chain
  {}
  #(yank % [two]) second
  #(yank % [five]) second
  #(assoc % ::two 222222)
  #(yank % [six]) second))

;; get raw format
(render-trace (yank {} [six]) :raw)

;; or graphviz dot
(render-trace (yank {} [six]) :dot)

@(doyank
  {one 10} {x six}
  (println ">>" x))