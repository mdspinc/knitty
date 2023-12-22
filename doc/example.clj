(require 'manifold.deferred)
(ns example
  (:require [knitty.core :refer [defyarn defyarn-method defyarn-multi
                                 doyank! link-yarn yank yarn]]
            [knitty.tracetxt :refer [print-trace]]
            [knitty.traceviz :refer [render-trace view-trace]]
            [manifold.deferred :as md]))


(defyarn zero;; define "yarn" - single slot/value
  {}         ;; no inputs
  0) ;; value, use explicit `do when needed

(yank {} [zero])

(defyarn one
  {_ zero}   ;; wait for zero, but don't use it
  1) ;; any manifold-like deferred can be finished

(defyarn one-slooow
  {}
  (md/future (Thread/sleep (long (rand-int 20))) 1))

(defyarn two
  {^:defer x one
   ^:defer y one-slooow}     ;; don't unwrap deferred (all values coerced to manifold/deffered)
  (md/chain' (md/alt x y) inc))


(defyarn three-stategy {}
  (rand-nth [::fast ::slow]))

(defyarn-multi three three-stategy)

(defyarn-method three ::fast {x one, y two}
  (md/future
    (Thread/sleep (long (rand-int 5))) (+ x y)))

(defyarn-method three ::slow {x one, y two}
  (md/future
    (Thread/sleep (long (rand-int 10))) (+ x y)))


;; use raw keywords (not recommended)
(defyarn four
  {x ::one, y three}
  (md/future (+ x y)))


;; predeclare yarn without implementation
(defyarn abs-three)

;; link implementation
(link-yarn abs-three three)

(defyarn five
  "doc string"               ;; doc
  {x ::two, y abs-three}
  (println "debug>>>" x y)
  (+ x y))

;; or

(defyarn six
  ^{:doc "doc string"}       ;; doc
  ^{:spec number?}           ;; spec
  {y ::three x ::two}
  (println "debug>>>" x y)
  (* x y))


@(yank {} [three])

;; yank - ensure all keys are inside the map - finishs deferred
@(yank {} [abs-three])
@(yank {} [five])
@(yank {one 1000} [four six])

@(yank {two 2000} [four five])

;; dynamically create 'yarn - not recommended
@(yank {} [(yarn ::eight {f ::four} (* f 2))])

;; dynamically create 'yarn & capture locals
(for [i (range 1 4)]
  @(yank {} [(yarn ::eight {s ::four} (* i s))]))

(alter-var-root #'knitty.core/*tracing* (constantly true))

(print-trace (yank {} [six]))

;; recommended to insntall 'xdot' (via pkg maager or pip)
(view-trace (yank {} [six]))

;; view all traces at once
(view-trace
 (md/chain
  {}
  #(yank % [two])
  #(yank % [five])
  #(assoc % ::two 222222)
  #(yank % [six])))

;; get raw format
(render-trace (yank {} [six]), :format :raw)

;; or graphviz dot
(render-trace (yank {} [six]), :format :dot)

;; yank & run code & return poy'
@(doyank!
  {one 10}
  {x six}
  (println "x =" x))
