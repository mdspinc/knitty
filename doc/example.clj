(require '[ag.knitty.core :refer [defyarn doyank! tieknot yank yarn]]
         '[ag.knitty.tracetxt :refer [print-trace]]
         '[ag.knitty.traceviz :refer [render-trace view-trace]]
         '[manifold.deferred :as md])

(defyarn zero;; define "yarn" - single slot/value
  {}         ;; no inputs
  0)         ;; value, use explicit `do when needed

@(yank {} [zero])

(defyarn one
  {_ zero}   ;; wait for zero, but don't use it
  1)         ;; any manifold-like deferred can be finished

(defyarn one-slooow
  {}
  (md/future (Thread/sleep (long (rand-int 20))) 1))

(defyarn two
  {^:defer x one
   ^:defer y one-slooow}     ;; don't unwrap deferred (all values coerced to manifold/deffered)
  (md/chain' (md/alt x y) inc))

(defyarn three-fast {x one, y two}
  (md/future
    (Thread/sleep (long (rand-int 5))) (+ x y)))

(defyarn three-slow {x one, y two}
  (md/future
    (Thread/sleep (long (rand-int 10))) (+ x y)))

(defyarn three        ;; put deferred into delay, enables branching
  {^:lazy f three-fast
   ^:lazy s three-slow}
  (if (zero? (rand-int 2)) f s))

(defyarn four
  {x ::one, y ::three};; use raw keywords (not recommended)
  (md/future (+ x y)))

(defyarn abs-three)

(defyarn five
  {x ::two, y abs-three}  ;; mixed approach
  (println ">>>" x y)
  (+ x y))

(defyarn six
  ^{:doc "doc string"}       ;; doc
  ^{:spec number?}           ;; spec
  {y ::three x ::two}
  (println "debug print")
  (* x y))

(tieknot three abs-three)

(yank {} [three])
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

(alter-var-root #'ag.knitty.core/*tracing* (constantly true))

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
