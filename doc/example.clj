(require
 '[knitty.core
   :as kt
   :refer [yarn
           defyarn
           defyarn-method
           defyarn-multi
           declare-yarn
           bind-yarn
           yank
           yank1]])

(require
 '[knitty.deferred :as kd])


(defyarn zero    ;; define "yarn" - single slot/value
  {}             ;; no dependencies
  0)             ;; default value

(type zero)
;; => clojure.lang.Keyword

(= zero ::zero)
;; => true

(yank {} [zero])
;; => #knitty/D[#:user{:zero 0}]

@(yank {} [zero])  ;; call 'deref
;; => #:user{:zero 0}



(defyarn one
  {z zero}   ;; assign value of ::zero yarn to local 'z
  (inc z))   ;; any expression here

@(yank {} [one])          ;; calc ::one
;; => #:user{:zero 0, :one 1}

@(yank {zero 0.0} [one])  ;; redefine ::zero value
;; => #:user{:zero 0.0, :one 1.0}

@(yank1 {zero 0.0} one)   ;; return only yanked yarn
;; => 1.0



(defyarn one-slow
  {z zero}
  (kd/future              ;; expression may be deferred
    (Thread/sleep (long (rand-int 20)))
    (inc z)))

(defyarn two
  {x one
   y one-slow}            ;; knitty await and auto-deref deferreds
  (+ x y))                ;; both x and y are numbers

(defyarn three
  {^:defer xd one-slow    ;; use raw 'deferred'
   ^:defer yd one         ;; autowrap synchronous values via md/success-deferred
   z two
   }
  (kd/bind                ;; all functions from md/ also can be used
   (kd/zip xd yd)         ;; including md/timeout!, md/let-flow, md/alt ...
   (fn [[x y]]
     (+ x y z))))



(defyarn four-stategy     ;; how to compute "four"
  {}
  (condp < (rand)
    0.8 "const"
    0.5 "2*2"
    0.1 "3+1"
    ::unknown))

(defyarn-multi
  four           ;; yarn name
  four-stategy   ;; select implementation based on this value
  )

(defyarn-method four "const" {} 4)
(defyarn-method four "2*2" {x two} (kd/future (* x x)))
(defyarn-method four "3+1" {x one, y three} (+ x y))
(defyarn-method four :default {s four-stategy} (throw (ex-info "unknown strategy" {::s s})))

@(yank {four-stategy "2*2"} four)
;; #:user{:four-stategy "2*2", :one-slow 1, :zero 0, :one 1, :two 2, :four 4}

@(yank {four-stategy "const"} four)
;; #:user{:four-stategy "const", :four 4}



(declare-yarn five)  ;; define "abstract" yarn without implementation

(yank {} five)
;; => #knitty/D[:err clojure.lang.ExceptionInfo "failed to yank"]

(defyarn ten
  {x five       ;; but we can depend on it
   y two}
  (* x y))

(defyarn five-impl
  {x one
   y four}
  (+ x y))

(bind-yarn five five-impl)

@(yank {} five)
;; => #:user{:one-slow 1, :two 2, ...}



@(yank1 {} ten)  ;; pick just one yarn, ignoring intermidiate results
;; => 10

;; override some yarns
@(yank {two 2000, four 444} [ten])

;; dynamically create 'yarn - not recommended
@(yank1 {} (yarn ::eight {f four} (* f 2)))

;; dynamically create 'yarn & capture locals
(for [i (range 1 4)]
  @(yank {} [(yarn ::eight {s ::four} (* i s))]))



;; globally enable tracing
(knitty.core/enable-tracing!)

(require '[knitty.tracetxt])  ;; render as text
(knitty.tracetxt/print-trace (yank {} [ten]))


(require '[knitty.traceviz])  ;; render via 'graphviz'

;; recommended to install "xdot" (via 'pip')
(knitty.traceviz/view-trace
 (yank {zero 0.001
        four-stategy "2*2"}
       ten))

;; view all traces at once
(knitty.traceviz/view-trace
 (kd/bind->
  {}
  #(yank % [two])
  #(yank % [five])
  #(assoc % two 222222)
  #(yank % [ten])))

;; get raw format
(knitty.traceviz/render-trace
 (yank {} [ten]), :format :raw)

;; or graphviz dot
(knitty.traceviz/render-trace
 (yank {} [ten]), :format :dot)
