# Knitty

is a library for declarative definitions of how data should be computed and what are dependencies between different pieces.
Knitty assigns data computation functions to qualified Clojure keywords. Each such function explicitly declares all needed dependencies (also keywords).  A user provides the initial data set as a map and requests what keys should be added -- Knitty takes the rest on itself: builds a dependency graph, checks there are no cycles, resolves [manifold.deferred](https://github.com/clj-commons/manifold/blob/master/doc/deferred.md), memoize all values, and supports tracing and profiling.

```clojure
(ns user
  (:require [knitty.core :refer [defyarn yank]]
            [manifold.deferred :as md]))
```

Macro `defyarn` defines a "node of computation" (referred to as "yarn").  Each node is identified by a qualified keyword, body expression is evaluated and added to the global "registry" of known nodes:

```clojure
(defyarn node-a)         ;; "input" node
(defyarn node-b {} 2)    ;; node with default value

(defyarn node-c  ;; yarn name
  {a node-a
   b node-b}     ;; dependencies
  (+ a b))       ;; value expression
```

The computation of nodes is started by running the function `yank`.  When requested nodes are already in the input map -- function just returns the same map. If some nodes are missing - they are computed and values are assoc'ed to the resulting map.


```clojure
@(yank {} [])
;; => {}

@(yank {node-a 1} [node-c])
;; => #:user{:node-a 1, :node-b 2, :node-c 3}

@(yank {node-a 10, node-b 20} [node-c])
;; => #:user{:node-a 10, :node-b 20, :node-c 30}
```

Knitty also integrates with [clj-commons/manifold](https://github.com/clj-commons/manifold):


```clojure
;; node may return async value
(defyarn anode-x {c node-c}
  (md/future (* c 10)))

;; all dependencies are automatically resolved
(defyarn anode-y {x anode-x, c node-c}
  (+ x c))

;; but raw async value still may be used
(defyarn anode-z {^:defer x anode-x}
  (md/chain' x dec))

(md/chain
  (yank {node-a 1, node-b 10} [anode-x, anode-y, anode-z])
  println)
;; #:user{:node-a 1, :node-b 10, :anode-z 109, :anode-y 121, :node-c 11, :anode-x 110}
;; => #<SuccessDeferred@6b4da3bf: nil>
```

Knitty also may track when and how nodes are computed.
This information may be used for visualizations:

```clojure
(require
  '[knitty.tracetxt :as ktt]
  '[knitty.traceviz :as ktv]
  )

(def m @(yank {node-a 1, node-b 10} [anode-x, anode-y, anode-z]))

(class m)
;; => clojure.lang.PersistentArrayMap

;; print execution log to *out*
(ktt/print-trace m)

;; open trace as svg image in the web browser
(ktv/view-trace m)
```

![](doc/img/readme_trace_example1.svg)

More examples can be found in the [documentation](/doc/intro.md).

## Alternatives

- https://github.com/plumatic/plumbing#graph-the-functional-swiss-army-knife
- https://github.com/troy-west/wire
- https://github.com/bortexz/graphcom
