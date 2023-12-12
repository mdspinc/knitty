# Knitty

There are two basic building blocks in Knitty: macro `defyarn` and function `yank`.

```clojure
(ns user
  (:require [knitty.core :refer [defyarn yank]]))
```

Macro `defyarn` defines a "node of computation" (referred to as "yarn").  Each node is identified by a qualified keyword, body expression is evaluated and added to the global "registry" of known nodes.  Also macro defines a var with a keyword value for convenience -- it helps with text editors and linters, but is not strictly required for library usage:

```clojure
(defyarn
  node1  ; yarn name
  {}     ; no dependencies
  1      ; value expression
  )

(keyword? node1) ;; => true
node1            ;; => :user/node1
```

Dependencies are specified as map in form of `{symbol :node/id, ...}`. This instructs Knitty to get (or compute) the value of `:node/id` and bind it to local variable `symbol`.  Also, a keyword may be replaced by var with that keyword (recommended approach):

```clojure
(defyarn node2
  {x node1}
  (+ 1 x))
```

The computation of nodes is triggered by running the function `yank`.  It takes map of `{:node/id <some-value>, ...}` and list of nodes in which we are interested.  When provided nodes are already in the map -- function just returns the same map. If some nodes are missing -- they are computed and values are assoc'ed to the resulting map.  Please note, that `yank` always wraps returned value into `manifold.Deferred`, so `deref` or `@` should be used to synchronously get the result:


```clojure
@(yank {} [])
;; => {}

@(yank {node2 200} [node2])
;; => {:user/node2 200}

@(yank {} [node2])
;; => {:user/node1 1, :user/node2 2}

@(yank {node1 100} [node2])
;; => {:user/node1 100, :user/node2 101}
```

### Asynchronous values

Yarns might return `manifold.Deferred`.
Knitty automatically awaits and unwrap deferreds.

```clojure
(defyarn async-node
  {}
  (md/future 10))

;; await & consume async value
(defyarn use-sync {x async-node}
  (class x) ;; => Integer
  (inc x))  ;; => 11
```

Non-manifold futures might be coerced by calling `->deferred`.

```clojure
(defyarn future-node
  {}
  (manifold.deferred/->deferred
    (future 10)))
```

Automatic unwrapping can be disabled by adding `^:defer` metadata to binding symbol.

```clojure
;; don't await, but consume value
(defyarn use-async {^:defer x future-node}
  (class x)          ;; => manifold.Deferred
  (md/chain x inc))  ;; => Deferred with 11
```

### Lazy dependencies

```clojure
(defyarn conditional-node {}
  (println "compute")
  (rand-int 10)))

(defyarn maybe-use {^:lazy x conditional-node}
  (if (< (random) 0.5)
    @x   ;; Deferred
    0))

(defyarn use-async {^:defer x async-node}
  (class x)          ;; => manifold.Deferred
  (md/chain x inc))  ;; => 11
```


### Handle errors

Working with raw deferred may be useful to handle expected exceptions.

```clojure
(defyarn success-node {}
  123)

(defyarn failed-node {}
  (throw (ex-info "error" {}))

(defyarn handle-error
  {^:defer x failed-node
   ^:defer y success-node}
  (-> (md/zip x y)
    (md/chain (fn [[x y]] (inc x)))
    (md/catch (fn [e] (str "handled error" e)))))
```

## TODO ...

- Execution model
- Use with specs
- Abstract yarns
- Dynamic yarns
- Tracing
- View traces and debugging
- Capture timings
