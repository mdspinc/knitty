(ns knitty.javaimpl
  (:require [manifold.deferred])
  (:import [knitty.java KnittyLoader]))

(defonce -init-java
  (KnittyLoader/init))