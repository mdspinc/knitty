(defproject knitty "0.2.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "MIT License" :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [manifold/manifold "0.4.1"]
                 [org.hdrhistogram/HdrHistogram "2.1.12"]
                 [macroz/tangle "0.2.2"]]
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :clean-non-project-classes true
  :aot [#"no-aot-really"]
  :javac-options ["-target" "11" "-source" "11"]
  :profiles {:precomp {:aot ^:replace [manifold.deferred]
                       :clean-non-project-classes false
                       :prep-tasks ^:replace ["compile"]}
             :dev {:dependencies [[criterium "0.4.6"]]
                   :global-vars {*warn-on-reflection* true
                                 ;;*unchecked-math* :warn-on-boxed
                                 }}}
  :prep-tasks [["with-profile" "precomp" "compile"]
               ["javac"]
               ["compile"]]
  :test-selectors {:default #(not
                              (some #{:benchmark :stress}
                                    (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress #(or (:stress %) (= :stress (:tag %)))
                   :all (constantly true)})
