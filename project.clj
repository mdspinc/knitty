(defproject knitty "0.3.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "MIT License" :url "http://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [manifold/manifold "0.4.1"]
                 [org.hdrhistogram/HdrHistogram "2.1.12"]
                 [macroz/tangle "0.2.2"]]

  :plugins [[lein-aot-filter "0.1.0"]
            [lein-shell "0.5.0"]]

  :source-paths ["src"]
  :java-source-paths ["src-java"]

  :aot-include [#"knitty\.java\..*"]
  :aot [knitty.core]

  :javac-options ["-target" "17" "-source" "17"]
  :profiles {:precompile {:aot ^:replace [manifold.deferred]
                          :prep-tasks ^:replace []}
             :dev {:dependencies [[criterium "0.4.6"]]
                   :global-vars {*warn-on-reflection* true}}}

  :prep-tasks [["with-profile" "precompile" "compile"]
               ["javac"]
               ["compile"]
               ["aot-filter"]
               ["shell" "find" "target/classes" "-type" "d" "-empty" "-delete"]
               ]

  :test-selectors {:default #(not (some #{:benchmark :stress} (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress #(or (:stress %) (= :stress (:tag %)))
                   :all (constantly true)})
