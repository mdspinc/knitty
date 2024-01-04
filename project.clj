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

  :aot-include [#"knitty\.javaimpl\..*"]
  :aot [knitty.core]
  :jvm-opts ["-Djdk.attach.allowAttachSelf"]

  :javac-options ["-target" "17" "-source" "17"]

  :profiles {:dev {:dependencies [[criterium "0.4.6"]
                                  [com.clojure-goes-fast/clj-async-profiler "1.1.1"]
                                  [prismatic/plumbing "0.6.0"]]
                   :global-vars {*warn-on-reflection* true}}}

  :prep-tasks [["javac"]
               ["compile"]
               ["aot-filter"]
               ["shell" "find" "target/classes" "-type" "d" "-empty" "-delete"]]

  :test-selectors {:default #(not (some #{:benchmark :stress} (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress #(or (:stress %) (= :stress (:tag %)))
                   :all (constantly true)})
