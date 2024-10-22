(defproject knitty "0.6.0-alpha1-SNAPSHOT"
  :description "Knitty is a library for a DAG-based processing of asynchronous data"
  :url "https://github.com/anjensan/knitty"
  :license {:name "MIT License" :url "http://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.11.3"]
                 [manifold/manifold "0.4.3"]
                 [org.hdrhistogram/HdrHistogram "2.2.2"]
                 [macroz/tangle "0.2.2"]]

  :plugins [[lein-aot-filter "0.1.0"]
            [lein-shell "0.5.0"]]

  :aot [knitty.core]
  :aot-include [#"knitty\.javaimpl\..*"]
  :java-source-paths ["src-java"]
  :javac-options ["--release" "17"]
  :jvm-opts ["-server" "-XX:-OmitStackTraceInFastThrow" "-Xmx4g" "-Xss8m"]

  :source-paths ["src"]
  :global-vars {*warn-on-reflection* true}

  :aliases
  {"bump-version" ["change" "version" "leiningen.release/bump-version"]}

  :profiles {:dev {:dependencies [[criterium/criterium "0.4.6"]
                                  [com.clojure-goes-fast/clj-async-profiler "1.3.3"]
                                  [prismatic/plumbing "0.6.0"]
                                  [funcool/promesa "11.0.678"]
                                  [clj-kondo/clj-kondo "2024.09.27"]]
                   :injections [(require 'knitty.test-util)]
                   :jvm-opts ["-Djdk.attach.allowAttachSelf" "-XX:+UnlockDiagnosticVMOptions" "-XX:+DebugNonSafepoints"]}}

  :prep-tasks [["javac"]
               ["compile"]
               ["aot-filter"]
               ["shell" "find" "target/classes" "-type" "d" "-empty" "-delete"]]

  :test-selectors {:default #(not (some #{:benchmark :stress} (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :stress #(or (:stress %) (= :stress (:tag %)))
                   :all (constantly true)})
