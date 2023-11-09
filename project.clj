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
  :javac-options ["-target" "17" "-source" "17"]
  :repl-options {:init-ns ag.knitty.core}
  :plugins [[io.github.borkdude/lein-lein2deps "0.1.0"]]
  :profiles {:precomp {:aot ^:replace [manifold.deferred]
                       :clean-non-project-classes false
                       :prep-tasks ^:replace ["compile"]}}
  :prep-tasks [["lein2deps" "--write-file" "deps.edn" "--print" "false"]
               ["with-profile" "precomp" "compile"]
               ["javac"]
               ["compile"]])
