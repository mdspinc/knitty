(defproject knitty "0.1.0"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0", :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [manifold/manifold "0.2.4"]
                 [org.hdrhistogram/HdrHistogram "2.1.12"]
                 [macroz/tangle "0.2.2"]]
  :repl-options {:init-ns ag.knitty.core}
  :plugins [[io.github.borkdude/lein-lein2deps "0.1.0"]]
  :prep-tasks [["lein2deps" "--write-file" "deps.edn" "--print" "false"]])
