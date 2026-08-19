(ns kss-run2
  (:require [clojure.test :as t]
            [kotobase.server.handler-test]
            [kotobase.server.runtime-test]
            [kotobase.server.trampoline-test]
            [kotobase.server.ipns-test]
            [kotobase.server.storage-test]
            [kotobase.server.pattern-source-test]
            [kotobase.server.query-exec-test]
            [kotobase.server.materialized-test]
            [kotobase.server.shadow-test]
            [kotobase.server.cypher-test]
            [kotobase.server.cypher-ldbc-test]
            [kotobase.server.sparql-snapshot-test]
            [kotobase.server.security.biscuit-authority-test]
            [kotobase.server.security.credential-test]))
(defmethod t/report [:cljs.test/default :end-run-tests] [m]
  (println "SUMMARY" (:test m) "tests" (:pass m) "pass" (:fail m) "fail" (:error m) "error")
  (set! (.-exitCode js/process) (if (t/successful? m) 0 1)))
(t/run-tests 'kotobase.server.handler-test 'kotobase.server.runtime-test
             'kotobase.server.trampoline-test 'kotobase.server.ipns-test
             'kotobase.server.storage-test 'kotobase.server.pattern-source-test
             'kotobase.server.query-exec-test 'kotobase.server.materialized-test
             'kotobase.server.shadow-test 'kotobase.server.cypher-test
             'kotobase.server.cypher-ldbc-test 'kotobase.server.sparql-snapshot-test
             'kotobase.server.security.biscuit-authority-test
             'kotobase.server.security.credential-test)
