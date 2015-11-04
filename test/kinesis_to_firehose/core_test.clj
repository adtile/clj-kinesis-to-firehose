(ns kinesis-to-firehose.core-test
  (:require [clojure.test :refer :all]
            [base64-clj.core :refer [encode]]
            [cheshire.core :refer [parse-string generate-string]]
            [kinesis-to-firehose.core :refer [parse-failures send-request kinesis->firehose!]])
  (:import [com.amazonaws.services.kinesisfirehose.model PutRecordBatchResult PutRecordBatchResponseEntry Record]
           [java.nio ByteBuffer]
           [java.io ByteArrayInputStream ByteArrayOutputStream]))

(def kinesis-template (parse-string (slurp "test/resources/kinesis-template.json") true))

(defn events->kinesis-stream [events]
  (let [compile-kinesis-data (fn [event] (assoc-in kinesis-template [:kinesis :data] (encode (generate-string event))))
        records {"Records" (map compile-kinesis-data events)}]
    (-> (generate-string records)
        (.getBytes)
        (ByteArrayInputStream.))))

(def sample-event-one
  {:type "session"
   :id 123
   :some {:foo "bar"}})

(def sample-event-two
  {:type "clicks"
   :id 234
   :some {:bar "foo"}})

(def sample-event-three
  {:type "open"
   :id 345
   :some {:foo "bar"}})

(defn- fake-request [expected-data request]
  (is (= (-> request .getRecords first .getData .array String.) expected-data))
  (doto (PutRecordBatchResult.)
    (.setFailedPutCount (int 0))))

(defn- first-one-failing-request [request]
  (doto (PutRecordBatchResult.)
    (.setFailedPutCount (int 1))
    (.setRequestResponses [(doto (PutRecordBatchResponseEntry.)
                             (.setErrorCode "666")
                             (.setErrorMessage "Unknown"))
                           (PutRecordBatchResponseEntry.)])))
(def test-mappings
  {:dispatch (fn [event] (:type (parse-string event true)))
   :rules
   [{:name :sessions
     :dispatch-value #{"session"}
     :transform (fn [event] (generate-string event))
     :streams ["stream://1" "stream://2"]}
    {:name :tracking-events
     :dispatch-value #{"clicks" "open" "foobar"}
     :transform (fn [event] (generate-string event))
     :streams ["stream://3" "stream://4"]}]})

(def expected-payload
  "{\"type\":\"session\",\"id\":123,\"some\":{\"foo\":\"bar\"}}")

(deftest sends-record-from-kinesis-to-firehose
  (with-redefs [send-request (partial fake-request expected-payload)]
    (let [input-stream (events->kinesis-stream [sample-event-one])]
      (is (= {:sessions {:succeeded 1 :failed 0} :skipped 0}
             (kinesis->firehose! input-stream test-mappings))))))

;(deftest throws-error-when-all-puts-fail
;  (with-redefs [send-request first-one-failing-request]
;    (is (thrown? Exception (handle-batch sample)))))

;(deftest logs-failed-records-in-case-of-partial-success
;  (with-redefs [send-request first-one-failing-request]
;    (is (= (handle-batch first-one-failing-sample)
;           {:succeeded-count 1
;            :failed [{:payload "Failing" :error-code "666" :error-msg "Unknown"}]}))))
