(ns kinesis-to-firehose.core-test
  (:require [clojure.test :refer :all]
            [clj-containment-matchers.clojure-test :refer :all]
            [base64-clj.core :refer [encode]]
            [cheshire.core :refer [parse-string generate-string]]
            [kinesis-to-firehose.core :as core :refer [send-to-firehose kinesis->firehose!]]
            [kinesis-to-firehose.test-helper :refer [events->kinesis-stream]])
  (:import [com.amazonaws.services.kinesisfirehose.model PutRecordBatchResult PutRecordBatchResponseEntry Record]
           [java.nio ByteBuffer]
           [java.io ByteArrayInputStream ByteArrayOutputStream]))

(def sample-event-one
  {:type "session"
   :id 123
   :some {:foo "bar"}})

(def sample-event-two
  {:type "clicks"
   :id 234
   :some {:bar "foo"}})

(def sample-event-unkown
  {:type "unknown"
   :id 345
   :some {:foo "bar"}})

(def test-mappings
  {:dispatch (fn [event] (:type (parse-string event true)))
   :rules
   [{:name :sessions
     :dispatch-value #{"session"}
     :transform (fn [event] (generate-string event))
     :streams ["stream://1"]}
    {:name :tracking-events
     :dispatch-value #{"clicks" "open" "foobar"}
     :transform (fn [event] (generate-string event))
     :streams ["stream://4"]}]})


(defn- fake-request [request]
  (doto (PutRecordBatchResult.)
    (.setFailedPutCount (int 0))))

(defn- first-one-failing-request [request]
  (doto (PutRecordBatchResult.)
    (.setFailedPutCount (int 1))
    (.setRequestResponses [(doto (PutRecordBatchResponseEntry.)
                             (.setErrorCode "666")
                             (.setErrorMessage "Unknown"))
                           (PutRecordBatchResponseEntry.)])))


(deftest sends-record-from-kinesis-to-firehose
  (with-redefs [core/send-request fake-request]
    (let [input-stream (events->kinesis-stream [sample-event-one sample-event-one])]
      (is (equal? {:sessions {:succeeded 2 :failed 0} :skipped 0}
             (kinesis->firehose! input-stream test-mappings))))))

(deftest send-record-from-kinesis-to-firehose-and-ignore-unknown-message
  (with-redefs [core/send-request fake-request]
    (let [input-stream (events->kinesis-stream [sample-event-one sample-event-one sample-event-unkown])]
      (is (equal? {:sessions {:succeeded 2 :failed 0} :skipped 1}
             (kinesis->firehose! input-stream test-mappings))))))

(deftest throws-error-when-all-puts-fail
  (with-redefs [core/send-request first-one-failing-request]
    (let [input-stream (events->kinesis-stream [sample-event-one])]
      (is (thrown? Exception (kinesis->firehose! input-stream test-mappings))))))

(deftest logs-failed-records-in-case-of-partial-success
  (with-redefs [core/send-request first-one-failing-request]
    (let [input-stream (events->kinesis-stream [sample-event-one sample-event-one])]
      (is (equal? (kinesis->firehose! input-stream test-mappings)
                  {:sessions {:succeeded 1
                              :failed [{:payload "{\"type\":\"session\",\"id\":123,\"some\":{\"foo\":\"bar\"}}" :error-code "666" :error-msg "Unknown"}]}
                   :skipped 0})))))

(defn send-to-firehose-fake [expected events stream]
  (is (equal? (vec events) expected)))

(def expected-payload
  ["{\"type\":\"session\",\"id\":123,\"some\":{\"foo\":\"bar\"}}"
   "{\"type\":\"session\",\"id\":123,\"some\":{\"foo\":\"bar\"}}"])

(deftest sends-record-from-kinesis-to-firehose-in-correct-format
  (with-redefs [send-to-firehose (partial send-to-firehose-fake expected-payload)]
    (let [input-stream (events->kinesis-stream [sample-event-one sample-event-one])]
      (kinesis->firehose! input-stream test-mappings))))

