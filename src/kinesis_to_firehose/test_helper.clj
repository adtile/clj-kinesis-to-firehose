(ns kinesis-to-firehose.test-helper
  (:require [base64-clj.core :refer [encode]]
            [cheshire.core :refer [parse-string generate-string]])
  (:import [com.amazonaws.services.kinesisfirehose.model PutRecordBatchResult PutRecordBatchResponseEntry Record]
           [java.nio ByteBuffer]
           [java.io ByteArrayInputStream ByteArrayOutputStream]))


(def kinesis-template (parse-string (slurp "resources/kinesis-template.json") true))

(defn events->kinesis-stream [events]
  (let [compile-kinesis-data (fn [event] (assoc-in kinesis-template [:kinesis :data] (encode (generate-string event))))
        records {"Records" (map compile-kinesis-data events)}]
    (-> (generate-string records)
        (.getBytes "UTF-8")
        (ByteArrayInputStream.))))

