(ns kinesis-to-firehose.core
  (:require [base64-clj.core :as base64]
            [cheshire.core :refer [parse-stream]]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:import [java.nio ByteBuffer]
           [org.apache.commons.lang3 StringEscapeUtils]
           [com.amazonaws.services.kinesisfirehose.model Record PutRecordBatchRequest]
           [com.amazonaws.services.kinesisfirehose AmazonKinesisFirehoseClient]))

(def client (AmazonKinesisFirehoseClient.))

(defn send-request [^PutRecordBatchRequest request]
  (. client putRecordBatch request))

(defn parse-failures [events responses]
  (let [failed-count (.getFailedPutCount responses)
        failed-events (->> (map vector events (.getRequestResponses responses))
                           (remove (fn [[_ response]] (empty? (.getErrorCode response))))
                           (map (fn [[event response]]
                                  {:payload event
                                   :error-code (.getErrorCode response)
                                   :error-msg (.getErrorMessage response)})))]
    [failed-count failed-events]))

(defn- throw-batch-failed-error [failed-events]
  ;Usually it is the same reason for all the failures
  ;So here we simply return the first error
  (let [probable-cause (first failed-events)]
    (throw (ex-info
             (str "Batch failed. Probable cause: " probable-cause)
             {:cause probable-cause}))))

;Kinesis firehose does not (yet) support sharding
;One stream has limit of 5000 puts / second
;By using multiple kinesis streams and sending
;requests evenly (rand is good enough) we can scale this
(defn- next-delivery-stream [streams]
  (rand-nth streams))

;Generate string return json string in escaped format
;"\"{\"dog\":\"pig\"}\"" -> {"dog":"pig"}
(defn cleanup-json-string-format [dirty-json]
  (let [json-safe-message (. StringEscapeUtils (unescapeJson dirty-json))
        without-extra-quotes (string/replace json-safe-message #"(^\"?)(.*)(\"$?)" "$2")]
    without-extra-quotes))

(defn- send-to-firehose [events streams]
  (let [records (map (fn [event]
                       (doto (Record.)
                         (.setData (ByteBuffer/wrap (.getBytes (cleanup-json-string-format event) "UTF-8")))))
                     events)
        request (doto (PutRecordBatchRequest.)
                  (.setDeliveryStreamName (next-delivery-stream streams))
                  (.setRecords records))
        response (send-request request)
        [failed-count failed-events] (parse-failures events response)]
    (cond
      (= 0 failed-count) {:succeeded (count events) :failed failed-count}
      (= (count events) failed-count) (throw-batch-failed-error failed-events)
      :else {:succeeded (- (count events) failed-count)
             :failed failed-events})))

(defn send-grouped-to-firehose [[{transform :transform streams :streams group-name :name} data]]
  (let [transformed-data (map transform data)]
    (try
      (let [response (send-to-firehose transformed-data streams)]
        {group-name response})
      (catch Exception e
        (throw e)))))

(defn group-by-matcher [events mappers]
  (reduce (fn [m event]
            (let [dispatch-value ((:dispatch mappers) event)
                  matched (first (filter #(some #{dispatch-value} (:dispatch-value %)) (:rules mappers)))]
              (if matched
                (update m matched conj event)
                m)))
          {}
          events))

(defn handle-batch [kinesis-batch mappers]
  (let [events (map #(-> % :kinesis :data (base64/decode)) (:Records kinesis-batch))
        grouped-events (group-by-matcher events mappers)]
    (assoc (into {} (map send-grouped-to-firehose grouped-events)) :skipped 0)))

(defn kinesis->firehose! [is mappers]
  (let [result (-> (parse-stream (io/reader is) true)
                   (handle-batch mappers))]
    result))
