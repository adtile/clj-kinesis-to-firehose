# kinesis-to-firehose

[![Clojars Project](http://clojars.org/kinesis-to-firehose/latest-version.svg)](http://clojars.org/kinesis-to-firehose)

Push events from a Kinesis stream to a Redshift/S3 Firehose stream

## Build status

[![Build Status](https://travis-ci.org/adtile/clj-kinesis-to-firehose.svg)](https://travis-ci.org/adtile/clj-kinesis-to-firehose)

## Installation

Add the following to your `project.clj` `:dependencies`:

```clojure
[kinesis-to-firehose "0.1.4"]
```

## Usage

### Lambda function example with uswitch

Simple lambda function that reads events in JSON format from a Kinesis stream and writes them to different
Firehoses based on it's type.

```clojure

(ns my-lambda.core
  (:require [uswitch.lambada.core :refer [deflambdafn]]
            [clojure.java.io :as io]
            [cheshire.core :refer [generate-string parse-string generate-stream]]
            [kinesis-to-firehose.core :refer [kinesis->firehose!]]))

; Create a config map
; { :dispatch (fn which picks values for dispatch-value check)
;   :rules [
;     {:name :name-of-rule
;      :dispatch-value #{"matches results for dispatch fn"} or :default
;      :transform #(fn which prepares record for S3 or Redshift write)
;      :streams ["vec of firehose streams"]}]

(def my-kinesis-stream-mappings-and-transformations
  {:dispatch (fn [event] (:type (cheshire.core/parse-string event true)))
   :rules
   [{:name :lol-cats
     :dispatch-value #{"cat"}
     :transform (fn [event] (cheshire.core/generate-string event))
     :streams ["lol-cats-redshift-firehose-1" "lol-cats-redshift-firehose-2"]}
    {:name :lol-dogs
     :dispatch-value #{"dogs"}
     :transform (fn [event] (cheshire.core/generate-string event))
     :streams ["lol-dogs-s3-firehose"]}
    {:name :all-the-rest-lol-things
     :dispatch-value :default
     :transform (fn [event] (cheshire.core/generate-string event))
     :streams ["all-the-rest-lol-things-s3-firehose1"]}]})

(deflambdafn my-lambda.LambdaFn [is os context]
  (let [stream-writer (io/writer os)
  	results (kinesis->firehose! is my-kinesis-stream-mappings-and-transformations)]
    (generate-strean results stream-writer)
    (.flush stream-writer)))

```
