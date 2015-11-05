(defproject kinesis-to-firehose "0.1.0"
  :description "Kinesis to Firehose"
  :url "http://github.com/adtile/clj-kinesis-to-firehose"
  :license {:name "MIT License"
            :url "http://www.opensource.org/licenses/mit-license.php"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.apache.commons/commons-lang3 "3.4"]
                 [cheshire "5.5.0"]
                 [com.amazonaws/aws-lambda-java-core "1.1.0"]
                 [com.amazonaws/aws-java-sdk-kinesis "1.10.28"]
                 [base64-clj "0.1.1"]]
  :profiles {:dev {:dependencies [[clj-containment-matchers "1.0.1"]]}}
  :signing {:gpg-key "webmaster@adtile.me"}
  :aot [kinesis-to-firehose.core])
