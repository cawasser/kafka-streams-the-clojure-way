(ns kafka-streams-the-clojure-way.sample-dag
  (:require [jackdaw.streams :as js]
            [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]
            [jackdaw.serdes.edn :refer [serde]]
            [willa.streams :refer [transduce-stream]]
            [willa.core :as w]
            [willa.viz :as wv]
            [willa.experiment :as we]
            [willa.specs :as ws]
            [clojure.spec.alpha :as s]))


(def kafka-config
  {"application.id" "kafka-streams-the-clojure-way"
   "bootstrap.servers" "localhost:9092"
   "default.key.serde" "jackdaw.serdes.EdnSerde"
   "default.value.serde" "jackdaw.serdes.EdnSerde"
   "cache.max.bytes.buffering" "0"})


;; Serdes tell Kafka how to serialize/deserialize messages
;; We'll just keep them as EDN
(def serdes
  {:key-serde (serde)
   :value-serde (serde)})


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Each topic needs a config.
; The important part to note is the :topic-name key.

; topic-a is a PRIMARY INPUT
(def topic-a
  (merge {:topic-name "topic-a"
          :partition-count 1
          :replication-factor 1
          :topic-config {}}
    serdes))

; topic-b is a PRIMARY INPUT
(def topic-b
  (merge {:topic-name "topic-b"
          :partition-count 1
          :replication-factor 1
          :topic-config {}}
    serdes))


; topic-c is a PRIMARY INPUT
(def topic-c
  (merge {:topic-name "topic-c"
          :partition-count 1
          :replication-factor 1
          :topic-config {}}
    serdes))

; topic-l is a INTERMEDIARY OUTPUT/INPUT
(def topic-l
  (merge {:topic-name "topic-l"
          :partition-count 1
          :replication-factor 1
          :topic-config {}}
    serdes))


;; An admin client is needed to do things like create and delete topics
(def admin-client (ja/->AdminClient kafka-config))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Part 1 - Simple Topology

(defn post-a!
  "Publish a message to the 'a' topic, with the specified amount"
  [amount]
  (let [purchase-id (rand-int 10000)
        user-id     (rand-int 10000)
        quantity    (inc (rand-int 10))]
    (with-open [producer (jc/producer kafka-config serdes)]
      @(jc/produce! producer topic-a purchase-id {:id purchase-id
                                                  :amount amount
                                                  :user-id user-id
                                                  :quantity quantity}))))

(defn view-messages
  "View the messages on the given topic"
  [topic]
  (with-open [consumer (jc/subscribed-consumer (assoc kafka-config "group.id" (str (java.util.UUID/randomUUID)))
                         [topic])]
    (jc/seek-to-beginning-eager consumer)
    (->> (jcl/log-until-inactivity consumer 100)
      (map :value)
      doall)))


(defn simple-topology [builder]
  (-> (js/kstream builder topic-a)
    (js/filter (fn [[_ purchase]]
                 (<= 20 (:amount purchase))))
    (js/map (fn [[key purchase]]
              [key (select-keys purchase [:amount :user-id])]))
    (js/to topic-l)))


(defn start!
  "Starts the simple topology"
  []
  (let [builder (js/streams-builder)]
    (simple-topology builder)
    (doto (js/kafka-streams builder kafka-config)
      (js/start))))

(defn stop!
  "Stops the given KafkaStreams application"
  [kafka-streams-app]
  (js/close kafka-streams-app))




(comment

  (ja/create-topics! admin-client [topic-a topic-l])

  (post-a! 10)
  (post-a! 20)
  (post-a! 30)

  (view-messages topic-a)

  (def kafka-streams-app (start!))

  (view-messages topic-l)


  (stop! kafka-streams-app)

  ())