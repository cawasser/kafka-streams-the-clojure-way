(ns kafka-streams-the-clojure-way.core
  (:require [jackdaw.streams :as js]
            [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]
            [jackdaw.serdes.edn :refer [serde]]
            [willa.streams :as wstream]
            [willa.core :as w]
            [willa.viz :as wv]
            [willa.experiment :as we]
            [willa.specs :as ws]
            [clojure.spec.alpha :as s])

  (:import org.apache.kafka.streams.KafkaStreams))


;; region - links to the source materials

;; see https://medium.com/funding-circle/kafka-streams-the-clojure-way-d62f6cefaba1

;; see also https://github.com/DaveWM/kafka-streams-the-clojure-way

;; see also https://github.com/DaveWM/willa

;; endregion


;; region - start kafka
;;
;; using one of the following:

; On Linux or Windows(?)
; docker run --rm --net=host landoop/fast-data-dev

; On Mac
; docker run --rm -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=localhost landoop/fast-data-dev:latest


;;;;;;;
;;; OR
;
; if you have cloned cp-docker-images, you can also
;
; cd cp-docker-images/examples/kafka-single-node
; docker-compose up -d
;     (or leave off the -d if you want to see the logs,
;       AND have the ability to 'ctrl-C' to stop Kafka and Zookeeper,
;           instead if leaving them running inside Docker)
;
;; endregion


;; region - compiled code (see the rich comments below)

;; region - defs and such
;; The config for our Kafka Streams app
(def kafka-config
  {"application.id"            "kafka-streams-the-clojure-way"
   "bootstrap.servers"         "localhost:9092"
   "default.key.serde"         "jackdaw.serdes.EdnSerde"
   "default.value.serde"       "jackdaw.serdes.EdnSerde"
   "cache.max.bytes.buffering" "0"})


;; Serdes tell Kafka how to serialize/deserialize messages
;; We'll just keep them as EDN
(def serdes
  {:key-serde   (serde)
   :value-serde (serde)})


;; Each topic needs a config. The important part to note is the :topic-name key.
(def purchase-made-topic
  (merge {:topic-name         "purchase-made"
          :partition-count    1
          :replication-factor 1
          :topic-config       {}}
    serdes))


(def humble-donation-made-topic
  (merge {:topic-name         "humble-donation-made"
          :partition-count    1
          :replication-factor 1
          :topic-config       {}}
    serdes))


(def large-transaction-made-topic
  (merge {:topic-name         "large-transaction-made"
          :partition-count    1
          :replication-factor 1
          :topic-config       {}}
    serdes))


;; An admin client is needed to do things like create and delete topics
(def admin-client (ja/->AdminClient kafka-config))

;; endregion


;; region Part 1 - Simple Topology

(defn make-purchase!
  "Publish a message to the purchase-made topic, with the specified amount"
  [amount]
  (let [purchase-id (rand-int 10000)
        user-id     (rand-int 10000)
        quantity    (inc (rand-int 10))]
    (with-open [producer (jc/producer kafka-config serdes)]
      @(jc/produce! producer purchase-made-topic purchase-id {:id       purchase-id
                                                              :amount   amount
                                                              :user-id  user-id
                                                              :quantity quantity}))))
; producer.produce!(purchase-made-topic purchase-id {:id purchase-id})


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
  (-> (js/kstream builder purchase-made-topic)
    (js/filter (fn [[_ purchase]]
                 (<= 100 (:amount purchase))))
    (js/map (fn [[key purchase]]
              [key (select-keys purchase [:amount :user-id])]))
    (js/to large-transaction-made-topic)))

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

;; endregion

;; region Part 2 - Transducers

(def purchase-made-transducer

  ; from (simple-topology ...)
  ;(js/filter (fn [[_ purchase]]
  ;             (<= 100 (:amount purchase))))
  ;(js/map (fn [[key purchase]]
  ;          [key (select-keys purchase [:amount :user-id])]))

  (comp
    (filter (fn [[_ purchase]]
              (<= 100 (:amount purchase))))
    (map (fn [[key purchase]]
           [key (select-keys purchase [:amount :user-id])]))))

(defn simple-topology-with-transducer [builder]
  (-> (js/kstream builder purchase-made-topic)
    (wstream/transduce-stream purchase-made-transducer)
    (js/to large-transaction-made-topic)))

(def humble-donation-made-transducer
  (comp
    (filter (fn [[_ donation]]
              (<= 10000 (:donation-amount-cents donation))),,,,)
    (map (fn [[key donation]]
           [key {:user-id (:user-id donation)
                 :amount  (int (/ (:donation-amount-cents donation) 100))}]))))

(defn make-humble-donation!
  "Publishes a message to humble-donation-made, with the specified amount"
  [amount-cents]
  (let [user-id (rand-int 10000)
        id      (rand-int 1000)]
    (with-open [producer (jc/producer kafka-config serdes)]
      @(jc/produce! producer humble-donation-made-topic id {:donation-amount-cents amount-cents
                                                            :user-id               user-id
                                                            :donation-date         "2019-01-01"}))))

(defn more-complicated-topology [builder]
  (js/merge
    (-> (js/kstream builder purchase-made-topic)
      (wstream/transduce-stream purchase-made-transducer))
    (-> (js/kstream builder humble-donation-made-topic)
      (wstream/transduce-stream humble-donation-made-transducer))))


;; endregion

;; region Part 3 - Willa

; a simple example (matches "simple-topology" but using purchase-made-transducer

(def topology {:entities {:topic/purchase-made          (assoc purchase-made-topic ::w/entity-type :topic)
                          :stream/large-purchase-made   {::w/entity-type :kstream
                                                         ::w/xform       purchase-made-transducer}
                          :topic/large-transaction-made (assoc large-transaction-made-topic ::w/entity-type :topic)}
               :workflow [[:topic/purchase-made :stream/large-purchase-made]
                          [:stream/large-purchase-made :topic/large-transaction-made]]})

; slightly more complex

(def topology-2
  {:entities {:topic/purchase-made          (assoc purchase-made-topic ::w/entity-type :topic)
              :topic/humble-donation-made   (assoc humble-donation-made-topic ::w/entity-type :topic)
              :topic/large-transaction-made (assoc large-transaction-made-topic ::w/entity-type :topic)

              :stream/large-purchase-made   {::w/entity-type :kstream-channel
                                             ::w/xform       purchase-made-transducer}
              :stream/large-donation-made   {::w/entity-type :kstream-channel
                                             ::w/xform       humble-donation-made-transducer}}

   :workflow [[:topic/purchase-made :stream/large-purchase-made]
              [:topic/humble-donation-made :stream/large-donation-made]
              [:stream/large-purchase-made :topic/large-transaction-made]
              [:stream/large-donation-made :topic/large-transaction-made]]

   :joins    {}})

;; endregion


;; endregion


;; region - rich comments for the repl (run from HERE!!!!)
(comment

  ;; region Part 1 - Simple Topology


  ;; create the "purchase-made" and "large-transaction-made" topics
  (ja/create-topics! admin-client [purchase-made-topic
                                   large-transaction-made-topic])


  ;; Make a few dummy purchases
  (make-purchase! 10)
  (make-purchase! 500)
  (make-purchase! 50)
  (make-purchase! 1000)


  ;; View the purchases on the topic - there should be 4
  (view-messages purchase-made-topic)

  ;; Start the topology
  (def kafka-streams-app (start!))

  (view-messages purchase-made-topic)

  ;; You should see 2 messages on the large-transaction-made-topic topic
  (view-messages large-transaction-made-topic)

  (make-purchase! 1500)
  (make-purchase! 800)
  (make-purchase! 3)

  (make-purchase! 750)

  ;; Stop the topology
  (stop! kafka-streams-app)
  ;; endregion

  ;; region Part 2 - Transducers


  ;; Check that the purchase-made-transducer works as expected
  (into []
    purchase-made-transducer
    [[1 {:purchase-id 1 :user-id 2 :amount 10 :quantity 1}]
     [3 {:purchase-id 3 :user-id 4 :amount 500 :quantity 100}]
     [9 {:purchase-id 9 :user-id 4 :amount 5100 :quantity 100}]
     [23 {:purchase-id 23 :user-id 4 :amount 5000 :quantity 100}]])

  (into []
    humble-donation-made-transducer
    [[1 {:purchase-id 1 :user-id 2 :donation-amount-cents 10 :quantity 1}]
     [3 {:purchase-id 3 :user-id 4 :donation-amount-cents 5000 :quantity 100}]
     [9 {:purchase-id 9 :user-id 4 :donation-amount-cents 51000 :quantity 100}]
     [23 {:purchase-id 23 :user-id 4 :donation-amount-cents 50000 :quantity 100}]])

  ;; endregion

  ;; region Part 3 - Willa

  (ja/create-topics! admin-client [purchase-made-topic
                                   large-transaction-made-topic])


  ;; Visualise the topology
  (wv/view-topology topology)
  (wv/view-topology topology-2)

  ;; Start topology
  (def kafka-streams-app
    (let [builder (js/streams-builder)]
      (w/build-topology! builder topology)
      (doto (js/kafka-streams builder kafka-config)
        (js/start))))

  (def builder (js/streams-builder))

  (make-purchase! 500)
  (make-purchase! 50)
  (make-purchase! 1000)

  (view-messages purchase-made-topic)
  (view-messages large-transaction-made-topic)


  (stop! kafka-streams-app)


  ;; the more complex willa example

  ;; Create the humble-donation-made topic
  (ja/create-topics! admin-client [humble-donation-made-topic])


  ;; Publish a couple of messages to the input topics
  (make-purchase! 200)
  (make-humble-donation! 5000)
  (make-humble-donation! 15000)

  ;; Check that messages appear on the large-transaction-made output topics
  (view-messages large-transaction-made-topic)

  (view-messages humble-donation-made-topic)

  ;; Run an experiment
  (def experiment-results
    (we/run-experiment
      topology
      {:topic/purchase-made        [{:key   1
                                     :value {:id       1
                                             :amount   200
                                             :user-id  1234
                                             :quantity 100}}]
       :topic/humble-donation-made [{:key   2
                                     :value {:user-id               2345
                                             :donation-amount-cents 15000
                                             :donation-date         "2019-01-02"}}]}))

  ;; Visualise experiment result
  (wv/view-topology experiment-results)

  (we/results-only experiment-results)

  (we/results-only experiment-results)

  ;; View results as data
  (->> experiment-results
    :entities
    (map (fn [[k v]]
           [k (::we/output v)]))
    (into {}))

  ;; Validate topology
  (s/explain ::ws/topology topology)

  ;; Check that the spec validation will catch an invalid topology
  (s/explain ::ws/topology
    ;; introduce a loop in our workflow
    (update topology :workflow conj
      [:topic/large-transaction-made :topic/purchase-made]))

  (wv/view-topology (update topology :workflow conj
                      [:topic/large-transaction-made :topic/purchase-made]))

  ;; endregion

  (ja/delete-topics! admin-client [purchase-made-topic
                                   large-transaction-made-topic])
  ())

;; endregion





; comparing to Bryce Covert's examples
(comment
  (def a {:flight "UA1496"})
  (def b [{:event-type          :departed
           :time                #inst "2019-03-16T00:00:00.000-00:00"
           :flight              "UA1496a"
           :scheduled-departure #inst "2019-03-15T00:00:00.000-00:00"}
          {:event-type          :departed
           :time                #inst "2019-03-16T00:00:00.000-00:00"
           :flight              "UA1496a"
           :scheduled-departure #inst "2019-03-15T00:00:00.000-00:00"}])
  (def pk [:flight :something-new])

  ((juxt :flight) b)
  ((juxt :a :b) {:a 1 :b 2 :c 3 :d 4})

  ((apply juxt pk) b)
  (merge b a)

  (map #(let [[k v] [nil %]
              n (assoc v :something-new 100)]
          [(zipmap pk ((apply juxt pk) n)) n])
    b)




  ())



; some join concepts
(comment
  (def message-1 [{:key "a" :event :add} {:content "one"}])
  (def message-2 [{:key "a" :event :add} {:content "two"}])
  (def message-3 [{:key "a" :event :add} {:content "three"}])
  (def message-4 [{:key "a" :event :remove} {:content "two"}])


  (def messages [[{:key "a" :event :add} {:content "one"}]
                 [{:key "a" :event :add} {:content "two"}]
                 [{:key "b" :event :add} {:content "three"}]
                 [{:key "b" :event :add} {:content "one"}]
                 [{:key "a" :event :remove} {:content "one"}]])

  (def starting-point #{})

  (map (fn [[kx cx]]
         (condp = (:event kx)
           :add (conj starting-point (:content cx))
           :remove (disj starting-point (:content cx))))

    [message-1 message-2 message-3 message-4])

  (reduce (fn [accum [kx cx]]
            (condp = (:event kx)
              :add (conj accum (:content cx))
              :remove (disj accum (:content cx))))
    #{} [message-1 message-2 message-3 message-4])

  (->> [message-1 message-2 message-3 message-4]
    (reduce (fn [accum [kx cx]]
              (condp = (:event kx)
                :add (conj accum (:content cx))
                :remove (disj accum (:content cx))))
      #{}))


  (reduce (fn [accum [kx cx]]
            (condp = (:event kx)
              :add (conj accum (:content cx))
              :remove (disj accum (:content cx))))
    #{})

  (def x (->> messages
           (group-by (fn [[k v]] (:key k)))
           first))
  (map (fn [[k v]] k) x)
  (class x)

  (map (fn [[k v]] (str k " -> " v))
    (->> messages
      (group-by (fn [[k v]] (:key k)))))

  (def y {"a"
          [[{:key "a", :event :add} {:content "one"}]
           [{:key "a", :event :add} {:content "two"}]
           [{:key "a", :event :remove} {:content "one"}]]})
  (class y)

  (->> messages
    (group-by (fn [[k v]] (:key k)))
    (map (fn [[k v]]
           (str k " -> " v))))


  (->> messages
    (group-by (fn [[k v]] (:key k)))
    (map (fn [k]
           (map (fn [m]
                  (str k " -> " m))
             k))))

  (->> messages
    (group-by (fn [[k v]] (:key k)))
    (map (fn [k]
           (map (fn [m]
                  (reduce (fn [accum [kx cx]]
                            (condp = (:event kx)
                              :add (conj accum (:content cx))
                              :remove (disj accum (:content cx))))
                    #{} m))
             k))))


  ; join by merging :content
  (defn combine-messages [[kx cx] [ky cy]]
    [kx (concat (:content cx) (:content cy))])

  (combine-messages message-1 message-2)

  ())



; looks like some stuff pulled from Bryce Covert's SeaJure demo...
(comment

  (defn req->open-request-ktable [req-events-stream]
    (-> req-events-stream
      (j/filter (fn [[k v]]
                  (#{:request-added :request-deleted :request-updated :request-commited} (:event-type v))))
      (j/group-by-key)                                      ; key is requester's ID
      (j/aggregate (constantly #{})
        (fn [open-requests [_ event]]
          (cond-> open-requests
            (= :request-added (:event-type event)) (add-requester event)
            (= :request-updated (:event-type event)) (update-requester event)
            (= :request-deleted (:event-type event)) (remove-requester event)
            (= :request-commited (:event-type event)) (remove-requester event)))
        (topic-config "open-requests"))))


  (defn get-open-requests [streams]
    (-> streams
      (.store "open-requests" (QueryableStoreTypes/keyValueStore))))


  (defn get-open-requests-by-id [streams id]
    (-> streams
      (.store "open-requests" (QueryableStoreTypes/keyValueStore))
      (.get {:requester id})))


  ())
