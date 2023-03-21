(ns kafka-streams-the-clojure-way.black-hammer.streams
  (:require [clojure.tools.logging :as log]
            [jackdaw.streams :as js]
            [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]
            [jackdaw.serdes.edn :refer [serde]]
            [willa.streams :as wstream]
            [willa.core :as w]
            [willa.viz :as wv]
            [willa.experiment :as we]
            [willa.specs :as ws]
            [clojure.spec.alpha :as s]))


; global registry of topology patterns
(def topo-reg (atom {}))


(defn register
  "dynamically add a topology to the in-memory registry"
  [topo]
  (swap! topo-reg merge topo))


(defn- load-topo
  "load a single topology template from the EDN file names by 'topo-file'"
  [topo-file]
  (->> topo-file
    slurp
    clojure.edn/read-string
    register))


(defn activate-registry
  "load all the topology template files found in the folder named
  'dir-name', uses (register)"
  [dir-name]

  (->> dir-name
    clojure.java.io/file
    file-seq
    (filter #(.isFile %))
    (map load-topo)
    dorun))


(defn compile-topo
  "find the topology definition identified in 'custom-topo' (:pattern)
  and perform all the substitution (defined in :substitutions) and
  any renaming of entities defined in :rename (in both :entities and :workflow
  of the topology definition)"
  [custom-topo]

  (let [topo    ((:pattern custom-topo) @topo-reg)
        subs    (:substitutes custom-topo)
        renames (:rename custom-topo)]
    (-> topo
      (#(assoc % :entities (reduce (fn [e [k v]] (assoc-in e k v)) (:entities %) subs)))
      (#(assoc % :entities (clojure.set/rename-keys (:entities %) renames)))
      (#(assoc % :workflow (map (fn [[from to]] [(from renames) (to renames)])
                             (:workflow %)))))))


(defn start-topo!
  "start the topology running, so it can process messages/events"
  [topo]

  ; TODO: (TD) start-topo! needs some error  checking...
  (log/info "STARTING TOPO!" (:pattern topo))

  (-> topo
    compile-topo

    ; TODO: (start-topo!) really needs to start the thing, not just show a picture...
    wv/view-topology))


(defn combine-topos
  "combine multiple topologies together, especially to draw a diagram
  of a 'complete system' (defined across multiple templates and tailorings.

  Works best if provided 'compiled' topologies, so everything has unique
  names and such.

  NOTE: templates all by themselves WILL work, but the diagram won't be
        very useful"
  [& t]
  (let [entities (reduce merge {} (map :entities t))
        workflow (reduce concat [] (map :workflow t))]
    {:entities entities :workflow workflow}))


; repl experiments
(comment
  ; register a topology template
  (register {:transform-topo {:entities {:topic/input  {:topic-name         :input-stream
                                                        :partition-count    1
                                                        :replication-factor 1
                                                        :topic-config       {}
                                                        :willa.core/entity-type     :topic}
                                         :stream/work  {:willa.core/entity-type :kstream
                                                        :willa.core/xform       :transducer}
                                         :topic/output {:topic-name         :output-stream
                                                        :partition-count    1
                                                        :replication-factor 1
                                                        :topic-config       {}
                                                        :willa.core/entity-type     :topic}}
                              :workflow [[:topic/input :stream/work]
                                         [:stream/work :topic/output]]}})

  ; play with loading templates form the file system (as EDN files)
  (map #(.getPath %)
    (file-seq (clojure.java.io/file "./resources/topologies/")))

  (->> "./resources/topologies/"
    clojure.java.io/file
    file-seq
    (filter #(.isFile %))
    (map load-topo)
    doall)


  (activate-registry "./resources/topologies/")
  topo-reg

  ; a transducer for repl play
  (def purchase-made-transducer
    (comp
      (filter (fn [[_ purchase]]
                (<= 100 (:amount purchase))))
      (map (fn [[key purchase]]
             [key (select-keys purchase [:user-id :amount])]))))


  ; "tailoring" a topology template for a specific purpose
  (def large-purchase {:pattern     :transform-topo
                       :substitutes [[[:topic/input :topic-name] "purchase-made"]
                                     [[:stream/work ::w/xform] purchase-made-transducer]
                                     [[:topic/output :topic-name] "large-purchase-made"]]
                       :rename      {:topic/input  :topic/purchase-made
                                     :stream/work  :stream/large-purchase
                                     :topic/output :topic/large-purchase-made}})


  ; what we want the resulting "complied" topology to look like...
  (def expanded-topo {:entities {:topic/purchase-made       {:topic-name         "purchase-made",
                                                             :partition-count    1,
                                                             :replication-factor 1,
                                                             :topic-config       {},
                                                             ::w/entity-type     :topic},
                                 :stream/large-purchase     {::w/entity-type :kstream,
                                                             ::w/xform       purchase-made-transducer},
                                 :topic/large-purchase-made {:topic-name         "purchase-made",
                                                             :partition-count    1,
                                                             :replication-factor 1,
                                                             :topic-config       {},
                                                             ::w/entity-type     :topic}}
                      :workflow [[:topic/purchase-made :stream/large-purchase]
                                 [:stream/large-purchase :topic/large-purchase-made]]})


  (wv/view-topology (combine-topos (:transform-topo topo-reg)
                      (:transform-topo topo-reg)))

  (wv/view-topology expanded-topo)


  (let [subs     (:substitutes large-purchase)
        entities (:entities ((:pattern large-purchase) @topo-reg))]
    (map (fn [[k v]]
           (assoc-in entities k v))
      subs))

  ; now combine the changes into a single hash-map
  (let [subs     (:substitutes large-purchase)
        entities (:entities ((:pattern large-purchase) @topo-reg))]
    (reduce (fn [e [k v]] (assoc-in e k v))
      entities
      subs))

  (clojure.set/rename-keys
    (get-in @topo-reg [:transform-topo :entities])
    (:rename large-purchase))


  (def renames (:rename large-purchase))
  (map (fn [[from to]]
         [(from renames) (to renames)])
    (get-in @topo-reg [:transform-topo :workflow]))






  (compile-topo large-purchase)

  (wv/view-topology (compile-topo large-purchase))





  ())

