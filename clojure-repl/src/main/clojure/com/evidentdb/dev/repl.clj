(ns com.evidentdb.dev.repl
  (:import [java.net URI]
           [io.grpc ManagedChannelBuilder]
           [io.cloudevents CloudEventData]
           [com.evidentdb.client EvidentDB EventProposal StreamState$Any]))

(defn cloudevent
  ([event-type]
   (cloudevent event-type nil))
  ([event-type data]
   (cloudevent event-type data nil))
  ([event-type data data-content-type]
   (cloudevent event-type data data-content-type nil))
  ([event-type data data-content-type data-schema]
   (cloudevent event-type data data-content-type data-schema nil))
  ([event-type data data-content-type data-schema subject]
   (cloudevent event-type data data-content-type data-schema subject []))
  ([^String event-type
    ^CloudEventData data
    ^String data-content-type
    ^URI data-schema
    ^String subject
    extensions]
   (EvidentDB/cloudevent event-type data data-content-type data-schema subject extensions)))

(comment

  (def client (EvidentDB.
               (-> (ManagedChannelBuilder/forAddress "localhost" 50051)
                   .usePlaintext)))
  (def database-name "clojure-repl")

  (.catalog client)

  (.createDatabase client database-name)

  (.shutdown client)

  (def conn (.connectDatabase client database-name 1000))

  (.shutdown conn)

  (def db1 (.db conn))

  db1

  (def batch [(EventProposal. (cloudevent "event.occurred") "my-stream" StreamState$Any/INSTANCE)])

  (def batch-result @(.transact conn batch))

  (def db2 @(.db conn (.getRevision batch-result)))

  (def db3 @(.sync conn))

  (= db2 db3)

  (count @(.stream db2 "my-stream"))

  (count (.log conn))

  (.deleteDatabase client database-name)

  (.shutdownNow client)

  ;; end sample usage
  )
