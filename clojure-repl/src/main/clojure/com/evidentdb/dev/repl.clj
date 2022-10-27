(ns com.evidentdb.dev.repl
  (:import [java.net URI]
           [io.grpc ManagedChannelBuilder]
           [io.cloudevents CloudEventData]
           [com.evidentdb.client EvidentDB StreamState StreamState$Any CloseableIterator]))

(defn event-proposal
  ([event-type stream-name]
   (event-proposal event-type stream-name StreamState$Any/INSTANCE))
  ([event-type stream-name stream-state]
   (event-proposal event-type stream-name stream-state nil))
  ([event-type stream-name stream-state event-id]
   (event-proposal event-type stream-name stream-state event-id nil))
  ([event-type stream-name stream-state event-id data]
   (event-proposal event-type stream-name stream-state event-id data nil))
  ([event-type stream-name stream-state event-id data data-content-type]
   (event-proposal event-type stream-name stream-state event-id data data-content-type nil))
  ([event-type stream-name stream-state event-id data data-content-type data-schema]
   (event-proposal event-type stream-name stream-state event-id data data-content-type data-schema nil))
  ([event-type stream-name stream-state event-id data data-content-type data-schema subject]
   (event-proposal event-type stream-name stream-state event-id data data-content-type data-schema subject []))
  ([^String event-type
    ^String stream-name
    ^StreamState stream-state
    ^String event-id
    ^CloudEventData data
    ^String data-content-type
    ^URI data-schema
    ^String subject
    extensions]
   (EvidentDB/eventProposal event-type stream-name stream-state event-id data data-content-type data-schema subject extensions)))

(defn eagerize
  [^CloseableIterator iter]
  (with-open [i iter]
    (into [] (iterator-seq i))))

(comment

  (def client (EvidentDB/javaClient
               (-> (ManagedChannelBuilder/forAddress "localhost" 50051)
                   .usePlaintext)))
  (def database-name "clojure-repl")

  (def catalog
    (with-open [catalog-iter (.catalog client)]
      (into [] (iterator-seq catalog-iter))))

  (.createDatabase client database-name)

  (.shutdown client)

  (def conn (.connectDatabase client database-name))

  (.shutdown conn)

  (def db1 (.db conn))

  db1

  (def batch [(event-proposal "event.occurred" "my-stream")])

  (def batch-result @(.transact conn batch))

  (def db2 @(.sync conn (.getRevision batch-result)))

  (def db3 @(.sync conn))

  (= db2 db3)

  (count (eagerize (.stream db2 "my-stream")))

  (count (eagerize (.log conn)))

  (.deleteDatabase client database-name)

  (.shutdownNow client)

  ;; end sample usage
  )
