(ns com.evidentdb.dev.repl
  (:require [clojure.string :as string])
  (:import [java.net URI]
           [io.grpc ManagedChannelBuilder]
           [io.cloudevents CloudEventData]
           [com.evidentdb.client CloseableIterator]
           [com.evidentdb.client.java.caching EvidentDb]))

(defn event-proposal
  ([event-type stream-name]
   (event-proposal event-type stream-name nil))
  ([event-type
    stream-name
    {:keys [stream-state
            subject
            event-id
            data
            data-content-type
            data-schema
            extensions]
     :or {extensions []}}]
   (EvidentDb/event ^String stream-name
                    ^String event-id
                    ^String event-type
                    ^String subject
                    ^CloudEventData data
                    ^String data-content-type
                    ^URI data-schema
                    extensions)))

(defn eagerize
  [^CloseableIterator iter]
  (with-open [i iter]
    (into [] (iterator-seq i))))

(comment
  (require 'com.evidentdb.dev.repl)
  (in-ns 'com.evidentdb.dev.repl)

  (def client (EvidentDb/javaClient
               (-> (ManagedChannelBuilder/forAddress "localhost" 50051)
                   #_.useTransportSecurity
                   .usePlaintext)))
  (def database-name "clojure-repl")

  (def catalog
    (eagerize (.fetchCatalog client)))

  (.createDatabase client database-name)

  (.shutdown client)

  (def conn (.connectDatabase client database-name))

  (.shutdown conn)

  (def db1 (.db conn))

  db1

  (def batch [(event-proposal "event.occurred"
                              (str "stream-" (rand-int 4))
                              {:event-id (random-uuid)
                               :subject (str "foo-" (rand-int 10))})
              (event-proposal "event.happened"
                              (str "stream-" (rand-int 4))
                              {:subject (str "foo-" (rand-int 10))})])

  (def batch-result @(.transact conn batch))

  (def db2 @(.fetchDbAsOf conn (.getRevision batch-result)))

  (def db3 @(.fetchLatestDb conn))

  (= db2 db3)

  (time (eagerize (.fetchStream db1 "stream-1")))

  (time (eagerize (.fetchSubjectStream db1 "stream-1" "foo-2")))

  (time (eagerize (.fetchLog conn)))

  (.deleteDatabase client database-name)

  (.shutdownNow client)

  (def db-prefix "/home/bobby/code/evidentsystems/evident-db/app/data/transactor/evidentdb-default-tenant-transactor/0_1/rocksdb/")

  (defn encode-string
    [s]
    (.getBytes s "utf-8"))

  (defn decode-string
    [b]
    (String. b "utf-8"))

  (defn connect
    [dir]
    (rocksdb/create-db (str db-prefix "/" dir)
                       {:create-if-missing? false
                        :key-encoder encode-string
                        :key-decoder decode-string}))

  (def events (connect "EVENT_STORE"))
  (def batches (connect "BATCH_STORE"))

  (rocksdb/get events (str "clojure-repl/" (CodecKt/longToBase32HexString 4)))

  (def event-seq (rocksdb/iterator events "clojure-repl/"))
  (def batches-seq (rocksdb/iterator batches))

  (count batches-seq)

  (count (map (comp #(CodecKt/base32HexStringToLong %)
                    last
                    #(string/split % #"/")
                    first)
              i))

  (.close events)
  (.close batches)
  ;; end sample usage
  )
