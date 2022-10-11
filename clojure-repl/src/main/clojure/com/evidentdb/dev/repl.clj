(ns com.evidentdb.dev.repl
  (:import [io.grpc ManagedChannelBuilder]
           [com.evidentdb.client EvidentDb]))

(comment

  (def client (EvidentDb.
               (-> (ManagedChannelBuilder/forAddress "localhost" 50051)
                   .usePlaintext
                   .build)))
  (def database-name "clojure-repl")

  (.catalog client)

  (.createDatabase client database-name)

  (def conn (.connectDatabase client database-name))

  (.deleteDatabase client database-name)

  )
