package com.evidentdb.client

import io.cloudevents.CloudEvent
import java.time.Instant
import java.util.concurrent.CompletableFuture

interface Client {
    fun createDatabase(name: DatabaseName): Boolean
    fun deleteDatabase(name: DatabaseName): Boolean
    fun catalog(): Iterable<DatabaseSummary>
    fun connectDatabase(
        name: DatabaseName,
        cacheSize: Long = 10_000,
    ): IConnection
}

interface IConnection {
    val database: DatabaseName

    // TODO: make transact async, or provide async alternative
    fun transact(events: List<EventProposal>): CompletableFuture<Batch>

    /* Immediately returns the latest database revision available locally */
    fun db(): IDatabase

    /* Returns a completable future bearing the next database revision
    * greater than or equal to the given `revision`. May block indefinitely
    * if revision is greater than the latest available on server, so callers
    * should use timeouts or similar. */
    fun db(revision: DatabaseRevision): CompletableFuture<IDatabase>

    /* Returns a completable future bearing the latest database available
    * on the server. */
    fun sync(): CompletableFuture<IDatabase>

    // TODO: Allow range iteration from a start revision (fuzzy) and
    //  iterating from there (possibly to a (fuzzy) end revision)
    fun log(): Iterable<Batch>

    fun shutdown()
    fun shutdownNow()
}

interface IDatabase {
    val name: DatabaseName
    val created: Instant
    val revision: DatabaseRevision
    val streamRevisions: Map<StreamName, StreamRevision>

//    fun asOf(revision: DatabaseRevision): IDatabase = TODO("Filter here, or fetch at connection?")
//    fun since(revision: DatabaseRevision): IDatabase = TODO("Is this needed/meaningful?")
    fun stream(streamName: StreamName): CompletableFuture<List<CloudEvent>>
    fun subjectStream(
        streamName: StreamName,
        subjectName: StreamSubject
    ): CompletableFuture<List<CloudEvent>?>
    fun event(eventId: EventId): CompletableFuture<CloudEvent?>
}

// val client = EvidentDB(ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext())
// client.createDatabase("foo") // => true
// val conn = client.connect("foo")
// val db1 = conn.db()
// db1.stream("my-stream") // => []
// val batch = conn.transactBatch(
//   listOf(EventProposal("my-stream", CloudEvent.newBuilder()....))
// ).get()
// val db2 = conn.db(batch.revision).get()
// db1.stream("my-stream").get() // => []
// db2.stream("my-stream").get() // => [CloudEvent()]