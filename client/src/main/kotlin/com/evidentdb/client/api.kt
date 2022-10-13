package com.evidentdb.client

import io.cloudevents.CloudEvent

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
    fun transact(events: List<EventProposal>): Batch
    fun db(): IDatabase
    fun dbAsOf(revision: DatabaseRevision): IDatabase // TODO:

    // TODO: make lazy, fetching on demand?
    //  or make streaming iterator w/ cancellation
    // TODO: Allow seeking into the log at a specific revision and
    //  iterating from there
    fun log(): Iterable<Batch>

    fun shutdown()
    fun shutdownNow()
}

interface IDatabase {
    val revision: DatabaseRevision
    val streamRevisions: Map<StreamName, StreamRevision>

//    fun asOf(revision: DatabaseRevision): IDatabase = TODO("Filter here, or fetch at connection?")
//    fun since(revision: DatabaseRevision): IDatabase = TODO("Is this needed/meaningful?")
    fun stream(streamName: StreamName): Iterable<CloudEvent>?
    fun subjectStream(streamName: StreamName, subjectName: StreamSubject): Iterable<CloudEvent>
    fun event(eventId: EventId): CloudEvent?
}

// val client = Client.forAddress("localhost", 50030)
// client.createDatabase("foo") // => true
// val conn = client.connect("foo")
// val db1 = conn.db()
// db1.stream("my-stream") // => []
// val batch = conn.transactBatch(
//   listOf(EventProposal("my-stream", CloudEvent.newBuilder()....))
// )
// val db2 = conn.dbAsOf(batch.revision)
// db1.stream("my-stream") // => []
// db2.stream("my-stream") // => [CloudEvent()]