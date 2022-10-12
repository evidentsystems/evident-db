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
    fun dbAsOf(revision: DatabaseRevision): IDatabase
    fun log(): Iterable<Batch> // TODO: make lazy, fetching on demand
    fun shutdown()
}

interface IDatabase {
    val revision: DatabaseRevision
    val streamRevisions: Map<StreamName, StreamRevision>

    fun stream(streamName: StreamName): Iterable<CloudEvent>?
    fun subjectStream(streamName: StreamName, subjectName: StreamSubject): Iterable<CloudEvent>
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