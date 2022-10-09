package com.evidentdb.client

interface IClient {
    fun createDatabase(name: DatabaseName): Boolean
    fun connectDatabase(name: DatabaseName): IConnection
    fun deleteDatabase(name: DatabaseName): Boolean
    fun catalog(): Iterable<DatabaseSummary>
}

interface IConnection {
    val database: DatabaseName

    fun transact(events: Iterable<UnvalidatedProposedEvent>)
    fun db(): IDatabase
    fun dbAsOf(revision: DatabaseRevision): IDatabase
    fun log(name: DatabaseName): Iterable<Batch>
}

interface IDatabase {
    val revision: DatabaseRevision
    val streamRevisions: Map<StreamName, StreamRevision>

    fun stream(streamName: StreamName): Iterable<Event>
    fun subjectStream(streamName: StreamName, subjectName: StreamSubject): Iterable<Event>
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