package com.evidentdb.server.adapter.tests

import arrow.core.Either
import arrow.core.right
import com.evidentdb.server.adapter.EvidentDbAdapter
import com.evidentdb.server.domain_model.*
import io.cloudevents.core.builder.CloudEventBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import java.net.URI

interface AdapterTests {
    val databaseName: String
    val adapter: EvidentDbAdapter
    val creationResult: Either<EvidentDbCommandError, Database>

    // TODO: write a test for fuzzy match of revision (next largest vs. given revision)
    //   for Adapter.databaseAtRevision

    fun `creating a database`() = runTest {
        Assertions.assertTrue(creationResult.isRight())
        Assertions.assertInstanceOf(Database::class.java, creationResult.getOrNull())

        // Ensure the updates are being sent upon connection
        val update = adapter.subscribeDatabaseUpdates(databaseName).first()
        Assertions.assertTrue(update.isRight())
        Assertions.assertEquals(0uL, update.getOrNull()?.revision)

        // Ensure it shows up in catalog
        val catalog = adapter.fetchCatalog().toList()
        Assertions.assertEquals(listOf(databaseName), catalog.map { it.value })
    }

    fun `transacting batches`() = runTest {
        // Ensure the updates are being sent upon connection
        val updates = initializeConnection(databaseName, 0u, this)

        // Transact the Batch
        val eventStream1 = "my-stream"
        val eventStream2 = "my-other-stream"
        val eventIdSuffix = "an+id@foo.com"
        val eventSubject1 = "a nice subject"
        val eventSubject2 = "another nice subject"
        val eventType1 = "com.evidentdb.test-event1"
        val eventType2 = "com.evidentdb.test-event2"
        val event1 = CloudEventBuilder.v1()
                .withSource(URI(eventStream1))
                .withId("1$eventIdSuffix")
                .withSubject(eventSubject1)
                .withType(eventType1)
                .build()
        val event2 = CloudEventBuilder.v1()
                .withSource(URI(eventStream1))
                .withId("2$eventIdSuffix")
                .withSubject(eventSubject2)
                .withType(eventType2)
                .build()
        val event3 = CloudEventBuilder.v1()
                .withSource(URI(eventStream2))
                .withId("1$eventIdSuffix")
                .withSubject(eventSubject2)
                .withType(eventType2)
                .build()
        // Transaction should succeed
        val result = adapter.transactBatch(
                databaseName,
                listOf(
                        ProposedEvent(event1),
                        ProposedEvent(event2),
                        ProposedEvent(event3),
                ),
                listOf()
        )
        val revisionAfterBatch = 3uL
        Assertions.assertTrue(result.isRight())
        Assertions.assertEquals(revisionAfterBatch, result.getOrNull()!!.revision)
        // New transaction should show up on log
        val log = adapter.scanDatabaseLog(databaseName, revisionAfterBatch).toList()
        Assertions.assertEquals(
                listOf(Pair(true, 3uL)),
                log.map { Pair(it.isRight(), it.getOrNull()!!.revision) }
        )
        // New transaction event should be indexed
        // By EventId + Stream
        listOf(event1, event2, event3).forEach { event ->
            val eventById = adapter.fetchEventById(
                    databaseName,
                    revisionAfterBatch,
                    event.source.toString(),
                    event.id
            )
            Assertions.assertTrue(eventById.isRight())
            Assertions.assertEquals(
                    Pair(event.source.toString(), event.id),
                    eventById.getOrNull()?.let {
                        Pair(it.stream.value, it.id.value)
                    }
            )
        }
        // By Stream
        val stream1 = adapter.fetchEventRevisionsByStream(
                databaseName,
                revisionAfterBatch,
                eventStream1,
        ).toList()
        Assertions.assertEquals(listOf(1uL.right(), 2uL.right()), stream1)
        val stream2 = adapter.fetchEventRevisionsByStream(
                databaseName,
                revisionAfterBatch,
                eventStream2,
        ).toList()
        Assertions.assertEquals(listOf(3uL.right()), stream2)
        // By Subject
        val subject1 = adapter.fetchEventRevisionsBySubject(
                databaseName,
                revisionAfterBatch,
                eventSubject1
        ).toList()
        Assertions.assertEquals(listOf(1uL.right()), subject1)
        val subject2 = adapter.fetchEventRevisionsBySubject(
                databaseName,
                revisionAfterBatch,
                eventSubject2
        ).toList()
        Assertions.assertEquals(listOf(2uL.right(), 3uL.right()), subject2)
        // By Subject + Stream
        val subjectStream11 = adapter.fetchEventRevisionsBySubjectAndStream(
                databaseName,
                revisionAfterBatch,
                eventStream1,
                eventSubject1
        ).toList()
        Assertions.assertEquals(listOf(1uL.right()), subjectStream11)
        val subjectStream12 = adapter.fetchEventRevisionsBySubjectAndStream(
                databaseName,
                revisionAfterBatch,
                eventStream1,
                eventSubject2
        ).toList()
        Assertions.assertEquals(listOf(2uL.right()), subjectStream12)
        val subjectStream21 = adapter.fetchEventRevisionsBySubjectAndStream(
                databaseName,
                revisionAfterBatch,
                eventStream2,
                eventSubject1
        ).toList()
        Assertions.assertEquals(emptyList<ULong>(), subjectStream21)
        val subjectStream22 = adapter.fetchEventRevisionsBySubjectAndStream(
                databaseName,
                revisionAfterBatch,
                eventStream2,
                eventSubject2
        ).toList()
        Assertions.assertEquals(listOf(3uL.right()), subjectStream22)
        // By Event Type
        val type1 = adapter.fetchEventRevisionsByType(
                databaseName,
                revisionAfterBatch,
                eventType1
        ).toList()
        Assertions.assertEquals(listOf(1uL.right()), type1)
        val type2 = adapter.fetchEventRevisionsByType(
                databaseName,
                revisionAfterBatch,
                eventType2
        ).toList()
        Assertions.assertEquals(listOf(2uL.right(), 3uL.right()), type2)
        // Ensure updates were properly received
        val update = updates.first()

        Assertions.assertTrue(update.isRight())
        Assertions.assertEquals(revisionAfterBatch, update.getOrNull()?.revision)
    }

    fun `event ID + stream-source uniqueness failure`() = runTest {
        // Transact an event w/ identical ID + stream/source as in happy path test
        val eventStream1 = "my-stream"
        val eventIdSuffix = "an+id@foo.com"
        val eventSubject1 = "a nice subject"
        val eventType1 = "com.evidentdb.test-event1"
        val event1 = CloudEventBuilder.v1()
            .withSource(URI(eventStream1))
            .withId("1$eventIdSuffix")
            .withSubject(eventSubject1)
            .withType(eventType1)
            .build()
        val result = adapter.transactBatch(
            databaseName,
            listOf(ProposedEvent(event1)),
            listOf()
        )
        Assertions.assertTrue(result.isLeft())
        val maybeErr = result.leftOrNull()
        Assertions.assertNotNull(maybeErr)
        Assertions.assertInstanceOf(InvalidEvents::class.java, maybeErr)
        val err = maybeErr as InvalidEvents
        Assertions.assertInstanceOf(
            DuplicateEventId::class.java,
            err.invalidEvents.first().errors.first()
        )
    }

    fun `batch constraint failures`() = runTest {
        val eventStream1 = "my-stream"
        val nonExistentEventStream = "nope"
        val eventSubject2 = "another nice subject"
        val nonExistentEventSubject = "solid nope"
        val event1 = CloudEventBuilder.v1()
            .withSource(URI(eventStream1))
            .withId("another completely unique event ID")
            .withSubject(eventSubject2)
            .withType("com.evidentdb.batch-constraint-failure")
            .build()

        var constraint: BatchConstraint = BatchConstraint.DatabaseMinRevision(
            4uL
        )
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )

        constraint = BatchConstraint.DatabaseMaxRevision(
            2uL
        )
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )

        constraint = BatchConstraint.DatabaseRevisionRange(
            1uL,
            2uL
        ).getOrNull()!!
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )

        constraint = BatchConstraint.streamExists(
            StreamName(nonExistentEventStream).getOrNull()!!
        )
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )

        constraint = BatchConstraint.streamDoesNotExist(
            StreamName(eventStream1).getOrNull()!!
        )
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )

        constraint = BatchConstraint.streamAtRevision(
            StreamName(eventStream1).getOrNull()!!,
            1uL
        ).getOrNull()!!
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )

        constraint = BatchConstraint.subjectExists(
            EventSubject(nonExistentEventSubject).getOrNull()!!
        )
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )

        constraint = BatchConstraint.subjectDoesNotExist(
            EventSubject(eventSubject2).getOrNull()!!
        )
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )

        constraint = BatchConstraint.subjectAtRevision(
            EventSubject(eventSubject2).getOrNull()!!,
            2uL,
        ).getOrNull()!!
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )

        constraint = BatchConstraint.subjectExistsOnStream(
            StreamName(eventStream1).getOrNull()!!,
            EventSubject(nonExistentEventSubject).getOrNull()!!,
        )
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )

        constraint = BatchConstraint.subjectDoesNotExistOnStream(
            StreamName(eventStream1).getOrNull()!!,
            EventSubject(eventSubject2).getOrNull()!!,
        )
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )

        constraint = BatchConstraint.subjectAtRevisionOnStream(
            StreamName(eventStream1).getOrNull()!!,
            EventSubject(eventSubject2).getOrNull()!!,
            1uL,
        ).getOrNull()!!
        validateConstraintFailure(
            constraint,
            adapter.transactBatch(
                databaseName,
                listOf(ProposedEvent(event1)),
                listOf(constraint)
            )
        )
    }

    private fun validateConstraintFailure(
        constraint: BatchConstraint,
        result: Either<EvidentDbCommandError, IndexedBatch>
    ) {
        Assertions.assertTrue(result.isLeft())
        val maybeErr = result.leftOrNull()
        Assertions.assertNotNull(maybeErr)
        Assertions.assertInstanceOf(BatchConstraintViolations::class.java, maybeErr)
        val err = maybeErr as BatchConstraintViolations
        Assertions.assertEquals(
            constraint,
            err.violations.first()
        )
    }

    // TODO: Happy path when constraints are provided and satisfied

    fun `deleting a database`() = runTest {
        // Ensure the updates are being sent upon connection
        val updateFlow = initializeConnection(databaseName, 3u, this)

        val result = adapter.deleteDatabase(databaseName)
        Assertions.assertTrue(result.isRight())
        val catalog = adapter.fetchCatalog().toList()
        Assertions.assertEquals(
                listOf<Database>(),
                catalog
        )

        val update = updateFlow.first()
        Assertions.assertTrue(update.isLeft())
        Assertions.assertInstanceOf(DatabaseNotFound::class.java, update.leftOrNull())

        val connectDatabaseNotFound = adapter.subscribeDatabaseUpdates(databaseName).first()
        Assertions.assertInstanceOf(
                DatabaseNotFound::class.java,
                connectDatabaseNotFound.leftOrNull()
        )

        // TODO: finish ensuring we get DatabaseNotFound for all relevant command/query operations
//        adapter.latestDatabase()
//        adapter.databaseAtRevision()
//        adapter.catalog()
//        adapter.databaseLog()
//        adapter.stream()
//        adapter.subject()
//        adapter.subjectStream()
//        adapter.eventById()
//        adapter.eventType()
//        adapter.eventsByRevision()
    }

    private suspend fun initializeConnection(
        database: String,
        expectedInitialRevision: Revision,
        scope: CoroutineScope,
    ): Flow<Either<DatabaseNotFound, Database>> {
        val init = CompletableDeferred<Boolean>()
        val flow = adapter.subscribeDatabaseUpdates(database)
        scope.async {
            val first = flow.first()
            Assertions.assertEquals(expectedInitialRevision, first.getOrNull()!!.revision)
            init.complete(true)
        }
        init.await()
        println("Connection initialized")
        return flow
    }
}
