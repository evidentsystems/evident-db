package com.evidentdb.batch

import com.evidentdb.batch.workflows.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.net.URI
import java.util.*

data class MockIndex(override val database: UUID,
                     override val revision: Long,
                     val streams: Map<String, Long>) : Index {
    override fun streamExists(stream: String) = streams.containsKey(stream)

    override fun streamRevision(stream: String) =
        streams[stream] ?: -1
}

class TransactionTests {
    @Test
    fun `fast reject transaction with invalid database URI`() {
        val database = URI("uuid:39aa4970-fb9c-4f5c-bdaf-")
        val events = emptyList<EventProposal>()
        Assertions.assertThrows(IllegalArgumentException::class.java) {
            validateBatchProposal(database, events)
        }
    }

    @Test
    fun `fast reject transaction with invalid event ids`() {
        val database = URI("uuid:39aa4970-fb9c-4f5c-bdaf-4e4d4105972e")
        val events = listOf(
            EventProposal("foo", "stream1", null)
        )
        Assertions.assertThrows(IllegalArgumentException::class.java) {
            validateBatchProposal(database, events)
        }
    }

    @Test
    fun `validate transaction syntax (input gate)`() {
        val database = URI("uuid:39aa4970-fb9c-4f5c-bdaf-4e4d4105972e")
        val events = listOf(
            EventProposal(UUID.randomUUID().toString(), "stream1", null)
        )
        var proposedBatch: ProposedBatch? = null
        Assertions.assertDoesNotThrow {
            proposedBatch = validateBatchProposal(database, events)
        }
        Assertions.assertEquals(proposedBatch?.database, UUID.fromString("39aa4970-fb9c-4f5c-bdaf-4e4d4105972e"))
        Assertions.assertEquals(proposedBatch?.events?.count(), 1)
    }

    @Test
    fun `accept transaction with various stream state constraints`() {
        val random = Random()
        val database = UUID.randomUUID()
        val stream1 = "stream-1"
        val stream2 = "stream-2"
        val stream3 = "stream-3"
        val stream4 = "stream-4"
        val stream4Revision: Long = 20
        val proposal = ProposedBatch(
            UUID.randomUUID(), database,
            listOf(
                ProposedEvent(UUID.randomUUID(), stream1, StreamState.Any),
                ProposedEvent(UUID.randomUUID(), stream2, StreamState.StreamExists),
                ProposedEvent(UUID.randomUUID(), stream3, StreamState.NoStream),
                ProposedEvent(UUID.randomUUID(), stream4, StreamState.AtRevision(stream4Revision))
            ))
        val index = MockIndex(
            database,
            random.nextInt(1000).toLong() + 101,
            mapOf(
                Pair(stream1, random.nextInt(100).toLong()),
                Pair(stream2, random.nextInt(100).toLong()),
                Pair(stream4, stream4Revision)
            ))
        when(val outcome = processBatchProposal(index, proposal)) {
            is BatchProposalOutcome.Accepted -> {
                val event = outcome.event()
                Assertions.assertEquals(event.id, proposal.id)
                Assertions.assertEquals(event.database, proposal.database)
                Assertions.assertEquals(event.events.count(), 4)
                Assertions.assertEquals(event.revision, index.revision + 4)
            }
            else -> Assertions.fail("Outcome should have been Accepted!: $outcome")
        }
    }

    @Test
    fun `reject transaction due to stream state constraints`() {
        val random = Random()
        val database = UUID.randomUUID()
        val stream1 = "stream-1"
        val stream2 = "stream-2"
        val stream3 = "stream-3"
        val stream4Revision: Long = 20
        val proposal = ProposedBatch(
            UUID.randomUUID(), database,
            listOf(
                ProposedEvent(UUID.randomUUID(), stream1, StreamState.StreamExists),
                ProposedEvent(UUID.randomUUID(), stream2, StreamState.NoStream),
                ProposedEvent(UUID.randomUUID(), stream3, StreamState.AtRevision(stream4Revision))
            ))
        val index = MockIndex(
            database,
            random.nextInt(1000).toLong() + 101,
            mapOf(
                Pair(stream2, random.nextInt(100).toLong()),
                Pair(stream3, stream4Revision + 1)
            ))
        when(val outcome = processBatchProposal(index, proposal)) {
            is BatchProposalOutcome.Rejected -> {
                val event = outcome.event()
                Assertions.assertEquals(event.id, proposal.id)
                Assertions.assertEquals(event.database, proposal.database)
                Assertions.assertEquals(event.invalidEvents.count(), 3)
                Assertions.assertEquals(event.revision, index.revision)

            }
            else -> Assertions.fail("Outcome should have been Rejected!: $outcome")
        }
    }
}