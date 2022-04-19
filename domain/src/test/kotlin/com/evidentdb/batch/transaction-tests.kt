package com.evidentdb.batch

import com.evidentdb.batch.workflows.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
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
                Assertions.assertEquals(event.database, proposal.database)
            }
            else -> Assertions.fail("Outcome should have been Accepted!: $outcome")
        }
    }
}