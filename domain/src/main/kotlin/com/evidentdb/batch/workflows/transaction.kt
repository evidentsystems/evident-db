package com.evidentdb.batch.workflows

import com.evidentdb.batch.Event
import com.evidentdb.batch.Index
import com.evidentdb.batch.StreamState
import java.net.URI
import java.util.*
import kotlin.collections.ArrayList

data class EventProposal(val id: String, val stream: String, val streamState: StreamState?)
data class ProposedEvent(val id: UUID, val stream: String, val streamState: StreamState)

data class ProposedBatch(val id: UUID, val database: UUID, val events: Iterable<ProposedEvent>)
data class BatchAccepted(val id: UUID,
                         val database: UUID,
                         val events: Iterable<Event>,
                         val revision: Long)
data class BatchRejected(val id: UUID,
                         val database: UUID,
                         val events: Iterable<ProposedEvent>,
                         val invalidEvents: Iterable<UUID>,
                         val revision: Long)

sealed class BatchProposalOutcome {
    class Accepted(
        private val proposal: ProposedBatch,
        private val events: Iterable<Event>,
        private val revision: Long) : BatchProposalOutcome() {
        fun event() =
            BatchAccepted(
                proposal.id,
                proposal.database,
                events,
                revision
            )
    }

    class Rejected(
        private val proposal: ProposedBatch,
        private val invalidEvents: Iterable<UUID>,
        private val revision: Long) : BatchProposalOutcome() {
        fun event() =
            BatchRejected(
                proposal.id,
                proposal.database,
                proposal.events,
                invalidEvents,
                revision
            )
    }
}

// Validates incoming string IDs by parsing to UUIDs (throws IllegalArgumentException if invalid)
fun validateBatchProposal(database: URI, events: Iterable<EventProposal>): ProposedBatch {
    val databaseId = UUID.fromString(database.schemeSpecificPart)
    val validatedEvents = events.map {
        ProposedEvent(UUID.fromString(it.id), it.stream, it.streamState ?: StreamState.Any)
    }
    return ProposedBatch(UUID.randomUUID(), databaseId, validatedEvents)
}

fun isEventValid(index: Index, proposedEvent: ProposedEvent) =
    when(proposedEvent.streamState) {
        is StreamState.Any -> true // any other validation?
        is StreamState.NoStream ->
            !index.streamExists(proposedEvent.stream)
        is StreamState.StreamExists ->
            index.streamExists(proposedEvent.stream)
        is StreamState.AtRevision ->
            index.streamRevision(proposedEvent.stream) == proposedEvent.streamState.revision
    }

fun processBatchProposal(index: Index, proposal: ProposedBatch): BatchProposalOutcome {
    var revision = index.revision
    val invalidEvents = ArrayList<UUID>()
    val events = proposal.events.map {
        revision += 1
        if (!isEventValid(index, it)) invalidEvents.add(it.id)
        Event(it.id, it.stream, revision)
    }
    return if (invalidEvents.isEmpty()) {
        BatchProposalOutcome.Accepted(
            proposal,
            events,
            revision
        )
    } else {
        BatchProposalOutcome.Rejected(
            proposal,
            invalidEvents,
            index.revision
        )
    }
}