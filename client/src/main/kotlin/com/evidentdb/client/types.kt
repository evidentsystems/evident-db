package com.evidentdb.client

import arrow.core.foldLeft
import io.cloudevents.CloudEvent
import java.time.Instant
import java.util.*

typealias DatabaseName = String
typealias DatabaseRevision = Long

data class DatabaseSummary(
    val name: DatabaseName,
    val created: Instant,
    val streamRevisions: Map<StreamName, StreamRevision>,
) {
    val revision: DatabaseRevision
        get() = streamRevisions.foldLeft(0L) { acc, (_, v) ->
            acc + v
        }
}

typealias EventId = UUID

data class EventProposal(
    val event: CloudEvent,
    val stream: StreamName,
    val streamState: ProposedEventStreamState = StreamState.Any,
)

data class Event(
    val event: CloudEvent,
    val stream: StreamName
) {
    val id: EventId
        get() = EventId.fromString(event.id)
}

typealias BatchId = UUID

data class Batch(
    val id: BatchId,
    val database: DatabaseName,
    val events: List<Event>,
    val streamRevisions: Map<StreamName, StreamRevision>,
) {
    val revision: DatabaseRevision
        get() = streamRevisions.foldLeft(0L) { acc, (_, v) ->
            acc + v
        }
}

data class BatchSummaryEvent(val id: EventId, val stream: StreamName)

data class BatchSummary(
    val id: BatchId,
    val database: DatabaseName,
    val events: List<BatchSummaryEvent>,
    val streamRevisions: Map<StreamName, StreamRevision>,
) {
    val revision: DatabaseRevision
        get() = streamRevisions.foldLeft(0L) { acc, (_, v) ->
            acc + v
        }
}

typealias StreamName = String
typealias StreamRevision = Long
typealias StreamSubject = String

sealed interface ProposedEventStreamState

sealed interface StreamState {
    object Any: ProposedEventStreamState
    object StreamExists: ProposedEventStreamState
    // TODO: object SubjectStreamExists: ProposedEventStreamState
    object NoStream: StreamState, ProposedEventStreamState
    // TODO: object NoSubjectStream: ProposedEventStreamState
    data class AtRevision(val revision: StreamRevision): StreamState, ProposedEventStreamState
    // TODO: data class SubjectStreamAtRevision(val revision: StreamRevision): ProposedEventStreamState
}
