package com.evidentdb.client

import arrow.core.foldLeft
import io.cloudevents.CloudEvent
import java.time.Instant
import java.util.*

typealias DatabaseName = String
typealias DatabaseRevision = Long
typealias TopicName = String

data class DatabaseSummary(
    val name: DatabaseName,
    val topic: TopicName,
    val created: Instant,
    val streamRevisions: Map<StreamName, StreamRevision>,
) {
    val revision: DatabaseRevision
        get() = streamRevisions.foldLeft(0L) { acc, (_, v) ->
            acc + v
        }
}

typealias EventId = Long

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
        get() = event.getExtension("sequence").let {
            when(it) {
                is String -> base32HexStringToLong(it)
                else -> throw IllegalStateException("Event Sequence must be a String")
            }
        }
}

typealias BatchId = UUID

data class Batch(
    val id: BatchId,
    val database: DatabaseName,
    val events: List<Event>,
    val streamRevisions: Map<StreamName, StreamRevision>,
    val timestamp: Instant,
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

// Errors

sealed interface DatabaseCreationError
sealed interface DatabaseDeletionError
sealed interface BatchTransactionError
sealed interface NotFoundError

data class InvalidDatabaseNameError(val name: String):
    DatabaseCreationError, DatabaseDeletionError, BatchTransactionError,
    IllegalArgumentException("Invalid database name: $name")
data class DatabaseNameAlreadyExistsError(val name: DatabaseName):
    DatabaseCreationError,
    IllegalStateException("Database already exists: $name")
data class DatabaseNotFoundError(val name: String):
    DatabaseDeletionError, BatchTransactionError, NotFoundError,
    IllegalStateException("Database not found: $name")
data class DatabaseTopicCreationError(val database: String, val topic: String): DatabaseCreationError,
    IllegalStateException("Database $database topic $topic could not be created.")
data class DatabaseTopicDeletionError(val database: String, val topic: String): DatabaseDeletionError,
    IllegalStateException("Database $database topic $topic could not be deleted.")

//data class BatchNotFoundError(val database: String, val batchId: BatchId):
//    NotFoundError,
//    IllegalStateException("Batch $batchId not found in database $database")
data class StreamNotFoundError(val database: String, val stream: StreamName):
    NotFoundError,
    IllegalStateException("Stream $stream not found in database $database")
data class EventNotFoundError(val database: String, val eventId: EventId):
    NotFoundError,
    IllegalStateException("Event $eventId not found in database $database")

sealed interface InvalidBatchError: BatchTransactionError

object NoEventsProvidedError:
    InvalidBatchError,
    IllegalArgumentException("Cannot transact an empty batch")

sealed interface EventInvalidation

data class InvalidStreamName(val streamName: String):
    EventInvalidation
data class InvalidEventSubject(val eventSubject: String)
    : EventInvalidation
data class InvalidEventType(val eventType: String):
    EventInvalidation

data class InvalidEvent(
    val event: EventProposal,
    val errors: List<EventInvalidation>
)
data class InvalidEventsError(val invalidEvents: List<InvalidEvent>):
    InvalidBatchError,
    IllegalArgumentException("Invalid events: $invalidEvents")

data class StreamStateConflict(val event: EventProposal, val streamState: StreamState)
data class StreamStateConflictsError(val conflicts: List<StreamStateConflict>):
    BatchTransactionError,
    IllegalStateException("Stream state conflicts: $conflicts")

data class InternalServerError(val error: String):
    DatabaseCreationError, DatabaseDeletionError, BatchTransactionError,
    RuntimeException("Internal server error: $error")
data class SerializationError(val error: String):
    RuntimeException("Serialization error: $error")

data class ClientClosedException(val client: Lifecycle):
    RuntimeException("This client is closed: $client")
data class ConnectionClosedException(val connection: Lifecycle):
    RuntimeException("This connection is closed: $connection")
