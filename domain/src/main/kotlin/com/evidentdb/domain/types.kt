package com.evidentdb.domain

import arrow.core.Validated
import io.cloudevents.CloudEvent
import org.valiktor.functions.matches
import org.valiktor.validate
import java.time.Instant
import java.util.*

const val NAME_PATTERN = """^[a-zA-Z][a-zA-Z0-9\-_]{0,127}$"""
const val DB_URI_SCHEME = "evidentdb"

// Internal Command & Event wrappers

typealias CommandId = UUID
typealias CommandType = String

sealed interface CommandBody

sealed interface CommandEnvelope {
    val id: CommandId
    val type: CommandType
        get() = "com.evidentdb.command.${this.javaClass.simpleName}"
    val database: DatabaseName
    val data: CommandBody
}

typealias EventId = UUID
typealias EventType = String

sealed interface EventBody
sealed interface ErrorBody: EventBody

sealed interface EventEnvelope {
    val id: EventId
    val type: EventType
        get() = "com.evidentdb.event.${this.javaClass.simpleName}"
    val commandId: CommandId
    val database: DatabaseName
    val data: EventBody
}

data class ErrorEnvelope(
    override val id: EventId,
    override val commandId: CommandId,
    override val database: DatabaseName,
    override val data: ErrorBody
): EventEnvelope {
    override val type: EventType
        get() = "com.evidentdb.error.${data.javaClass.simpleName}"
}

// Database

typealias DatabaseRevision = Long
typealias TenantRevision = Long

@JvmInline
value class DatabaseName private constructor(val value: String) {
    companion object {
        fun build(value: String): DatabaseName =
            validate(DatabaseName(value)) {
                validate(DatabaseName::value).matches(Regex(NAME_PATTERN))
            }

        fun of(value: String): Validated<InvalidDatabaseNameError, DatabaseName> =
            valikate {
                build(value)
            }.mapLeft { InvalidDatabaseNameError(value) }
    }
}

data class Database(
    val name: DatabaseName,
    val created: Instant,
    val latestEvent: EventId,
    val revision: DatabaseRevision = 0,
    val streamRevisions: Map<StreamName, StreamRevision> = mapOf(),
)

data class DatabaseCreationInfo(val name: DatabaseName): CommandBody
data class DatabaseCreationResult(val database: Database): EventBody

data class CreateDatabase(
    override val id: CommandId,
    override val database: DatabaseName,
    override val data: DatabaseCreationInfo
): CommandEnvelope

sealed interface DatabaseCreationError: ErrorBody

data class DatabaseCreated(
    override val id: EventId,
    override val commandId: CommandId,
    override val database: DatabaseName,
    override val data: DatabaseCreationResult,
): EventEnvelope

data class DatabaseDeletionInfo(val name: DatabaseName): CommandBody
data class DatabaseDeletionResult(val database: Database): EventBody

data class DeleteDatabase(
    override val id: CommandId,
    override val database: DatabaseName,
    override val data: DatabaseDeletionInfo,
): CommandEnvelope

sealed interface DatabaseDeletionError: ErrorBody

data class DatabaseDeleted(
    override val id: EventId,
    override val commandId: CommandId,
    override val database: DatabaseName,
    override val data: DatabaseDeletionResult
): EventEnvelope

// Streams & Indexes

typealias StreamName = String
typealias StreamKey = String
typealias StreamRevision = Long
typealias StreamSubject = String

sealed interface ProposedEventStreamState

sealed interface StreamState {
    object Any: ProposedEventStreamState
    object StreamExists: ProposedEventStreamState
    object NoStream: StreamState, ProposedEventStreamState
    data class AtRevision(val revision: StreamRevision): StreamState, ProposedEventStreamState
}

interface Stream {
    val database: DatabaseName
    val name: StreamName
    val revision: StreamRevision

    companion object {
        fun create(databaseName: DatabaseName, name: StreamName, revision: StreamRevision = 0): Stream {
            //
            // TODO: construct either a SimpleStream or an SubjectStream depending on parsing the naming rules for subject streams
            return SimpleStream(databaseName, name, revision)
        }
    }
}

data class SimpleStream(
    override val database: DatabaseName,
    override val name: StreamName,
    override val revision: StreamRevision
): Stream

data class SubjectStream(
    override val database: DatabaseName,
    override val name: StreamName,
    override val revision: StreamRevision,
    val subject: StreamSubject
): Stream

interface StreamWithEvents: Stream {
    val events: List<Event>

    companion object {
        fun create(
            databaseName: DatabaseName,
            name: StreamName,
            revision: StreamRevision = 0,
            events: List<Event>
        ): StreamWithEvents {
            // TODO: construct either a SimpleStream or an SubjectStream depending on parsing the naming rules for subject streams
            TODO()
        }
    }
}

data class SimpleStreamWithEvents(
    override val database: DatabaseName,
    override val name: StreamName,
    override val revision: StreamRevision,
    override val events: List<Event>
): StreamWithEvents

data class SubjectStreamWithEvents(
    override val database: DatabaseName,
    override val name: StreamName,
    override val revision: StreamRevision,
    val subject: StreamSubject,
    override val events: List<Event>
): StreamWithEvents

// Events & Batches

typealias EventAttributeKey = String

data class UnvalidatedProposedEvent(
    val event: CloudEvent,
    val stream: StreamName,
    val streamState: ProposedEventStreamState = StreamState.Any,
)

data class ProposedEvent(
    val id: EventId,
    val event: CloudEvent,
    val stream: StreamName,
    val streamState: ProposedEventStreamState = StreamState.Any,
)

data class Event(
    val id: EventId,
    val database: DatabaseName,
    val event: CloudEvent,
    val stream: StreamName? = null
)

typealias BatchId = UUID
typealias BatchKey = String

sealed interface BatchEvent

data class ProposedBatch(
    val id: BatchId,
    val database: DatabaseName,
    val events: List<ProposedEvent>
): CommandBody

// TODO: as-of/revision?
data class Batch(
    val id: BatchId,
    val database: DatabaseName,
    val events: List<Event>
)

data class BatchSummary(
    val database: DatabaseName,
    val eventIds: List<EventId>
)

data class TransactBatch(
    override val id: CommandId,
    override val database: DatabaseName,
    override val data: ProposedBatch
): CommandEnvelope

sealed interface BatchTransactionError: ErrorBody

data class BatchTransactionResult(
    val batch: Batch,
    val database: Database,
): EventBody

data class BatchTransacted(
    override val id: EventId,
    override val commandId: CommandId,
    override val database: DatabaseName,
    override val data: BatchTransactionResult
): EventEnvelope, BatchEvent

// Errors

data class InvalidDatabaseNameError(val name: String): DatabaseCreationError, DatabaseDeletionError, BatchTransactionError
data class DatabaseNameAlreadyExistsError(val name: DatabaseName): DatabaseCreationError
data class DatabaseNotFoundError(val name: DatabaseName): DatabaseDeletionError, BatchTransactionError

sealed interface InvalidBatchError: BatchTransactionError

object NoEventsProvidedError: InvalidBatchError

sealed interface EventInvalidation

data class InvalidStreamName(val streamName: String): EventInvalidation
data class InvalidEventType(val eventType: EventType): EventInvalidation

data class InvalidEvent(val event: UnvalidatedProposedEvent, val errors: List<EventInvalidation>)
data class InvalidEventsError(val invalidEvents: List<InvalidEvent>): InvalidBatchError

data class DuplicateBatchError(val batch: ProposedBatch): BatchTransactionError
data class StreamStateConflict(val event: ProposedEvent, val streamState: StreamState)
data class StreamStateConflictsError(val conflicts: List<StreamStateConflict>): BatchTransactionError
data class InternalServerError(val message: String): DatabaseCreationError, DatabaseDeletionError, BatchTransactionError
