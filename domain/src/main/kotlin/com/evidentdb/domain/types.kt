package com.evidentdb.domain

import arrow.core.*
import io.cloudevents.CloudEvent
import org.valiktor.functions.matches
import org.valiktor.validate
import java.time.Instant
import java.util.*

const val NAME_PATTERN = """^[a-zA-Z][a-zA-Z0-9\-_.]{0,127}$"""
const val DB_URI_SCHEME = "evidentdb"

// Internal Command & Event wrappers

typealias EnvelopeId = UUID
typealias EnvelopeType = String

sealed interface CommandBody

sealed interface CommandEnvelope {
    val id: EnvelopeId
    val type: EnvelopeType
        get() = "com.evidentdb.command.${this.javaClass.simpleName}"
    val database: DatabaseName
    val data: CommandBody
}

sealed interface EventBody
sealed interface ErrorBody: EventBody

sealed interface EventEnvelope {
    val id: EnvelopeId
    val type: EnvelopeType
        get() = "com.evidentdb.event.${this.javaClass.simpleName}"
    val commandId: EnvelopeId
    val database: DatabaseName
    val data: EventBody
}

data class ErrorEnvelope(
    override val id: EnvelopeId,
    override val commandId: EnvelopeId,
    override val database: DatabaseName,
    override val data: ErrorBody
): EventEnvelope {
    override val type: EnvelopeType
        get() = "com.evidentdb.error.${data.javaClass.simpleName}"
}

// Tenant

//typealias TenantRevision = Long

@JvmInline
value class TenantName private constructor(val value: String) {
    companion object {
        fun build(value: String): TenantName =
            validate(TenantName(value)) {
                validate(TenantName::value).matches(Regex(NAME_PATTERN))
            }
    }
}

// Database

typealias DatabaseRevision = Long
typealias DatabaseLogKey = String
typealias TopicName = String

@JvmInline
value class DatabaseName private constructor(val value: String) {
    companion object {
        fun build(value: String): DatabaseName =
            validate(DatabaseName(value)) {
                validate(DatabaseName::value).matches(Regex(NAME_PATTERN))
            }

        fun of(value: String): Validated<InvalidDatabaseNameError, DatabaseName> =
            valikate { build(value) }.mapLeft { InvalidDatabaseNameError(value) }
    }
}

data class DatabaseSummary(
    val name: DatabaseName,
    val topic: TopicName,
    val created: Instant,
)

data class Database(
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

data class DatabaseCreationInfo(
    val name: DatabaseName,
    val topic: TopicName
): CommandBody

data class CreateDatabase(
    override val id: EnvelopeId,
    override val database: DatabaseName,
    override val data: DatabaseCreationInfo
): CommandEnvelope

sealed interface DatabaseCreationError: ErrorBody

data class DatabaseCreationResult(val database: Database): EventBody

data class DatabaseCreated(
    override val id: EnvelopeId,
    override val commandId: EnvelopeId,
    override val database: DatabaseName,
    override val data: DatabaseCreationResult,
): EventEnvelope

data class DatabaseDeletionInfo(val name: DatabaseName): CommandBody
data class DeleteDatabase(
    override val id: EnvelopeId,
    override val database: DatabaseName,
    override val data: DatabaseDeletionInfo,
): CommandEnvelope

sealed interface DatabaseDeletionError: ErrorBody

data class DatabaseDeletionResult(val database: Database): EventBody

data class DatabaseDeleted(
    override val id: EnvelopeId,
    override val commandId: EnvelopeId,
    override val database: DatabaseName,
    override val data: DatabaseDeletionResult
): EventEnvelope

// Streams & Indexes

typealias StreamKey = String
typealias StreamRevision = Long

@JvmInline
value class StreamName private constructor(val value: String) {
    companion object {
        fun build(value: String): StreamName =
            validate(StreamName(value)) {
                validate(StreamName::value).matches(Regex(NAME_PATTERN))
            }

        fun of(value: String): Validated<InvalidStreamName, StreamName> =
            valikate { build(value) }.mapLeft { InvalidStreamName(value) }
    }
}

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

// Events & Batches

typealias EventId = Long
typealias EventKey = String
typealias EventType = String

@JvmInline
value class EventSubject private constructor(val value: String?) {
    companion object {
        fun build(value: String?): EventSubject =
            validate(EventSubject(value)) {
                when(it.value) {
                    null -> Unit
                    else -> validate(EventSubject::value).matches(Regex(NAME_PATTERN))
                }
            }

        fun of(value: String?): Validated<InvalidEventSubject, EventSubject> =
            valikate { build(value) }.mapLeft { InvalidEventSubject(value!!) }
    }
}

data class UnvalidatedProposedEvent(
    val event: CloudEvent,
    val stream: String,
    val streamState: ProposedEventStreamState = StreamState.Any,
)

data class ProposedEvent(
    val event: CloudEvent,
    val stream: StreamName,
    val streamState: ProposedEventStreamState = StreamState.Any,
)

data class Event(
    val database: DatabaseName,
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
typealias BatchKey = String

sealed interface BatchEvent

data class ProposedBatch(
    val id: BatchId,
    val database: DatabaseName,
    val events: List<ProposedEvent>
): CommandBody

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

data class BatchSummaryEvent(val id: EventId, val stream: StreamName)

data class BatchSummary(
    val id: BatchId,
    val database: DatabaseName,
    val events: List<BatchSummaryEvent>,
    val streamRevisions: Map<StreamName, StreamRevision>,
    val timestamp: Instant,
) {
    val revision: DatabaseRevision
        get() = streamRevisions.foldLeft(0L) { acc, (_, v) ->
            acc + v
        }
}

data class TransactBatch(
    override val id: EnvelopeId,
    override val database: DatabaseName,
    override val data: ProposedBatch
): CommandEnvelope

sealed interface BatchTransactionError: ErrorBody

data class BatchTransactionResult(
    val batch: Batch,
    val databaseBefore: Database,
): EventBody {
    val databaseAfter: Database
        get() = Database(
            databaseBefore.name,
            databaseBefore.topic,
            databaseBefore.created,
            batch.streamRevisions,
        )
}

data class BatchTransacted(
    override val id: EnvelopeId,
    override val commandId: EnvelopeId,
    override val database: DatabaseName,
    override val data: BatchTransactionResult
): EventEnvelope, BatchEvent

// Errors

sealed interface NotFoundError

data class InvalidDatabaseNameError(val name: String): DatabaseCreationError, DatabaseDeletionError, BatchTransactionError
data class DatabaseNameAlreadyExistsError(val name: DatabaseName): DatabaseCreationError
data class DatabaseNotFoundError(val name: String): DatabaseDeletionError, BatchTransactionError, NotFoundError
data class DatabaseTopicCreationError(val database: String, val topic: TopicName): DatabaseCreationError
data class DatabaseTopicDeletionError(val database: String, val topic: TopicName): DatabaseDeletionError

data class BatchNotFoundError(val database: String, val batchId: BatchId): NotFoundError
data class StreamNotFoundError(val database: String, val stream: String): NotFoundError
data class EventNotFoundError(val database: String, val eventId: EventId): NotFoundError

sealed interface InvalidBatchError: BatchTransactionError

object NoEventsProvidedError: InvalidBatchError

sealed interface EventInvalidation

data class InvalidStreamName(val streamName: String): EventInvalidation
data class InvalidEventSubject(val eventSubject: String): EventInvalidation
data class InvalidEventType(val eventType: EventType): EventInvalidation

data class InvalidEvent(val event: UnvalidatedProposedEvent, val errors: List<EventInvalidation>)
data class InvalidEventsError(val invalidEvents: List<InvalidEvent>): InvalidBatchError

data class DuplicateBatchError(val batch: ProposedBatch): BatchTransactionError
data class StreamStateConflict(val event: ProposedEvent, val streamState: StreamState)
data class StreamStateConflictsError(val conflicts: List<StreamStateConflict>): BatchTransactionError
data class InternalServerError(val message: String): DatabaseCreationError, DatabaseDeletionError, BatchTransactionError
