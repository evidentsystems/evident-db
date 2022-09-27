package com.evidentdb.domain

import io.cloudevents.CloudEvent
import java.util.*

// TODO: Anti-corruption layer via public constructors/static-initializers!!!

// Internal Command & Event wrappers

typealias CommandId = UUID
typealias CommandType = String

sealed interface CommandBody

sealed interface CommandEnvelope {
    val id: CommandId
    val type: CommandType
        get() = "com.evidentdb.command.${this.javaClass.simpleName}"
    val databaseId: DatabaseId
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
    val databaseId: DatabaseId
    val data: EventBody
}

data class ErrorEnvelope(
    override val id: EventId,
    override val commandId: CommandId,
    override val databaseId: DatabaseId,
    override val data: ErrorBody
): EventEnvelope {
    override val type: EventType
        get() = "com.evidentdb.error.${data.javaClass.simpleName}"
}

// Database

typealias TenantRevision = Long
typealias DatabaseId = UUID
typealias DatabaseName = String
typealias DatabaseRevision = Long

sealed interface DatabaseEvent

data class Database(
    val id: DatabaseId,
    val name: DatabaseName,
    // val created: TenantRevision,
    // val revision: DatabaseRevision
)

data class DatabaseCreationInfo(val name: DatabaseName): CommandBody

data class CreateDatabase(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseCreationInfo
): CommandEnvelope

sealed interface DatabaseCreationError: ErrorBody

data class DatabaseCreatedInfo(val database: Database): EventBody

data class DatabaseCreated(
    override val id: EventId,
    override val commandId: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseCreatedInfo
): EventEnvelope, DatabaseEvent

data class DatabaseRenameInfo(
    val oldName: DatabaseName,
    val newName: DatabaseName
): CommandBody, EventBody

data class RenameDatabase(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseRenameInfo
): CommandEnvelope

sealed interface DatabaseRenameError: ErrorBody

data class DatabaseRenamed(
    override val id: EventId,
    override val commandId: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseRenameInfo
): EventEnvelope, DatabaseEvent

data class DatabaseDeletionInfo(
    val name: DatabaseName
): CommandBody, EventBody

data class DeleteDatabase(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseDeletionInfo
): CommandEnvelope

sealed interface DatabaseDeletionError: ErrorBody

data class DatabaseDeleted(
    override val id: EventId,
    override val commandId: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseDeletionInfo
): EventEnvelope, DatabaseEvent

// Streams & Indexes

typealias StreamName = String
typealias StreamKey = String
typealias StreamRevision = Long
typealias StreamEntityId = String

sealed interface ProposedEventStreamState

sealed interface StreamState {
    object Any: ProposedEventStreamState
    object StreamExists: ProposedEventStreamState
    object NoStream: StreamState, ProposedEventStreamState
    data class AtRevision(val revision: StreamRevision): StreamState, ProposedEventStreamState
}

interface Stream {
    val databaseId: DatabaseId
    val name: StreamName
    val revision: StreamRevision

    companion object {
        fun create(databaseId: DatabaseId, name: StreamName, revision: StreamRevision = 0): Stream {
            // TODO: construct either a SimpleStream or an EntityStream depending on parsing the naming rules for entity streams
            return SimpleStream(databaseId, name, revision)
        }
    }
}

data class SimpleStream(
    override val databaseId: DatabaseId,
    override val name: StreamName,
    override val revision: StreamRevision
): Stream

data class EntityStream(
    override val databaseId: DatabaseId,
    override val name: StreamName,
    override val revision: StreamRevision,
    val entityId: StreamEntityId
): Stream

interface StreamWithEvents: Stream {
    val events: List<Event>

    companion object {
        fun create(
            databaseId: DatabaseId,
            name: StreamName,
            revision: StreamRevision = 0,
            events: List<Event>
        ): StreamWithEvents {
            // TODO: construct either a SimpleStream or an EntityStream depending on parsing the naming rules for entity streams
            TODO()
        }
    }
}

data class SimpleStreamWithEvents(
    override val databaseId: DatabaseId,
    override val name: StreamName,
    override val revision: StreamRevision,
    override val events: List<Event>
): StreamWithEvents

data class EntityStreamWithEvents(
    override val databaseId: DatabaseId,
    override val name: StreamName,
    override val revision: StreamRevision,
    val entityId: StreamEntityId,
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
    val databaseId: DatabaseId,
    val event: CloudEvent,
    val stream: StreamName? = null
)

typealias BatchId = UUID
typealias BatchKey = String

sealed interface BatchEvent

data class ProposedBatch(
    val id: BatchId,
    val databaseId: DatabaseId,
    val events: List<ProposedEvent>
): CommandBody

// TODO: as-of/revision?
data class Batch(
    val id: BatchId,
    val databaseId: DatabaseId,
    val events: List<Event>
): EventBody

data class TransactBatch(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: ProposedBatch
): CommandEnvelope

sealed interface BatchTransactionError: ErrorBody

data class BatchTransacted(
    override val id: EventId,
    override val commandId: CommandId,
    override val databaseId: DatabaseId,
    override val data: Batch
): EventEnvelope, BatchEvent

// Errors

data class InvalidDatabaseNameError(val name: DatabaseName): DatabaseCreationError, DatabaseRenameError
data class DatabaseNameAlreadyExistsError(val name: DatabaseName): DatabaseCreationError, DatabaseRenameError
data class DatabaseNotFoundError(val name: DatabaseName): DatabaseRenameError, DatabaseDeletionError, BatchTransactionError

sealed interface InvalidBatchError: BatchTransactionError

object NoEventsProvidedError: InvalidBatchError

sealed interface EventInvalidation

data class InvalidStreamName(val streamName: StreamName): EventInvalidation
data class InvalidEventType(val eventType: EventType): EventInvalidation

data class InvalidEvent(val event: UnvalidatedProposedEvent, val errors: List<EventInvalidation>)
data class InvalidEventsError(val invalidEvents: List<InvalidEvent>): InvalidBatchError

data class StreamStateConflict(val event: ProposedEvent, val streamState: StreamState)
data class StreamStateConflictsError(val conflicts: List<StreamStateConflict>): BatchTransactionError
data class InternalServerError(val message: String): DatabaseCreationError, DatabaseRenameError, DatabaseDeletionError, BatchTransactionError
