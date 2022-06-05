package com.evidentdb.domain

import java.net.URI
import java.time.Instant
import java.util.*

// Internal Command & Event wrappers

typealias CommandId = UUID
typealias CommandType = String

sealed interface InternalCommand<T> {
    val id: CommandId
    val type: CommandType
        get() = this.javaClass.name
    val databaseId: DatabaseId
    val data: T
}

typealias EventId = UUID
typealias EventType = String

sealed interface InternalEvent<T> {
    val id: EventId
    val type: CommandType
        get() = this.javaClass.name
    val databaseId: DatabaseId
    val data: T
}

// Database

typealias TenantRevision = Long
typealias DatabaseId = UUID
typealias DatabaseName = String
typealias DatabaseRevision = Long

data class Database(
    val id: DatabaseId,
    val name: DatabaseName,
    // val created: TenantRevision,
    // val revision: DatabaseRevision
)

data class DatabaseCreationInfo(val name: DatabaseName)

data class CreateDatabase(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseCreationInfo
): InternalCommand<DatabaseCreationInfo>

sealed interface DatabaseCreationError

data class DatabaseCreated(
    override val id: EventId,
    override val databaseId: DatabaseId,
    override val data: Database
): InternalEvent<Database>

data class DatabaseRenameInfo(
    val oldName: DatabaseName,
    val newName: DatabaseName
)

data class RenameDatabase(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseRenameInfo
): InternalCommand<DatabaseRenameInfo>

sealed interface DatabaseRenameError

data class DatabaseRenamed(
    override val id: EventId,
    override val databaseId: DatabaseId,
    override val data: DatabaseRenameInfo
): InternalEvent<DatabaseRenameInfo>

data class DatabaseDeletionInfo(
    val name: DatabaseName
)

data class DeleteDatabase(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: DatabaseDeletionInfo
): InternalCommand<DatabaseDeletionInfo>

sealed interface DatabaseDeletionError

data class DatabaseDeleted(
    override val id: EventId,
    override val databaseId: DatabaseId,
    override val data: DatabaseDeletionInfo
): InternalEvent<DatabaseDeletionInfo>

// Streams & Indexes

typealias StreamName = String
typealias DatabaseStreamName = String
typealias StreamRevision = Long
typealias StreamEntityId = String

sealed interface ProposedEventStreamState

sealed interface StreamState {
    object NoStream : StreamState, ProposedEventStreamState
    object StreamExists : ProposedEventStreamState
    data class AtRevision(val revision: StreamRevision): StreamState, ProposedEventStreamState
    object Any : ProposedEventStreamState
}

interface Stream {
    val databaseId: DatabaseId
    val name: StreamName
    val revision: StreamRevision

    companion object {
        fun create(databaseId: DatabaseId, name: StreamName, revision: StreamRevision = 0): Stream {
            // TODO: construct either a SimpleStream or an EntityStream depending on parsing the naming rules for entity streams
            TODO()
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

interface StreamWithEventIds: Stream {
    val eventIds: Iterable<EventId>
}

data class SimpleStreamWithEventIds(
    override val databaseId: DatabaseId,
    override val name: StreamName,
    override val revision: StreamRevision,
    override val eventIds: Iterable<EventId>
): StreamWithEventIds

data class EntityStreamWithEventIds(
    override val databaseId: DatabaseId,
    override val name: StreamName,
    override val revision: StreamRevision,
    val entityId: StreamEntityId,
    override val eventIds: Iterable<EventId>
): StreamWithEventIds

// Events & Batches

typealias EventAttributeKey = String

sealed interface EventAttributeValue {
    data class BooleanValue(val value: Boolean): EventAttributeValue
    data class IntegerValue(val value: Long): EventAttributeValue
    data class StringValue(val value: String): EventAttributeValue
    data class UriValue(val value: URI): EventAttributeValue
    data class UriRefValue(val value: URI): EventAttributeValue
    data class TimestampValue(val value: Instant)
    data class ByteArrayValue(val value: ByteArray): EventAttributeValue {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as ByteArrayValue

            if (!value.contentEquals(other.value)) return false

            return true
        }

        override fun hashCode(): Int {
            return value.contentHashCode()
        }
    }
}

data class UnvalidatedProposedEvent(
    val type: EventType,
    val attributes: Map<EventAttributeKey, EventAttributeValue>,
    val data: ByteArray?,
    val stream: StreamName,
    val streamState: ProposedEventStreamState
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as UnvalidatedProposedEvent

        if (type != other.type) return false
        if (stream != other.stream) return false
        if (streamState != other.streamState) return false
        if (attributes != other.attributes) return false
        if (data != null) {
            if (other.data == null) return false
            if (!data.contentEquals(other.data)) return false
        } else if (other.data != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + stream.hashCode()
        result = 31 * result + streamState.hashCode()
        result = 31 * result + attributes.hashCode()
        result = 31 * result + (data?.contentHashCode() ?: 0)
        return result
    }
}

data class ProposedEvent(
    val id: EventId,
    val type: EventType,
    val attributes: Map<EventAttributeKey, EventAttributeValue>,
    val data: ByteArray?,
    val stream: StreamName,
    val streamState: ProposedEventStreamState
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ProposedEvent

        if (id != other.id) return false
        if (stream != other.stream) return false
        if (type != other.type) return false
        if (streamState != other.streamState) return false
        if (attributes != other.attributes) return false
        if (data != null) {
            if (other.data == null) return false
            if (!data.contentEquals(other.data)) return false
        } else if (other.data != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + stream.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + streamState.hashCode()
        result = 31 * result + attributes.hashCode()
        result = 31 * result + (data?.contentHashCode() ?: 0)
        return result
    }
}

data class Event(
    val id: EventId,
    val type: EventType,
    val attributes: Map<EventAttributeKey, EventAttributeValue>,
    val data: ByteArray?,
    val stream: StreamName,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Event

        if (id != other.id) return false
        if (type != other.type) return false
        if (attributes != other.attributes) return false
        if (data != null) {
            if (other.data == null) return false
            if (!data.contentEquals(other.data)) return false
        } else if (other.data != null) return false
        if (stream != other.stream) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + attributes.hashCode()
        result = 31 * result + (data?.contentHashCode() ?: 0)
        result = 31 * result + stream.hashCode()
        return result
    }
}

typealias BatchId = UUID

data class ProposedBatch(
    val id: BatchId,
    val databaseName: DatabaseName,
    val events: Iterable<ProposedEvent>
)

// TODO: as-of/revision?
data class Batch(
    val id: BatchId,
    val databaseId: DatabaseId,
    val events: Iterable<Event>
)

data class TransactBatch(
    override val id: CommandId,
    override val databaseId: DatabaseId,
    override val data: ProposedBatch
): InternalCommand<ProposedBatch>

sealed interface BatchTransactionError

data class BatchTransacted(
    override val id: EventId,
    override val databaseId: DatabaseId,
    override val data: Batch
): InternalEvent<Batch>

// Errors

data class InvalidDatabaseNameError(val name: DatabaseName): DatabaseCreationError, DatabaseRenameError
data class DatabaseNameAlreadyExistsError(val name: DatabaseName): DatabaseCreationError, DatabaseRenameError
data class DatabaseNotFoundError(val oldName: DatabaseName): DatabaseRenameError, DatabaseDeletionError, BatchTransactionError

sealed interface EventInvalidation

data class InvalidStreamName(val streamName: StreamName): EventInvalidation
data class InvalidEventType(val eventType: EventType): EventInvalidation
data class InvalidEventAttribute(val attributeKey: EventAttributeKey): EventInvalidation

data class InvalidEventError(val event: UnvalidatedProposedEvent, val errors: Iterable<EventInvalidation>)
data class InvalidEventsError(val errors: Iterable<InvalidEventError>): BatchTransactionError

data class StreamStateConflictError(val event: ProposedEvent)
data class StreamStateConflictsError(val errors: Iterable<StreamStateConflictError>): BatchTransactionError
