package com.evidentdb.dto

import com.evidentdb.domain.*
import com.evidentdb.dto.v1.proto.EventInvalidation.InvalidationCase.*
import com.google.protobuf.Message
import com.evidentdb.dto.v1.proto.DatabaseCreationInfo as ProtoDatabaseCreationInfo
import com.evidentdb.dto.v1.proto.DatabaseDeletionInfo as ProtoDatabaseDeletionInfo
import com.evidentdb.dto.v1.proto.ProposedBatch        as ProtoProposedBatch
import com.evidentdb.dto.v1.proto.ProposedEvent        as ProtoProposedEvent
import com.evidentdb.dto.v1.proto.Batch                as ProtoBatch
import com.evidentdb.dto.v1.proto.BatchSummary         as ProtoBatchSummary
import com.evidentdb.dto.v1.proto.Event                as ProtoEvent
import com.evidentdb.dto.v1.proto.StreamState          as ProtoStreamState
import com.evidentdb.dto.v1.proto.Database             as ProtoDatabase
import com.evidentdb.dto.v1.proto.InvalidDatabaseNameError       as ProtoInvalidDatabaseNameError
import com.evidentdb.dto.v1.proto.DatabaseNameAlreadyExistsError as ProtoDatabaseNameAlreadyExistsError
import com.evidentdb.dto.v1.proto.DatabaseNotFoundError          as ProtoDatabaseNotFoundError
import com.evidentdb.dto.v1.proto.NoEventsProvidedError          as ProtoNoEventsProvidedError
import com.evidentdb.dto.v1.proto.EventInvalidation              as ProtoEventInvalidation
import com.evidentdb.dto.v1.proto.InvalidEvent                   as ProtoInvalidEvent
import com.evidentdb.dto.v1.proto.InvalidEventsError             as ProtoInvalidEventsError
import com.evidentdb.dto.v1.proto.DuplicateBatchError            as ProtoDuplicateBatchError
import com.evidentdb.dto.v1.proto.StreamStateConflict            as ProtoStreamStateConflict
import com.evidentdb.dto.v1.proto.StreamStateConflictsError      as ProtoStreamStateConflictsError
import com.evidentdb.dto.v1.proto.InternalServerError            as ProtoInternalServerError
import io.cloudevents.protobuf.cloudEventFromProto
import io.cloudevents.protobuf.toProto

fun databaseCreationInfoFromProto(proto: ProtoDatabaseCreationInfo) =
    DatabaseCreationInfo(
        DatabaseName.build(proto.name)
    )

fun databaseCreationInfoFromBytes(bytes: ByteArray): DatabaseCreationInfo =
    databaseCreationInfoFromProto(ProtoDatabaseCreationInfo.parseFrom(bytes))

fun DatabaseCreationInfo.toProto(): ProtoDatabaseCreationInfo =
    ProtoDatabaseCreationInfo.newBuilder()
        .setName(this.name.value)
        .build()

fun databaseDeletionInfoFromProto(proto: ProtoDatabaseDeletionInfo) =
    DatabaseDeletionInfo(
        DatabaseName.build(proto.name)
    )

fun databaseDeletionInfoFromBytes(bytes: ByteArray): DatabaseDeletionInfo =
    databaseDeletionInfoFromProto(ProtoDatabaseDeletionInfo.parseFrom(bytes))

fun DatabaseDeletionInfo.toProto(): ProtoDatabaseDeletionInfo =
    ProtoDatabaseDeletionInfo.newBuilder()
        .setName(this.name.value)
        .build()

fun proposedEventFromProto(proposedEvent: ProtoProposedEvent): ProposedEvent {
    val protoEvent = proposedEvent.event
    val event = cloudEventFromProto(protoEvent)

    return ProposedEvent(
        EventId.fromString(event.id),
        event,
        proposedEvent.stream,
        when(proposedEvent.streamState) {
            ProtoStreamState.Any -> StreamState.Any
            ProtoStreamState.StreamExists -> StreamState.StreamExists
            ProtoStreamState.NoStream -> StreamState.NoStream
            ProtoStreamState.AtRevision -> StreamState.AtRevision(proposedEvent.atRevision)
            else -> throw IllegalArgumentException("Error parsing proposed event stream state from protobuf")
        },

    )
}

fun proposedBatchFromProto(proto: ProtoProposedBatch) =
    ProposedBatch(
        BatchId.fromString(proto.id),
        DatabaseName.build(proto.database),
        proto.eventsList.map { proposedEventFromProto(it) }
    )

fun proposedBatchFromBytes(bytes: ByteArray): ProposedBatch =
    proposedBatchFromProto(ProtoProposedBatch.parseFrom(bytes))

fun ProposedEvent.toProto(): ProtoProposedEvent {
    val builder = ProtoProposedEvent.newBuilder()
        .setStream(this.stream)
    when(this.streamState) {
        is StreamState.Any ->
            builder.streamState = ProtoStreamState.Any
        is StreamState.AtRevision -> {
            builder.streamState = ProtoStreamState.AtRevision
            builder.atRevision = (this.streamState as StreamState.AtRevision).revision
        }
        is StreamState.NoStream ->
            builder.streamState = ProtoStreamState.NoStream
        is StreamState.StreamExists ->
            builder.streamState = ProtoStreamState.StreamExists
    }
    builder.event = this.event.toProto()
    return builder.build()
}

fun ProposedBatch.toProto(): ProtoProposedBatch =
    ProtoProposedBatch.newBuilder()
        .setId(this.id.toString())
        .setDatabase(this.database.value)
        .addAllEvents(this.events.map{ it.toProto() })
        .build()

fun CommandBody.toProto(): Message =
    when(this) {
        is DatabaseCreationInfo -> this.toProto()
        is DatabaseDeletionInfo -> this.toProto()
        is ProposedBatch -> this.toProto()
    }

fun Event.toProto(): ProtoEvent {
    val builder = ProtoEvent.newBuilder()
        .setDatabase(this.database.value)
        .setEvent(this.event.toProto())
    this.stream?.let { builder.setStream(it) }

    return builder.build()
}

fun Event.toByteArray(): ByteArray =
    this.toProto().toByteArray()

fun eventFromProto(proto: ProtoEvent): Event =
    Event(
        EventId.fromString(proto.event.id),
        DatabaseName.build(proto.database),
        cloudEventFromProto(proto.event),
        proto.stream.ifEmpty { null }
    )

fun eventFromBytes(bytes: ByteArray): Event =
    eventFromProto(ProtoEvent.parseFrom(bytes))

fun Batch.toProto(): ProtoBatch =
    ProtoBatch.newBuilder()
        .setId(this.id.toString())
        .setDatabase(this.database.value)
        .addAllEvents(this.events.map { it.event.toProto() })
        .build()

fun batchFromProto(proto: ProtoBatch): Batch {
    val databaseName = DatabaseName.build(proto.database)
    return Batch(
        BatchId.fromString(proto.id),
        databaseName,
        proto.eventsList.map {
            Event(
                EventId.fromString(it.id),
                databaseName,
                cloudEventFromProto(it)
            )
        }
    )
}

fun batchFromBytes(bytes: ByteArray): Batch =
    batchFromProto(ProtoBatch.parseFrom(bytes))

fun BatchSummary.toProto(): ProtoBatchSummary =
    ProtoBatchSummary.newBuilder()
        .setDatabase(this.database.value)
        .addAllEventIds(this.eventIds.map { it.toString() })
        .build()

fun BatchSummary.toByteArray(): ByteArray =
    this.toProto().toByteArray()

fun batchSummaryFromProto(proto: ProtoBatchSummary): BatchSummary {
    val databaseName = DatabaseName.build(proto.database)
    return BatchSummary(
        databaseName,
        proto.eventIdsList.map {
            EventId.fromString(it)
        }
    )
}

fun batchSummaryFromBytes(bytes: ByteArray): BatchSummary =
    batchSummaryFromProto(ProtoBatchSummary.parseFrom(bytes))

fun InvalidDatabaseNameError.toProto(): ProtoInvalidDatabaseNameError =
    ProtoInvalidDatabaseNameError.newBuilder()
        .setName(this.name)
        .build()

fun invalidDatabaseNameErrorFromProto(proto: ProtoInvalidDatabaseNameError): InvalidDatabaseNameError =
    InvalidDatabaseNameError(proto.name)

fun invalidDatabaseNameErrorFromBytes(bytes: ByteArray): InvalidDatabaseNameError =
    invalidDatabaseNameErrorFromProto(ProtoInvalidDatabaseNameError.parseFrom(bytes))

fun DatabaseNameAlreadyExistsError.toProto(): ProtoDatabaseNameAlreadyExistsError =
    ProtoDatabaseNameAlreadyExistsError.newBuilder()
        .setName(this.name.value)
        .build()

fun databaseNameAlreadyExistsErrorFromProto(proto: ProtoDatabaseNameAlreadyExistsError): DatabaseNameAlreadyExistsError =
    DatabaseNameAlreadyExistsError(DatabaseName.build(proto.name))

fun databaseNameAlreadyExistsErrorFromBytes(bytes: ByteArray): DatabaseNameAlreadyExistsError =
    databaseNameAlreadyExistsErrorFromProto(ProtoDatabaseNameAlreadyExistsError.parseFrom(bytes))

fun DatabaseNotFoundError.toProto(): ProtoDatabaseNotFoundError =
    ProtoDatabaseNotFoundError.newBuilder()
        .setName(this.name.value)
        .build()

fun databaseNotFoundErrorFromProto(proto: ProtoDatabaseNotFoundError): DatabaseNotFoundError =
    DatabaseNotFoundError(DatabaseName.build(proto.name))

fun databaseNotFoundErrorFromBytes(bytes: ByteArray): DatabaseNotFoundError =
    databaseNotFoundErrorFromProto(ProtoDatabaseNotFoundError.parseFrom(bytes))

fun NoEventsProvidedError.toProto(): ProtoNoEventsProvidedError =
    ProtoNoEventsProvidedError.newBuilder().build()

fun noEventsProvidedErrorFromProto(proto: ProtoNoEventsProvidedError): NoEventsProvidedError =
    NoEventsProvidedError

fun noEventsProvidedErrorFromBytes(bytes: ByteArray): NoEventsProvidedError =
    noEventsProvidedErrorFromProto(ProtoNoEventsProvidedError.parseFrom(bytes))

// TODO: DRY this up a bit w/r/t ProposedEvent.toProto()
fun UnvalidatedProposedEvent.toProto(): ProtoProposedEvent {
    val builder = ProtoProposedEvent.newBuilder()
        .setStream(this.stream)
    when(this.streamState) {
        is StreamState.Any ->
            builder.streamState = ProtoStreamState.Any
        is StreamState.AtRevision -> {
            builder.streamState = ProtoStreamState.AtRevision
            builder.atRevision = (this.streamState as StreamState.AtRevision).revision
        }
        is StreamState.NoStream ->
            builder.streamState = ProtoStreamState.NoStream
        is StreamState.StreamExists ->
            builder.streamState = ProtoStreamState.StreamExists
    }
    builder.event = this.event.toProto()

    return builder.build()
}

fun unvalidatedProposedEventFromProto(proposedEvent: ProtoProposedEvent): UnvalidatedProposedEvent =
    UnvalidatedProposedEvent(
        cloudEventFromProto(proposedEvent.event),
        proposedEvent.stream,
        when(proposedEvent.streamState) {
            ProtoStreamState.Any -> StreamState.Any
            ProtoStreamState.StreamExists -> StreamState.StreamExists
            ProtoStreamState.NoStream -> StreamState.NoStream
            ProtoStreamState.AtRevision -> StreamState.AtRevision(proposedEvent.atRevision)
            else -> throw IllegalArgumentException("Error parsing proposed event stream state from protobuf")
        }
    )

fun InvalidEventsError.toProto(): ProtoInvalidEventsError =
    ProtoInvalidEventsError.newBuilder()
        .addAllInvalidEvents(this.invalidEvents.map { invalid ->
            ProtoInvalidEvent.newBuilder()
                .setEvent(invalid.event.toProto())
                .addAllInvalidations(invalid.errors.map { error ->
                    val builder = ProtoEventInvalidation.newBuilder()
                    when(error) {
                        is InvalidEventType ->
                            builder.invalidEventTypeBuilder.eventType = error.eventType
                        is InvalidStreamName ->
                            builder.invalidStreamNameBuilder.streamName = error.streamName
                    }
                    builder.build()
                })
                .build()
        })
        .build()

fun invalidEventsErrorFromProto(proto: ProtoInvalidEventsError): InvalidEventsError =
    InvalidEventsError(
        proto.invalidEventsList.map { invalidEvent ->
            InvalidEvent(
                unvalidatedProposedEventFromProto(invalidEvent.event),
                invalidEvent.invalidationsList.map { error ->
                    when(error.invalidationCase) {
                        INVALID_STREAM_NAME -> InvalidStreamName(error.invalidStreamName.streamName)
                        INVALID_EVENT_TYPE -> InvalidEventType(error.invalidEventType.eventType)
                        else -> throw IllegalArgumentException("Error parsing invalid event error from protobuf")
                    }
                }
            )
        }
    )

fun invalidEventsErrorFromBytes(bytes: ByteArray): InvalidEventsError =
    invalidEventsErrorFromProto(ProtoInvalidEventsError.parseFrom(bytes))

fun DuplicateBatchError.toProto(): ProtoDuplicateBatchError =
    ProtoDuplicateBatchError.newBuilder()
        .setBatch(this.batch.toProto())
        .build()

fun duplicateBatchErrorFromProto(proto: ProtoDuplicateBatchError) =
    DuplicateBatchError(proposedBatchFromProto(proto.batch))

fun duplicateBatchErrorFromBytes(bytes: ByteArray) =
    duplicateBatchErrorFromProto(ProtoDuplicateBatchError.parseFrom(bytes))

fun StreamStateConflictsError.toProto(): ProtoStreamStateConflictsError =
    ProtoStreamStateConflictsError.newBuilder()
        .addAllConflicts(this.conflicts.map { conflict ->
            val builder = ProtoStreamStateConflict.newBuilder()
                .setEvent(conflict.event.toProto())
            when(val state = conflict.streamState){
                is StreamState.AtRevision -> {
                    builder.streamState = ProtoStreamState.AtRevision
                    builder.atRevision = state.revision
                }
                StreamState.NoStream -> builder.streamState = ProtoStreamState.NoStream
            }
            builder.build()
        })
        .build()

fun streamStateConflictsErrorFromProto(proto: ProtoStreamStateConflictsError): StreamStateConflictsError =
    StreamStateConflictsError(
        proto.conflictsList.map { conflict ->
            StreamStateConflict(
                proposedEventFromProto(conflict.event),
                when(conflict.streamState) {
                    ProtoStreamState.NoStream -> StreamState.NoStream
                    ProtoStreamState.AtRevision -> StreamState.AtRevision(conflict.atRevision)
                    else -> throw IllegalArgumentException("Error parsing stream state error from protobuf")
                }
            )
        }
    )

fun streamStateConflictsErrorFromBytes(bytes: ByteArray): StreamStateConflictsError =
    streamStateConflictsErrorFromProto(ProtoStreamStateConflictsError.parseFrom(bytes))

fun InternalServerError.toProto(): ProtoInternalServerError =
    ProtoInternalServerError.newBuilder().setMessage(this.message).build()

fun internalServerErrorFromProto(proto: ProtoInternalServerError): InternalServerError =
    InternalServerError(proto.message)

fun internalServerErrorFromBytes(bytes: ByteArray): InternalServerError =
    internalServerErrorFromProto(ProtoInternalServerError.parseFrom(bytes))

fun EventBody.toProto(): Message =
    when(this) {
        is DatabaseCreationInfo -> this.toProto()
        is DatabaseDeletionInfo -> this.toProto()
        is Batch -> this.toProto()

        is InvalidDatabaseNameError -> this.toProto()
        is DatabaseNameAlreadyExistsError -> this.toProto()
        is DatabaseNotFoundError -> this.toProto()
        is NoEventsProvidedError -> this.toProto()
        is InvalidEventsError -> this.toProto()
        is DuplicateBatchError -> this.toProto()
        is StreamStateConflictsError -> this.toProto()
        is InternalServerError -> this.toProto()
    }

fun Database.toProto(): ProtoDatabase =
    ProtoDatabase.newBuilder()
        .setName(this.name.value)
        .build()

fun Database.toByteArray(): ByteArray =
    this.toProto().toByteArray()

fun databaseFromProto(proto: ProtoDatabase) =
    Database(DatabaseName.build(proto.name))

fun databaseFromBytes(data: ByteArray): Database =
    databaseFromProto(ProtoDatabase.parseFrom(data))
