package com.evidentdb.dto.protobuf

import com.evidentdb.domain_model.*
import com.evidentdb.domain_model.command.*
import com.evidentdb.dto.v1.proto.EventInvalidation
import com.evidentdb.event_model.*
import com.google.protobuf.Message
import com.google.protobuf.Timestamp
import io.cloudevents.protobuf.cloudEventFromProto
import io.cloudevents.protobuf.toProto
import java.time.Instant
import com.evidentdb.dto.v1.proto.Batch as ProtoBatch
import com.evidentdb.dto.v1.proto.BatchSummaryEvent as ProtoBatchSummaryEvent
import com.evidentdb.dto.v1.proto.BatchSummary as ProtoBatchSummary
import com.evidentdb.dto.v1.proto.BatchTransactionResult as ProtoBatchTransactionResult
import com.evidentdb.dto.v1.proto.Database as ProtoDatabase
import com.evidentdb.dto.v1.proto.DatabaseSummary as ProtoDatabaseSummary
import com.evidentdb.dto.v1.proto.DatabaseCreationInfo as ProtoDatabaseCreationInfo
import com.evidentdb.dto.v1.proto.DatabaseCreationResult as ProtoDatabaseCreationResult
import com.evidentdb.dto.v1.proto.DatabaseDeletionInfo as ProtoDatabaseDeletionInfo
import com.evidentdb.dto.v1.proto.DatabaseDeletionResult as ProtoDatabaseDeletionResult
import com.evidentdb.dto.v1.proto.DatabaseNameAlreadyExistsError as ProtoDatabaseNameAlreadyExistsError
import com.evidentdb.dto.v1.proto.DatabaseNotFoundError as ProtoDatabaseNotFoundError
import com.evidentdb.dto.v1.proto.DuplicateBatchError as ProtoDuplicateBatchError
import com.evidentdb.dto.v1.proto.Event as ProtoEvent
import com.evidentdb.dto.v1.proto.EventInvalidation as ProtoEventInvalidation
import com.evidentdb.dto.v1.proto.InternalServerError as ProtoInternalServerError
import com.evidentdb.dto.v1.proto.DatabaseTopicCreationError as ProtoDatabaseTopicCreationError
import com.evidentdb.dto.v1.proto.DatabaseTopicDeletionError as ProtoDatabaseTopicDeletionError
import com.evidentdb.dto.v1.proto.InvalidDatabaseNameError as ProtoInvalidDatabaseNameError
import com.evidentdb.dto.v1.proto.InvalidEvent as ProtoInvalidEvent
import com.evidentdb.dto.v1.proto.InvalidEventsError as ProtoInvalidEventsError
import com.evidentdb.dto.v1.proto.NoEventsProvidedError as ProtoNoEventsProvidedError
import com.evidentdb.dto.v1.proto.ProposedBatch as ProtoProposedBatch
import com.evidentdb.dto.v1.proto.ProposedEvent as ProtoProposedEvent
import com.evidentdb.dto.v1.proto.StreamState as ProtoStreamState
import com.evidentdb.dto.v1.proto.StreamStateConflict as ProtoStreamStateConflict
import com.evidentdb.dto.v1.proto.StreamStateConflictsError as ProtoStreamStateConflictsError

fun Timestamp.toInstant(): Instant =
    Instant.ofEpochSecond(seconds, nanos.toLong())

fun Instant.toTimestamp(): Timestamp =
    Timestamp.newBuilder()
        .setSeconds(this.epochSecond)
        .setNanos(this.nano)
        .build()

fun databaseCreationInfoFromProto(proto: ProtoDatabaseCreationInfo) =
    DatabaseCreationInfo(
        DatabaseName.build(proto.name),
        proto.topic
    )

fun databaseCreationInfoFromBytes(bytes: ByteArray): DatabaseCreationInfo =
    databaseCreationInfoFromProto(ProtoDatabaseCreationInfo.parseFrom(bytes))

fun DatabaseCreationInfo.toProto(): ProtoDatabaseCreationInfo =
    ProtoDatabaseCreationInfo.newBuilder()
        .setName(this.name.value)
        .setTopic(this.topic)
        .build()

fun databaseCreationResultFromProto(proto: ProtoDatabaseCreationResult) =
    DatabaseCreationResult(databaseFromProto(proto.database))

fun databaseCreationResultFromBytes(bytes: ByteArray): DatabaseCreationResult =
    databaseCreationResultFromProto(ProtoDatabaseCreationResult.parseFrom(bytes))

fun DatabaseCreationResult.toProto(): ProtoDatabaseCreationResult =
    ProtoDatabaseCreationResult.newBuilder()
        .setDatabase(this.database.toProto())
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

fun databaseDeletionResultFromProto(proto: ProtoDatabaseDeletionResult) =
    DatabaseDeletionResult(databaseFromProto(proto.database))

fun databaseDeletionResultFromBytes(bytes: ByteArray): DatabaseDeletionResult =
    databaseDeletionResultFromProto(ProtoDatabaseDeletionResult.parseFrom(bytes))

fun DatabaseDeletionResult.toProto(): ProtoDatabaseDeletionResult =
    ProtoDatabaseDeletionResult.newBuilder()
        .setDatabase(this.database.toProto())
        .build()

fun proposedEventFromProto(proposedEvent: ProtoProposedEvent): ProposedEvent {
    val protoEvent = proposedEvent.event
    val event = cloudEventFromProto(protoEvent)

    return ProposedEvent(
        event,
        StreamName.build(proposedEvent.stream),
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
        .setStream(this.stream.value)
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
        .setDatabase(this.databaseName.value)
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
        .setEvent(this.event.toProto())
        .setStream(this.stream.value)

    return builder.build()
}

fun streamRevisionsFromDto(
    streamRevisions: Map<String, StreamRevision>
): Map<StreamName, StreamRevision> =
    streamRevisions.mapKeys { StreamName.build(it.key) }

fun streamRevisionsToDto(
    streamRevisions: Map<StreamName, StreamRevision>
): Map<String, StreamRevision> =
    streamRevisions.mapKeys { it.key.value }

fun Batch.toProto(): ProtoBatch =
    ProtoBatch.newBuilder()
        .setId(this.id.toString())
        .setDatabase(this.database.value)
        .addAllEvents(this.events.map { it.toProto() })
        .putAllStreamRevisions(streamRevisionsToDto(this.streamRevisions))
        .build()

fun batchFromProto(proto: ProtoBatch): Batch {
    val databaseName = DatabaseName.build(proto.database)
    return Batch(
        BatchId.fromString(proto.id),
        databaseName,
        proto.eventsList.map {
            Event(
                databaseName,
                StreamName.build(it.stream),
                cloudEventFromProto(it.event),
            )
        },
        streamRevisionsFromDto(proto.streamRevisionsMap),
        proto.timestamp.toInstant(),
    )
}

fun batchFromBytes(bytes: ByteArray): Batch =
    batchFromProto(ProtoBatch.parseFrom(bytes))

fun LogBatchEvent.toProto(): ProtoBatchSummaryEvent =
    ProtoBatchSummaryEvent.newBuilder()
        .setId(this.id)
        .setStream(this.stream.value)
        .build()

fun batchSummaryEventFromProto(proto: ProtoBatchSummaryEvent) =
    LogBatchEvent(
        proto.id,
        StreamName.build(proto.stream),
    )

fun LogBatch.toProto(): ProtoBatchSummary =
    ProtoBatchSummary.newBuilder()
        .setId(this.id.toString())
        .setDatabase(this.database.value)
        .addAllEvents(this.events.map { it.toProto() })
        .putAllStreamRevisions(streamRevisionsToDto(this.streamRevisions))
        .setTimestamp(this.timestamp.toTimestamp())
        .build()

fun LogBatch.toByteArray(): ByteArray =
    this.toProto().toByteArray()

fun batchSummaryFromProto(proto: ProtoBatchSummary): LogBatch {
    val databaseName = DatabaseName.build(proto.database)
    return LogBatch(
        BatchId.fromString(proto.id),
        databaseName,
        proto.eventsList.map(::batchSummaryEventFromProto),
        streamRevisionsFromDto(proto.streamRevisionsMap),
        proto.timestamp.toInstant(),
    )
}

fun batchSummaryFromBytes(bytes: ByteArray): LogBatch =
    batchSummaryFromProto(ProtoBatchSummary.parseFrom(bytes))

fun BatchTransactionResult.toProto(): ProtoBatchTransactionResult =
    ProtoBatchTransactionResult.newBuilder()
        .setBatch(this.batch.toProto())
        .setDatabase(this.databaseBefore.toProto())
        .build()

fun batchTransactionResultFromProto(proto: ProtoBatchTransactionResult): BatchTransactionResult =
    BatchTransactionResult(
        batchFromProto(proto.batch),
        databaseFromProto(proto.database),
    )

fun batchTransactionResultFromBytes(bytes: ByteArray): BatchTransactionResult =
    batchTransactionResultFromProto(ProtoBatchTransactionResult.parseFrom(bytes))

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
        .setName(this.name)
        .build()

fun databaseNotFoundErrorFromProto(proto: ProtoDatabaseNotFoundError): DatabaseNotFoundError =
    DatabaseNotFoundError(proto.name)

fun databaseNotFoundErrorFromBytes(bytes: ByteArray): DatabaseNotFoundError =
    databaseNotFoundErrorFromProto(ProtoDatabaseNotFoundError.parseFrom(bytes))

fun NoEventsProvidedError.toProto(): ProtoNoEventsProvidedError =
    ProtoNoEventsProvidedError.newBuilder().build()

fun noEventsProvidedErrorFromProto(_proto: ProtoNoEventsProvidedError): NoEventsProvidedError =
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
                        is InvalidEventSubject ->
                            builder.invalidEventSubjectBuilder.eventSubject = error.eventSubject
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
                        EventInvalidation.InvalidationCase.INVALID_STREAM_NAME -> InvalidStreamName(error.invalidStreamName.streamName)
                        EventInvalidation.InvalidationCase.INVALID_EVENT_TYPE -> InvalidEventType(error.invalidEventType.eventType)
                        EventInvalidation.InvalidationCase.INVALID_EVENT_SUBJECT -> InvalidEventSubject(error.invalidEventSubject.eventSubject)
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

fun DatabaseTopicCreationError.toProto(): ProtoDatabaseTopicCreationError =
    ProtoDatabaseTopicCreationError.newBuilder()
        .setDatabase(this.database)
        .setTopic(this.topic)
        .build()

fun databaseTopicCreationErrorFromProto(
    proto: ProtoDatabaseTopicCreationError
): DatabaseTopicCreationError =
    DatabaseTopicCreationError(
        proto.database,
        proto.topic
    )

fun databaseTopicCreationErrorFromBytes(
    bytes: ByteArray
): DatabaseTopicCreationError =
    databaseTopicCreationErrorFromProto(ProtoDatabaseTopicCreationError.parseFrom(bytes))

fun DatabaseTopicDeletionError.toProto(): ProtoDatabaseTopicDeletionError =
    ProtoDatabaseTopicDeletionError.newBuilder()
        .setDatabase(this.database)
        .setTopic(this.topic)
        .build()

fun databaseTopicDeletionErrorFromProto(
    proto: ProtoDatabaseTopicDeletionError
): DatabaseTopicDeletionError =
    DatabaseTopicDeletionError(
        proto.database,
        proto.topic
    )

fun databaseTopicDeletionErrorFromBytes(
    bytes: ByteArray
): DatabaseTopicDeletionError =
    databaseTopicDeletionErrorFromProto(ProtoDatabaseTopicDeletionError.parseFrom(bytes))

fun EventBody.toProto(): Message =
    when(this) {
        is DatabaseCreationResult -> this.toProto()
        is DatabaseDeletionResult -> this.toProto()
        is BatchTransactionResult -> this.toProto()

        is InvalidDatabaseNameError -> this.toProto()
        is DatabaseNameAlreadyExistsError -> this.toProto()
        is DatabaseNotFoundError -> this.toProto()
        is NoEventsProvidedError -> this.toProto()
        is InvalidEventsError -> this.toProto()
        is DuplicateBatchError -> this.toProto()
        is StreamStateConflictsError -> this.toProto()
        is InternalServerError -> this.toProto()
        is DatabaseTopicCreationError -> this.toProto()
        is DatabaseTopicDeletionError -> this.toProto()
    }

fun ActiveDatabaseCommandModel.toProto(): ProtoDatabase =
    ProtoDatabase.newBuilder()
        .setName(this.name.value)
        .setTopic(this.topic)
        .setCreated(this.created.toTimestamp())
        .putAllStreamRevisions(streamRevisionsToDto(this.clock))
        .build()

fun ActiveDatabaseCommandModel.toByteArray(): ByteArray =
    this.toProto().toByteArray()

fun databaseFromProto(proto: ProtoDatabase) =
    ActiveDatabaseCommandModel(
        DatabaseName.build(proto.name),
        proto.topic,
        proto.created.toInstant(),
        streamRevisionsFromDto(proto.streamRevisionsMap)
    )

fun databaseFromBytes(data: ByteArray): ActiveDatabaseCommandModel =
    databaseFromProto(ProtoDatabase.parseFrom(data))

fun DatabaseSummary.toProto(): ProtoDatabaseSummary =
    ProtoDatabaseSummary.newBuilder()
        .setName(this.name.value)
        .setTopic(this.topic)
        .setCreated(this.created.toTimestamp())
        .build()

fun DatabaseSummary.toByteArray(): ByteArray =
    this.toProto().toByteArray()

fun databaseSummaryFromProto(proto: ProtoDatabaseSummary) =
    DatabaseSummary(
        DatabaseName.build(proto.name),
        proto.topic,
        proto.created.toInstant()
    )

fun databaseSummaryFromBytes(data: ByteArray): DatabaseSummary =
    databaseSummaryFromProto(ProtoDatabaseSummary.parseFrom(data))
