package com.evidentdb.domain

import com.google.protobuf.Message
import io.cloudevents.v1.proto.CloudEvent
import java.net.URI
import io.cloudevents.v1.proto.CloudEvent.CloudEventAttributeValue.AttrCase as AttrCase
import io.cloudevents.v1.proto.CloudEvent.CloudEventAttributeValue
import java.nio.charset.Charset
import java.time.Instant
import com.evidentdb.domain.v1.proto.DatabaseCreationInfo as ProtoDatabaseCreationInfo
import com.evidentdb.domain.v1.proto.DatabaseCreatedInfo  as ProtoDatabaseCreatedInfo
import com.evidentdb.domain.v1.proto.DatabaseRenameInfo   as ProtoDatabaseRenameInfo
import com.evidentdb.domain.v1.proto.DatabaseDeletionInfo as ProtoDatabaseDeletionInfo
import com.evidentdb.domain.v1.proto.ProposedBatch        as ProtoProposedBatch
import com.evidentdb.domain.v1.proto.ProposedEvent        as ProtoProposedEvent
import com.evidentdb.domain.v1.proto.Batch                as ProtoBatch
import com.evidentdb.domain.v1.proto.ProposedEventStreamState
import com.evidentdb.domain.v1.proto.Database             as ProtoDatabase
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp

fun databaseCreationInfoFromProto(proto: ProtoDatabaseCreationInfo) =
    DatabaseCreationInfo(
        proto.name
    )

fun databaseCreationInfoFromBytes(bytes: ByteArray): DatabaseCreationInfo =
    databaseCreationInfoFromProto(ProtoDatabaseCreationInfo.parseFrom(bytes))

fun DatabaseCreationInfo.toProto(): ProtoDatabaseCreationInfo =
    ProtoDatabaseCreationInfo.newBuilder()
        .setName(this.name)
        .build()

fun databaseCreattedInfoFromProto(proto: ProtoDatabaseCreatedInfo) =
    DatabaseCreatedInfo(
        Database(
            DatabaseId.fromString(proto.database.id),
            proto.database.name,
        )
    )

fun databaseCreatedInfoFromBytes(bytes: ByteArray): DatabaseCreatedInfo =
    databaseCreattedInfoFromProto(ProtoDatabaseCreatedInfo.parseFrom(bytes))

fun DatabaseCreatedInfo.toProto(): ProtoDatabaseCreatedInfo =
    ProtoDatabaseCreatedInfo.newBuilder()
        .setDatabase(
            ProtoDatabase.newBuilder()
                .setId(this.database.id.toString())
                .setName(this.database.name)
        )
        .build()

fun databaseRenameInfoFromProto(proto: ProtoDatabaseRenameInfo) =
    DatabaseRenameInfo(
        proto.oldName,
        proto.newName,
    )

fun databaseRenameInfoFromBytes(bytes: ByteArray): DatabaseRenameInfo =
    databaseRenameInfoFromProto(ProtoDatabaseRenameInfo.parseFrom(bytes))

fun DatabaseRenameInfo.toProto(): ProtoDatabaseRenameInfo =
    ProtoDatabaseRenameInfo.newBuilder()
        .setOldName(this.oldName)
        .setNewName(this.newName)
        .build()

fun databaseDeletionInfoFromProto(proto: ProtoDatabaseDeletionInfo) =
    DatabaseDeletionInfo(
        proto.name
    )

fun databaseDeletionInfoFromBytes(bytes: ByteArray): DatabaseDeletionInfo =
    databaseDeletionInfoFromProto(ProtoDatabaseDeletionInfo.parseFrom(bytes))

fun DatabaseDeletionInfo.toProto(): ProtoDatabaseDeletionInfo =
    ProtoDatabaseDeletionInfo.newBuilder()
        .setName(this.name)
        .build()

fun proposedEventFromProto(proposedEvent: ProtoProposedEvent): ProposedEvent {
    val event = proposedEvent.event
    return ProposedEvent(
        EventId.fromString(event.id),
        event.type,
        proposedEvent.stream,
        when(proposedEvent.streamState) {
            ProposedEventStreamState.Any -> StreamState.Any
            ProposedEventStreamState.StreamExists -> StreamState.StreamExists
            ProposedEventStreamState.NoStream -> StreamState.NoStream
            ProposedEventStreamState.AtRevision -> StreamState.AtRevision(proposedEvent.atRevision)
            else -> throw IllegalArgumentException("Error parsing proposed event stream state from protobuf")
        },
        event.attributesMap.mapValues { (_, v) ->
            when (v.attrCase) {
                AttrCase.CE_BOOLEAN -> EventAttributeValue.BooleanValue(v.ceBoolean)
                AttrCase.CE_INTEGER -> EventAttributeValue.IntegerValue(v.ceInteger)
                AttrCase.CE_STRING -> EventAttributeValue.StringValue(v.ceString)
                AttrCase.CE_BYTES -> EventAttributeValue.ByteArrayValue(v.ceBytes.toByteArray())
                AttrCase.CE_URI -> EventAttributeValue.UriValue(URI(v.ceUri))
                AttrCase.CE_URI_REF -> EventAttributeValue.UriRefValue(URI(v.ceUri))
                AttrCase.CE_TIMESTAMP -> EventAttributeValue.TimestampValue(
                    Instant.ofEpochSecond(v.ceTimestamp.seconds, v.ceTimestamp.nanos.toLong())
                )
                else -> throw IllegalArgumentException("Error parsing proposed event attribute value from protobuf")
            }
        },
        when(event.dataCase) {
            CloudEvent.DataCase.BINARY_DATA -> event.binaryData.toByteArray()
            CloudEvent.DataCase.TEXT_DATA -> event.textData.toByteArray(Charset.forName("utf-8"))
            CloudEvent.DataCase.PROTO_DATA -> event.protoData.toByteArray()
            else -> null
        }
    )
}

fun proposedBatchFromProto(proto: ProtoProposedBatch) =
    ProposedBatch(
        BatchId.fromString(proto.id),
        DatabaseId.fromString(proto.databaseId),
        proto.eventsList.map { proposedEventFromProto(it) }
    )

fun proposedBatchFromBytes(bytes: ByteArray): ProposedBatch =
    proposedBatchFromProto(ProtoProposedBatch.parseFrom(bytes))

fun ProposedEvent.toProto(): ProtoProposedEvent {
    val builder = ProtoProposedEvent.newBuilder()
        .setStream(this.stream)
    when(this.streamState) {
        is StreamState.Any ->
            builder.streamState = ProposedEventStreamState.Any
        is StreamState.AtRevision -> {
            builder.streamState = ProposedEventStreamState.AtRevision
            builder.atRevision = (this.streamState as StreamState.AtRevision).revision
        }
        is StreamState.NoStream ->
            builder.streamState = ProposedEventStreamState.NoStream
        is StreamState.StreamExists ->
            builder.streamState = ProposedEventStreamState.StreamExists
    }
    val eventBuilder = CloudEvent.newBuilder()
        .setId(this.id.toString())
        .setType(this.type)
        .putAllAttributes(this.attributes.mapValues { (_, v) ->
            val attrBuilder = CloudEventAttributeValue.newBuilder()
            when(v) {
                is EventAttributeValue.BooleanValue ->
                    attrBuilder.ceBoolean = v.value
                is EventAttributeValue.ByteArrayValue ->
                    attrBuilder.ceBytes = ByteString.copyFrom(v.value)
                is EventAttributeValue.IntegerValue ->
                    attrBuilder.ceInteger = v.value
                is EventAttributeValue.StringValue ->
                    attrBuilder.ceString = v.value
                is EventAttributeValue.TimestampValue ->
                    attrBuilder.ceTimestamp = Timestamp.newBuilder()
                        .setSeconds(v.value.epochSecond)
                        .setNanos(v.value.nano)
                        .build()
                is EventAttributeValue.UriRefValue -> attrBuilder.ceUriRef = v.value.toString()
                is EventAttributeValue.UriValue -> attrBuilder.ceUri = v.value.toString()
            }
            attrBuilder.build()
        })

    if (this.data != null)
        eventBuilder.binaryData = ByteString.copyFrom(this.data)
    builder.event = eventBuilder.build()

    return builder.build()
}

fun ProposedBatch.toProto(): ProtoProposedBatch =
    ProtoProposedBatch.newBuilder()
        .setId(this.id.toString())
        .setDatabaseId(this.databaseId.toString())
        .addAllEvents(this.events.map{ it.toProto() })
        .build()

fun CommandBody.toProto(): Message =
    when(this) {
        is DatabaseCreationInfo -> this.toProto()
        is DatabaseDeletionInfo -> this.toProto()
        is DatabaseRenameInfo -> this.toProto()
        is ProposedBatch -> this.toProto()
    }

fun Event.toProto(): CloudEvent {
    val builder = CloudEvent.newBuilder()
        .setId(this.id.toString())
        .setSource(databaseUri(this.databaseId).toString())
        .setType(this.type)
        .putAllAttributes(this.attributes.mapValues { (_, v) ->
            val attrBuilder = CloudEventAttributeValue.newBuilder()
            when(v) {
                is EventAttributeValue.BooleanValue ->
                    attrBuilder.ceBoolean = v.value
                is EventAttributeValue.ByteArrayValue ->
                    attrBuilder.ceBytes = ByteString.copyFrom(v.value)
                is EventAttributeValue.IntegerValue ->
                    attrBuilder.ceInteger = v.value
                is EventAttributeValue.StringValue ->
                    attrBuilder.ceString = v.value
                is EventAttributeValue.TimestampValue ->
                    attrBuilder.ceTimestamp = Timestamp.newBuilder()
                        .setSeconds(v.value.epochSecond)
                        .setNanos(v.value.nano)
                        .build()
                is EventAttributeValue.UriRefValue -> attrBuilder.ceUriRef = v.value.toString()
                is EventAttributeValue.UriValue -> attrBuilder.ceUri = v.value.toString()
            }
            attrBuilder.build()
        })
    if (this.data != null)
        builder.binaryData = ByteString.copyFrom(this.data)
    return builder.build()
}

// TODO: harmonize with cloudevent adapter
fun eventFromProto(proto: CloudEvent): Event =
    Event(
        EventId.fromString(proto.id),
        databaseIdFromUriString(proto.source),
        proto.type,
        proto.attributesMap.mapValues { (_, v) ->
            when (v.attrCase) {
                AttrCase.CE_BOOLEAN -> EventAttributeValue.BooleanValue(v.ceBoolean)
                AttrCase.CE_INTEGER -> EventAttributeValue.IntegerValue(v.ceInteger)
                AttrCase.CE_STRING -> EventAttributeValue.StringValue(v.ceString)
                AttrCase.CE_BYTES -> EventAttributeValue.ByteArrayValue(v.ceBytes.toByteArray())
                AttrCase.CE_URI -> EventAttributeValue.UriValue(URI(v.ceUri))
                AttrCase.CE_URI_REF -> EventAttributeValue.UriRefValue(URI(v.ceUri))
                AttrCase.CE_TIMESTAMP -> EventAttributeValue.TimestampValue(
                    Instant.ofEpochSecond(v.ceTimestamp.seconds, v.ceTimestamp.nanos.toLong())
                )
                else -> throw IllegalArgumentException("Error parsing proposed event attribute value from protobuf")
            }
        },
        when(proto.dataCase) {
            CloudEvent.DataCase.BINARY_DATA -> proto.binaryData.toByteArray()
            CloudEvent.DataCase.TEXT_DATA -> proto.textData.toByteArray(Charset.forName("utf-8"))
            CloudEvent.DataCase.PROTO_DATA -> proto.protoData.toByteArray()
            else -> null
        }
    )

// TODO: is this necessary, given cloudevent adapter?
fun eventFromBytes(bytes: ByteArray): Event =
    eventFromProto(CloudEvent.parseFrom(bytes))

fun Batch.toProto(): ProtoBatch =
    ProtoBatch.newBuilder()
        .setId(this.id.toString())
        .setDatabaseId(this.databaseId.toString())
        .addAllEvents(this.events.map { it.toProto() })
        .build()

fun batchFromProto(proto: ProtoBatch) =
    Batch(
        BatchId.fromString(proto.id),
        DatabaseId.fromString(proto.databaseId),
        proto.eventsList.map(::eventFromProto)
    )

fun batchFromBytes(bytes: ByteArray): Batch =
    batchFromProto(ProtoBatch.parseFrom(bytes))

fun InvalidDatabaseNameError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun invalidDatabaseNameErrorFromBytes(bytes: ByteArray): InvalidDatabaseNameError {
    TODO()
}

fun DatabaseNameAlreadyExistsError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun databaseNameAlreadyExistsErrorFromBytes(bytes: ByteArray): DatabaseNameAlreadyExistsError {
    TODO()
}

fun DatabaseNotFoundError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun databaseNotFoundErrorFromBytes(bytes: ByteArray): DatabaseNotFoundError {
    TODO()
}

fun NoEventsProvidedError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun noEventsProvidedErrorFromBytes(bytes: ByteArray): NoEventsProvidedError {
    TODO()
}

fun InvalidEventsError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun invalidEventsErrorFromBytes(bytes: ByteArray): InvalidEventsError {
    TODO()
}

fun StreamStateConflictsError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun streamStateConflictsErrorFromBytes(bytes: ByteArray): StreamStateConflictsError {
    TODO()
}

fun EventBody.toProto(): Message =
    when(this) {
        is DatabaseCreatedInfo -> this.toProto()
        is DatabaseRenameInfo -> this.toProto()
        is DatabaseDeletionInfo -> this.toProto()
        is Batch -> this.toProto()

        is InvalidDatabaseNameError -> this.toProto()
        is DatabaseNameAlreadyExistsError -> this.toProto()
        is DatabaseNotFoundError -> this.toProto()
        is NoEventsProvidedError -> this.toProto()
        is InvalidEventsError -> this.toProto()
        is StreamStateConflictsError -> this.toProto()
    }

fun Database.toByteArray(): ByteArray =
    ProtoDatabase.newBuilder()
        .setId(this.id.toString())
        .setName(this.name)
        .build()
        .toByteArray()

fun databaseFromProto(proto: ProtoDatabase) =
    Database(
        DatabaseId.fromString(proto.id),
        proto.name
    )

fun databaseFromBytes(data: ByteArray): Database =
    databaseFromProto(ProtoDatabase.parseFrom(data))
