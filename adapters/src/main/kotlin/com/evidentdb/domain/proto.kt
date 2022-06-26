package com.evidentdb.domain

import com.google.protobuf.Any
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
import com.evidentdb.domain.v1.proto.ProposedEventStreamState
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp

fun databaseCreationInfoFromProto(message: Any): DatabaseCreationInfo {
    val proto = message.unpack(ProtoDatabaseCreationInfo::class.java)
    return DatabaseCreationInfo(
        proto.name
    )
}

fun databaseCreatedInfoFromProto(message: Any): DatabaseCreatedInfo {
    val proto = message.unpack(ProtoDatabaseCreatedInfo::class.java)
    return DatabaseCreatedInfo(
        Database(
            DatabaseId.fromString(proto.database.id),
            proto.database.name,
        )
    )
}

fun DatabaseCreationInfo.toProto(): ProtoDatabaseCreationInfo =
    ProtoDatabaseCreationInfo.newBuilder()
        .setName(this.name)
        .build()

fun databaseRenameInfoFromProto(message: Any): DatabaseRenameInfo {
    val proto = message.unpack(ProtoDatabaseRenameInfo::class.java)
    return DatabaseRenameInfo(
        proto.oldName,
        proto.newName,
    )
}

fun DatabaseRenameInfo.toProto(): ProtoDatabaseRenameInfo =
    ProtoDatabaseRenameInfo.newBuilder()
        .setOldName(this.oldName)
        .setNewName(this.newName)
        .build()

fun databaseDeletionInfoFromProto(message: Any): DatabaseDeletionInfo {
    val proto = message.unpack(ProtoDatabaseDeletionInfo::class.java)
    return DatabaseDeletionInfo(
        proto.name
    )
}

fun DatabaseDeletionInfo.toProto(): ProtoDatabaseDeletionInfo =
    ProtoDatabaseDeletionInfo.newBuilder()
        .setName(this.name)
        .build()

fun proposedBatchFromProto(message: Any): ProposedBatch {
    val proto = message.unpack(ProtoProposedBatch::class.java)
    return ProposedBatch(
        BatchId.fromString(proto.id),
        proto.databaseName,
        proto.eventsList.map {
            val event = it.event
            ProposedEvent(
                EventId.fromString(event.id),
                event.type,
                it.stream,
                when(it.streamState) {
                    ProposedEventStreamState.Any -> StreamState.Any
                    ProposedEventStreamState.StreamExists -> StreamState.StreamExists
                    ProposedEventStreamState.NoStream -> StreamState.NoStream
                    ProposedEventStreamState.AtRevision -> StreamState.AtRevision(it.atRevision)
                    else -> throw IllegalArgumentException("Error parsing proposed event stream state from protobuf")
                },
                when(event.dataCase) {
                    CloudEvent.DataCase.BINARY_DATA -> event.binaryData.toByteArray()
                    CloudEvent.DataCase.TEXT_DATA -> event.textData.toByteArray(Charset.forName("utf-8"))
                    CloudEvent.DataCase.PROTO_DATA -> event.protoData.toByteArray()
                    else -> throw IllegalArgumentException("Error parsing proposed event data from protobuf")
                },
                event.attributesMap.mapValues { (_, v) ->
                    when(v.attrCase) {
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
                }
            )
        }
    )
}

fun ProposedBatch.toProto(): ProtoProposedBatch =
    ProtoProposedBatch.newBuilder()
        .setId(this.id.toString())
        .setDatabaseName(this.databaseName)
        .addAllEvents(this.events.map {
            val builder = ProtoProposedEvent.newBuilder()
                .setStream(it.stream)
            when(it.streamState) {
                is StreamState.Any ->
                    builder.streamState = ProposedEventStreamState.Any
                is StreamState.AtRevision -> {
                    builder.streamState = ProposedEventStreamState.AtRevision
                    builder.atRevision = (it.streamState as StreamState.AtRevision).revision
                }
                is StreamState.NoStream ->
                    builder.streamState = ProposedEventStreamState.NoStream
                is StreamState.StreamExists ->
                    builder.streamState = ProposedEventStreamState.StreamExists
            }
            builder.event = CloudEvent.newBuilder()
                .setId(it.id.toString())
                .setType(it.type)
                .setBinaryData(ByteString.copyFrom(it.data))
                .putAllAttributes(it.attributes.mapValues { (_, v) ->
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
                .build()

            builder.build()
        })
        .build()

fun CommandBody.toProto(): Message =
    when(this) {
        is DatabaseCreationInfo -> this.toProto()
        is DatabaseDeletionInfo -> this.toProto()
        is DatabaseRenameInfo -> this.toProto()
        is ProposedBatch -> this.toProto()
    }

fun Batch.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun batchFromProto(message: Any): Batch {
    TODO()
}

fun InvalidDatabaseNameError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun invalidDatabaseNameErrorFromProto(message: Any): InvalidDatabaseNameError {
    TODO()
}

fun DatabaseNameAlreadyExistsError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun databaseNameAlreadyExistsErrorFromProto(message: Any): DatabaseNameAlreadyExistsError {
    TODO()
}

fun DatabaseNotFoundError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun databaseNotFoundErrorFromProto(message: Any): DatabaseNotFoundError {
    TODO()
}

fun NoEventsProvidedError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun noEventsProvidedErrorFromProto(message: Any): NoEventsProvidedError {
    TODO()
}

fun InvalidEventsError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun invalidEventsErrorFromProto(message: Any): InvalidEventsError {
    TODO()
}

fun StreamStateConflictsError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun streamStateConflictsErrorFromProto(message: Any): StreamStateConflictsError {
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
    TODO("Replace return type with more specific generated class")

fun databaseFromProto(data: ByteArray): Database =
    TODO()
