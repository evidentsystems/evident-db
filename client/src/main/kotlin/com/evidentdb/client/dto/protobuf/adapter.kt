package com.evidentdb.client.dto.protobuf

import com.evidentdb.client.*
import com.evidentdb.dto.v1.proto.EventInvalidation
import com.evidentdb.dto.v1.proto.InvalidEvent as ProtoInvalidEvent
import com.evidentdb.dto.v1.proto.StreamStateConflict as ProtoStreamStateConflict
import com.evidentdb.dto.v1.proto.ProposedEvent as ProtoProposedEvent
import com.evidentdb.dto.v1.proto.StreamState as ProtoStreamState
import com.evidentdb.dto.v1.proto.Event as ProtoEvent
import com.evidentdb.dto.v1.proto.Batch as ProtoBatch
import com.evidentdb.dto.v1.proto.Database as ProtoDatabase
import com.google.protobuf.Timestamp
import io.cloudevents.protobuf.toDomain
import io.cloudevents.protobuf.toProto
import java.time.Instant

fun Timestamp.toInstant(): Instant =
    Instant.ofEpochSecond(seconds, nanos.toLong())

fun EventProposal.toProto(): ProtoProposedEvent {
    val builder = ProtoProposedEvent.newBuilder()
    builder.stream = this.stream
    builder.streamState = when(this.streamState) {
        StreamState.Any -> com.evidentdb.dto.v1.proto.StreamState.Any
        is StreamState.AtRevision -> {
            builder.atRevision = this.streamState.revision
            com.evidentdb.dto.v1.proto.StreamState.AtRevision
        }
        StreamState.NoStream -> com.evidentdb.dto.v1.proto.StreamState.NoStream
        StreamState.StreamExists -> com.evidentdb.dto.v1.proto.StreamState.StreamExists
    }
    builder.event = this.event.toProto()
    return builder.build()
}

fun ProtoProposedEvent.toDomain(): EventProposal {
    val protoEvent = this.event
    val event = protoEvent.toDomain()

    return EventProposal(
        event,
        this.stream,
        when(this.streamState) {
            ProtoStreamState.Any -> StreamState.Any
            ProtoStreamState.StreamExists -> StreamState.StreamExists
            ProtoStreamState.NoStream -> StreamState.NoStream
            ProtoStreamState.AtRevision -> StreamState.AtRevision(this.atRevision)
            else -> throw IllegalArgumentException("Error parsing proposed event stream state from protobuf")
        }
    )
}

fun ProtoDatabase.toDomain(): DatabaseSummary =
    DatabaseSummary(
        this.name,
        this.created.toInstant(),
        this.streamRevisionsMap
    )

fun ProtoBatch.toDomain(): Batch =
    Batch(
        BatchId.fromString(this.id),
        this.database,
        this.eventsList.map { it.toDomain() },
        this.streamRevisionsMap,
    )

fun ProtoEvent.toDomain(): Event =
    Event(
        this.event.toDomain(),
        this.stream,
    )

// Errors

fun ProtoInvalidEvent.toDomain(): InvalidEvent =
    InvalidEvent(
        this.event.toDomain(),
        this.invalidationsList.map { error ->
            when(error.invalidationCase) {
                EventInvalidation.InvalidationCase.INVALID_STREAM_NAME ->
                    InvalidStreamName(error.invalidStreamName.streamName)
                EventInvalidation.InvalidationCase.INVALID_EVENT_TYPE ->
                    InvalidEventType(error.invalidEventType.eventType)
                else ->
                    throw IllegalStateException("Error parsing invalid event error from protobuf")
            }
        }
    )

fun ProtoStreamStateConflict.toDomain(): StreamStateConflict =
    StreamStateConflict(
        this.event.toDomain(),
        when(this.streamState) {
            com.evidentdb.dto.v1.proto.StreamState.NoStream ->
                StreamState.NoStream
            com.evidentdb.dto.v1.proto.StreamState.AtRevision ->
                StreamState.AtRevision(this.atRevision)
            else -> throw IllegalStateException("Invalid stream state in StreamStateConflict")
        }
    )