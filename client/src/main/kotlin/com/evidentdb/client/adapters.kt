package com.evidentdb.client

import com.evidentdb.dto.v1.proto.ProposedEvent
import com.evidentdb.dto.v1.proto.Event as ProtoEvent
import com.evidentdb.dto.v1.proto.Batch as ProtoBatch
import com.google.protobuf.Timestamp
import io.cloudevents.protobuf.toDomain
import io.cloudevents.protobuf.toProto
import java.time.Instant

fun Timestamp.toInstant(): Instant =
    Instant.ofEpochSecond(seconds, nanos.toLong())

fun EventProposal.toProto(): ProposedEvent {
    val builder = ProposedEvent.newBuilder()
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