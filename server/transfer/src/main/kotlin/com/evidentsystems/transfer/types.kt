package com.evidentsystems.transfer

import com.evidentdb.domain_model.*
import com.google.protobuf.Timestamp
import io.cloudevents.protobuf.toTransfer
import java.time.Instant
import io.cloudevents.v1.proto.CloudEvent as ProtoCloudEvent
import com.evidentdb.dto.v1.proto.Database as ProtoDatabase
import com.evidentdb.dto.v1.proto.Batch as ProtoBatch
import com.evidentdb.dto.v1.proto.BatchConstraint as ProtoBatchConstraint
import com.evidentdb.dto.v1.proto.EvidentDbCommandError as ProtoEvidentDbCommandError
import com.evidentdb.dto.v1.proto.QueryError as ProtoQueryError

fun Timestamp.toInstant(): Instant =
    Instant.ofEpochSecond(seconds, nanos.toLong())

fun Instant.toTimestamp(): Timestamp =
    Timestamp.newBuilder()
        .setSeconds(this.epochSecond)
        .setNanos(this.nano)
        .build()

fun ProtoBatchConstraint.toDomain(): BatchConstraint = TODO()

fun Database.toTransfer(): ProtoDatabase = TODO()

fun Batch.toTransfer(): ProtoBatch = TODO()

fun Event.toTransfer(): ProtoCloudEvent = this.event.toTransfer()

// Errors (toTransfer only)
fun EvidentDbCommandError.toTransfer(): ProtoEvidentDbCommandError = TODO()

fun QueryError.toTransfer(): ProtoQueryError = TODO()
