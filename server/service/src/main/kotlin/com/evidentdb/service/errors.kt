package com.evidentdb.service

import com.evidentdb.domain_model.*
import com.evidentdb.transfer.toTransfer
import com.google.protobuf.Any
import com.google.rpc.Code
import com.google.rpc.Status
import io.cloudevents.protobuf.toTransfer
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import com.evidentdb.dto.v1.proto.BatchConstraintInvalidation as ProtoBatchConstraintInvalidation
import com.evidentdb.dto.v1.proto.DuplicateEventId as ProtoDuplicateEventId
import com.evidentdb.dto.v1.proto.EmptyBatchConstraint as ProtoEmptyBatchConstraint
import com.evidentdb.dto.v1.proto.EventInvalidation as ProtoEventInvalidation
import com.evidentdb.dto.v1.proto.InvalidBatchConstraint as ProtoInvalidBatchConstraint
import com.evidentdb.dto.v1.proto.InvalidEvent as ProtoInvalidEvent
import com.evidentdb.dto.v1.proto.InvalidEventId as ProtoInvalidEventId
import com.evidentdb.dto.v1.proto.InvalidEventSource as ProtoInvalidEventSource
import com.evidentdb.dto.v1.proto.InvalidEventSubject as ProtoInvalidEventSubject
import com.evidentdb.dto.v1.proto.InvalidEventType as ProtoInvalidEventType
import com.evidentdb.dto.v1.proto.InvalidStreamName as ProtoInvalidStreamName

fun EvidentDbCommandError.toRuntimeException(): StatusRuntimeException =
    when (this) {
        is ConcurrentWriteCollision -> throw IllegalStateException("ConcurrentWriteCollision isn't public")
        is BatchConstraintViolations -> this.toRuntimeException()
        is DatabaseNotFound -> this.toRuntimeException()
        is InternalServerError -> this.toRuntimeException()
        EmptyBatch -> EmptyBatch.toRuntimeException()
        is InvalidBatchConstraints -> this.toRuntimeException()
        is InvalidEvents -> this.toRuntimeException()
        is DatabaseNameAlreadyExists -> this.toRuntimeException()
        is InvalidDatabaseName -> this.toRuntimeException()
    }

fun QueryError.toRuntimeException(): StatusRuntimeException =
    when (this) {
        is DatabaseNotFound -> this.toRuntimeException()
        is EventNotFound -> this.toRuntimeException()
        is InternalServerError -> this.toRuntimeException()
        is InvalidEventId -> this.toRuntimeException()
        is InvalidEventSubject -> this.toRuntimeException()
        is InvalidEventType -> this.toRuntimeException()
        is InvalidStreamName -> this.toRuntimeException()
    }

fun DatabaseNameAlreadyExists.toRuntimeException(): StatusRuntimeException {
    val status = Status.newBuilder()
        .setCode(Code.ALREADY_EXISTS_VALUE)
        .setMessage("A database named '${name.value}' already exists")
        .build()
    return StatusProto.toStatusRuntimeException(status)
}

fun InvalidDatabaseName.toRuntimeException(): StatusRuntimeException {
    val status = Status.newBuilder()
        .setCode(Code.INVALID_ARGUMENT_VALUE)
        .setMessage("Invalid database name: '$name'. Names must match ${DatabaseName.PATTERN}")
        .build()
    return StatusProto.toStatusRuntimeException(status)
}

fun DatabaseNotFound.toRuntimeException(): StatusRuntimeException {
    val status = Status.newBuilder()
        .setCode(Code.NOT_FOUND_VALUE)
        .setMessage("No database named '$name' could be found")
        .build()
    return StatusProto.toStatusRuntimeException(status)
}

fun EmptyBatch.toRuntimeException(): StatusRuntimeException =
    StatusProto.toStatusRuntimeException(Status.newBuilder()
        .setCode(Code.INVALID_ARGUMENT_VALUE)
        .setMessage("A transaction batch must contain at least one event")
        .build())

fun InvalidBatchConstraints.toRuntimeException(): StatusRuntimeException {
    val details = invalidConstraints.map { invalidBatchConstraint ->
        Any.pack(invalidBatchConstraint.toTransfer())
    }
    return StatusProto.toStatusRuntimeException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("The provided batch included some invalid constraints")
            .addAllDetails(details)
            .build()
    )
}

fun InvalidEvents.toRuntimeException(): StatusRuntimeException {
    val details = invalidEvents.map { invalidEvent ->
        Any.pack(invalidEvent.toTransfer())
    }
    return StatusProto.toStatusRuntimeException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("The provided batch included some invalid events")
            .addAllDetails(details)
            .build()
    )
}

fun BatchConstraintViolations.toRuntimeException(): StatusRuntimeException {
    val details = violations.map { violation ->
        Any.pack(violation.toTransfer())
    }
    return StatusProto.toStatusRuntimeException(
        Status.newBuilder()
            .setCode(Code.FAILED_PRECONDITION_VALUE)
            .setMessage("Batch optimistic concurrency control constraints don't match current database state")
            .addAllDetails(details)
            .build()
    )
}

fun EventNotFound.toRuntimeException(): StatusRuntimeException =
    StatusProto.toStatusRuntimeException(
        Status.newBuilder()
            .setCode(Code.NOT_FOUND_VALUE)
            .setMessage(message)
            .build()
    )

fun InternalServerError.toRuntimeException(): StatusRuntimeException =
    StatusProto.toStatusRuntimeException(
        Status.newBuilder()
            .setCode(Code.INTERNAL_VALUE)
            .setMessage(message)
            .build()
    )

fun InvalidEventId.toRuntimeException(): StatusRuntimeException =
    StatusProto.toStatusRuntimeException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("Event ID cannot be empty")
            .build()
)

fun InvalidEventSubject.toRuntimeException(): StatusRuntimeException =
    StatusProto.toStatusRuntimeException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("Event subject cannot be empty")
            .build()
    )

fun InvalidEventType.toRuntimeException(): StatusRuntimeException =
    StatusProto.toStatusRuntimeException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("Event type cannot be empty, and should be a reverse DNS name")
            .build()
    )

fun InvalidStreamName.toRuntimeException(): StatusRuntimeException =
    StatusProto.toStatusRuntimeException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("Invalid stream name '$streamName'. Stream names must match ${StreamName.PATTERN}")
            .build()
    )

// Error Details

fun InvalidBatchConstraint.toTransfer(): ProtoInvalidBatchConstraint {
    val builder = ProtoInvalidBatchConstraint.newBuilder()
    builder.index = index
    builder.addAllErrors(errors.map { it.toTransfer() })
    return builder.build()
}

private fun BatchConstraintInvalidation.toTransfer(): ProtoBatchConstraintInvalidation {
    val builder = ProtoBatchConstraintInvalidation.newBuilder()
    when (this) {
        EmptyBatchConstraint ->
            builder.emptyBatchConstraint = ProtoEmptyBatchConstraint.newBuilder().build()
        is InvalidEventSubject ->
            builder.invalidEventSubject = this.toTransfer()
        is InvalidStreamName ->
            builder.invalidStreamName = this.toTransfer()
    }
    return builder.build()
}

fun InvalidEvent.toTransfer(): ProtoInvalidEvent {
    val builder = ProtoInvalidEvent.newBuilder()
    builder.event = event.toTransfer()
    builder.addAllErrors(errors.map { it.toTransfer() })
    builder.build()
    return builder.build()
}

private fun EventInvalidation.toTransfer(): ProtoEventInvalidation {
    val builder = ProtoEventInvalidation.newBuilder()
    when (this) {
        is DuplicateEventId ->
            builder.duplicateEventId = this.toTransfer()
        is InvalidEventId ->
            builder.invalidEventId = this.toTransfer()
        is InvalidEventSource ->
            builder.invalidEventSource = this.toTransfer()
        is InvalidEventSubject ->
            builder.invalidEventSubject = this.toTransfer()
        is InvalidEventType ->
            builder.invalidEventType = this.toTransfer()
        is InvalidStreamName ->
            builder.invalidStreamName = this.toTransfer()
    }
    return builder.build()
}

fun DuplicateEventId.toTransfer(): ProtoDuplicateEventId {
    val builder = ProtoDuplicateEventId.newBuilder()
    builder.stream = stream
    builder.eventId = eventId
    return builder.build()
}

fun InvalidEventSource.toTransfer(): ProtoInvalidEventSource {
    val builder = ProtoInvalidEventSource.newBuilder()
    builder.eventSource = eventSource
    return builder.build()
}

fun InvalidEventId.toTransfer(): ProtoInvalidEventId {
    val builder = ProtoInvalidEventId.newBuilder()
    builder.eventId = eventId
    return builder.build()
}

fun InvalidEventSubject.toTransfer(): ProtoInvalidEventSubject {
    val builder = ProtoInvalidEventSubject.newBuilder()
    builder.eventSubject = eventSubject
    return builder.build()
}

fun InvalidEventType.toTransfer(): ProtoInvalidEventType {
    val builder = ProtoInvalidEventType.newBuilder()
    builder.eventType = eventType
    return builder.build()
}

fun InvalidStreamName.toTransfer(): ProtoInvalidStreamName {
    val builder = ProtoInvalidStreamName.newBuilder()
    builder.streamName = streamName
    return builder.build()
}
