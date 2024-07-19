package com.evidentdb.server.service

import com.evidentdb.server.domain_model.*
import com.evidentdb.server.transfer.toTransfer
import com.google.protobuf.Any
import com.google.rpc.Code
import com.google.rpc.Status
import io.cloudevents.protobuf.toTransfer
import io.grpc.StatusException
import io.grpc.protobuf.StatusProto
import com.evidentdb.v1.proto.domain.BatchConstraintInvalidation as ProtoBatchConstraintInvalidation
import com.evidentdb.v1.proto.domain.BatchConstraintConflict as ProtoBatchConstraintConflict
import com.evidentdb.v1.proto.domain.DuplicateEventId as ProtoDuplicateEventId
import com.evidentdb.v1.proto.domain.EventInvalidation as ProtoEventInvalidation
import com.evidentdb.v1.proto.domain.InvalidBatchConstraint as ProtoInvalidBatchConstraint
import com.evidentdb.v1.proto.domain.InvalidBatchConstraintRange as ProtoInvalidBatchConstraintRange
import com.evidentdb.v1.proto.domain.InvalidEvent as ProtoInvalidEvent
import com.evidentdb.v1.proto.domain.InvalidEventId as ProtoInvalidEventId
import com.evidentdb.v1.proto.domain.InvalidEventSource as ProtoInvalidEventSource
import com.evidentdb.v1.proto.domain.InvalidEventSubject as ProtoInvalidEventSubject
import com.evidentdb.v1.proto.domain.InvalidEventType as ProtoInvalidEventType
import com.evidentdb.v1.proto.domain.InvalidStreamName as ProtoInvalidStreamName

fun EvidentDbCommandError.toStatusException(): StatusException =
    when (this) {
        is ConcurrentWriteCollision ->
            // Don't surface ConcurrentWriteCollision to client
            InternalServerError("Write collision timeout").toStatusException()
        is BatchConstraintViolations -> this.toStatusException()
        is DatabaseNotFound -> this.toStatusException()
        is InternalServerError -> this.toStatusException()
        EmptyBatch -> EmptyBatch.toStatusException()
        is InvalidBatchConstraints -> this.toStatusException()
        is BatchConstraintConflicts -> this.toStatusException()
        is InvalidEvents -> this.toStatusException()
        is DatabaseNameAlreadyExists -> this.toStatusException()
        is InvalidDatabaseName -> this.toStatusException()
    }

fun QueryError.toStatusException(): StatusException =
    when (this) {
        is DatabaseNotFound -> this.toStatusException()
        is EventNotFound -> this.toStatusException()
        is InternalServerError -> this.toStatusException()
        is InvalidEventId -> this.toStatusException()
        is InvalidEventSubject -> this.toStatusException()
        is InvalidEventType -> this.toStatusException()
        is InvalidStreamName -> this.toStatusException()
        InvalidIndexQuery -> this.toStatusException()
    }

fun DatabaseNameAlreadyExists.toStatusException(): StatusException {
    val status = Status.newBuilder()
        .setCode(Code.ALREADY_EXISTS_VALUE)
        .setMessage("A database named '${name.value}' already exists")
        .build()
    return StatusProto.toStatusException(status)
}

fun InvalidDatabaseName.toStatusException(): StatusException {
    val status = Status.newBuilder()
        .setCode(Code.INVALID_ARGUMENT_VALUE)
        .setMessage("Invalid database name: '$name'. Names must match /${DatabaseName.PATTERN}/")
        .build()
    return StatusProto.toStatusException(status)
}

fun DatabaseNotFound.toStatusException(): StatusException {
    val status = Status.newBuilder()
        .setCode(Code.NOT_FOUND_VALUE)
        .setMessage("No database named '$name' found")
        .build()
    return StatusProto.toStatusException(status)
}

fun InvalidIndexQuery.toStatusException(): StatusException {
    val status = Status.newBuilder()
        .setCode(Code.INVALID_ARGUMENT_VALUE)
        .setMessage("Malformed event index query")
        .build()
    return StatusProto.toStatusException(status)
}

fun EmptyBatch.toStatusException(): StatusException =
    StatusProto.toStatusException(Status.newBuilder()
        .setCode(Code.INVALID_ARGUMENT_VALUE)
        .setMessage("A transaction batch must contain at least one event")
        .build())

fun InvalidBatchConstraints.toStatusException(): StatusException {
    val details = invalidConstraints.map { invalidBatchConstraint ->
        Any.pack(invalidBatchConstraint.toTransfer())
    }
    return StatusProto.toStatusException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("The provided batch includes some invalid constraints")
            .addAllDetails(details)
            .build()
    )
}

fun BatchConstraintConflicts.toStatusException(): StatusException {
    val details = conflicts.map { conflict ->
        Any.pack(conflict.toTransfer())
    }
    return StatusProto.toStatusException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("The provided batch includes some constraints that conflicted with each other")
            .addAllDetails(details)
            .build()
    )
}

fun InvalidEvents.toStatusException(): StatusException {
    val details = invalidEvents.map { invalidEvent ->
        Any.pack(invalidEvent.toTransfer())
    }
    return StatusProto.toStatusException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("The provided batch includes some invalid events")
            .addAllDetails(details)
            .build()
    )
}

fun BatchConstraintViolations.toStatusException(): StatusException {
    val details = violations.map { violation ->
        Any.pack(violation.toTransfer())
    }
    return StatusProto.toStatusException(
        Status.newBuilder()
            .setCode(Code.FAILED_PRECONDITION_VALUE)
            .setMessage("Current database doesn't conform to the provided batch constraints: ${violations}")
            .addAllDetails(details)
            .build()
    )
}

fun EventNotFound.toStatusException(): StatusException =
    StatusProto.toStatusException(
        Status.newBuilder()
            .setCode(Code.NOT_FOUND_VALUE)
            .setMessage(message)
            .build()
    )

fun InternalServerError.toStatusException(): StatusException =
    StatusProto.toStatusException(
        Status.newBuilder()
            .setCode(Code.INTERNAL_VALUE)
            .setMessage(message)
            .build()
    )

fun InvalidEventId.toStatusException(): StatusException =
    StatusProto.toStatusException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("Event ID cannot be empty")
            .build()
)

fun InvalidEventSubject.toStatusException(): StatusException =
    StatusProto.toStatusException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("Event subject cannot be empty")
            .build()
    )

fun InvalidEventType.toStatusException(): StatusException =
    StatusProto.toStatusException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("Event type cannot be empty, and should be a reverse DNS name")
            .build()
    )

fun InvalidStreamName.toStatusException(): StatusException =
    StatusProto.toStatusException(
        Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("Invalid stream name '$streamName'. Stream names must match /${StreamName.PATTERN}/")
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
        is InvalidEventSubject ->
            builder.invalidEventSubject = this.toTransfer()
        is InvalidStreamName ->
            builder.invalidStreamName = this.toTransfer()
        is InvalidBatchConstraintRange -> this.toTransfer()
        is BatchConstraintConflict -> this.toTransfer()
    }
    return builder.build()
}

fun InvalidBatchConstraintRange.toTransfer(): ProtoInvalidBatchConstraintRange {
    val builder = ProtoInvalidBatchConstraintRange.newBuilder()
    builder.min = min.toLong()
    builder.max = max.toLong()
    return builder.build()
}

fun BatchConstraintConflict.toTransfer(): ProtoBatchConstraintConflict {
    val builder = ProtoBatchConstraintConflict.newBuilder()
    builder.lhs = lhs.toTransfer()
    builder.rhs = rhs.toTransfer()
    return builder.build()
}

fun InvalidEvent.toTransfer(): ProtoInvalidEvent {
    val builder = ProtoInvalidEvent.newBuilder()
    builder.event = event.toTransfer()
    builder.addAllErrors(errors.map { it.toTransfer() })
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
