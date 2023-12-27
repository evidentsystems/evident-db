package com.evidentsystems.transfer

import arrow.core.Either
import arrow.core.NonEmptyList
import arrow.core.invalid
import arrow.core.nonEmptyListOf
import arrow.core.raise.either
import arrow.core.raise.zipOrAccumulate
import com.evidentdb.domain_model.*
import com.google.protobuf.Timestamp
import io.cloudevents.protobuf.toTransfer
import java.time.Instant
import io.cloudevents.v1.proto.CloudEvent as ProtoCloudEvent
import com.evidentdb.dto.v1.proto.Database as ProtoDatabase
import com.evidentdb.dto.v1.proto.Batch as ProtoBatch
import com.evidentdb.dto.v1.proto.BatchConstraint as ProtoBatchConstraint
import com.evidentdb.dto.v1.proto.BatchConstraint.ConstraintCase.*
import com.evidentdb.dto.v1.proto.EvidentDbCommandError as ProtoEvidentDbCommandError
import com.evidentdb.dto.v1.proto.DatabaseCreationError as ProtoDatabaseCreationError
import com.evidentdb.dto.v1.proto.BatchTransactionError as ProtoBatchTransactionError
import com.evidentdb.dto.v1.proto.DatabaseDeletionError as ProtoDatabaseDeletionError
import com.evidentdb.dto.v1.proto.QueryError as ProtoQueryError
import com.evidentdb.dto.v1.proto.BatchConstraintInvalidation as ProtoBatchConstraintInvalidation
import com.evidentdb.dto.v1.proto.BatchConstraintViolations as ProtoBatchConstraintViolations
import com.evidentdb.dto.v1.proto.DatabaseNameAlreadyExists as ProtoDatabaseNameAlreadyExists
import com.evidentdb.dto.v1.proto.DatabaseNotFound as ProtoDatabaseNotFound
import com.evidentdb.dto.v1.proto.DuplicateEventId as ProtoDuplicateEventId
import com.evidentdb.dto.v1.proto.EmptyBatch as ProtoEmptyBatch
import com.evidentdb.dto.v1.proto.EmptyBatchConstraint as ProtoEmptyBatchConstraint
import com.evidentdb.dto.v1.proto.EventInvalidation as ProtoEventInvalidation
import com.evidentdb.dto.v1.proto.EventNotFound as ProtoEventNotFound
import com.evidentdb.dto.v1.proto.InternalServerError as ProtoInternalServerError
import com.evidentdb.dto.v1.proto.InvalidBatchConstraints as ProtoInvalidBatchConstraints
import com.evidentdb.dto.v1.proto.InvalidBatchError as ProtoInvalidBatchError
import com.evidentdb.dto.v1.proto.InvalidDatabaseName as ProtoInvalidDatabaseName
import com.evidentdb.dto.v1.proto.InvalidEvents as ProtoInvalidEvents
import com.evidentdb.dto.v1.proto.InvalidEventId as ProtoInvalidEventId
import com.evidentdb.dto.v1.proto.InvalidEventSource as ProtoInvalidEventSource
import com.evidentdb.dto.v1.proto.InvalidEventSubject as ProtoInvalidEventSubject
import com.evidentdb.dto.v1.proto.InvalidEventType as ProtoInvalidEventType
import com.evidentdb.dto.v1.proto.InvalidStreamName as ProtoInvalidStreamName
import com.evidentdb.dto.v1.proto.WellFormedProposedBatch as ProtoWellFormedProposedBatch

fun Timestamp.toInstant(): Instant =
    Instant.ofEpochSecond(seconds, nanos.toLong())

fun Instant.toTimestamp(): Timestamp =
    Timestamp.newBuilder()
        .setSeconds(epochSecond)
        .setNanos(nano)
        .build()

fun Database.toTransfer(): ProtoDatabase = ProtoDatabase.newBuilder()
    .setName(name.value)
    .setSubscriptionUri(subscriptionURI.value.toString())
    .setCreated(created.toTimestamp())
    .setRevision(revision.toLong())
    .build()

fun Batch.toTransfer(): ProtoBatch = ProtoBatch.newBuilder()
    .setDatabase(database.value)
    .addAllEventRevisions(eventRevisions.map { it.toLong() })
    .setTimestamp(timestamp.toTimestamp())
    .setBasisRevision(revision.toLong())
    .build()

fun WellFormedProposedBatch.toTransfer(): ProtoWellFormedProposedBatch {
    val builder = ProtoWellFormedProposedBatch.newBuilder()
    builder.database = databaseName.value
    builder.addAllEvents(events.map { it.toTransfer() })
    builder.addAllConstraints(constraints.map { it.toTransfer() })
    return builder.build()
}

private fun WellFormedProposedEvent.toTransfer(): ProtoCloudEvent = event.toTransfer()

fun Event.toTransfer(): ProtoCloudEvent = event.toTransfer()

private fun BatchConstraint.toTransfer(): ProtoBatchConstraint {
    val builder = ProtoBatchConstraint.newBuilder()
    when (this) {
        is BatchConstraint.StreamExists ->
            builder.streamExistsBuilder.stream = stream.value
        is BatchConstraint.StreamDoesNotExist ->
            builder.streamDoesNotExistBuilder.stream = stream.value
        is BatchConstraint.StreamMaxRevision -> {
            builder.streamMaxRevisionBuilder.stream = stream.value
            builder.streamMaxRevisionBuilder.revision = revision.toLong()
        }
        is BatchConstraint.SubjectExists ->
            builder.subjectExistsBuilder.subject = subject.value
        is BatchConstraint.SubjectDoesNotExist ->
            builder.subjectDoesNotExistBuilder.subject = subject.value
        is BatchConstraint.SubjectMaxRevision -> {
            builder.subjectMaxRevisionBuilder.subject = subject.value
            builder.subjectMaxRevisionBuilder.revision = revision.toLong()
        }
        is BatchConstraint.SubjectExistsOnStream -> {
            builder.subjectExistsOnStreamBuilder.stream = stream.value
            builder.subjectExistsOnStreamBuilder.subject = subject.value
        }
        is BatchConstraint.SubjectDoesNotExistOnStream -> {
            builder.subjectDoesNotExistOnStreamBuilder.stream = stream.value
            builder.subjectDoesNotExistOnStreamBuilder.subject = subject.value
        }
        is BatchConstraint.SubjectMaxRevisionOnStream -> {
            builder.subjectMaxRevisionOnStreamBuilder.stream = stream.value
            builder.subjectMaxRevisionOnStreamBuilder.subject = subject.value
            builder.subjectMaxRevisionOnStreamBuilder.revision = revision.toLong()
        }
    }
    return builder.build()
}

fun ProtoBatchConstraint.toDomain(): Either<NonEmptyList<BatchConstraintInvalidation>, BatchConstraint> =
    either {
        val proto = this@toDomain
        when (proto.constraintCase) {
            STREAM_EXISTS -> {
                val streamName = StreamName(proto.streamExists.stream)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.StreamExists(streamName)
            }
            STREAM_DOES_NOT_EXIST -> {
                val streamName = StreamName(proto.streamDoesNotExist.stream)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.StreamDoesNotExist(streamName)
            }
            STREAM_MAX_REVISION -> {
                val streamName = StreamName(proto.streamMaxRevision.stream)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.StreamMaxRevision(
                    streamName,
                    proto.streamMaxRevision.revision.toULong()
                )
            }
            SUBJECT_EXISTS -> {
                val subjectName = EventSubject(proto.subjectExists.subject)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.SubjectExists(subjectName)
            }
            SUBJECT_DOES_NOT_EXIST -> {
                val subjectName = EventSubject(proto.subjectDoesNotExist.subject)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.SubjectDoesNotExist(subjectName)
            }
            SUBJECT_MAX_REVISION ->  {
                val subjectName = EventSubject(proto.subjectMaxRevision.subject)
                    .mapLeft { nonEmptyListOf(it) }
                    .bind()
                BatchConstraint.SubjectMaxRevision(
                    subjectName,
                    proto.subjectMaxRevision.revision.toULong()
                )
            }
            SUBJECT_EXISTS_ON_STREAM -> zipOrAccumulate(
                { StreamName(proto.subjectExistsOnStream.stream).bind() },
                { EventSubject(proto.subjectExistsOnStream.subject).bind() }
            ) { streamName, subject ->
                BatchConstraint.SubjectExistsOnStream(streamName, subject)
            }
            SUBJECT_DOES_NOT_EXIST_ON_STREAM -> zipOrAccumulate(
                { StreamName(proto.subjectDoesNotExistOnStream.stream).bind() },
                { EventSubject(proto.subjectDoesNotExistOnStream.subject).bind() }
            ) { streamName, subject ->
                BatchConstraint.SubjectDoesNotExistOnStream(streamName, subject)
            }
            SUBJECT_MAX_REVISION_ON_STREAM -> zipOrAccumulate(
                { StreamName(proto.subjectMaxRevisionOnStream.stream).bind() },
                { EventSubject(proto.subjectMaxRevisionOnStream.subject).bind() }
            ) { streamName, subject ->
                BatchConstraint.SubjectMaxRevisionOnStream(
                    streamName,
                    subject,
                    proto.subjectMaxRevisionOnStream.revision.toULong(),
                )
            }
            CONSTRAINT_NOT_SET -> raise(nonEmptyListOf(EmptyBatchConstraint))
            null -> throw IllegalArgumentException("constraintCase enum cannot be null")
        }
    }

// Errors (toTransfer only)

fun EvidentDbCommandError.toTransfer(): ProtoEvidentDbCommandError {
    val builder = ProtoEvidentDbCommandError.newBuilder()
    when (this) {
        is DatabaseCreationError -> builder.databaseCreationError = this.toTransfer()
        is BatchTransactionError -> builder.batchTransactionError = this.toTransfer()
        is DatabaseDeletionError -> builder.databaseDeletionError = this.toTransfer()
    }
    return builder.build()
}

fun DatabaseCreationError.toTransfer(): ProtoDatabaseCreationError {
    val builder = ProtoDatabaseCreationError.newBuilder()
    when (this) {
        is DatabaseNameAlreadyExists ->
            builder.databaseNameAlreadyExists = this.toTransfer()
        is InternalServerError ->
            builder.internalServerError = this.toTransfer()
        is InvalidDatabaseName ->
            builder.invalidDatabaseName = this.toTransfer()
    }
    return builder.build()
}

fun BatchTransactionError.toTransfer(): ProtoBatchTransactionError {
    val builder = ProtoBatchTransactionError.newBuilder()
    when (this) {
        is DatabaseNotFound ->
            builder.databaseNotFound = this.toTransfer()
        is InvalidBatchError ->
            builder.invalidBatch = this.toTransfer()
        is BatchConstraintViolations ->
            builder.batchConstraintViolations = this.toTransfer()
        is InternalServerError ->
            builder.internalServerError = this.toTransfer()
        is ConcurrentWriteCollision ->
            throw IllegalStateException("ConcurrentWriteCollision must be handled locally")
    }
    return builder.build()
}

fun DatabaseDeletionError.toTransfer(): ProtoDatabaseDeletionError {
    val builder = ProtoDatabaseDeletionError.newBuilder()
    when (this) {
        is DatabaseNotFound -> builder.databaseNotFound = this.toTransfer()
        is InternalServerError -> builder.internalServerError = this.toTransfer()
    }
    return builder.build()
}

fun QueryError.toTransfer(): ProtoQueryError {
    val builder = ProtoQueryError.newBuilder()
    when (this) {
        is DatabaseNotFound -> builder.databaseNotFound = this.toTransfer()
        is EventNotFound -> builder.eventNotFound = this.toTransfer()
        is InternalServerError -> builder.internalServerError = this.toTransfer()
        is InvalidEventId -> builder.invalidEventId = this.toTransfer()
        is InvalidEventSubject -> builder.invalidEventSubject = this.toTransfer()
        is InvalidEventType -> builder.invalidEventType = this.toTransfer()
        is InvalidStreamName -> builder.invalidStreamName = this.toTransfer()
    }
    return builder.build()
}

fun DatabaseNameAlreadyExists.toTransfer(): ProtoDatabaseNameAlreadyExists {
    val builder = ProtoDatabaseNameAlreadyExists.newBuilder()
    builder.name = name.value
    return builder.build()
}

fun InvalidDatabaseName.toTransfer(): ProtoInvalidDatabaseName {
    val builder = ProtoInvalidDatabaseName.newBuilder()
    builder.name = name
    return builder.build()
}

fun DatabaseNotFound.toTransfer(): ProtoDatabaseNotFound {
    val builder = ProtoDatabaseNotFound.newBuilder()
    builder.name = name
    return builder.build()
}

fun InvalidBatchError.toTransfer(): ProtoInvalidBatchError {
    val builder = ProtoInvalidBatchError.newBuilder()
    when (this) {
        EmptyBatch ->
            builder.emptyBatch = ProtoEmptyBatch.newBuilder().build()
        is InvalidBatchConstraints ->
            builder.invalidBatchConstraints = this.toTransfer()
        is InvalidEvents ->
            builder.invalidEvents = this.toTransfer()
    }
    return builder.build()
}

fun InvalidBatchConstraints.toTransfer(): ProtoInvalidBatchConstraints {
    val builder = ProtoInvalidBatchConstraints.newBuilder()
    invalidConstraints.forEachIndexed { i, invalidBatchConstraint ->
        val b = builder.addErrorsBuilder(i)
        b.addAllErrors(invalidBatchConstraint.errors.map { it.toTransfer() })
        b.build()
    }
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

fun InvalidEvents.toTransfer(): ProtoInvalidEvents {
    val builder = ProtoInvalidEvents.newBuilder()
    invalidEvents.forEachIndexed { i, invalidEvent ->
        val b = builder.addInvalidEventsBuilder(i)
        b.event = invalidEvent.event.toTransfer()
        b.addAllErrors(invalidEvent.errors.map { it.toTransfer() })
        b.build()
    }
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

fun BatchConstraintViolations.toTransfer(): ProtoBatchConstraintViolations {
    val builder = ProtoBatchConstraintViolations.newBuilder()
    builder.batch = batch.toTransfer()
    builder.addAllViolations(violations.map { violation ->
        violation.toTransfer()
    })
    return builder.build()
}

fun EventNotFound.toTransfer(): ProtoEventNotFound {
    val builder = ProtoEventNotFound.newBuilder()
    builder.message = message
    return builder.build()
}

fun InternalServerError.toTransfer(): ProtoInternalServerError {
    val builder = ProtoInternalServerError.newBuilder()
    builder.message = message
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