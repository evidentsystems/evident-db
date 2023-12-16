package com.evidentdb.domain_model

import arrow.core.NonEmptyList
import io.cloudevents.CloudEvent

sealed interface EvidentDbCommandError

sealed interface DatabaseCreationError: EvidentDbCommandError
sealed interface BatchTransactionError: EvidentDbCommandError
sealed interface DatabaseDeletionError: EvidentDbCommandError

sealed interface QueryError

data class InvalidDatabaseName(val name: String): DatabaseCreationError
data class DatabaseNameAlreadyExists(val name: DatabaseName): DatabaseCreationError

data class DatabaseNotFound(val name: String): DatabaseDeletionError, BatchTransactionError, QueryError

sealed interface InvalidBatchError : BatchTransactionError

object EmptyBatch : InvalidBatchError

sealed interface EventInvalidation
sealed interface BatchConstraintInvalidation

data class InvalidEventSource(val eventSource: String) : EventInvalidation
data class InvalidStreamName(val streamName: String) : EventInvalidation, BatchConstraintInvalidation, QueryError
data class InvalidEventId(val eventId: String) : EventInvalidation, QueryError
data class DuplicateEventId(val stream: String, val eventId: String) : EventInvalidation
data class InvalidEventSubject(val eventSubject: String) : EventInvalidation, BatchConstraintInvalidation, QueryError
data class InvalidEventType(val eventType: String) : EventInvalidation, QueryError

data class InvalidEvent(val event: CloudEvent, val errors: List<EventInvalidation>)
data class InvalidEvents(val invalidEvents: NonEmptyList<InvalidEvent>) : InvalidBatchError

data class InvalidBatchConstraints(val invalidConstraints: NonEmptyList<BatchConstraintInvalidation>): InvalidBatchError

data class BatchConstraintViolations(
    val batch: WellFormedProposedBatch,
    val violations: NonEmptyList<BatchConstraint>
) : BatchTransactionError

// Not for use in external interface, only for internal conflict resolution
data class ConcurrentWriteCollision(
    val expectedRevision: DatabaseRevision,
    val actualRevision: DatabaseRevision
): BatchTransactionError

data class EventNotFound(val message: String): QueryError

data class InternalServerError(val message: String):
    DatabaseCreationError,
    DatabaseDeletionError,
    BatchTransactionError,
    QueryError
