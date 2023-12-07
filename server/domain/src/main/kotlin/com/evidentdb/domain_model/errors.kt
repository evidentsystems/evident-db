package com.evidentdb.domain_model

import arrow.core.NonEmptyList
import io.cloudevents.CloudEvent

sealed interface EvidentDbError

interface DatabaseCreationError: EvidentDbError
interface BatchTransactionError: EvidentDbError
interface DatabaseDeletionError: EvidentDbError

sealed interface QueryError

data class DatabaseNotFound(val name: String): DatabaseDeletionError, BatchTransactionError, QueryError
data class EventNotFound(val database: String, val eventRevision: EventRevision): QueryError
data class InvalidDatabaseName(val name: String): DatabaseCreationError, QueryError

data class DatabaseNameAlreadyExists(val name: DatabaseName): DatabaseCreationError
data class IllegalDatabaseCreationState(val database: String): DatabaseCreationError
data class IllegalBatchTransactionState(val database: String): BatchTransactionError
data class IllegalDatabaseDeletionState(val database: String): DatabaseDeletionError

sealed interface InvalidBatchError : BatchTransactionError

object EmptyBatch : InvalidBatchError

interface EventInvalidation

data class InvalidEventSource(val eventSource: String) : EventInvalidation, InvalidBatchError
data class InvalidStreamName(val streamName: String) : EventInvalidation, QueryError
data class InvalidEventId(val eventId: String) : EventInvalidation, QueryError
data class DuplicateEventId(val stream: String, val eventId: String) : EventInvalidation
data class InvalidEventSubject(val eventSubject: String) : EventInvalidation, QueryError
data class InvalidEventType(val eventType: String) : EventInvalidation, QueryError
data class InvalidEvent(val event: CloudEvent, val errors: List<EventInvalidation>)

data class InvalidEventsError(val invalidEvents: NonEmptyList<InvalidEvent>) : InvalidBatchError

data class DuplicateBatchError(val batch: WellFormedProposedBatch) : BatchTransactionError
data class StreamStateConflict(val constraint: BatchConstraint)
data class StreamStateConflictsError(val conflicts: NonEmptyList<StreamStateConflict>) : BatchTransactionError

data class InternalServerError(val message: String):
    DatabaseCreationError,
    DatabaseDeletionError,
    BatchTransactionError
