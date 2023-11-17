package com.evidentdb.domain_model

import com.evidentdb.domain_model.command.BatchId
import com.evidentdb.domain_model.command.EventIndex

// Errors

sealed interface EvidentDbError

interface DatabaseCreationError: EvidentDbError
interface BatchTransactionError: EvidentDbError
interface DatabaseDeletionError: EvidentDbError

sealed interface NotFoundError

data class BatchNotFoundError(val database: String, val batchId: BatchId): NotFoundError
data class StreamNotFoundError(val database: String, val stream: String): NotFoundError
data class EventNotFoundError(val database: String, val eventIndex: EventIndex): NotFoundError

data class InvalidDatabaseNameError(val name: String):
    DatabaseCreationError,
    DatabaseDeletionError,
    BatchTransactionError
data class DatabaseNotFoundError(val name: String): DatabaseDeletionError, BatchTransactionError, NotFoundError
data class DatabaseTopicCreationError(val database: String, val topic: TopicName): DatabaseCreationError
data class DatabaseTopicDeletionError(val database: String, val topic: TopicName): DatabaseDeletionError

data class InternalServerError(val message: String):
    DatabaseCreationError,
    DatabaseDeletionError,
    BatchTransactionError

interface EventInvalidation

data class DatabaseNameAlreadyExistsError(val name: DatabaseName): DatabaseCreationError
