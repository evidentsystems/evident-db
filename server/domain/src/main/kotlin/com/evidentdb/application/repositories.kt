package com.evidentdb.application

import arrow.core.Either
import com.evidentdb.domain_model.*
import kotlinx.coroutines.flow.Flow

// Repositories

interface DatabaseRepository {
    fun databaseCatalog(): Flow<Database>
    fun subscribe(name: DatabaseName): Flow<Either<DatabaseNotFound, Database>>

    suspend fun latestDatabase(name: DatabaseName): Either<DatabaseNotFound, DatabaseReadModel>
    suspend fun databaseAtRevision(
        name: DatabaseName,
        revision: DatabaseRevision
    ): Either<DatabaseNotFound, DatabaseReadModel>

    fun eventsByRevision(name: DatabaseName, revisions: Flow<EventRevision>): Flow<Event>
}

interface WritableDatabaseRepository: DatabaseRepository {
    suspend fun databaseCommandModel(name: DatabaseName): DatabaseCommandModel

    // Database Creation
    suspend fun setupDatabase(name: DatabaseName): Either<DatabaseCreationError, DatabaseSubscriptionURI>
    suspend fun addDatabase(database: NewlyCreatedDatabaseCommandModel): Either<DatabaseCreationError, Unit>

    // Batch Transaction
    suspend fun saveDatabase(database: DirtyDatabaseCommandModel): Either<BatchTransactionError, Unit>

    // Database Deletion
    suspend fun teardownDatabase(database: DatabaseCommandModelAfterDeletion): Either<DatabaseDeletionError, Unit>
}

interface DatabaseReadModel: Database {
    fun log(): Flow<Batch>

    // Simple event lookup by user-provided ID and stream
    suspend fun eventById(stream: StreamName, id: EventId): Either<EventNotFound, Event>

    // Flows of event revisions from index
    fun stream(stream: StreamName): Flow<EventRevision>
    fun subjectStream(stream: StreamName, subject: EventSubject): Flow<EventRevision>
    fun subject(subject: EventSubject): Flow<EventRevision>
    fun eventType(type: EventType): Flow<EventRevision>
}