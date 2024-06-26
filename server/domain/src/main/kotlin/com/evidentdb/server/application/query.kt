package com.evidentdb.server.application

import arrow.core.Either
import com.evidentdb.server.domain_model.*
import kotlinx.coroutines.flow.Flow

// Repositories

interface DatabaseUpdateStream: Lifecycle {
    fun subscribe(name: DatabaseName): Flow<Either<DatabaseNotFound, Database>>
}

interface DatabaseRepository: Lifecycle {
    fun databaseCatalog(): Flow<DatabaseName>

    suspend fun latestDatabase(name: DatabaseName): Either<DatabaseNotFound, DatabaseReadModel>
    suspend fun databaseAtRevision(
        name: DatabaseName,
        revision: Revision
    ): Either<DatabaseNotFound, DatabaseReadModel>
}

interface DatabaseReadModel: Database {
    fun log(): Flow<Batch>

    // Simple event lookup by user-provided ID and stream
    suspend fun eventById(stream: StreamName, id: EventId): Either<EventNotFound, Event>

    // Flows of event revisions from index
    fun stream(stream: StreamName): Flow<Revision>
    fun subjectStream(stream: StreamName, subject: EventSubject): Flow<Revision>
    fun subject(subject: EventSubject): Flow<Revision>
    fun eventType(type: EventType): Flow<Revision>

    fun eventsByRevision(revisions: List<Revision>): Flow<Either<QueryError, Event>>
}
