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

    /**
     * @return [Either.Right] w/ [DatabaseReadModel] immediately after lookup,
     *   i.e. doesn't await revision to become available,
     *   otherwise, returns [Either.Left] w/ [DatabaseNotFound]
     */
    suspend fun databaseAtRevision(
        name: DatabaseName,
        revision: Revision
    ): Either<DatabaseNotFound, DatabaseReadModel>
}

interface DatabaseReadModel: Database {
    fun log(startAtRevision: Revision = 0uL): Flow<Batch>
    fun logDetail(startAtRevision: Revision = 0uL): Flow<BatchDetail>

    // Simple event lookup by user-provided ID and stream
    suspend fun eventById(stream: StreamName, id: EventId): Either<EventNotFound, Event>

    // Flows of event revisions from index
    fun stream(stream: StreamName): Flow<Revision>
    fun streamDetail(stream: StreamName): Flow<Event>
    fun subjectStream(stream: StreamName, subject: EventSubject): Flow<Revision>
    fun subjectStreamDetail(stream: StreamName, subject: EventSubject): Flow<Event>
    fun subject(subject: EventSubject): Flow<Revision>
    fun subjectDetail(subject: EventSubject): Flow<Event>
    fun eventType(type: EventType): Flow<Revision>
    fun eventTypeDetail(type: EventType): Flow<Event>

    fun eventsByRevision(revisions: List<Revision>): Flow<Either<QueryError, Event>>
}
