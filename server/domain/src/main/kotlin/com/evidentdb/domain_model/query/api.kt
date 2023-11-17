package com.evidentdb.domain_model.query

import arrow.core.Either
import com.evidentdb.domain_model.*
import com.evidentdb.domain_model.command.BatchId
import com.evidentdb.domain_model.command.EventId
import com.evidentdb.domain_model.command.EventIndex
import com.evidentdb.domain_model.command.EventSubject
import io.cloudevents.CloudEvent
import kotlinx.coroutines.flow.Flow

// Repositories

interface DatabaseRepository {
//    fun exists(name: DatabaseName): Boolean =
//        database(name).isRight()
//    fun database(name: DatabaseName, revision: DatabaseRevision? = null): Either<DatabaseNotFoundError, CleanDatabaseWriteModel>
//    fun catalog(): Flow<CleanDatabaseWriteModel>
}

interface BatchRepository {
    fun log(database: DatabaseName): Either<DatabaseNotFoundError, Flow<BatchId>>
    //fun batch(database: DatabaseName, id: BatchId): Either<BatchNotFoundError, Batch>
}

interface StreamRepository {
    fun streamState(databaseName: DatabaseName, stream: StreamName): StreamState
    fun stream(
        databaseName: DatabaseName,
        stream: StreamName
    ): Either<StreamNotFoundError, Flow<Pair<StreamRevision, EventIndex>>>
    fun subjectStream(
        databaseName: DatabaseName,
        stream: StreamName,
        subject: EventSubject,
    ): Either<StreamNotFoundError, Flow<Pair<StreamRevision, EventIndex>>>
    fun streamRevisionsAtDatabaseRevision(
        databaseName: DatabaseName,
        revision: DatabaseRevision
    ): Either<StreamNotFoundError, Map<StreamName, StreamRevision>>
}

interface EventRepository {
    fun eventByIndex(database: DatabaseName, index: EventIndex): Either<EventNotFoundError, CloudEvent>
    fun eventById(databaseName: DatabaseName, id: EventId): Either<EventNotFoundError, CloudEvent>
}

