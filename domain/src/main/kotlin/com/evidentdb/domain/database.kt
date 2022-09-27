package com.evidentdb.domain

import arrow.core.*
import java.net.URI

fun databaseUri(name: DatabaseName): URI =
    URI(DB_URI_SCHEME, name.value, null)

fun databaseNameFromUri(uri: URI): DatabaseName =
    DatabaseName.build(uri.schemeSpecificPart)

//fun databaseNameFromUriString(uri: String): Validated<InvalidDatabaseNameError, DatabaseName> =
//    databaseNameFromUri(URI.create(uri))

fun validateDatabaseExists(
    databaseReadModel: DatabaseReadModel,
    name: DatabaseName
) : Either<DatabaseNotFoundError, Database> =
    databaseReadModel.database(name)?.right()
        ?: DatabaseNotFoundError(name).left()

fun validateDatabaseNameNotTaken(
    databaseReadModel: DatabaseReadModel,
    name: DatabaseName
) : Validated<DatabaseNameAlreadyExistsError, DatabaseName> =
    if (databaseReadModel.exists(name))
        DatabaseNameAlreadyExistsError(name).invalid()
    else
        name.valid()

fun nextStreamRevisions(
    streamRevisions: Map<StreamName, StreamRevision>,
    batch: Batch,
): Map<StreamName, StreamRevision> {
    val ret = streamRevisions.toMutableMap()
    batch.events.fold(ret) { acc, event ->
        event.stream?.let {
            val revision = acc[event.stream] ?: 0
            acc[it] = revision + 1
        }
        acc
    }
    return ret
}

fun databaseAfterBatchTransacted(database: Database, batch: Batch, eventId: EventId): Database {
    val streamRevisions = nextStreamRevisions(database.streamRevisions, batch)
    return Database(
        database.name,
        database.created,
        eventId,
        streamRevisions.foldLeft(0L) { acc, (_, v) ->
            acc + v
        },
        streamRevisions,
    )
}