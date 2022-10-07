package com.evidentdb.domain

import arrow.core.*
import org.apache.commons.codec.binary.Base32
import java.net.URI
import java.nio.ByteBuffer

fun databaseUri(name: DatabaseName): URI =
    URI(DB_URI_SCHEME, name.value, null)

private fun longToByteArray(long: Long): ByteArray {
    val buffer = ByteBuffer.allocate(Long.SIZE_BYTES)
    buffer.putLong(long)
    return buffer.array()
}

fun buildDatabaseLogKey(name: DatabaseName, revision: DatabaseRevision): DatabaseLogKey {
    val revisionBase32hex = Base32(true)
        .encodeToString(
            longToByteArray(revision)
        )
    return "${name.value}$revisionBase32hex"
}

fun minDatabaseLogKey(name: DatabaseName): DatabaseLogKey =
    buildDatabaseLogKey(name, 0)

fun maxDatabaseLogKey(name: DatabaseName): DatabaseLogKey =
    buildDatabaseLogKey(name, Long.MAX_VALUE)

fun databaseNameFromUri(uri: URI): DatabaseName =
    DatabaseName.build(uri.schemeSpecificPart)

//fun databaseNameFromUriString(uri: String): Validated<InvalidDatabaseNameError, DatabaseName> =
//    databaseNameFromUri(URI.create(uri))

fun lookupDatabase(
    databaseReadModel: DatabaseReadModel,
    name: DatabaseName,
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

fun databaseAfterBatchTransacted(database: Database, batch: Batch): Database {
    return Database(
        database.name,
        database.created,
        batch.streamRevisions,
    )
}