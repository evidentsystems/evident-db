package com.evidentdb.domain

import arrow.core.*
import java.net.URI

fun databaseOutputTopic(
    tenantName: TenantName,
    databaseName: DatabaseName,
) = "evidentdb-${tenantName.value}-${databaseName.value}-event-log"

fun databaseUri(name: DatabaseName): URI =
    URI(DB_URI_SCHEME, name.value, null)

fun buildDatabaseLogKey(name: DatabaseName, revision: DatabaseRevision): DatabaseLogKey =
    "${name.value}/${revision.toBase32HexString()}"

fun minDatabaseLogKey(name: DatabaseName): DatabaseLogKey =
    buildDatabaseLogKey(name, 0)

fun maxDatabaseLogKey(name: DatabaseName): DatabaseLogKey =
    buildDatabaseLogKey(name, Long.MAX_VALUE)

fun databaseNameFromUri(uri: URI): DatabaseName =
    DatabaseName.build(uri.schemeSpecificPart)

fun lookupDatabase(
    databaseReadModel: DatabaseReadModel,
    name: DatabaseName,
) : Either<DatabaseNotFoundError, Database> =
    databaseReadModel.database(name)?.right()
        ?: DatabaseNotFoundError(name.value).left()

fun validateDatabaseNameNotTaken(
    databaseReadModel: DatabaseReadModel,
    name: DatabaseName
) : Validated<DatabaseNameAlreadyExistsError, DatabaseName> =
    if (databaseReadModel.exists(name))
        DatabaseNameAlreadyExistsError(name).invalid()
    else
        name.valid()

fun databaseRevisionFromEvent(event: EventEnvelope): Database? =
    when(event) {
        is BatchTransacted -> event.data.databaseAfter
        is DatabaseCreated -> event.data.database
        else -> null
    }
