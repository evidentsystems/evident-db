package com.evidentdb.domain

import arrow.core.*
import java.net.URI

fun databaseUri(id: DatabaseId): URI =
    URI("evidentdb", id.toString(), null)

fun databaseIdFromUri(uri: URI): DatabaseId =
    DatabaseId.fromString(uri.schemeSpecificPart)

// TODO: regex check: #"^[a-zA-Z]\w+$"
fun validateDatabaseName(proposedName: DatabaseName)
        : Validated<InvalidDatabaseNameError, DatabaseName> =
    if (proposedName.isNotEmpty())
        proposedName.valid()
    else
        InvalidDatabaseNameError(proposedName).invalid()

suspend fun validateDatabaseNameNotTaken(
    databaseReadModel: DatabaseReadModel,
    name: DatabaseName
) : Validated<DatabaseNameAlreadyExistsError, DatabaseName> =
    if (databaseReadModel.exists(name))
        DatabaseNameAlreadyExistsError(name).invalid()
    else
        name.valid()

suspend fun lookupDatabaseIdFromDatabaseName(
    databaseReadModel: DatabaseReadModel,
    name: DatabaseName
) : Either<DatabaseNotFoundError, DatabaseId> =
    databaseReadModel.database(name)?.id?.right()
        ?: DatabaseNotFoundError(name).left()
