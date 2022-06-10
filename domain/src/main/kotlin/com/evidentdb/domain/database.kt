package com.evidentdb.domain

import arrow.core.*

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
