package com.evidentdb.domain

import arrow.core.*

fun validateDatabaseName(proposedName: DatabaseName)
        : Validated<InvalidDatabaseNameError, DatabaseName> =
    if (proposedName.isNotEmpty())
        proposedName.valid()
    else
        InvalidDatabaseNameError(proposedName).invalid()

suspend fun validateDatabaseNameNotTaken(
    databaseStore: DatabaseStore,
    name: DatabaseName
) : Validated<DatabaseNameAlreadyExistsError, DatabaseName> =
    if (databaseStore.exists(name))
        DatabaseNameAlreadyExistsError(name).invalid()
    else
        name.valid()

suspend fun lookupDatabaseIdFromDatabaseName(
    databaseStore: DatabaseStore,
    name: DatabaseName
) : Either<DatabaseNotFoundError, DatabaseId> =
    databaseStore.get(name)?.id?.right()
        ?: DatabaseNotFoundError(name).left()
