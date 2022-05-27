package com.evidentdb.domain

import arrow.core.Either

interface CommandHandler {
    suspend fun createDatabase(proposedName: DatabaseName)
            : Either<DatabaseCreationError, DatabaseCreated> = TODO()
    suspend fun renameDatabase(oldName: DatabaseName, newName: DatabaseName)
            : Either<DatabaseRenameError, DatabaseRenamed> = TODO()
    suspend fun transactBatch(databaseId: DatabaseId, events: Iterable<ProposedEvent>)
            : Either<BatchTransactionError, BatchTransacted> = TODO()
}

interface EventHandler {
    fun handleCreateDatabase(command: CreateDatabase)
            : Either<DatabaseCreationError, DatabaseCreated> = TODO()
    fun handleRenameDatabase(command: RenameDatabase)
            : Either<DatabaseRenameError, DatabaseRenamed> = TODO()
    fun handleTransactBatch(command: TransactBatch)
            : Either<BatchTransactionError, BatchTransacted> = TODO()
}

// Read Models

interface EventProvider {
    suspend fun events(): Iterable<Event>
}

interface StreamProvider {
    suspend fun streams(): Iterable<Stream>
}
