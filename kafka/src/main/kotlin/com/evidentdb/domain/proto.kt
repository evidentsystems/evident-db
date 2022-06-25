package com.evidentdb.domain

import com.google.protobuf.Any
import com.google.protobuf.Message

fun databaseCreationInfoFromProto(message: Any): DatabaseCreationInfo {
    TODO()
}

fun databaseCreatedInfoFromProto(message: Any): DatabaseCreatedInfo {
    TODO()
}

fun DatabaseCreationInfo.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun databaseRenameInfoFromProto(message: Any): DatabaseRenameInfo {
    TODO()
}

fun DatabaseRenameInfo.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun databaseDeletionInfoFromProto(message: Any): DatabaseDeletionInfo {
    TODO()
}

fun DatabaseDeletionInfo.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun proposedBatchFromProto(message: Any): ProposedBatch {
    TODO()
}

fun ProposedBatch.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun CommandBody.toProto(): Message =
    when(this) {
        is DatabaseCreationInfo -> this.toProto()
        is DatabaseDeletionInfo -> this.toProto()
        is DatabaseRenameInfo -> this.toProto()
        is ProposedBatch -> this.toProto()
    }

fun Batch.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun batchFromProto(message: Any): Batch {
    TODO()
}

fun InvalidDatabaseNameError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun invalidDatabaseNameErrorFromProto(message: Any): InvalidDatabaseNameError {
    TODO()
}

fun DatabaseNameAlreadyExistsError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun databaseNameAlreadyExistsErrorFromProto(message: Any): DatabaseNameAlreadyExistsError {
    TODO()
}

fun DatabaseNotFoundError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun databaseNotFoundErrorFromProto(message: Any): DatabaseNotFoundError {
    TODO()
}

fun NoEventsProvidedError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun noEventsProvidedErrorFromProto(message: Any): NoEventsProvidedError {
    TODO()
}

fun InvalidEventsError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun invalidEventsErrorFromProto(message: Any): InvalidEventsError {
    TODO()
}

fun StreamStateConflictsError.toProto(): Message {
    TODO("Replace return type with more specific generated class")
}

fun streamStateConflictsErrorFromProto(message: Any): StreamStateConflictsError {
    TODO()
}

fun EventBody.toProto(): Message =
    when(this) {
        is DatabaseCreatedInfo -> this.toProto()
        is DatabaseRenameInfo -> this.toProto()
        is DatabaseDeletionInfo -> this.toProto()
        is Batch -> this.toProto()

        is InvalidDatabaseNameError -> this.toProto()
        is DatabaseNameAlreadyExistsError -> this.toProto()
        is DatabaseNotFoundError -> this.toProto()
        is NoEventsProvidedError -> this.toProto()
        is InvalidEventsError -> this.toProto()
        is StreamStateConflictsError -> this.toProto()
    }

fun Database.toProto(): Message =
    TODO("Replace return type with more specific generated class")

fun databaseFromProto(data: ByteArray): Database =
    TODO()
