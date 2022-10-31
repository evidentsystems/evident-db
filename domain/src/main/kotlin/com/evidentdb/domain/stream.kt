package com.evidentdb.domain

fun buildStreamKeyPrefix(databaseName: DatabaseName): String =
    "${databaseName.value}/"

fun buildStreamKeyPrefix(
    databaseName: DatabaseName,
    streamName: StreamName
): String =
    "${buildStreamKeyPrefix(databaseName)}${streamName.value}@"

fun buildStreamKey(
    databaseName: DatabaseName,
    streamName: StreamName,
    index: Long,
): String =
    "${buildStreamKeyPrefix(databaseName, streamName)}${index.toBase32HexString()}"

fun buildSubjectStreamKeyPrefix(
    databaseName: DatabaseName,
    streamName: StreamName,
    subject: EventSubject
): String =
    "${databaseName.value}/${streamName.value}/${subject.value}@"

fun buildSubjectStreamKey(
    databaseName: DatabaseName,
    streamName: StreamName,
    subject: EventSubject,
    index: Long,
): String =
    "${buildSubjectStreamKeyPrefix(databaseName, streamName, subject)}${index.toBase32HexString()}"

//fun parseStreamKey(streamKey: StreamKey)
//        : Triple<DatabaseName, StreamName, EventId> {
//    val split = streamKey.split('/')
//    require(split.size == 3) { "Invalid StreamKey" }
//    return Triple(
//        DatabaseName.build(split[0]),
//        StreamName.build(split[1]),
//        split[2].toLong(),
//    )
//}

fun streamStateFromRevisions(
    streamRevisions: Map<StreamName, StreamRevision>,
    streamName: StreamName
): StreamState =
    streamRevisions[streamName]
        ?.let {
            StreamState.AtRevision(it)
        }
        ?: StreamState.NoStream

fun streamRevisionsFromDto(
    streamRevisions: Map<String, StreamRevision>
): Map<StreamName, StreamRevision> =
    streamRevisions.mapKeys { StreamName.build(it.key) }

fun streamRevisionsToDto(
    streamRevisions: Map<StreamName, StreamRevision>
): Map<String, StreamRevision> =
    streamRevisions.mapKeys { it.key.value }