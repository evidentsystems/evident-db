package com.evidentdb.domain

import arrow.core.ValidatedNel
import arrow.core.invalidNel
import arrow.core.validNel
import java.net.URI

const val STREAM_SUBJECT_DELIMITER = '/'
const val STREAM_URI_PATH_PREFIX = "/streams/"

// TODO: naming rules? must be URL-friendly but provide
// for a single delimiter for subject id
fun validateStreamName(streamName: StreamName)
        : ValidatedNel<InvalidStreamName, StreamName> =
    if (streamName.isNotEmpty())
        streamName.validNel()
    else
        InvalidStreamName(streamName).invalidNel()

fun parseStreamName(streamName: StreamName)
        : Pair<StreamName, StreamSubject?> {
    val segments = streamName.split(STREAM_SUBJECT_DELIMITER, limit = 2)
    return Pair(segments[0], segments[1])
}

fun buildStreamKey(databaseName: DatabaseName, streamName: StreamName): String =
    URI(
        "evdb",
        databaseName.value,
        "${STREAM_URI_PATH_PREFIX}${streamName}",
        null
    ).toString()

fun buildStreamKeyPrefix(databaseName: DatabaseName): String =
    URI(
        "evdb",
        databaseName.value,
        "${STREAM_URI_PATH_PREFIX}",
        null
    ).toString()


fun parseStreamKey(streamKey: StreamKey)
        : Pair<DatabaseName, StreamName> {
    val uri = URI(streamKey)
    return Pair(
        DatabaseName.build(uri.host),
        uri.path.substring(STREAM_URI_PATH_PREFIX.length)
    )
}

fun streamStateFromRevisions(
    streamRevisions: Map<StreamName, StreamRevision>,
    streamName: StreamName
): StreamState =
    streamRevisions[streamName]
        ?.let {
            StreamState.AtRevision(it)
        }
        ?: StreamState.NoStream

fun filterStreamByRevisions(
    stream: StreamSummary,
    streamRevisions: Map<StreamName, StreamRevision>,
): StreamSummary? =
    streamRevisions[stream.name]?.let { revision ->
        when (stream) {
            is BaseStreamSummary -> BaseStreamSummary(
                stream.database,
                stream.name,
                stream.eventIds.take(revision.toInt())
            )
            is BaseStream -> BaseStream(
                stream.database,
                stream.name,
                stream.events.take(revision.toInt())
            )
            is SubjectStreamSummary -> SubjectStreamSummary(
                stream.database,
                stream.name,
                stream.subject,
                stream.eventIds.take(revision.toInt()),
            )
            is SubjectStream -> SubjectStream(
                stream.database,
                stream.name,
                stream.subject,
                stream.events.take(revision.toInt()),
            )
        }
    }