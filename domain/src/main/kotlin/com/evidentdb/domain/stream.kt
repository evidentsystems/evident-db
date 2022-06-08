package com.evidentdb.domain

import arrow.core.ValidatedNel
import arrow.core.invalidNel
import arrow.core.validNel
import java.net.URI
import java.util.*

const val STREAM_ENTITY_DELIMITER = '/'

// TODO: naming rules?
fun validateStreamName(streamName: StreamName)
        : ValidatedNel<InvalidStreamName, StreamName> =
    if (streamName.isNotEmpty())
        streamName.validNel()
    else
        InvalidStreamName(streamName).invalidNel()

fun parseStreamName(streamName: StreamName)
        : Pair<StreamName, StreamEntityId?> {
    val segments = streamName.split(STREAM_ENTITY_DELIMITER, limit = 2)
    return Pair(segments[0], segments[1])
}


fun buildStreamKey(databaseId: DatabaseId, streamName: StreamName): String =
    URI(
        "evdb",
        databaseId.toString(),
        "/${streamName}",
        null
    ).toString()


fun parseStreamKey(streamKey: StreamKey)
        : Pair<DatabaseId, StreamName> {
    val uri = URI(streamKey)
    return Pair(
        UUID.fromString(uri.host),
        uri.path.substring(1)
    )
}
