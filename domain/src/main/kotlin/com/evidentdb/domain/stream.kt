package com.evidentdb.domain

import arrow.core.ValidatedNel
import arrow.core.invalidNel
import arrow.core.validNel
import java.net.URI

// TODO: naming rules?
fun validateStreamName(streamName: StreamName)
        : ValidatedNel<InvalidStreamName, StreamName> =
    if (streamName.isNotEmpty())
        streamName.validNel()
    else
        InvalidStreamName(streamName).invalidNel()

fun streamKey(databaseId: DatabaseId, streamName: StreamName): String =
    URI(
        "evidentdb",
        databaseId.toString(),
        streamName,
        null
    ).toString()