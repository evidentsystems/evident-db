package com.evidentdb.domain

import java.net.URI

// TODO: add tenant
fun eventSource(
    databaseName: DatabaseName,
    event: UnvalidatedProposedEvent
): URI =
    URI("$DB_URI_SCHEME://${databaseName.value}/${event.stream}")

fun buildEventKey(
    databaseName: DatabaseName,
    eventId: EventId
): EventKey =
    "${databaseName.value}/${eventId.toBase32HexString()}"

fun parseEventKey(eventKey: EventKey)
        : Pair<DatabaseName, EventId> {
    val split = eventKey.split('/')
    require(split.size == 2) { "Invalid EventKey" }
    return Pair(
        DatabaseName.build(split[0]),
        split[1].toLong(),
    )
}
