package com.evidentdb.application

import com.evidentdb.domain_model.DatabaseLogKey
import com.evidentdb.domain_model.DatabaseName
import com.evidentdb.domain_model.DatabaseRevision
import com.evidentdb.domain_model.command.*
import org.apache.commons.codec.binary.Base32
import java.nio.ByteBuffer

internal fun longToByteArray(long: Long): ByteArray {
    val buffer = ByteBuffer.allocate(Long.SIZE_BYTES)
    buffer.putLong(long)
    return buffer.array()
}

fun Long.toBase32HexString(): String = Base32(true)
    .encodeToString(longToByteArray(this))

internal fun longFromByteArray(bytes: ByteArray): Long {
    val buffer = ByteBuffer.allocate(Long.SIZE_BYTES)
    buffer.put(bytes)
    buffer.flip()
    return buffer.long
}

fun base32HexStringToLong(base32Hex: String): Long =
    longFromByteArray(
        Base32(true).decode(
            base32Hex.toByteArray(Charsets.UTF_8)
        )
    )

fun buildDatabaseLogKey(name: DatabaseName, revision: DatabaseRevision): DatabaseLogKey =
    "${name.value}/${revision.toBase32HexString()}"

fun minDatabaseLogKey(name: DatabaseName): DatabaseLogKey =
    buildDatabaseLogKey(name, 0)

fun maxDatabaseLogKey(name: DatabaseName): DatabaseLogKey =
    buildDatabaseLogKey(name, Long.MAX_VALUE)

fun buildEventKey(
    databaseName: DatabaseName,
    eventIndex: EventIndex
): EventKey =
    "${databaseName.value}/${eventIndex.toBase32HexString()}"

fun parseEventKey(eventKey: EventKey): Pair<DatabaseName, EventIndex> {
    val split = eventKey.split('/')
    require(split.size == 2) { "Invalid EventKey" }
    return Pair(
        DatabaseName.build(split[0]),
        split[1].toLong(),
    )
}