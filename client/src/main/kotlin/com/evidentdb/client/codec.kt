package com.evidentdb.client

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