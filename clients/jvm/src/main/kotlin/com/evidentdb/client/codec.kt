package com.evidentdb.client

const val LENGTH_RUNE = ';'

/**
 * Encodes the given unsigned long as a string per Peter Seymour's [ELEN](https://www.zanopha.com/docs/elen.pdf) paper.
 * Uses ';' as the sequence-length rune, rather than '+' as in the original paper, due to ASCII/UTF-8 values.
 *
 * @param l the long to encode
 * @return a String that sorts lexicographically in the same order that the original ulong sorted numerically.
 */
fun elenEncode(l: ULong): String {
    val buf = StringBuffer()
    val s = l.toString()
    if (l > 0u) {
        buf.append(LENGTH_RUNE)
    }
    if (s.length > 1) {
        buf.append(elenEncode(s.length.toULong()))
    }
    buf.append(s)
    return buf.toString()
}

/**
 * Decodes the given ELEN string (per Peter Seymour's [ELEN](https://www.zanopha.com/docs/elen.pdf) paper)
 * into an unsigned long.
 *
 * @param s the String to decode
 * @return an unsigned long
 */
fun elenDecode(s: String): ULong {
    var sequenceLength = 0;
    while (s[sequenceLength] == LENGTH_RUNE) {
        sequenceLength += 1
    }
    var elementsRemaining = sequenceLength
    var stringPosition = sequenceLength
    var nextParseLength = 1
    while (elementsRemaining > 1) {
        val lString = s.substring(stringPosition, stringPosition + nextParseLength)
        nextParseLength = lString.toInt()
        stringPosition += lString.length
        elementsRemaining -= 1
    }
    return s.substring(stringPosition, stringPosition + nextParseLength).toULong()
}