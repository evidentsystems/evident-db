package com.evidentdb.cloudevents

import com.evidentdb.event_model.EnvelopeId
import com.evidentdb.application.base32HexStringToLong
import com.evidentdb.application.toBase32HexString
import io.cloudevents.CloudEventExtension
import io.cloudevents.CloudEventExtensions
import io.cloudevents.core.extensions.impl.ExtensionUtils

data class CommandIdExtension(var commandId: EnvelopeId): CloudEventExtension {
    companion object {
        const val COMMAND_ID = "commandid"
        private val KEY_SET = setOf(COMMAND_ID)
    }

    override fun readFrom(extensions: CloudEventExtensions) {
        extensions.getExtension(COMMAND_ID)?.let {
            this.commandId = EnvelopeId.fromString(it.toString())
        }
    }

    override fun getValue(key: String): Any =
        if (COMMAND_ID == key) {
            commandId.toString()
        } else {
            throw ExtensionUtils.generateInvalidKeyException(this.javaClass, key)
        }

    override fun getKeys(): Set<String> =
        KEY_SET
}

data class EventSequenceExtension(var sequence: Long): CloudEventExtension {
    companion object {
        const val SEQUENCE_KEY = "sequence"
        const val SEQUENCE_TYPE_KEY = "sequencetype"
        const val SEQUENCE_TYPE = "Long"
        private val KEY_SET = setOf(SEQUENCE_KEY, SEQUENCE_TYPE_KEY)
    }

    override fun readFrom(extensions: CloudEventExtensions) {
        extensions.getExtension(SEQUENCE_KEY)?.let {
            this.sequence = when(it) {
                is String -> base32HexStringToLong(it)
                else -> throw IllegalArgumentException("Invalid")
            }
        }
    }

    override fun getValue(key: String): Any =
        when (key) {
            SEQUENCE_KEY -> sequence.toBase32HexString()
            SEQUENCE_TYPE_KEY -> SEQUENCE_TYPE
            else -> {
                throw ExtensionUtils.generateInvalidKeyException(this.javaClass, key)
            }
        }

    override fun getKeys(): Set<String> =
        KEY_SET
}
