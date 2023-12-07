package com.evidentdb.cloudevents

import com.evidentdb.domain_model.elenDecode
import io.cloudevents.CloudEventExtension
import io.cloudevents.CloudEventExtensions
import io.cloudevents.core.extensions.impl.ExtensionUtils
import java.time.Instant

data class SequenceExtension(var sequence: ULong): CloudEventExtension {
    companion object {
        const val SEQUENCE_KEY = "sequence"
        private val KEY_SET = setOf(SEQUENCE_KEY)
    }

    override fun readFrom(extensions: CloudEventExtensions) {
        extensions.getExtension(SEQUENCE_KEY)?.let {
            this.sequence = when(it) {
                is String -> elenDecode(it)
                else -> throw IllegalArgumentException("Invalid sequence type")
            }
        }
    }

    override fun getValue(key: String): ULong =
        when (key) {
            SEQUENCE_KEY -> sequence
            else -> {
                throw ExtensionUtils.generateInvalidKeyException(this.javaClass, key)
            }
        }

    override fun getKeys(): Set<String> =
        KEY_SET
}

data class RecordedTimeExtension(var recordedTime: Instant): CloudEventExtension {
    companion object {
        const val RECORDED_TIME_KEY = "recordedtime"
        private val KEY_SET = setOf(RECORDED_TIME_KEY)
    }

    override fun readFrom(extensions: CloudEventExtensions) {
        extensions.getExtension(RECORDED_TIME_KEY)?.let {
            this.recordedTime = when(it) {
                is String -> Instant.parse(it)
                else -> throw IllegalArgumentException("Invalid timestamp string")
            }
        }
    }

    override fun getValue(key: String): Instant =
        when (key) {
            RECORDED_TIME_KEY -> recordedTime
            else -> {
                throw ExtensionUtils.generateInvalidKeyException(this.javaClass, key)
            }
        }

    override fun getKeys(): Set<String> =
        KEY_SET
}
