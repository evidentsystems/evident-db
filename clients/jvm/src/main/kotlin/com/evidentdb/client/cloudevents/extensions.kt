package com.evidentdb.client.cloudevents

import com.evidentdb.client.elenDecode
import com.evidentdb.client.elenEncode
import io.cloudevents.CloudEventExtension
import io.cloudevents.CloudEventExtensions
import io.cloudevents.core.extensions.impl.ExtensionUtils
import io.cloudevents.core.provider.ExtensionProvider
import java.time.Instant

class SequenceExtension: CloudEventExtension {
    var sequence: ULong = 0uL

    companion object {
        const val SEQUENCE_KEY = "sequence"
        private val KEY_SET = setOf(SEQUENCE_KEY)
        fun register() {
            ExtensionProvider.getInstance()
                    .registerExtension(
                            SequenceExtension::class.java,
                            ::SequenceExtension
                    )
        }
    }

    override fun readFrom(extensions: CloudEventExtensions) {
        extensions.getExtension(SEQUENCE_KEY)?.let {
            this.sequence = when(it) {
                is String -> elenDecode(it)
                else -> throw IllegalArgumentException("Sequence must be an ELEN-encoded string")
            }
        }
    }

    override fun getValue(key: String) =
        when (key) {
            SEQUENCE_KEY -> elenEncode(sequence)
            else -> {
                throw ExtensionUtils.generateInvalidKeyException(this.javaClass, key)
            }
        }

    override fun getKeys(): Set<String> =
        KEY_SET
}

class RecordedTimeExtension: CloudEventExtension {
    lateinit var recordedTime: Instant

    companion object {
        const val RECORDED_TIME_KEY = "recordedtime"
        private val KEY_SET = setOf(RECORDED_TIME_KEY)
        fun register() {
            ExtensionProvider.getInstance()
                    .registerExtension(
                            RecordedTimeExtension::class.java,
                            ::RecordedTimeExtension
                    )
        }
    }

    override fun readFrom(extensions: CloudEventExtensions) {
        extensions.getExtension(RECORDED_TIME_KEY)?.let {
            this.recordedTime = when(it) {
                is String -> Instant.parse(it)
                else -> throw IllegalArgumentException("Timestamp must be a string")
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
