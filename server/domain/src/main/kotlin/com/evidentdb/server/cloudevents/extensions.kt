package com.evidentdb.server.cloudevents

import com.evidentdb.server.domain_model.elenDecode
import com.evidentdb.server.domain_model.elenEncode
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

        /**
         * This method must be invoked before this extension is used.
         */
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

        /**
         * This method must be invoked before this extension is used.
         */
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
