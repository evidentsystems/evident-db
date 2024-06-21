package com.evidentdb.test.simulation

import arrow.core.getOrElse
import arrow.core.toNonEmptyListOrNull
import com.evidentdb.client.Batch
import com.evidentdb.client.BatchConstraint
import com.evidentdb.client.BatchProposal
import com.evidentdb.client.Revision
import com.evidentdb.client.kotlin.Connection
import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.core.builder.CloudEventBuilder
import io.micronaut.core.type.Argument
import io.micronaut.serde.Decoder
import io.micronaut.serde.Encoder
import io.micronaut.serde.ObjectMapper
import io.micronaut.serde.annotation.Serdeable
import io.micronaut.serde.exceptions.SerdeException
import jakarta.inject.Singleton
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URI
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime

class JsonMapCloudEventData(val map: Map<String, String>): CloudEventData {
    override fun toBytes(): ByteArray =
        ObjectMapper.getDefault().writeValueAsBytes(map)
}

@Singleton
class CloudEventSerde: io.micronaut.serde.Serde<CloudEvent> {
    override fun serialize(
        encoder: Encoder,
        context: io.micronaut.serde.Serializer.EncoderContext,
        type: Argument<out CloudEvent>,
        value: CloudEvent
    ) {
        encoder.encodeObject(type).use { inner ->
            inner.encodeKey("id")
            inner.encodeString(value.id)
            inner.encodeKey("source")
            inner.encodeString(value.source.toString())
            inner.encodeKey("type")
            inner.encodeString(value.type)
            if (value.subject != null) {
                inner.encodeKey("subject")
                inner.encodeString(value.subject)
            }
            value.time?.let {
                inner.encodeKey("time")
                inner.encodeString(it.toString())
            }
            inner.encodeKey("datacontenttype")
            inner.encodeString(value.dataContentType)
            val data = value.data
            if (data is JsonMapCloudEventData) {
                inner.encodeKey("data")
                inner.encodeObject(
                    Argument.mapOf(String::class.java, String::class.java)
                ).use { dataEncoder ->
                    for ((k, v) in data.map) {
                        dataEncoder.encodeKey(k)
                        dataEncoder.encodeString(v)
                    }
                }
            }
        }
    }

    override fun deserialize(
        decoder: Decoder,
        context: io.micronaut.serde.Deserializer.DecoderContext,
        argument: Argument<in CloudEvent>
    ): CloudEvent = decoder.decodeObject(argument).use { inner ->
        var id: String? = null
        var source: String? = null
        var type: String? = null
        var subject: String? = null
        var time: String? = null
        var dataContentType = "application/json"
        val data = mutableMapOf<String, String>()
        var key = inner.decodeKey()
        while (key != null) {
            when(key) {
                "id" -> id = inner.decodeString()
                "source" -> source = inner.decodeString()
                "type" -> type = inner.decodeString()
                "subject" -> subject = inner.decodeString()
                "time" -> time = inner.decodeString()
                "datacontenttype" -> dataContentType = inner.decodeString()
                "data" -> inner.decodeObject().use { dataDecoder ->
                    var dataKey = dataDecoder.decodeKey()
                    while (dataKey != null) {
                        data[dataKey] = dataDecoder.decodeString()
                        dataKey = dataDecoder.decodeKey()
                    }
                }
                "specversion" -> inner.decodeString()
                else -> throw SerdeException("Unexpected key '$key' while deserializing CloudEvent")
            }
            key = inner.decodeKey()
        }
        CloudEventBuilder.v1()
            .withId(id)
            .withSource(URI(source!!))
            .withType(type)
            .withSubject(subject)
            .withTime(OffsetDateTime.parse(time))
            .withData(dataContentType, JsonMapCloudEventData(data))
            .build()
    }

}

@Singleton
class BatchConstraintSerde: io.micronaut.serde.Serde<BatchConstraint> {
    override fun serialize(
        encoder: Encoder,
        context: io.micronaut.serde.Serializer.EncoderContext,
        type: Argument<out BatchConstraint>,
        value: BatchConstraint
    ) {
        encoder.encodeObject(type).use { wrapper ->
            when(value) {
                is BatchConstraint.DatabaseMinRevision -> {
                    wrapper.encodeKey("database_min_revision")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("revision")
                        it.encodeLong(value.revision.toLong())
                    }
                }
                is BatchConstraint.DatabaseMaxRevision -> {
                    wrapper.encodeKey("database_max_revision")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("revision")
                        it.encodeLong(value.revision.toLong())
                    }
                }
                is BatchConstraint.DatabaseRevisionRange -> {
                    wrapper.encodeKey("database_revision_range")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("min")
                        it.encodeLong(value.min.toLong())
                        it.encodeKey("max")
                        it.encodeLong(value.max.toLong())
                    }
                }

                is BatchConstraint.StreamMinRevision -> {
                    wrapper.encodeKey("stream_min_revision")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("stream")
                        it.encodeString(value.stream)
                        it.encodeKey("revision")
                        it.encodeLong(value.revision.toLong())
                    }
                }
                is BatchConstraint.StreamMaxRevision -> {
                    wrapper.encodeKey("stream_max_revision")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("stream")
                        it.encodeString(value.stream)
                        it.encodeKey("revision")
                        it.encodeLong(value.revision.toLong())
                    }
                }
                is BatchConstraint.StreamRevisionRange -> {
                    wrapper.encodeKey("stream_revision_range")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("stream")
                        it.encodeString(value.stream)
                        it.encodeKey("min")
                        it.encodeLong(value.min.toLong())
                        it.encodeKey("max")
                        it.encodeLong(value.max.toLong())
                    }
                }

                is BatchConstraint.SubjectMinRevision -> {
                    wrapper.encodeKey("subject_min_revision")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("subject")
                        it.encodeString(value.subject)
                        it.encodeKey("revision")
                        it.encodeLong(value.revision.toLong())
                    }
                }
                is BatchConstraint.SubjectMaxRevision -> {
                    wrapper.encodeKey("subject_max_revision")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("subject")
                        it.encodeString(value.subject)
                        it.encodeKey("revision")
                        it.encodeLong(value.revision.toLong())
                    }
                }
                is BatchConstraint.SubjectRevisionRange -> {
                    wrapper.encodeKey("subject_revision_range")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("subject")
                        it.encodeString(value.subject)
                        it.encodeKey("min")
                        it.encodeLong(value.min.toLong())
                        it.encodeKey("max")
                        it.encodeLong(value.max.toLong())
                    }
                }

                is BatchConstraint.SubjectMinRevisionOnStream -> {
                    wrapper.encodeKey("subject_min_revision_on_stream")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("stream")
                        it.encodeString(value.stream)
                        it.encodeKey("subject")
                        it.encodeString(value.subject)
                        it.encodeKey("revision")
                        it.encodeLong(value.revision.toLong())
                    }
                }
                is BatchConstraint.SubjectMaxRevisionOnStream -> {
                    wrapper.encodeKey("subject_max_revision_on_stream")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("stream")
                        it.encodeString(value.stream)
                        it.encodeKey("subject")
                        it.encodeString(value.subject)
                        it.encodeKey("revision")
                        it.encodeLong(value.revision.toLong())
                    }
                }
                is BatchConstraint.SubjectStreamRevisionRange -> {
                    wrapper.encodeKey("subject_stream_revision_range")
                    wrapper.encodeObject(type).use {
                        it.encodeKey("stream")
                        it.encodeString(value.stream)
                        it.encodeKey("subject")
                        it.encodeString(value.subject)
                        it.encodeKey("min")
                        it.encodeLong(value.min.toLong())
                        it.encodeKey("max")
                        it.encodeLong(value.max.toLong())
                    }
                }
            }
        }
    }

    override fun deserialize(
        decoder: Decoder,
        context: io.micronaut.serde.Deserializer.DecoderContext,
        type: Argument<in BatchConstraint>
    ): BatchConstraint = decoder.decodeObject().use { outer ->
        val constraintType: String = outer.decodeKey()
        outer.decodeObject().use { inner ->
            var stream: String? = null
            var subject: String? = null
            var revision: Revision? = null
            var min: Revision? = null
            var max: Revision? = null
            var key = inner.decodeKey()
            while (key != null) {
                when(key) {
                    "stream" -> stream = inner.decodeString()
                    "subject" -> subject = inner.decodeString()
                    "revision" -> revision = inner.decodeLong().toULong()
                    "min" -> min = inner.decodeLong().toULong()
                    "max" -> max = inner.decodeLong().toULong()
                    else -> throw SerdeException("Illegal key: $key")
                }
                key = inner.decodeKey()
            }
            when (constraintType) {
                "database_min_revision" -> BatchConstraint.DatabaseMinRevision(
                    revision!!
                )
                "database_max_revision" -> BatchConstraint.DatabaseMaxRevision(
                    revision!!
                )
                "database_revision_range" -> BatchConstraint.DatabaseRevisionRange(
                    min!!, max!!
                ).getOrElse {
                    BatchConstraint.DatabaseRevisionRange(max, min).getOrNull()!!
                }

                "stream_min_revision" -> BatchConstraint.StreamMinRevision(
                    stream!!, revision!!
                )
                "stream_max_revision" -> BatchConstraint.StreamMaxRevision(
                    stream!!, revision!!
                )
                "stream_revision_range" -> BatchConstraint.StreamRevisionRange(
                    stream!!, min!!, max!!
                ).getOrElse {
                    BatchConstraint.StreamRevisionRange(
                        stream, max, min
                    ).getOrNull()!!
                }

                "subject_min_revision" -> BatchConstraint.SubjectMinRevision(
                    subject!!, revision!!
                )
                "subject_max_revision" -> BatchConstraint.SubjectMaxRevision(
                    subject!!, revision!!
                )
                "subject_revision_range" -> BatchConstraint.SubjectRevisionRange(
                    subject!!, min!!, max!!
                ).getOrElse {
                    BatchConstraint.SubjectRevisionRange(
                        subject, max, min
                    ).getOrNull()!!
                }

                "subject_min_revision_on_stream" -> BatchConstraint.SubjectMinRevisionOnStream(
                    stream!!, subject!!, revision!!
                )
                "subject_max_revision_on_stream" -> BatchConstraint.SubjectMaxRevisionOnStream(
                    stream!!, subject!!, revision!!
                )
                "subject_stream_revision_range" -> BatchConstraint.SubjectStreamRevisionRange(
                    stream!!, subject!!, min!!, max!!
                ).getOrElse {
                    BatchConstraint.SubjectStreamRevisionRange(
                        stream, subject, max, min
                    ).getOrNull()!!
                }

                else -> throw SerdeException("error deserializing type: $constraintType, context: $context")
            }
        }
    }
}

@Serdeable
data class BatchInput(
    var database: String,
    var events: List<CloudEvent>,
    var constraints: List<BatchConstraint>
) {
    fun toBatchProposal() = BatchProposal(
        events.toNonEmptyListOrNull()!!,
        constraints
    )
}

sealed interface TransactionResultOutput {
    val latency: Long

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TransactionResultOutput::class.java)

        fun fromBatchInput(
            input: BatchInput,
            conn: Connection,
            start: Instant
        ): TransactionResultOutput =
            conn.transact(input.toBatchProposal())
                .handle { batch, ex ->
                    val latency = Duration.between(start, Instant.now()).toNanos()
                    if (ex != null) {
                        LOGGER.warn("failure latency nanos: $latency")
                        Failure(input, ex, latency)
                    } else {
                        LOGGER.info("success latency nanos: $latency")
                        Success(batch, latency)
                    }
                }.get()
    }

    @Serdeable
    data class Failure internal constructor(
        val message: String,
        val events: List<CloudEvent>,
        val constraints: List<BatchConstraint>,
        override val latency: Long
    ): TransactionResultOutput {
        companion object {
            operator fun invoke(input: BatchInput, ex: Throwable, latency: Long) = Failure(
                ex.message!!,
                input.events,
                input.constraints,
                latency
            )
        }
    }

    @Serdeable
    data class Success internal constructor(
        val database: String,
        val basis: Revision,
        val events: List<CloudEvent>,
        val timestamp: Instant,
        override val latency: Long
    ): TransactionResultOutput {
        companion object {
            operator fun invoke(batch: Batch, latency: Long) = Success(
                batch.database,
                batch.basis,
                batch.events.map { it.event },
                batch.timestamp,
                latency
            )
        }
    }
}

@Singleton
class TransactionResultOutputSerde: io.micronaut.serde.Serde<TransactionResultOutput> {
    override fun serialize(
        encoder: Encoder,
        context: io.micronaut.serde.Serializer.EncoderContext,
        type: Argument<out TransactionResultOutput>,
        value: TransactionResultOutput
    ) {
        encoder.encodeObject(type).use { result ->
            result.encodeKey("latency")
            result.encodeLong(value.latency)
            result.encodeKey("type")
            when(value) {
                is TransactionResultOutput.Failure -> {
                    result.encodeString("failure")

                    result.encodeKey("message")
                    result.encodeString(value.message)

                    result.encodeKey("events")
                    val cloudEventType = Argument.of(CloudEvent::class.java)
                    val cloudEventSerializer = context.findSerializer(cloudEventType)
                    result.encodeArray(cloudEventType).use {
                        for (event in value.events) {
                            cloudEventSerializer.serialize(it, context, cloudEventType, event)
                        }
                    }

                    result.encodeKey("constraints")
                    val batchConstraintType = Argument.of(BatchConstraint::class.java)
                    val batchConstraintSerializer = context.findSerializer(batchConstraintType)
                    result.encodeArray(batchConstraintType).use {
                        for (constraint in value.constraints) {
                            batchConstraintSerializer.serialize(
                                it,
                                context,
                                batchConstraintType,
                                constraint
                            )
                        }
                    }
                }
                is TransactionResultOutput.Success -> {
                    result.encodeString("success")

                    result.encodeKey("database")
                    result.encodeString(value.database)

                    result.encodeKey("basis")
                    result.encodeLong(value.basis.toLong())

                    result.encodeKey("events")
                    val cloudEventType = Argument.of(CloudEvent::class.java)
                    val cloudEventSerializer = context.findSerializer(cloudEventType)
                    result.encodeArray(cloudEventType).use {
                        for (event in value.events) {
                            cloudEventSerializer.serialize(it, context, cloudEventType, event)
                        }
                    }

                    result.encodeKey("timestamp")
                    result.encodeString(value.timestamp.toString())
                }
            }
        }
    }

    override fun deserialize(
        decoder: Decoder,
        context: io.micronaut.serde.Deserializer.DecoderContext,
        argument: Argument<in TransactionResultOutput>
    ): TransactionResultOutput = decoder.decodeObject(argument).use { inner ->
        var type: String? = null
        var message: String? = null
        val constraints = mutableListOf<BatchConstraint>()
        val events = mutableListOf<CloudEvent>()
        var database: String? = null
        var basis: Long? = null
        var timestamp: String? = null
        var latency: Long? = null
        var key = inner.decodeKey()
        while (key != null) {
            when(key) {
                "type" -> type = inner.decodeString()
                "message" -> message = inner.decodeString()
                "constraints" -> {
                    val batchConstraintType = Argument.of(BatchConstraint::class.java)
                    val batchConstraintDeserializer = context.findDeserializer(batchConstraintType)
                    inner.decodeArray(batchConstraintType).use {
                        while(it.hasNextArrayValue()) {
                            constraints.add(
                                batchConstraintDeserializer.deserialize(
                                    it,
                                    context,
                                    batchConstraintType
                                )
                            )
                        }
                    }
                }
                "events" -> {
                    val cloudEventType = Argument.of(CloudEvent::class.java)
                    val cloudEventDeserializer = context.findDeserializer(cloudEventType)
                    inner.decodeArray(cloudEventType).use {
                        while(it.hasNextArrayValue()) {
                            events.add(
                                cloudEventDeserializer.deserialize(
                                    it,
                                    context,
                                    cloudEventType
                                )
                            )
                        }
                    }
                }
                "database" -> database = inner.decodeString()
                "basis" -> basis = inner.decodeLong()
                "timestamp" -> timestamp = inner.decodeString()
                "latency" -> latency = inner.decodeLong()
                else -> throw SerdeException("Unexpected key '$key' found when deserializing TransactionResult")
            }
            key = inner.decodeKey()
        }
        when(type) {
            "success" -> TransactionResultOutput.Success(
                database!!, basis!!.toULong(), events, Instant.parse(timestamp), latency!!
            )
            "failure" -> TransactionResultOutput.Failure(message!!, events, constraints, latency!!)
            else -> throw SerdeException("Unexpected variant '$type' found when deserializing TransactionResult")
        }
    }
}

class MicronautSerializer<T>(
    private val objectMapper: ObjectMapper = ObjectMapper.getDefault()
): Serializer<T> {
    override fun serialize(topic: String, data: T): ByteArray = try {
        objectMapper.writeValueAsBytes(data)
    } catch (e: Exception) {
        throw SerializationException("Error serializing BatchInput", e)
    }
}

class MicronautDeserializer<T>(
    private val objectMapper: ObjectMapper = ObjectMapper.getDefault(),
    type: Class<T>
): Deserializer<T> {
    private val argument = Argument.of(type)

    override fun deserialize(topic: String, data: ByteArray): T = try {
        objectMapper.readValue(data, argument)
    } catch (e: IOException) {
        throw SerializationException("Error deserializing: $argument via Micronaut", e)
    }
}

class MicronautSerde<T>(
    private val objectMapper: ObjectMapper = ObjectMapper.getDefault(),
    private val type: Class<T>
): Serde<T> {
    override fun serializer() = MicronautSerializer<T>(objectMapper)
    override fun deserializer() = MicronautDeserializer<T>(objectMapper, type)
}
