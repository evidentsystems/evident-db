package com.evidentdb.kafka

import com.evidentdb.domain.*
import com.evidentdb.dto.*
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.core.message.Encoding
import io.cloudevents.kafka.CloudEventDeserializer
import io.cloudevents.kafka.CloudEventSerializer
import io.cloudevents.protobuf.ProtoCloudEventData
import io.cloudevents.protobuf.ProtobufFormat
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.*

class ListSerde<Inner> : Serdes.WrapperSerde<List<Inner>> {
    constructor() : super(ListSerializer<Inner>(), ListDeserializer<Inner>())
    constructor(serializer: ListSerializer<Inner>, deserializer: ListDeserializer<Inner>) : super(serializer, deserializer)
}

fun <Inner> listSerde(serde: Serde<Inner>) = ListSerde(
    ListSerializer(serde.serializer()),
    ListDeserializer(ArrayList<Inner>().javaClass, serde.deserializer())
)

abstract class EvidentDbSerializer<T>: Serializer<T> {
    private val serializer = CloudEventSerializer()

    abstract fun toCloudEvent(content: T): CloudEvent

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        super.configure(configs, isKey)
        serializer.configure(configs, isKey)
    }

    // When calling the two-arity, we assume it's for Stores, not Topics
    override fun serialize(topic: String?, data: T): ByteArray? =
        serialize(topic, RecordHeaders(), data)

    // When calling the three-arity, we assume it's for Topics, not Stores
    override fun serialize(topic: String?, headers: Headers?, data: T): ByteArray? =
        serializer.serialize(topic, headers, toCloudEvent(data))

    companion object {
        fun structuredConfig() =
            mutableMapOf(
                CloudEventSerializer.ENCODING_CONFIG to Encoding.STRUCTURED,
                CloudEventSerializer.EVENT_FORMAT_CONFIG to ProtobufFormat()
            )
    }
}

abstract class EvidentDbDeserializer<T>: Deserializer<T> {
    private val deserializer = CloudEventDeserializer()

    abstract fun fromCloudEvent(cloudEvent: CloudEvent): T

    override fun deserialize(topic: String?, data: ByteArray?): T =
        deserialize(topic, null, data)

    override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): T =
        fromCloudEvent(deserializer.deserialize(topic, headers, data))
}

class EventEnvelopeSerde:
    Serdes.WrapperSerde<EventEnvelope>(EventEnvelopeSerializer(), EventEnvelopeDeserializer()) {

    class EventEnvelopeSerializer : EvidentDbSerializer<EventEnvelope>() {
        override fun toCloudEvent(content: EventEnvelope): CloudEvent  =
            CloudEventBuilder.v1()
                .withId(content.id.toString())
                .withSource(databaseUri(content.database))
                .withType(content.type)
                .withData(ProtoCloudEventData.wrap(content.data.toProto()))
                .build()
    }

    class EventEnvelopeDeserializer: EvidentDbDeserializer<EventEnvelope>() {
        override fun fromCloudEvent(cloudEvent: CloudEvent): EventEnvelope {
            val dataBytes = cloudEvent.data!!.toBytes()
            val eventId = EventId.fromString(cloudEvent.id)
            val databaseId = databaseNameFromUri(cloudEvent.source)
            return when (cloudEvent.type.split('.').last()) {
                "DatabaseCreated" -> DatabaseCreated(
                    eventId,
                    databaseCreationInfoFromBytes(dataBytes),
                )
                "DatabaseDeleted" -> DatabaseDeleted(
                    eventId,
                    databaseDeletionInfoFromBytes(dataBytes),
                )
                "BatchTransacted" -> BatchTransacted(
                    eventId,
                    batchFromBytes(dataBytes),
                )

                "InvalidDatabaseNameError" -> ErrorEnvelope(
                    eventId,
                    databaseId,
                    invalidDatabaseNameErrorFromBytes(dataBytes)
                )
                "DatabaseNameAlreadyExistsError" -> ErrorEnvelope(
                    eventId,
                    databaseId,
                    databaseNameAlreadyExistsErrorFromBytes(dataBytes)
                )
                "DatabaseNotFoundError" -> ErrorEnvelope(
                    eventId,
                    databaseId,
                    databaseNotFoundErrorFromBytes(dataBytes)
                )
                "NoEventsProvidedError" -> ErrorEnvelope(
                    eventId,
                    databaseId,
                    noEventsProvidedErrorFromBytes(dataBytes)
                )
                "InvalidEventsError" -> ErrorEnvelope(
                    eventId,
                    databaseId,
                    invalidEventsErrorFromBytes(dataBytes)
                )
                "StreamStateConflictsError" -> ErrorEnvelope(
                    eventId,
                    databaseId,
                    streamStateConflictsErrorFromBytes(dataBytes)
                )
                else -> throw IllegalArgumentException("unknown event type ${cloudEvent.type}")
            }
        }
    }
}

class DatabaseNameSerde: Serdes.WrapperSerde<DatabaseName>(DatabaseNameSerializer(), DatabaseNameDeserializer()) {
    companion object {
        val stringSerializer = StringSerializer()
        val stringDeserializer = StringDeserializer()
    }

    class DatabaseNameSerializer: Serializer<DatabaseName> {
        override fun serialize(topic: String?, data: DatabaseName?): ByteArray =
            stringSerializer.serialize(topic, data?.value)
    }

    class DatabaseNameDeserializer: Deserializer<DatabaseName> {
        override fun deserialize(topic: String?, data: ByteArray?): DatabaseName =
            DatabaseName.build(stringDeserializer.deserialize(topic, data))

    }
}

class DatabaseSerde: Serdes.WrapperSerde<Database>(DatabaseSerializer(), DatabaseDeserializer()) {
    class DatabaseSerializer : Serializer<Database> {
        override fun serialize(topic: String?, data: Database?): ByteArray? =
            data?.toByteArray()
    }

    class DatabaseDeserializer : Deserializer<Database> {
        override fun deserialize(topic: String?, data: ByteArray?): Database? =
            data?.let { databaseFromBytes(it) }
    }
}

class EventSerde: Serdes.WrapperSerde<Event>(EventSerializer(), EventDeserializer()) {
    class EventSerializer : Serializer<Event> {
        override fun serialize(topic: String?, data: Event?): ByteArray? =
            data?.toByteArray()
    }

    class EventDeserializer : Deserializer<Event> {
        override fun deserialize(topic: String?, data: ByteArray?): Event? =
            data?.let { eventFromBytes(it) }
    }
}
