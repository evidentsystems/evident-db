package com.evidentdb.kafka

import com.evidentdb.domain.*
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
import java.time.ZoneOffset

// TODO: extract to/from CloudEvent functions into a separate cloudevent adapter
// TODO: harmonize/DRY cloudevent adapter with protobuf adapter

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

class CommandEnvelopeSerde:
    Serdes.WrapperSerde<CommandEnvelope>(CommandEnvelopeSerializer(), CommandEnvelopeDeserializer()) {

    class CommandEnvelopeSerializer: EvidentDbSerializer<CommandEnvelope>() {
        override fun toCloudEvent(content: CommandEnvelope): CloudEvent =
            CloudEventBuilder.v1()
                .withId(content.id.toString())
                .withSource(databaseUri(content.databaseId))
                .withType(content.type)
                .withData(ProtoCloudEventData.wrap(content.data.toProto()))
                .build()
    }

    class CommandEnvelopeDeserializer: EvidentDbDeserializer<CommandEnvelope>() {
        // TODO: extract to domain?
        override fun fromCloudEvent(cloudEvent: CloudEvent): CommandEnvelope {
            val dataBytes = cloudEvent.data!!.toBytes()
            val commandId = CommandId.fromString(cloudEvent.id)
            val databaseId = databaseIdFromUri(cloudEvent.source)
            return when (cloudEvent.type.split('.').last()) {
                "CreateDatabase" -> CreateDatabase(
                    commandId,
                    databaseId,
                    databaseCreationInfoFromBytes(dataBytes),
                )
                "RenameDatabase" -> RenameDatabase(
                    commandId,
                    databaseId,
                    databaseRenameInfoFromBytes(dataBytes),
                )
                "DeleteDatabase" -> DeleteDatabase(
                    commandId,
                    databaseId,
                    databaseDeletionInfoFromBytes(dataBytes),
                )
                "TransactBatch" -> TransactBatch(
                    commandId,
                    databaseId,
                    proposedBatchFromBytes(dataBytes)
                )
                else -> throw IllegalArgumentException("unknown command type ${cloudEvent.type}")
            }
        }
    }
}

class EventEnvelopeSerde:
    Serdes.WrapperSerde<EventEnvelope>(EventEnvelopeSerializer(), EventEnvelopeDeserializer()) {

    class EventEnvelopeSerializer : EvidentDbSerializer<EventEnvelope>() {
        override fun toCloudEvent(content: EventEnvelope): CloudEvent  =
            CloudEventBuilder.v1()
                .withId(content.id.toString())
                .withSource(databaseUri(content.databaseId))
                .withType(content.type)
                // TODO: commandId (via extension)
                .withData(ProtoCloudEventData.wrap(content.data.toProto()))
                .build()
    }

    class EventEnvelopeDeserializer: EvidentDbDeserializer<EventEnvelope>() {
        override fun fromCloudEvent(cloudEvent: CloudEvent): EventEnvelope {
            val dataBytes = cloudEvent.data!!.toBytes()
            val eventId = EventId.fromString(cloudEvent.id)
            val databaseId = databaseIdFromUri(cloudEvent.source)
            return when (cloudEvent.type.split('.').last()) {
                "DatabaseCreated" -> DatabaseCreated(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    databaseCreatedInfoFromBytes(dataBytes)
                )
                "DatabaseRenamed" -> DatabaseRenamed(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    databaseRenameInfoFromBytes(dataBytes)
                )
                "DatabaseDeleted" -> DatabaseDeleted(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    databaseDeletionInfoFromBytes(dataBytes)
                )
                "BatchTransacted" -> BatchTransacted(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    batchFromBytes(dataBytes)
                )

                "InvalidDatabaseNameError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    invalidDatabaseNameErrorFromBytes(dataBytes)
                )
                "DatabaseNameAlreadyExistsError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    databaseNameAlreadyExistsErrorFromBytes(dataBytes)
                )
                "DatabaseNotFoundError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    databaseNotFoundErrorFromBytes(dataBytes)
                )
                "NoEventsProvidedError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    noEventsProvidedErrorFromBytes(dataBytes)
                )
                "InvalidEventsError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    invalidEventsErrorFromBytes(dataBytes)
                )
                "StreamStateConflictsError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    streamStateConflictsErrorFromBytes(dataBytes)
                )
                else -> throw IllegalArgumentException("unknown event type ${cloudEvent.type}")
            }
        }
    }
}

class DatabaseSerde: Serdes.WrapperSerde<Database>(DatabaseSerializer(), DatabaseDeserializer()) {
    class DatabaseSerializer : Serializer<Database> {
        override fun serialize(topic: String?, data: Database?): ByteArray =
            data?.toByteArray() ?: byteArrayOf()
    }

    class DatabaseDeserializer : Deserializer<Database> {
        override fun deserialize(topic: String?, data: ByteArray?): Database? =
            data?.let { databaseFromBytes(it) }
    }
}

class EventSerde: Serdes.WrapperSerde<Event>(EventSerializer(), EventDeserializer()) {
    class EventSerializer : EvidentDbSerializer<Event>() {
        override fun toCloudEvent(content: Event): CloudEvent {
            val builder = CloudEventBuilder.v1()
                .withId(content.id.toString())
                .withSource(databaseUri(content.databaseId))
                .withType(content.type)
                .withSubject(content.stream)
            if (content.data != null)
                builder.withData(content.data)
            else
                builder.withoutData()
            for ((k, v) in content.attributes)
                when (v) {
                    is EventAttributeValue.BooleanValue ->
                        builder.withContextAttribute(k, v.value)
                    is EventAttributeValue.ByteArrayValue ->
                        builder.withContextAttribute(k, v.value)
                    is EventAttributeValue.IntegerValue ->
                        builder.withContextAttribute(k, v.value)
                    is EventAttributeValue.StringValue ->
                        builder.withContextAttribute(k, v.value)
                    is EventAttributeValue.UriRefValue ->
                        builder.withContextAttribute(k, v.value)
                    is EventAttributeValue.TimestampValue ->
                        builder.withContextAttribute(k, v.value.atOffset(ZoneOffset.UTC))
                    is EventAttributeValue.UriValue ->
                        builder.withContextAttribute(k, v.value)
                }
            return builder.build()
        }
    }

    class EventDeserializer : EvidentDbDeserializer<Event>() {
        override fun fromCloudEvent(cloudEvent: CloudEvent): Event =
            Event(
                EventId.fromString(cloudEvent.id),
                databaseIdFromUri(cloudEvent.source),
                cloudEvent.type,
                cloudEvent.attributeNames.fold(mutableMapOf()) { acc, k ->
                    cloudEvent.getAttribute(k)?.let {
                        acc[k] = EventAttributeValue.create(it)
                    }
                    acc
                },
                cloudEvent.data?.toBytes(),
                cloudEvent.subject
            )
    }
}
