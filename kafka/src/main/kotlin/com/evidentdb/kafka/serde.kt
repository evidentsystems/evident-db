package com.evidentdb.kafka

import com.evidentdb.domain.*
import com.google.protobuf.Any
import com.google.protobuf.Message
import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.kafka.CloudEventDeserializer
import io.cloudevents.kafka.CloudEventSerializer
import io.cloudevents.protobuf.ProtoCloudEventData
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import java.time.ZoneOffset

fun protoCloudEventData(message: Message): CloudEventData =
    ProtoCloudEventData.wrap(Any.pack(message))

interface EvidentDbSerializer<T>: Serializer<T> {
    val cloudEventSerializer: CloudEventSerializer
        get() = CloudEventSerializer()
    fun toCloudEvent(content: T): CloudEvent

    override fun serialize(topic: String?, data: T): ByteArray =
        throw NotImplementedError("this serializer requires the 3-arity implementation")

    override fun serialize(topic: String?, headers: Headers?, data: T): ByteArray =
        cloudEventSerializer.serialize(topic, headers, toCloudEvent(data))
}

interface EvidentDbDeserializer<T>: Deserializer<T> {
    val cloudEventDeserializer: CloudEventDeserializer
        get() = CloudEventDeserializer()
    fun fromCloudEvent(cloudEvent: CloudEvent): T

    override fun deserialize(topic: String?, data: ByteArray?): T =
        throw NotImplementedError("this deserializer requires the 3-arity implementation")

    override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): T =
        fromCloudEvent(cloudEventDeserializer.deserialize(topic, headers, data))
}

class CommandEnvelopeSerde: Serde<CommandEnvelope> {
    override fun serializer(): Serializer<CommandEnvelope> =
        CommandEnvelopeSerializer()

    override fun deserializer(): Deserializer<CommandEnvelope> =
        CommandEnvelopeDeserializer()

    class CommandEnvelopeSerializer: EvidentDbSerializer<CommandEnvelope> {
        override fun toCloudEvent(content: CommandEnvelope): CloudEvent =
            CloudEventBuilder.v1()
                .withId(content.id.toString())
                .withSource(databaseUri(content.databaseId))
                .withType(content.type)
                .withData(protoCloudEventData(content.data.toProto()))
                .build()
    }

    class CommandEnvelopeDeserializer: EvidentDbDeserializer<CommandEnvelope> {
        // TODO: extract to domain?
        override fun fromCloudEvent(cloudEvent: CloudEvent): CommandEnvelope {
            val message = (cloudEvent.data as ProtoCloudEventData).message as Any
            val commandId = CommandId.fromString(cloudEvent.id)
            val databaseId = databaseIdFromUri(cloudEvent.source)
            return when (cloudEvent.type.split('.').last()) {
                "CreateDatabase" -> CreateDatabase(
                    commandId,
                    databaseId,
                    databaseCreationInfoFromProto(message),
                )
                "RenameDatabase" -> RenameDatabase(
                    commandId,
                    databaseId,
                    databaseRenameInfoFromProto(message),
                )
                "DeleteDatabase" -> DeleteDatabase(
                    commandId,
                    databaseId,
                    databaseDeletionInfoFromProto(message),
                )
                "TransactBatch" -> TransactBatch(
                    commandId,
                    databaseId,
                    proposedBatchFromProto(message)
                )
                else -> throw IllegalArgumentException("unknown command type ${cloudEvent.type}")
            }
        }
    }
}

class EventEnvelopeSerde: Serde<EventEnvelope> {
    override fun serializer(): Serializer<EventEnvelope> =
        EventEnvelopeSerializer()

    override fun deserializer(): Deserializer<EventEnvelope> =
        EventEnvelopeDeserializer()

    class EventEnvelopeSerializer : EvidentDbSerializer<EventEnvelope> {
        override fun toCloudEvent(content: EventEnvelope): CloudEvent  =
            CloudEventBuilder.v1()
                .withId(content.id.toString())
                .withSource(databaseUri(content.databaseId))
                .withType(content.type)
                // TODO: commandId (via extension)
                .withData(protoCloudEventData(content.data.toProto()))
                .build()
    }

    class EventEnvelopeDeserializer: EvidentDbDeserializer<EventEnvelope> {
        override fun fromCloudEvent(cloudEvent: CloudEvent): EventEnvelope {
            val message = (cloudEvent.data as ProtoCloudEventData).message as Any
            val eventId = EventId.fromString(cloudEvent.id)
            val databaseId = databaseIdFromUri(cloudEvent.source)
            return when (cloudEvent.type.split('.').last()) {
                "DatabaseCreated" -> DatabaseCreated(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    databaseCreatedInfoFromProto(message)
                )
                "DatabaseRenamed" -> DatabaseRenamed(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    databaseRenameInfoFromProto(message)
                )
                "DatabaseDeleted" -> DatabaseDeleted(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    databaseDeletionInfoFromProto(message)
                )
                "BatchTransacted" -> BatchTransacted(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    batchFromProto(message)
                )

                "InvalidDatabaseNameError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    invalidDatabaseNameErrorFromProto(message)
                )
                "DatabaseNameAlreadyExistsError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    databaseNameAlreadyExistsErrorFromProto(message)
                )
                "DatabaseNotFoundError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    databaseNotFoundErrorFromProto(message)
                )
                "NoEventsProvidedError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    noEventsProvidedErrorFromProto(message)
                )
                "InvalidEventsError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    invalidEventsErrorFromProto(message)
                )
                "StreamStateConflictsError" -> ErrorEnvelope(
                    eventId,
                    eventId, // TODO: commandId
                    databaseId,
                    streamStateConflictsErrorFromProto(message)
                )
                else -> throw IllegalArgumentException("unknown event type ${cloudEvent.type}")
            }
        }
    }
}

class DatabaseSerde: Serde<Database> {
    override fun serializer(): Serializer<Database> =
        DatabaseSerializer()

    override fun deserializer(): Deserializer<Database> =
        DatabaseDeserializer()

    class DatabaseSerializer : Serializer<Database> {
        override fun serialize(topic: String?, data: Database?): ByteArray =
            data?.toProto()?.toByteArray() ?: byteArrayOf()
    }

    class DatabaseDeserializer : Deserializer<Database> {
        override fun deserialize(topic: String?, data: ByteArray?): Database? =
            data?.let { databaseFromProto(it) }
    }
}

class EventSerde: Serde<Event> {
    override fun serializer(): Serializer<Event> =
        EventSerializer()

    override fun deserializer(): Deserializer<Event> =
        EventDeserializer()

    class EventSerializer : EvidentDbSerializer<Event> {
        override fun toCloudEvent(content: Event): CloudEvent {
            val builder = CloudEventBuilder.v1()
                .withId(content.id.toString())
                .withSource(databaseUri(content.databaseId))
                .withType(content.type)
                .withSubject(content.stream)
                .withData(content.data)
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

    class EventDeserializer : EvidentDbDeserializer<Event> {
        override fun fromCloudEvent(cloudEvent: CloudEvent): Event =
            Event(
                EventId.fromString(cloudEvent.id),
                cloudEvent.type,
                cloudEvent.attributeNames.fold(mutableMapOf()) { acc, k ->
                    cloudEvent.getAttribute(k)?.let {
                        acc[k] = EventAttributeValue.create(it)
                    }
                    acc
                },
                cloudEvent.data?.toBytes(),
                databaseIdFromUri(cloudEvent.source),
                cloudEvent.subject
            )
    }
}