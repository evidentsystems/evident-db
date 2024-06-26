package com.evidentdb.kafka

import com.evidentdb.application.StreamKey
import com.evidentdb.cloudevents.CommandIdExtension
import com.evidentdb.domain_model.*
import com.evidentdb.domain_model.DatabaseName
import com.evidentdb.domain_model.databaseNameFromUri
import com.evidentdb.domain_model.databaseUri
import com.evidentdb.dto.protobuf.*
import com.evidentdb.event_model.*
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.core.message.Encoding
import io.cloudevents.kafka.CloudEventDeserializer
import io.cloudevents.kafka.CloudEventSerializer
import io.cloudevents.kafka.impl.KafkaHeaders
import io.cloudevents.protobuf.ProtoCloudEventData
import io.cloudevents.protobuf.ProtobufFormat
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.*

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
}

abstract class EvidentDbDeserializer<T>: Deserializer<T> {
    private val deserializer = CloudEventDeserializer()

    abstract fun fromCloudEvent(cloudEvent: CloudEvent): T

    override fun deserialize(topic: String?, data: ByteArray?): T =
        deserialize(
            topic,
            RecordHeaders(STRUCTURED_HEADERS),
            data
        )

    override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): T =
        fromCloudEvent(deserializer.deserialize(topic, headers, data))

    companion object {
        val STRUCTURED_HEADERS = listOf<Header>(
            RecordHeader(
                KafkaHeaders.CONTENT_TYPE,
                ProtobufFormat().serializedContentType().toByteArray()
            )
        )

    }
}

class CommandEnvelopeSerde:
    Serdes.WrapperSerde<EvidentDbCommand>(CommandEnvelopeSerializer(), CommandEnvelopeDeserializer()) {

    class CommandEnvelopeSerializer: EvidentDbSerializer<EvidentDbCommand>() {
        override fun toCloudEvent(content: EvidentDbCommand): CloudEvent =
            CloudEventBuilder.v1()
                .withId(content.id.toString())
                .withSource(databaseUri(content.database))
                .withType(content.type)
                .withData(ProtoCloudEventData.wrap(content.data.toProto()))
                .build()
    }

    class CommandEnvelopeDeserializer: EvidentDbDeserializer<EvidentDbCommand>() {
        override fun fromCloudEvent(cloudEvent: CloudEvent): EvidentDbCommand {
            val dataBytes = cloudEvent.data!!.toBytes()
            val commandId = EnvelopeId.fromString(cloudEvent.id)
            val databaseId = databaseNameFromUri(cloudEvent.source)
            return when (cloudEvent.type.split('.').last()) {
                "CreateDatabase" -> CreateDatabase(
                    commandId,
                    databaseId,
                    databaseCreationInfoFromBytes(dataBytes),
                )
                "DeleteDatabase" -> DeleteDatabase(
                    commandId,
                    databaseId,
                    databaseDeletionInfoFromBytes(dataBytes),
                )
                "TransactBatch" -> TransactBatch(
                    commandId,
                    databaseId,
                    proposedBatchFromBytes(dataBytes),
                )
                else -> throw IllegalArgumentException("unknown command type ${cloudEvent.type}")
            }
        }
    }
}

class EventEnvelopeSerde:
    Serdes.WrapperSerde<EvidentDbEvent>(EventEnvelopeSerializer(), EventEnvelopeDeserializer()) {

    class EventEnvelopeSerializer : EvidentDbSerializer<EvidentDbEvent>() {
        override fun toCloudEvent(content: EvidentDbEvent): CloudEvent  =
            CloudEventBuilder.v1()
                .withId(content.id.toString())
                .withSource(databaseUri(content.database))
                .withType(content.type)
                .withExtension(CommandIdExtension(content.commandId))
                .withData(ProtoCloudEventData.wrap(content.data.toProto()))
                .build()
    }

    class EventEnvelopeDeserializer: EvidentDbDeserializer<EvidentDbEvent>() {
        override fun fromCloudEvent(cloudEvent: CloudEvent): EvidentDbEvent {
            val dataBytes = cloudEvent.data!!.toBytes()
            val eventId = EnvelopeId.fromString(cloudEvent.id)
            val databaseId = databaseNameFromUri(cloudEvent.source)
            val commandId: EnvelopeId = when(val commandIdString = cloudEvent.getExtension(CommandIdExtension.COMMAND_ID)) {
                is String -> EnvelopeId.fromString(commandIdString)
                else -> throw IllegalStateException("Invalid commandid: $commandIdString parsed from cloud event: $cloudEvent")
            }
            return when (cloudEvent.type.split('.').last()) {
                "DatabaseCreated" -> DatabaseCreated(
                    eventId,
                    commandId,
                    databaseId,
                    databaseCreationResultFromBytes(dataBytes),
                )
                "DatabaseDeleted" -> DatabaseDeleted(
                    eventId,
                    commandId,
                    databaseId,
                    databaseDeletionResultFromBytes(dataBytes),
                )
                "BatchTransacted" -> BatchTransacted(
                    eventId,
                    commandId,
                    databaseId,
                    batchTransactionResultFromBytes(dataBytes),
                )

                "InvalidDatabaseNameError" -> EvidentDbCommandError(
                    eventId,
                    commandId,
                    databaseId,
                    invalidDatabaseNameErrorFromBytes(dataBytes)
                )
                "DatabaseNameAlreadyExistsError" -> EvidentDbCommandError(
                    eventId,
                    commandId,
                    databaseId,
                    databaseNameAlreadyExistsErrorFromBytes(dataBytes)
                )
                "DatabaseNotFoundError" -> EvidentDbCommandError(
                    eventId,
                    commandId,
                    databaseId,
                    databaseNotFoundErrorFromBytes(dataBytes)
                )
                "NoEventsProvidedError" -> EvidentDbCommandError(
                    eventId,
                    commandId,
                    databaseId,
                    noEventsProvidedErrorFromBytes(dataBytes)
                )
                "InvalidEventsError" -> EvidentDbCommandError(
                    eventId,
                    commandId,
                    databaseId,
                    invalidEventsErrorFromBytes(dataBytes)
                )
                "DuplicateBatchError" -> EvidentDbCommandError(
                    eventId,
                    commandId,
                    databaseId,
                    duplicateBatchErrorFromBytes(dataBytes)
                )
                "StreamStateConflictsError" -> EvidentDbCommandError(
                    eventId,
                    commandId,
                    databaseId,
                    streamStateConflictsErrorFromBytes(dataBytes)
                )
                "DatabaseTopicCreationError" -> EvidentDbCommandError(
                    eventId,
                    commandId,
                    databaseId,
                    databaseTopicCreationErrorFromBytes(dataBytes)
                )
                "DatabaseTopicDeletionError" -> EvidentDbCommandError(
                    eventId,
                    commandId,
                    databaseId,
                    databaseTopicDeletionErrorFromBytes(dataBytes)
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

class DatabaseSummarySerde: Serdes.WrapperSerde<DatabaseSummary>(DatabaseSummarySerializer(), DatabaseSummaryDeserializer()) {
    class DatabaseSummarySerializer : Serializer<DatabaseSummary> {
        override fun serialize(topic: String?, data: DatabaseSummary?): ByteArray? =
            data?.toByteArray()
    }

    class DatabaseSummaryDeserializer : Deserializer<DatabaseSummary> {
        override fun deserialize(topic: String?, data: ByteArray?): DatabaseSummary? =
            data?.let { databaseSummaryFromBytes(it) }
    }
}

class EventSerde: Serdes.WrapperSerde<CloudEvent>(EventSerializer(), EventDeserializer()) {
    class EventSerializer: EvidentDbSerializer<CloudEvent>() {
        override fun toCloudEvent(content: CloudEvent): CloudEvent =
            content
    }

    class EventDeserializer : EvidentDbDeserializer<CloudEvent>() {
        override fun fromCloudEvent(cloudEvent: CloudEvent): CloudEvent =
            cloudEvent
    }

    companion object {
        fun structuredConfig() =
            mutableMapOf(
                CloudEventSerializer.ENCODING_CONFIG to Encoding.STRUCTURED,
                CloudEventSerializer.EVENT_FORMAT_CONFIG to ProtobufFormat()
            )
    }
}

class BatchSummarySerde: Serdes.WrapperSerde<LogBatch>(
    BatchSummarySerializer(),
    BatchSummaryDeserializer(),
) {
    class BatchSummarySerializer : Serializer<LogBatch> {
        override fun serialize(topic: String?, data: LogBatch?): ByteArray? =
            data?.toByteArray()
    }

    class BatchSummaryDeserializer : Deserializer<LogBatch> {
        override fun deserialize(topic: String?, data: ByteArray?): LogBatch? =
            data?.let { batchSummaryFromBytes(it) }
    }
}

class StreamKeySerde: Serdes.WrapperSerde<StreamKey>(
    StreamKeySerializer(),
    StreamKeyDeserializer(),
) {
    class StreamKeySerializer : Serializer<StreamKey> {
        override fun serialize(topic: String?, data: StreamKey?): ByteArray? =
            data?.toBytes()
    }

    class StreamKeyDeserializer : Deserializer<StreamKey> {
        override fun deserialize(topic: String?, data: ByteArray?): StreamKey? =
            data?.let { StreamKey.fromBytes(it) }
    }
}