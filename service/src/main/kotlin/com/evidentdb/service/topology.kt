package com.evidentdb.service

import com.evidentdb.domain.*
import com.evidentdb.kafka.DatabaseSerde
import com.evidentdb.kafka.EventSerde
import com.evidentdb.kafka.EvidentDbSerializer
import com.evidentdb.kafka.listSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore

// TODO: this duplicates storage!  Consider configuring a gRPC endpoint on transactors to answer these queries:
//   https://kafka.apache.org/documentation/streams/developer-guide/interactive-queries.html#id7
object ServiceReadModelTopology {
    const val DATABASE_STORE = "DATABASE_STORE"
    const val DATABASE_NAME_LOOKUP = "DATABASE_NAME_LOOKUP"
    const val BATCH_STORE = "BATCH_STORE"
    const val STREAM_STORE = "STREAM_STORE"
    const val EVENT_STORE = "EVENT_STORE"

    fun build(
        databasesTopic: String,
        databaseNamesTopic: String,
        batchesTopic: String,
        streamsTopic: String,
        eventsTopic: String
    ): Topology {
        val builder = StreamsBuilder()

        builder.globalTable(
            databasesTopic,
            Consumed.with(Serdes.UUID(), DatabaseSerde()),
            Materialized.`as`<DatabaseId, Database, KeyValueStore<Bytes, ByteArray>>(DATABASE_STORE)
        )
        builder.globalTable(
            databaseNamesTopic,
            Consumed.with(Serdes.String(), Serdes.UUID()),
            Materialized.`as`<DatabaseName, DatabaseId, KeyValueStore<Bytes, ByteArray>>(DATABASE_NAME_LOOKUP)
        )
        builder.globalTable(
            batchesTopic,
            Consumed.with(Serdes.String(), listSerde(Serdes.UUID())),
            Materialized.`as`<BatchKey, List<EventId>, KeyValueStore<Bytes, ByteArray>>(BATCH_STORE)
        )
        builder.globalTable(
            streamsTopic,
            Consumed.with(Serdes.String(), listSerde(Serdes.UUID())),
            Materialized.`as`<StreamKey, List<EventId>, KeyValueStore<Bytes, ByteArray>>(STREAM_STORE)
        )
        // Configure structured event serialization for storage (i.e. no headers)
        val eventStoreSerde = EventSerde()
        eventStoreSerde.configure(EvidentDbSerializer.structuredConfig(), false)

        builder.globalTable(
            eventsTopic,
            Consumed.with(Serdes.UUID(), eventStoreSerde),
            Materialized.`as`<EventId, Event, KeyValueStore<Bytes, ByteArray>>(EVENT_STORE)
        )

        return builder.build()
    }
}