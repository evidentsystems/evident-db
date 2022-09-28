package com.evidentdb.transactor

import com.evidentdb.domain.*
import com.evidentdb.kafka.*

class KafkaStreamsCommandHandler: CommandHandler {
    override lateinit var databaseReadModel: DatabaseReadModel
    override lateinit var streamSummaryReadModel: StreamSummaryReadModel
    override lateinit var batchSummaryReadModel: BatchSummaryReadModel

    fun init(
        databaseStore: DatabaseKeyValueStore,
        streamStore: StreamKeyValueStore,
        batchStore: BatchKeyValueStore,
    ) {
        this.databaseReadModel = DatabaseStore(databaseStore)
        this.streamSummaryReadModel = StreamSummaryStore(streamStore)
        this.batchSummaryReadModel = BatchSummaryStore(batchStore)
    }
}
