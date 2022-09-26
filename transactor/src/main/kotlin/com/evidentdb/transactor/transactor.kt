package com.evidentdb.transactor

import com.evidentdb.domain.*
import com.evidentdb.kafka.*

class KafkaStreamsCommandHandler: CommandHandler {
    override lateinit var databaseReadModel: DatabaseReadModel
    override lateinit var streamReadModel: StreamReadModel
    override lateinit var batchReadModel: BatchReadModel

    fun init(
        databaseStore: DatabaseKeyValueStore,
        streamStore: StreamKeyValueStore,
        batchStore: BatchKeyValueStore,
    ) {
        this.databaseReadModel = DatabaseStore(databaseStore)
        this.streamReadModel = StreamStore(streamStore)
        this.batchReadModel = BatchStore(batchStore)
    }
}
