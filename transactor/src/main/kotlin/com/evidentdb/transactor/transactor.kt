package com.evidentdb.transactor

import com.evidentdb.domain.*
import com.evidentdb.kafka.*

class KafkaStreamsCommandHandler: CommandHandler {
    override lateinit var databaseReadModel: DatabaseReadModel
    override lateinit var streamReadModel: StreamReadModel

    fun init(
        databaseStore: DatabaseKeyValueStore,
        databaseNameStore: DatabaseNameStore,
        streamStore: StreamKeyValueStore
    ) {
        this.databaseReadModel = DatabaseStore(databaseStore, databaseNameStore)
        this.streamReadModel = StreamStore(streamStore)
    }
}
