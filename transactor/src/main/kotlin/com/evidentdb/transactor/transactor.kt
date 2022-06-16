package com.evidentdb.transactor

import com.evidentdb.domain.*
import com.evidentdb.kafka.*

class KafkaStreamsCommandHandler: CommandHandler {
    override lateinit var databaseReadModel: DatabaseReadModel
    override lateinit var streamReadModel: StreamReadModel

    fun init(
        databaseStore: DatabaseKeyValueStore,
        databaseNameLookupStore: DatabaseNameLookupStore,
        streamStore: StreamKeyValueStore
    ) {
        this.databaseReadModel = DatabaseStore(databaseStore, databaseNameLookupStore)
        this.streamReadModel = StreamStore(streamStore)
    }
}
