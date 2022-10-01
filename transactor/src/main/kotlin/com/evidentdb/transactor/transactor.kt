package com.evidentdb.transactor

import com.evidentdb.domain.BatchSummaryReadModel
import com.evidentdb.domain.CommandHandler
import com.evidentdb.domain.DatabaseReadModel
import com.evidentdb.kafka.*

class KafkaStreamsCommandHandler: CommandHandler {
    override lateinit var databaseReadModel: DatabaseReadModel
    override lateinit var batchSummaryReadModel: BatchSummaryReadModel

    fun init(
        databaseStore: DatabaseReadOnlyKeyValueStore,
        logStore: DatabaseLogReadOnlyKeyValueStore,
        batchStore: BatchIndexReadOnlyKeyValueStore,
    ) {
        this.databaseReadModel = DatabaseReadModelStore(
            databaseStore,
            logStore,
        )
        this.batchSummaryReadModel = BatchSummaryReadOnlyStore(batchStore, logStore)
    }
}
