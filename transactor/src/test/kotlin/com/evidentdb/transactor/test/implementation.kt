package com.evidentdb.transactor.test

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.evidentdb.domain.*
import com.evidentdb.kafka.DatabaseStore
import com.evidentdb.kafka.EventEnvelopeSerde
import com.evidentdb.transactor.TransactorTopology
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.TopologyTestDriver

const val LOG_TOPIC = "evidentdb-log"

fun topology() =
    TransactorTopology.build(LOG_TOPIC)

fun driver(): TopologyTestDriver =
    TopologyTestDriver(topology())

class TopologyTestDriverCommandManager(
    driver: TopologyTestDriver
): CommandManager {
    private val inputTopic =
        driver.createInputTopic(
            LOG_TOPIC,
            Serdes.UUID().deserializer(),
            EventEnvelopeSerde.EventEnvelopeDeserializer()
        )

    override suspend fun createDatabase(command: CreateDatabase): Either<DatabaseCreationError, DatabaseCreated> {
        inputTopic.pipeInput(command.id, command)
        val eventKV = inputTopic.readKeyValue()
        return when(val event = eventKV.value) {
            is DatabaseCreated -> event.right()
            is ErrorEnvelope -> when(val data = event.data) {
                is DatabaseCreationError -> data.left()
                else -> throw IllegalStateException("Invalid error returned from renameDatabase $event")
            }
            else -> throw IllegalStateException("Invalid event returned from renameDatabase $event")
        }
    }

    override suspend fun deleteDatabase(command: DeleteDatabase): Either<DatabaseDeletionError, DatabaseDeleted> {
        inputTopic.pipeInput(command.id, command)
        val eventKV = inputTopic.readKeyValue()
        return when(val event = eventKV.value) {
            is DatabaseDeleted -> event.right()
            is ErrorEnvelope -> when(val data = event.data) {
                is DatabaseDeletionError -> data.left()
                else -> throw IllegalStateException("Invalid error returned from renameDatabase $event")
            }
            else -> throw IllegalStateException("Invalid event returned from renameDatabase $event")
        }
    }


    override suspend fun transactBatch(command: TransactBatch): Either<BatchTransactionError, BatchTransacted> {
        inputTopic.pipeInput(command.id, command)
        val eventKV = inputTopic.readKeyValue()
        return when(val event = eventKV.value) {
            is BatchTransacted -> event.right()
            is ErrorEnvelope -> when(val data = event.data) {
                is BatchTransactionError -> data.left()
                else -> throw IllegalStateException("Invalid error returned from renameDatabase $event")
            }
            else -> throw IllegalStateException("Invalid event returned from renameDatabase $event")
        }
    }

}

class TopologyTestDriverService(
    driver: TopologyTestDriver
): Service {
    override val databaseReadModel = DatabaseStore(
        driver.getKeyValueStore(TransactorTopology.DATABASE_STORE),
    )
    override val commandManager = TopologyTestDriverCommandManager(driver)
}