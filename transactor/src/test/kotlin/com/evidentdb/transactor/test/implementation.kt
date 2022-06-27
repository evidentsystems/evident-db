package com.evidentdb.transactor.test

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.evidentdb.domain.*
import com.evidentdb.kafka.CommandEnvelopeSerde
import com.evidentdb.kafka.DatabaseStore
import com.evidentdb.kafka.EventEnvelopeSerde
import com.evidentdb.transactor.KafkaStreamsCommandHandler
import com.evidentdb.transactor.Topology
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.TopologyTestDriver

class TopologyTestDriverCommandBroker(
    driver: TopologyTestDriver
): CommandBroker {
    private val inputTopic =
        driver.createInputTopic(
            INTERNAL_COMMAND_TOPIC,
            Serdes.UUID().serializer(),
            CommandEnvelopeSerde.CommandEnvelopeSerializer()
        )
    private val outputTopic =
        driver.createOutputTopic(
            INTERNAL_EVENTS_TOPIC,
            Serdes.UUID().deserializer(),
            EventEnvelopeSerde.EventEnvelopeDeserializer()
        )

    override suspend fun createDatabase(command: CreateDatabase): Either<DatabaseCreationError, DatabaseCreated> {
        inputTopic.pipeInput(command.id, command)
        val eventKV = outputTopic.readKeyValue()
        return when(val event = eventKV.value) {
            is DatabaseCreated -> event.right()
            is DatabaseCreationError -> event.left()
            else -> throw IllegalStateException("Invalid event returned from createDatabase")
        }
    }

    override suspend fun renameDatabase(command: RenameDatabase): Either<DatabaseRenameError, DatabaseRenamed>  {
        inputTopic.pipeInput(command.id, command)
        val eventKV = outputTopic.readKeyValue()
        return when(val event = eventKV.value) {
            is DatabaseRenamed -> event.right()
            is DatabaseRenameError -> event.left()
            else -> throw IllegalStateException("Invalid event returned from renameDatabase")
        }
    }


    override suspend fun deleteDatabase(command: DeleteDatabase): Either<DatabaseDeletionError, DatabaseDeleted> {
        inputTopic.pipeInput(command.id, command)
        val eventKV = outputTopic.readKeyValue()
        return when(val event = eventKV.value) {
            is DatabaseDeleted -> event.right()
            is DatabaseDeletionError -> event.left()
            else -> throw IllegalStateException("Invalid event returned from deleteDatabase")
        }
    }


    override suspend fun transactBatch(command: TransactBatch): Either<BatchTransactionError, BatchTransacted> {
        inputTopic.pipeInput(command.id, command)
        val eventKV = outputTopic.readKeyValue()
        return when(val event = eventKV.value) {
            is BatchTransacted -> event.right()
            is BatchTransactionError -> event.left()
            else -> throw IllegalStateException("Invalid event returned from transactBatch")
        }
    }

}

class TopologyTestDriverService(
    driver: TopologyTestDriver
): Service {
    override val databaseReadModel = DatabaseStore(
        driver.getKeyValueStore(Topology.DATABASE_STORE),
        driver.getKeyValueStore(Topology.DATABASE_NAME_LOOKUP),
    )
    override val commandBroker = TopologyTestDriverCommandBroker(driver)
}