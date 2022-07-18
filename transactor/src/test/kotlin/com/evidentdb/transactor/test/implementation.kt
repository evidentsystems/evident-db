package com.evidentdb.transactor.test

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.evidentdb.domain.*
import com.evidentdb.kafka.CommandEnvelopeSerde
import com.evidentdb.kafka.DatabaseStore
import com.evidentdb.kafka.EventEnvelopeSerde
import com.evidentdb.transactor.Topology
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.TopologyTestDriver

const val INTERNAL_COMMAND_TOPIC = "internal-commands"
const val INTERNAL_EVENTS_TOPIC = "internal-events"
const val DATABASES_TOPIC = "databases"
const val DATABASE_NAMES_TOPIC = "database-names"
const val BATCHES_TOPIC = "batches"
const val STREAMS_TOPIC = "streams"
const val EVENTS_TOPIC = "events"

fun topology() =
    Topology.build(
        INTERNAL_COMMAND_TOPIC,
        INTERNAL_EVENTS_TOPIC,
        DATABASES_TOPIC,
        DATABASE_NAMES_TOPIC,
        BATCHES_TOPIC,
        STREAMS_TOPIC,
        EVENTS_TOPIC,
    )

fun driver(): TopologyTestDriver =
    TopologyTestDriver(topology())

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
            is ErrorEnvelope -> when(val data = event.data) {
                is DatabaseCreationError -> data.left()
                else -> throw IllegalStateException("Invalid error returned from renameDatabase $event")
            }
            else -> throw IllegalStateException("Invalid event returned from renameDatabase $event")
        }
    }

    override suspend fun renameDatabase(command: RenameDatabase): Either<DatabaseRenameError, DatabaseRenamed>  {
        inputTopic.pipeInput(command.id, command)
        val eventKV = outputTopic.readKeyValue()
        return when(val event = eventKV.value) {
            is DatabaseRenamed -> event.right()
            is ErrorEnvelope -> when(val data = event.data) {
                is DatabaseRenameError -> data.left()
                else -> throw IllegalStateException("Invalid error returned from renameDatabase $event")
            }
            else -> throw IllegalStateException("Invalid event returned from renameDatabase $event")
        }
    }


    override suspend fun deleteDatabase(command: DeleteDatabase): Either<DatabaseDeletionError, DatabaseDeleted> {
        inputTopic.pipeInput(command.id, command)
        val eventKV = outputTopic.readKeyValue()
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
        val eventKV = outputTopic.readKeyValue()
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
        driver.getKeyValueStore(Topology.DATABASE_STORE),
        driver.getKeyValueStore(Topology.DATABASE_NAME_LOOKUP),
    )
    override val commandBroker = TopologyTestDriverCommandBroker(driver)
}