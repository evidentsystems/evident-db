package com.evidentdb.transactor.test

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.evidentdb.application.CommandManager
import com.evidentdb.application.CommandService
import com.evidentdb.domain_model.*
import com.evidentdb.event_model.*
import com.evidentdb.kafka.CommandEnvelopeSerde
import com.evidentdb.kafka.EventEnvelopeSerde
import com.evidentdb.transactor.TransactorTopology
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.TopologyTestDriver

const val INTERNAL_COMMAND_TOPIC = "internal-commands"
const val INTERNAL_EVENTS_TOPIC = "internal-events"

fun topology() =
    TransactorTopology.build(
        INTERNAL_COMMAND_TOPIC,
        INTERNAL_EVENTS_TOPIC,
        SimpleMeterRegistry()
    )

fun driver(): TopologyTestDriver =
    TopologyTestDriver(topology())

class TopologyTestDriverCommandManager(
    driver: TopologyTestDriver
): CommandManager {
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
            is EvidentDbCommandError -> when(val data = event.data) {
                is DatabaseCreationError -> data.left()
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
            is EvidentDbCommandError -> when(val data = event.data) {
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
            is EvidentDbCommandError -> when(val data = event.data) {
                is BatchTransactionError -> data.left()
                else -> throw IllegalStateException("Invalid error returned from renameDatabase $event")
            }
            else -> throw IllegalStateException("Invalid event returned from renameDatabase $event")
        }
    }

}

class TopologyTestDriverCommandService(
    driver: TopologyTestDriver
): CommandService {
    override val commandManager = TopologyTestDriverCommandManager(driver)
}