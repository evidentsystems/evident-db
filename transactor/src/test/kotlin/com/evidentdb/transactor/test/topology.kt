package com.evidentdb.transactor.test

import com.evidentdb.domain.*
import com.evidentdb.kafka.CommandEnvelopeSerde
import com.evidentdb.transactor.Topology
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Test

const val INTERNAL_COMMAND_TOPIC = "internal-commands"
const val INTERNAL_EVENTS_TOPIC = "internal-events"
const val DATABASES_TOPIC = "databases"
const val DATABASE_NAMES_TOPIC = "database-names"
const val BATCHES_TOPIC = "batches"
const val STREAMS_TOPIC = "streams"
const val EVENTS_TOPIC = "events"

class TopologyTests {
    private fun topology() =
        Topology.build(
            INTERNAL_COMMAND_TOPIC,
            INTERNAL_EVENTS_TOPIC,
            DATABASES_TOPIC,
            DATABASE_NAMES_TOPIC,
            BATCHES_TOPIC,
            STREAMS_TOPIC,
            EVENTS_TOPIC,
        )

    private fun driver(): TopologyTestDriver =
        TopologyTestDriver(topology())

    private fun internalCommandOutputTopic(driver: TopologyTestDriver) =
        driver.createInputTopic(
            INTERNAL_COMMAND_TOPIC,
            Serdes.UUID().serializer(),
            CommandEnvelopeSerde.CommandEnvelopeSerializer()
        )

//    private fun internalEventOutputTopic(driver: TopologyTestDriver) =
//        TODO() // driver.createOutputTopic()

    @Test
    fun `topology creates a database`() {
        topology()
        val driver = driver()
        val inputTopic = internalCommandOutputTopic(driver)
        val commandId = CommandId.randomUUID()
        inputTopic.pipeInput(
            commandId,
            CreateDatabase(
                commandId,
                DatabaseId.randomUUID(),
                DatabaseCreationInfo("foo")
            )
        )
        println(driver.producedTopicNames())
        TODO("add some assertions")
    }
}