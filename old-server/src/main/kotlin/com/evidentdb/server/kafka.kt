package com.evidentdb.server

import com.evidentdb.batch.BatchCommandLog
import com.evidentdb.database.DatabaseCommandLog
import com.evidentdb.domain.database.ProposedDatabase
import com.evidentdb.domain.database.ProposedRenaming
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import io.micronaut.configuration.kafka.annotation.KafkaClient
import jakarta.inject.Singleton
import java.util.*
import com.evidentdb.command.InFlightCoordinator
import com.evidentdb.domain.database.CreationProposalOutcome
import com.evidentdb.domain.database.RenamingProposalOutcome
import io.micronaut.configuration.kafka.annotation.KafkaListener

@KafkaClient(id="command-producer")
interface CommandProducer {
    fun sendEvent(id: UUID, event: CloudEvent)
}

@Singleton
class KafkaCommandLog(private val producer: CommandProducer): DatabaseCommandLog, BatchCommandLog {
    override fun append(proposal: ProposedDatabase): UUID {
        val id = UUID.randomUUID()
        // TODO: Create CloudEvent for given ProposedDatabase event
        val event = CloudEventBuilder.v1().build()
        producer.sendEvent(id, event)
        return id
    }

    override fun append(proposal: ProposedRenaming): UUID {
        TODO("Not yet implemented")
    }
}

@KafkaListener
class KafkaInFlightCoordinator {

//    @Singleton
//    fun create() : InFlightCoordinator<CreationProposalOutcome> {
//        return InFlightCoordinator()
//    }
}