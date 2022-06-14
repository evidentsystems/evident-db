package com.evidentdb.kafka

import com.evidentdb.domain.*
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

// TODO: Serdes for serializing CommandEnvelope and EventEnvelop as CloudEvents
class CommandEnvelopeSerde(): Serde<CommandEnvelope> {
    override fun serializer(): Serializer<CommandEnvelope> {
        TODO("Not yet implemented")
    }

    override fun deserializer(): Deserializer<CommandEnvelope> {
        TODO("Not yet implemented")
    }
}

class EventEnvelopeSerde(): Serde<EventEnvelope> {
    override fun serializer(): Serializer<EventEnvelope> {
        TODO("Not yet implemented")
    }

    override fun deserializer(): Deserializer<EventEnvelope> {
        TODO("Not yet implemented")
    }
}

// TODO: Serdes for domain types
class DatabaseSerde(): Serde<Database> {
    override fun serializer(): Serializer<Database> {
        TODO("Not yet implemented")
    }

    override fun deserializer(): Deserializer<Database> {
        TODO("Not yet implemented")
    }
}

class EventSerde(): Serde<Event> {
    override fun serializer(): Serializer<Event> {
        TODO("Not yet implemented")
    }

    override fun deserializer(): Deserializer<Event> {
        TODO("Not yet implemented")
    }
}