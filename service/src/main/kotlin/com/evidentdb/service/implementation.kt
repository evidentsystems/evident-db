package com.evidentdb.service

import arrow.core.Either
import com.evidentdb.domain.*
import com.evidentdb.domain.CommandBroker
import com.evidentdb.kafka.DatabaseReadModelStore
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

class KafkaCommandBroker(
    val producer: KafkaProducer<CommandId, CommandEnvelope>,
    val consumer: KafkaConsumer<EventId, EventEnvelope>
): CommandBroker {
    // TODO: Figure out how to do in-flight with coroutines
    val inFlight = ConcurrentHashMap<CommandId, CompletableFuture<EventEnvelope>>()

    override suspend fun createDatabase(command: CreateDatabase): Either<DatabaseCreationError, DatabaseCreated> {
        TODO("Not yet implemented")
    }

    override suspend fun renameDatabase(command: RenameDatabase): Either<DatabaseRenameError, DatabaseRenamed> {
        TODO("Not yet implemented")
    }

    override suspend fun deleteDatabase(command: DeleteDatabase): Either<DatabaseDeletionError, DatabaseDeleted> {
        TODO("Not yet implemented")
    }

    override suspend fun transactBatch(command: TransactBatch): Either<BatchTransactionError, BatchTransacted> {
        TODO("Not yet implemented")
    }
}

class KafkaService(
    producer: KafkaProducer<CommandId, CommandEnvelope>,
    consumer: KafkaConsumer<EventId, EventEnvelope>,
    streams: KafkaStreams,
    databaseStoreName: String,
    databaseNameStoreName: String,
): Service {
    override val databaseReadModel = DatabaseReadModelStore(
        streams.store(StoreQueryParameters.fromNameAndType(
                    databaseStoreName,
                    QueryableStoreTypes.keyValueStore()
                )
        ),
        streams.store(
            StoreQueryParameters.fromNameAndType(
                databaseNameStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        ),
    )
    override val commandBroker = KafkaCommandBroker(producer, consumer)
}