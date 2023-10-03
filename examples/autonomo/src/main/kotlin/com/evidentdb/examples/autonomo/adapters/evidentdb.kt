package com.evidentdb.examples.autonomo.adapters

import com.evidentdb.client.EventProposal
import com.evidentdb.client.StreamState
import com.evidentdb.client.kotlin.Connection
import com.evidentdb.client.kotlin.Database
import com.evidentdb.examples.autonomo.IEventRepository
import com.evidentdb.examples.autonomo.domain.RideEvent
import com.evidentdb.examples.autonomo.domain.RideId
import com.evidentdb.examples.autonomo.domain.VehicleEvent
import com.evidentdb.examples.autonomo.domain.Vin
import com.evidentdb.examples.autonomo.transfer.toTransfer
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.protobuf.toCloudEventData
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import java.util.UUID

class VehiclesEventRepository(
    private val connection: Connection,
    private val asOf: Long? = null
): IEventRepository<VehicleEvent> {
    private val db: Database = if (asOf == null) {
        connection.db()
    } else {
        runBlocking { connection.fetchDbAsOfAsync(asOf) }
    }

    companion object {
        private const val STREAM = "vehicles"
    }

    override fun events(): Flow<VehicleEvent> =
        db.fetchStreamAsync(STREAM).map(::cloudEventToVehicleEvent)

    override suspend fun store(events: List<VehicleEvent>): Result<Unit> =
        try {
            connection.transactAsync(events.map {
                val cloudEvent = vehicleEventToCloudEvent(it)
                val streamState = if (asOf == null) {
                    StreamState.StreamExists
                } else {
                    StreamState.AtRevision(asOf)
                }
                EventProposal(cloudEvent, STREAM, streamState)
            })
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }

    fun forEntity(vin: Vin) = VehicleEventRepository(vin)

    private fun cloudEventToVehicleEvent(event: CloudEvent): VehicleEvent {
        TODO()
    }
    private fun vehicleEventToCloudEvent(event: VehicleEvent): CloudEvent {
        val transfer = event.toTransfer()
        return CloudEventBuilder.v1()
            .withId(UUID.randomUUID().toString())
            .withSubject(event.vin.toString())
            .withType(event.type)
            .withData(transfer.toCloudEventData())
            .build()
    }

    inner class VehicleEventRepository(
        val vin: Vin
    ): IEventRepository<VehicleEvent> {
        override fun events(): Flow<VehicleEvent> =
            db.fetchSubjectStreamAsync(STREAM, vin.value).map(::cloudEventToVehicleEvent)

        override suspend fun store(events: List<VehicleEvent>): Result<Unit> =
            try {
                connection.transactAsync(events.map {
                    val cloudEvent = vehicleEventToCloudEvent(it)
                    val streamState = if (asOf == null) {
                        StreamState.StreamExists // TODO: subject exists/does not exist?
                    } else {
                        StreamState.AtRevision(asOf) // TODO: subject at state
                    }
                    EventProposal(cloudEvent, STREAM, streamState)
                })
                Result.success(Unit)
            } catch (e: Throwable) {
                Result.failure(e)
            }
    }
}

class RidesEventRepository(
    private val connection: Connection,
    private val asOf: Long? = null
): IEventRepository<RideEvent> {
    private val db: Database = if (asOf == null) {
        connection.db()
    } else {
        runBlocking { connection.fetchDbAsOfAsync(asOf) }
    }

    override fun events(): Flow<RideEvent> =
        db.fetchStreamAsync(STREAM).map(::cloudEventToRideEvent)

    override suspend fun store(events: List<RideEvent>): Result<Unit> =
        try {
            connection.transactAsync(events.map {
                val cloudEvent = rideEventToCloudEvent(it)
                val streamState = if (asOf == null) {
                    StreamState.StreamExists
                } else {
                    StreamState.AtRevision(asOf)
                }
                EventProposal(cloudEvent, STREAM, streamState)
            })
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }

    companion object {
        private const val STREAM = "rides"
    }

    fun forEntity(rideId: RideId) = RideEventRepository(rideId)

    private fun cloudEventToRideEvent(event: CloudEvent): RideEvent = TODO()
    private fun rideEventToCloudEvent(event: RideEvent): CloudEvent = TODO()

    inner class RideEventRepository(
        val rideId: RideId
    ): IEventRepository<RideEvent> {
        override fun events(): Flow<RideEvent> =
            db.fetchSubjectStreamAsync(STREAM, rideId.toString()).map(::cloudEventToRideEvent)

        override suspend fun store(events: List<RideEvent>): Result<Unit> =
            try {
                connection.transactAsync(events.map {
                    val cloudEvent = rideEventToCloudEvent(it)
                    val streamState = if (asOf == null) {
                        StreamState.StreamExists // TODO: subject exists/does not exist?
                    } else {
                        StreamState.AtRevision(asOf) // TODO: subject at state
                    }
                    EventProposal(cloudEvent, STREAM, streamState)
                })
                Result.success(Unit)
            } catch (e: Throwable) {
                Result.failure(e)
            }
    }
}
