package com.evidentdb.examples.autonomo.adapters

import arrow.core.toNonEmptyListOrNull
import com.evidentdb.client.BatchConstraint
import com.evidentdb.client.BatchProposal
import com.evidentdb.client.DatabaseRevision
import com.evidentdb.client.kotlin.Connection
import com.evidentdb.client.kotlin.Database
import com.evidentdb.examples.autonomo.EventRepository
import com.evidentdb.examples.autonomo.domain.RideEvent
import com.evidentdb.examples.autonomo.domain.RideId
import com.evidentdb.examples.autonomo.domain.VehicleEvent
import com.evidentdb.examples.autonomo.domain.Vin
import com.evidentdb.examples.autonomo.transfer.toRideEvent
import com.evidentdb.examples.autonomo.transfer.toTransfer
import com.evidentdb.examples.autonomo.transfer.toVehicleEvent
import io.cloudevents.CloudEvent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import java.net.URI

class VehiclesEventRepository(
    private val connection: Connection,
    private val asOf: DatabaseRevision? = null
): EventRepository<VehicleEvent> {
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
            val (cloudEvents, constraints) = events.fold(
                Pair(mutableListOf<CloudEvent>(), mutableSetOf<BatchConstraint>())
            ) { acc, event ->
                val cloudEvent = vehicleEventToCloudEvent(event)
                acc.first += cloudEvent
                cloudEvent.subject?.let {
                    acc.second += BatchConstraint.SubjectDoesNotExistOnStream(
                        cloudEvent.source.toString(),
                        it
                    )
                }
                acc
            }
            val eventsNel = cloudEvents.toNonEmptyListOrNull()!!
            connection.transactAsync(
                BatchProposal(
                    eventsNel,
                    constraints.toList()
                )
            )
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }

    fun forEntity(vin: Vin) = VehicleEventRepository(vin)

    private fun cloudEventToVehicleEvent(event: CloudEvent) =
        event.toVehicleEvent().toDomain()

    private fun vehicleEventToCloudEvent(event: VehicleEvent): CloudEvent =
        event.toTransfer().cloudEventBuilder()
            .withSource(URI(STREAM))
            .build()

    inner class VehicleEventRepository(
        val vin: Vin
    ): EventRepository<VehicleEvent> {
        override fun events(): Flow<VehicleEvent> =
            db.fetchSubjectStreamAsync(STREAM, vin.value).map(::cloudEventToVehicleEvent)

        override suspend fun store(events: List<VehicleEvent>): Result<Unit> =
            try {
                val (cloudEvents, constraints) = events.fold(
                    Pair(mutableListOf<CloudEvent>(), mutableSetOf<BatchConstraint>())
                ) { acc, event ->
                    val cloudEvent = vehicleEventToCloudEvent(event)
                    acc.first += cloudEvent
                    cloudEvent.subject?.let {
                        acc.second += BatchConstraint.SubjectMaxRevisionOnStream(
                            cloudEvent.source.toString(),
                            it,
                            db.revision
                        )
                    }
                    acc
                }
                val eventsNel = cloudEvents.toNonEmptyListOrNull()!!
                connection.transactAsync(
                    BatchProposal(
                        eventsNel,
                        constraints.toList()
                    )
                )
                Result.success(Unit)
            } catch (e: Throwable) {
                Result.failure(e)
            }
    }
}

class RidesEventRepository(
    private val connection: Connection,
    private val asOf: DatabaseRevision? = null
): EventRepository<RideEvent> {
    private val db: Database = if (asOf == null) {
        connection.db()
    } else {
        runBlocking { connection.fetchDbAsOfAsync(asOf) }
    }

    override fun events(): Flow<RideEvent> =
        db.fetchStreamAsync(STREAM).map(::cloudEventToRideEvent)

    override suspend fun store(events: List<RideEvent>): Result<Unit> =
        try {
            val (cloudEvents, constraints) = events.fold(
                Pair(mutableListOf<CloudEvent>(), mutableSetOf<BatchConstraint>())
            ) { acc, event ->
                val cloudEvent = rideEventToCloudEvent(event)
                acc.first += cloudEvent
                cloudEvent.subject?.let {
                    acc.second += BatchConstraint.SubjectDoesNotExistOnStream(
                        cloudEvent.source.toString(),
                        it
                    )
                }
                acc
            }
            val eventsNel = cloudEvents.toNonEmptyListOrNull()!!
            connection.transactAsync(
                BatchProposal(
                    eventsNel,
                    constraints.toList()
                )
            )
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }

    companion object {
        private const val STREAM = "rides"
    }

    fun forEntity(rideId: RideId) = RideEventRepository(rideId)

    private fun cloudEventToRideEvent(event: CloudEvent) =
        event.toRideEvent().toDomain()

    private fun rideEventToCloudEvent(event: RideEvent) =
        event.toTransfer().toCloudEvent()

    inner class RideEventRepository(
        val rideId: RideId
    ): EventRepository<RideEvent> {
        override fun events(): Flow<RideEvent> =
            db.fetchSubjectStreamAsync(STREAM, rideId.toString()).map(::cloudEventToRideEvent)

        override suspend fun store(events: List<RideEvent>): Result<Unit> =
            try {
                val (cloudEvents, constraints) = events.fold(
                    Pair(mutableListOf<CloudEvent>(), mutableSetOf<BatchConstraint>())
                ) { acc, event ->
                    val cloudEvent = rideEventToCloudEvent(event)
                    acc.first += cloudEvent
                    cloudEvent.subject?.let {
                        acc.second += BatchConstraint.SubjectMaxRevisionOnStream(
                            cloudEvent.source.toString(),
                            it,
                            db.revision
                        )
                    }
                    acc
                }
                val eventsNel = cloudEvents.toNonEmptyListOrNull()!!
                connection.transactAsync(
                    BatchProposal(
                        eventsNel,
                        constraints.toList()
                    )
                )
                Result.success(Unit)
            } catch (e: Throwable) {
                Result.failure(e)
            }
    }
}
