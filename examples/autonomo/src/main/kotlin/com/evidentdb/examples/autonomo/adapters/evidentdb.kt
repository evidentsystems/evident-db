package com.evidentdb.examples.autonomo.adapters

import arrow.core.toNonEmptyListOrNull
import com.evidentdb.client.BatchConstraint
import com.evidentdb.client.BatchProposal
import com.evidentdb.client.DatabaseRevision
import com.evidentdb.client.kotlin.Connection
import com.evidentdb.client.kotlin.Database
import com.evidentdb.examples.autonomo.*
import com.evidentdb.examples.autonomo.domain.*
import com.evidentdb.examples.autonomo.transfer.toRideEvent
import com.evidentdb.examples.autonomo.transfer.toTransfer
import com.evidentdb.examples.autonomo.transfer.toVehicleEvent
import io.cloudevents.CloudEvent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import java.net.URI
import java.util.*

interface EvidentDbEventRepository<E>: EventRepository<E> {
    val connection: Connection
    val db: Database
    val eventQuery: (Database) -> Flow<CloudEvent>
    val constraints: (Database) -> List<BatchConstraint>

    override fun events(): Flow<E> =
        eventQuery(db).map(::cloudEventToDomainEvent)

    override suspend fun store(events: List<E>): Result<Unit> =
        try {
            val cloudEvents = events.map(::domainEventToCloudEvent).toNonEmptyListOrNull()!!
            connection.transactAsync(BatchProposal(cloudEvents, constraints(db)))
            Result.success(Unit)
        } catch (e: Throwable) {
            Result.failure(e)
        }

    fun cloudEventToDomainEvent(event: CloudEvent): E

    fun domainEventToCloudEvent(event: E): CloudEvent
}

class VehiclesEventRepository(
    override val connection: Connection,
    override val eventQuery: (Database) -> Flow<CloudEvent>,
    override val constraints: (Database) -> List<BatchConstraint> = {_ -> listOf()},
    revision: DatabaseRevision? = null
): EvidentDbEventRepository<VehicleEvent> {
    override val db: Database = if (revision == null) {
        connection.db()
    } else {
        runBlocking { connection.fetchDbAsOfAsync(revision) }
    }

    override fun cloudEventToDomainEvent(event: CloudEvent) =
        event.toVehicleEvent().toDomain()

    override fun domainEventToCloudEvent(event: VehicleEvent): CloudEvent =
        event.toTransfer().cloudEventBuilder()
            .withSource(URI(STREAM))
            .build()

    companion object {
        private const val STREAM = "vehicles"

        fun repositoryBeforeVehicleCreation(
            conn: Connection, vehicle: Vin, revision: DatabaseRevision? = null
        ) = VehiclesEventRepository(
            conn,
            {db -> db.fetchSubjectStreamAsync(STREAM, vehicle.toString())},
            {_ -> listOf(BatchConstraint.subjectDoesNotExistOnStream(STREAM, vehicle.toString()))},
            revision
        )

        fun repositoryForVehicle(
            conn: Connection, vehicle: Vin, revision: DatabaseRevision? = null
        ) = VehiclesEventRepository(
            conn,
            {db -> db.fetchSubjectStreamAsync(STREAM, vehicle.toString())},
            {db -> listOf(BatchConstraint.SubjectMaxRevisionOnStream(
                STREAM, vehicle.toString(), db.revision
            ))},
            revision
        )
    }
}

class EvidentDbVehicleService(
    private val conn: Connection,
): VehicleCommandService {
    override val domainLogic = vehiclesDecider()

    override suspend fun getMyVehicles(): List<Vehicle> {
        TODO("Not yet implemented")
    }

    override suspend fun getAvailableVehicles(): List<Vehicle> {
        TODO("Not yet implemented")
    }

    override suspend fun getVehicleByVin(vinString: String): Vehicle = try {
        val vin = Vin.build(vinString)
        val vehiclesEventRepository = VehiclesEventRepository
            .repositoryForVehicle(conn, vin)
        projectView(vehiclesEventRepository)
    } catch (error: InvalidVinError) {
        throw IllegalArgumentException(error)
    }

    override suspend fun addVehicle(user: UserId, vinString: String) = try {
        val vin = Vin.build(vinString)
        val vehiclesEventRepository = VehiclesEventRepository
            .repositoryBeforeVehicleCreation(conn, vin)
        executeDecision(vehiclesEventRepository, AddVehicle(user, vin))
    } catch (error: InvalidVinError) {
        Result.failure(error)
    }

    override suspend fun makeVehicleAvailable(vinString: String) = try {
        val vin = Vin.build(vinString)
        val vehiclesEventRepository = VehiclesEventRepository.repositoryForVehicle(conn, vin)
        executeDecision(vehiclesEventRepository, MakeVehicleAvailable(vin))
    } catch (error: InvalidVinError) {
        Result.failure(error)
    }

    override suspend fun requestVehicleReturn(vinString: String) = try {
        val vin = Vin.build(vinString)
        val vehiclesEventRepository = VehiclesEventRepository
            .repositoryForVehicle(conn, vin)
        executeDecision(vehiclesEventRepository, RequestVehicleReturn(vin))
    } catch (error: InvalidVinError) {
        Result.failure(error)
    }

    override suspend fun removeVehicle(user: UserId, vinString: String) = try {
        val vin = Vin.build(vinString)
        val vehiclesEventRepository = VehiclesEventRepository
            .repositoryForVehicle(conn, vin)
        executeDecision(vehiclesEventRepository, RemoveVehicle(user, vin))
    } catch (error: InvalidVinError) {
        Result.failure(error)
    }
}

class RidesEventRepository(
    override val connection: Connection,
    override val eventQuery: (Database) -> Flow<CloudEvent>,
    override val constraints: (Database) -> List<BatchConstraint> = {_ -> listOf()},
    revision: DatabaseRevision? = null,
): EvidentDbEventRepository<RideEvent> {
    override val db: Database = if (revision == null) {
        connection.db()
    } else {
        runBlocking { connection.fetchDbAsOfAsync(revision) }
    }

    override fun cloudEventToDomainEvent(event: CloudEvent)=
        event.toRideEvent().toDomain()

    override fun domainEventToCloudEvent(event: RideEvent): CloudEvent =
        event.toTransfer().cloudEventBuilder()
            .withSource(URI(STREAM))
            .build()

    companion object {
        private const val STREAM = "rides"

        fun repositoryBeforeRideCreation(
            conn: Connection, ride: RideId, revision: DatabaseRevision? = null
        ) = RidesEventRepository(
            conn,
            {db -> db.fetchSubjectStreamAsync(STREAM, ride.toString())},
            {_ -> listOf(BatchConstraint.subjectDoesNotExistOnStream(STREAM, ride.toString()))},
            revision
        )

        fun repositoryForRide(
            conn: Connection, ride: RideId, revision: DatabaseRevision? = null
        ) = RidesEventRepository(
            conn,
            {db -> db.fetchSubjectStreamAsync(STREAM, ride.toString())},
            {db -> listOf(BatchConstraint.SubjectMaxRevisionOnStream(
                STREAM, ride.toString(), db.revision
            ))},
            revision
        )
    }
}

class EvidentDbRideService(
    private val conn: Connection,
): RideCommandService {
    override val domainLogic = ridesDecider()

    override suspend fun getRideById(rideId: UUID): Ride {
        val repository = RidesEventRepository.repositoryForRide(conn, rideId)
        return projectView(repository)
    }

    override suspend fun requestRide(command: RequestRide): Result<Ride> {
        val repository = RidesEventRepository.repositoryBeforeRideCreation(conn, command.ride)
        val decision = executeDecision(repository, command)
        return decision
    }

    override suspend fun cancelRide(ride: RideId): Result<Ride> {
        val repository = RidesEventRepository.repositoryForRide(conn, ride)
        return executeDecision(repository, CancelRide(ride))
    }

    override suspend fun confirmRidePickup(command: ConfirmPickup): Result<Ride> {
        val repository = RidesEventRepository.repositoryForRide(conn, command.ride)
        return executeDecision(repository, command)
    }

    override suspend fun endRide(command: EndRide): Result<Ride> {
        val repository = RidesEventRepository.repositoryForRide(conn, command.ride)
        return executeDecision(repository, command)
    }
}
