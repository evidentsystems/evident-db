package com.evidentdb.examples.autonomo

import com.evidentdb.client.kotlin.Connection
import com.evidentdb.examples.autonomo.adapters.RidesEventRepository
import com.evidentdb.examples.autonomo.adapters.VehiclesEventRepository
import com.evidentdb.examples.autonomo.domain.*
import java.util.UUID


interface VehicleQueryService: EventSourcingViewProjector<Vehicle, VehicleEvent> {
    suspend fun getMyVehicles(): List<Vehicle>
    suspend fun getAvailableVehicles(): List<Vehicle>
    suspend fun getVehicleByVin(vinString: String): Vehicle
}

interface VehicleCommandService:
    VehicleQueryService,
    EventSourcingDecisionExecutor<VehicleCommand, Vehicle, VehicleEvent>  {
    suspend fun addVehicle(user: UserId, vinString: String): Result<Vehicle>
    suspend fun makeVehicleAvailable(vinString: String): Result<Vehicle>
    suspend fun requestVehicleReturn(vinString: String): Result<Vehicle>
    suspend fun removeVehicle(user: UserId, vinString: String): Result<Vehicle>
}

class EvidentDbVehicleService(
    private val conn: Connection,
): VehicleCommandService {
    override val domainLogic = vehiclesDecider()
    private val vehiclesEventRepository = VehiclesEventRepository(conn)

    override suspend fun getMyVehicles(): List<Vehicle> {
        TODO("Not yet implemented")
    }

    override suspend fun getAvailableVehicles(): List<Vehicle> {
        TODO("Not yet implemented")
    }

    override suspend fun getVehicleByVin(vinString: String): Vehicle = try {
        val vin = Vin.build(vinString)
        projectView(vehiclesEventRepository.forEntity(vin))
    } catch (error: InvalidVinError) {
        throw IllegalArgumentException(error)
    }

    override suspend fun addVehicle(user: UserId, vinString: String) = try {
        executeDecision(vehiclesEventRepository, AddVehicle(user, Vin.build(vinString))).map { it }
    } catch (error: InvalidVinError) {
        Result.failure(error)
    }

    override suspend fun makeVehicleAvailable(vinString: String) = try {
        executeDecision(vehiclesEventRepository, MakeVehicleAvailable(Vin.build(vinString))).map { it }
    } catch (error: InvalidVinError) {
        Result.failure(error)
    }

    override suspend fun requestVehicleReturn(vinString: String) = try {
        executeDecision(vehiclesEventRepository, RequestVehicleReturn(Vin.build(vinString))).map { it }
    } catch (error: InvalidVinError) {
        Result.failure(error)
    }

    override suspend fun removeVehicle(user: UserId, vinString: String) = try {
        executeDecision(vehiclesEventRepository, RemoveVehicle(user, Vin.build(vinString))).map { it }
    } catch (error: InvalidVinError) {
        Result.failure(error)
    }
}

interface RideQueryService: EventSourcingViewProjector<Ride, RideEvent> {
    suspend fun getRideById(rideId: UUID): Ride
}

interface RideCommandService: RideQueryService, EventSourcingDecisionExecutor<RideCommand, Ride, RideEvent> {
    suspend fun requestRide(command: RequestRide): Result<Ride>
    suspend fun cancelRide(ride: RideId): Result<Ride>
    suspend fun confirmRidePickup(command: ConfirmPickup): Result<Ride>
    suspend fun endRide(command: EndRide): Result<Ride>
}

class EvidentDbRideService(
    conn: Connection,
): RideCommandService {
    override val domainLogic = ridesDecider()
    private val ridesEventRepository = RidesEventRepository(conn)

    override suspend fun getRideById(rideId: UUID): Ride =
        projectView(ridesEventRepository.forEntity(rideId))

    override suspend fun requestRide(command: RequestRide) =
        executeDecision(ridesEventRepository, command).map { it }

    override suspend fun cancelRide(ride: RideId) =
        executeDecision(ridesEventRepository, CancelRide(ride)).map { it }

    override suspend fun confirmRidePickup(command: ConfirmPickup) =
        executeDecision(ridesEventRepository, command).map { it }

    override suspend fun endRide(command: EndRide): Result<Ride> =
        executeDecision(ridesEventRepository, command).map { it }
}
