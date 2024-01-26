package com.evidentdb.examples.autonomo

import com.evidentdb.examples.autonomo.domain.*
import java.util.*

// TODO: support passing point-in-time revision

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

interface RideQueryService: EventSourcingViewProjector<Ride, RideEvent> {
    suspend fun getRideById(rideId: UUID): Ride
}

interface RideCommandService: RideQueryService,
    EventSourcingDecisionExecutor<RideCommand, Ride, RideEvent> {
    suspend fun requestRide(command: RequestRide): Result<Ride>
    suspend fun cancelRide(ride: RideId): Result<Ride>
    suspend fun confirmRidePickup(command: ConfirmPickup): Result<Ride>
    suspend fun endRide(command: EndRide): Result<Ride>
}
