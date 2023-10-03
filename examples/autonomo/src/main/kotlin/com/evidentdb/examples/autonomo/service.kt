package com.evidentdb.examples.autonomo

import com.evidentdb.client.kotlin.Connection
import com.evidentdb.examples.autonomo.transfer.AddVehicle
import com.evidentdb.examples.autonomo.transfer.CancelRide
import com.evidentdb.examples.autonomo.transfer.ConfirmPickup
import com.evidentdb.examples.autonomo.transfer.EndRide
import com.evidentdb.examples.autonomo.transfer.MakeVehicleAvailable
import com.evidentdb.examples.autonomo.transfer.RemoveVehicle
import com.evidentdb.examples.autonomo.transfer.RequestRide
import com.evidentdb.examples.autonomo.transfer.RequestVehicleReturn
import com.evidentdb.examples.autonomo.transfer.RideReadModel
import com.evidentdb.examples.autonomo.transfer.VehicleReadModel
import java.util.UUID

interface QueryService {
    // Rides
    fun getRideById(rideId: UUID): RideReadModel?

    // Vehicles
    fun getMyVehicles(): List<VehicleReadModel>
    fun getAvailableVehicles(): List<VehicleReadModel>
    fun getVehicleByVin(vin: String): VehicleReadModel?
}

interface CommandService: QueryService {
    fun requestRide(command: RequestRide): Result<RideReadModel>
    fun cancelRide(command: CancelRide): Result<RideReadModel>
    fun confirmRidePickup(command: ConfirmPickup): Result<RideReadModel>
    fun endRide(command: EndRide): Result<RideReadModel>

    fun addVehicle(command: AddVehicle): Result<VehicleReadModel>
    fun makeVehicleAvailable(command: MakeVehicleAvailable): Result<VehicleReadModel>
    fun requestVehicleReturn(command: RequestVehicleReturn): Result<VehicleReadModel>
    fun removeVehicle(command: RemoveVehicle): Result<VehicleReadModel>
}

class EvidentDbService(
    private val conn: Connection,
): CommandService {
    override fun getRideById(rideId: UUID): RideReadModel? {
        TODO("Not yet implemented")
    }

    override fun getMyVehicles(): List<VehicleReadModel> {
        TODO("Not yet implemented")
    }

    override fun getAvailableVehicles(): List<VehicleReadModel> {
        TODO("Not yet implemented")
    }

    override fun getVehicleByVin(vin: String): VehicleReadModel? {
        TODO("Not yet implemented")
    }

    override fun requestRide(command: RequestRide): Result<RideReadModel> {
        TODO("Not yet implemented")
    }

    override fun cancelRide(command: CancelRide): Result<RideReadModel> {
        TODO("Not yet implemented")
    }

    override fun confirmRidePickup(command: ConfirmPickup): Result<RideReadModel> {
        TODO("Not yet implemented")
    }

    override fun endRide(command: EndRide): Result<RideReadModel> {
        TODO("Not yet implemented")
    }

    override fun addVehicle(command: AddVehicle): Result<VehicleReadModel> {
        TODO("Not yet implemented")
    }

    override fun makeVehicleAvailable(command: MakeVehicleAvailable): Result<VehicleReadModel> {
        TODO("Not yet implemented")
    }

    override fun requestVehicleReturn(command: RequestVehicleReturn): Result<VehicleReadModel> {
        TODO("Not yet implemented")
    }

    override fun removeVehicle(command: RemoveVehicle): Result<VehicleReadModel> {
        TODO("Not yet implemented")
    }
}
