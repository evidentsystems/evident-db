package com.evidentdb.examples.autonomo.adapters

import com.evidentdb.client.kotlin.Connection
import com.evidentdb.examples.autonomo.*
import com.evidentdb.examples.autonomo.transfer.*
import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.*
import java.net.URI
import java.util.UUID

@Controller("/rides")
class RidesController(
    conn: Connection
) {
    private val service = EvidentDbRideService(conn)

    @Get(value = "/{id}", produces = [MediaType.APPLICATION_JSON])
    suspend fun getRideById(id: UUID): HttpResponse<Ride> =
        service.getRideById(id).toTransfer()
            ?.let { HttpResponse.ok(it) }
            ?: HttpResponse.notFound()

    @Post(value = "/", consumes = [MediaType.APPLICATION_JSON])
    suspend fun requestRide(@Body command: RequestRide): HttpResponse<Ride> =
        service.requestRide(command.toDomain()).fold(
            { ride ->
                ride.toTransfer()
                    ?.let { HttpResponse.created<Ride>(URI("/rides/${it.id}")).body(it) }
                    ?: HttpResponse.notFound()
            },
            { HttpResponse.notFound() } // TODO: better error reporting from Result
        )

    @Delete(value = "/{id}", consumes = [MediaType.APPLICATION_JSON])
    suspend fun cancelRide(@PathVariable id: UUID): HttpResponse<Ride> =
        service.cancelRide(id).fold(
            { HttpResponse.accepted<Ride>().body(it.toTransfer()) },
            { HttpResponse.notFound() } // TODO: better error reporting from Result
        )

    @Put(value = "/{id}/pickup", consumes = [MediaType.APPLICATION_JSON])
    suspend fun confirmPickup(@PathVariable id: UUID, @Body command: ConfirmPickup): HttpResponse<Ride> =
        service.confirmRidePickup(command.toDomain(id)).fold(
            { HttpResponse.accepted<Ride>().body(it.toTransfer()) },
            { HttpResponse.notFound() } // TODO: better error reporting from Result
        )

    @Put(value = "/{id}/drop-off", consumes = [MediaType.APPLICATION_JSON])
    suspend fun endRide(@PathVariable id: UUID, @Body command: EndRide): HttpResponse<Ride> =
        service.endRide(command.toDomain(id)).fold(
            { HttpResponse.accepted<Ride>().body(it.toTransfer()) },
            { HttpResponse.notFound() } // TODO: better error reporting from Result
        )
}

@Controller("/vehicles")
class VehiclesController(
    conn: Connection
) {
    private val service = EvidentDbVehicleService(conn)
    // TODO: hook up a real user management system
    private val userId = UUID.fromString("fe9f2aba-1713-4cb5-bb53-14ba7a89e43e")

    @Get(value = "/{vin}", produces = [MediaType.APPLICATION_JSON])
    suspend fun vehicleByVin(@PathVariable vin: String): HttpResponse<Vehicle> =
        service.getVehicleByVin(vin).toTransfer()
            ?.let { HttpResponse.ok(it) }
            ?: HttpResponse.notFound()

    @Get(value = "/mine", produces = [MediaType.APPLICATION_JSON])
    suspend fun myVehicles(): HttpResponse<List<Vehicle>> =
        HttpResponse.ok(service.getMyVehicles().map { it.toTransfer()!! })

    @Get(value = "/available", produces = [MediaType.APPLICATION_JSON])
    suspend fun availableVehicles(): HttpResponse<List<Vehicle>> =
        HttpResponse.ok(service.getAvailableVehicles().map { it.toTransfer()!! })

    @Post(value = "/mine/{vin}", consumes = [MediaType.APPLICATION_JSON])
    suspend fun addVehicle(@PathVariable vin: String): HttpResponse<Vehicle> =
        service.addVehicle(userId, vin).fold(
            { vehicle ->
                vehicle.toTransfer()
                    ?.let { HttpResponse.accepted<Vehicle>().body(it) }
                    ?: HttpResponse.notFound()
            },
            { HttpResponse.notFound() } // TODO: better error reporting from Result
        )

    @Put(value = "/mine/{vin}/availability", consumes = [MediaType.APPLICATION_JSON])
    suspend fun makeVehicleAvailable(@PathVariable vin: String): HttpResponse<Vehicle> =
        service.makeVehicleAvailable(vin).fold(
            { vehicle ->
                vehicle.toTransfer()
                    ?.let {HttpResponse.accepted<Vehicle>().body(it) }
                    ?: HttpResponse.notFound()
            },
            { HttpResponse.notFound() } // TODO: better error reporting from Result
        )

    @Delete(value = "/mine/{vin}/availability", consumes = [MediaType.APPLICATION_JSON])
    suspend fun requestVehicleReturn(@PathVariable vin: String): HttpResponse<Vehicle> =
        service.requestVehicleReturn(vin).fold(
            { vehicle ->
                vehicle.toTransfer()
                    ?.let {HttpResponse.accepted<Vehicle>().body(it) }
                    ?: HttpResponse.notFound()
            },
            { HttpResponse.notFound() } // TODO: better error reporting from Result
        )

    @Delete(value = "/mine/{vin}", consumes = [MediaType.APPLICATION_JSON])
    suspend fun removeVehicle(@PathVariable vin: String): HttpResponse<Vehicle> =
        service.removeVehicle(userId, vin).fold(
            { vehicle ->
                vehicle.toTransfer()
                    ?.let {HttpResponse.accepted<Vehicle>().body(it) }
                    ?: HttpResponse.notFound()
            },
            { HttpResponse.notFound() } // TODO: better error reporting from Result
        )
}
