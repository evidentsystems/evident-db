package com.evidentdb.examples.autonomo

import com.evidentdb.examples.autonomo.transfer.*
import com.evidentdb.examples.autonomo.domain.VehicleEvent as DomainVehicleEvent
import com.evidentdb.examples.autonomo.domain.RideScheduled as DomainRideScheduled
import com.evidentdb.examples.autonomo.domain.ScheduledRideCancelled as DomainScheduledRideCancelled
import com.evidentdb.examples.autonomo.domain.RiderDroppedOff as DomainRiderDroppedOff
import com.evidentdb.examples.autonomo.domain.MarkVehicleOccupied as DomainMarkVehicleOccupied
import com.evidentdb.examples.autonomo.domain.MarkVehicleUnoccupied as DomainMarkVehicleUnoccupied
import com.evidentdb.examples.autonomo.domain.VehicleCommand as DomainVehicleCommand
import com.evidentdb.examples.autonomo.domain.RideEvent as DomainRideEvent

// ***** Vehicles *****

fun decide(command: VehicleCommand, state: VehicleReadModel): Result<List<VehicleEvent>> =
    runCatching { command.toDomain().decide(state.toDomain()).map(DomainVehicleEvent::toTransfer) }

fun evolve(state: VehicleReadModel, event: VehicleEvent): VehicleReadModel =
    state.toDomain().evolve(event.toDomain()).toTransfer()

// ***** Rides *****

fun decide(command: RideCommand, state: RideReadModel): Result<List<RideEvent>> =
    runCatching { command.toDomain().decide(state.toDomain()).map(DomainRideEvent::toTransfer) }

fun evolve(state: RideReadModel, event: RideEvent): RideReadModel =
    state.toDomain().evolve(event.toDomain()).toTransfer()

fun react(event: RideEvent): List<VehicleCommand> =
    when(val domainEvent = event.toDomain()) {
        is DomainRideScheduled -> listOf(
            DomainMarkVehicleOccupied(domainEvent.vin)
        )
        is DomainScheduledRideCancelled -> listOf(
            DomainMarkVehicleUnoccupied(domainEvent.vin)
        )
        is DomainRiderDroppedOff -> listOf(
            DomainMarkVehicleUnoccupied(domainEvent.vin)
        )
        else -> emptyList()
    }.map(DomainVehicleCommand::toTransfer)
