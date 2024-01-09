fun decide(command: RideCommand, state: RideReadModel): Result<List<RideEvent>> =
    runCatching { command.toDomain()
                      .decide(state.toDomain())
                      .map(DomainRideEvent::toTransfer) }

sealed interface RideCommand: Command {
  fun decide(state: Ride): List<RideEvent>
}

data class RequestRide(
    val rider: UserId,
    val origin: GeoCoordinates,
    val destination: GeoCoordinates,
    val pickupTime: Instant
): RideCommand {
  override fun decide(state: Ride): List<RideEvent> = when(state) {
    InitialRideState -> listOf(RideRequested(RideId.randomUUID(), rider, origin, destination, pickupTime, Instant.now()))
    else -> throw RideCommandError(this, state, "Ride already exists")
  }
}

fun evolve(state: RideReadModel, event: RideEvent): RideReadModel =
    state.toDomain().evolve(event.toDomain()).toTransfer()

sealed interface Ride: ReadModel {
  fun evolve(event: RideEvent): Ride
}

object InitialRideState: Ride {
  override fun evolve(event: RideEvent): Ride =
      when(event) {
        is RideRequested -> RequestedRide(event.ride, event.rider, event.pickupTime, event.origin,
                                          event.destination, event.requestedAt)
        else -> this
      }
}

fun react(event: RideEvent): List<VehicleCommand> =
    when(val domainEvent = event.toDomain()) {
        is RideScheduled -> listOf(
            MarkVehicleOccupied(domainEvent.vin)
        )
        is ScheduledRideCancelled -> listOf(
            MarkVehicleUnoccupied(domainEvent.vin)
        )
        is RiderDroppedOff -> listOf(
            MarkVehicleUnoccupied(domainEvent.vin)
        )
        else -> emptyList()
    }.map(VehicleCommand::toTransfer)
