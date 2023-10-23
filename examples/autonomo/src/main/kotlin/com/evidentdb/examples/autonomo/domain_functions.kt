package com.evidentdb.examples.autonomo

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.fold
import com.evidentdb.examples.autonomo.domain.*

// ***** Domain Functions API *****

interface Evolve<S, in E> {
    val initialState: () -> S
    val evolve: (S, E) -> S
}

interface Decide<in C, S, E>: Evolve<S, E> {
    val decide: (C, S) -> Result<List<E>>
}

interface React<in AR, out A> {
    val react: (AR) -> List<A>
}

data class View<S, in E>(
    override val initialState: () -> S,
    override val evolve: (S, E) -> S,
): Evolve<S, E>

data class Decider<in C, S, E>(
    override val initialState: () -> S,
    override val evolve: (S, E) -> S,
    override val decide: (C, S) -> Result<List<E>>
): Decide<C, S, E>

data class Saga<in Ar, out A>(
    override val react: (Ar) -> List<A>
): React<Ar, A>


// ***** Vehicles *****

fun vehiclesDecider(): Decide<VehicleCommand, Vehicle, VehicleEvent> = Decider(
    { InitialVehicleState },
    { state, event -> state.evolve(event) },
    { command, state -> runCatching { command.decide(state) } }
)

// ***** Rides *****

fun ridesDecider(): Decide<RideCommand, Ride, RideEvent> = Decider(
    { InitialRideState },
    { state, event -> state.evolve(event) },
    { command, state -> runCatching { command.decide(state) } }
)

//fun react(event: RideEvent): List<VehicleCommand> =
//	when(val domainEvent = event.toDomain()) {
//		is DomainRideScheduled -> listOf(
//			DomainMarkVehicleOccupied(domainEvent.vin)
//		)
//		is DomainScheduledRideCancelled -> listOf(
//			DomainMarkVehicleUnoccupied(domainEvent.vin)
//		)
//		is DomainRiderDroppedOff -> listOf(
//			DomainMarkVehicleUnoccupied(domainEvent.vin)
//		)
//		else -> emptyList()
//	}.map(DomainVehicleCommand::toTransfer)

// ***** Domain Functions Application *****

interface EventSourcingViewProjector<S, E> {
    val domainLogic: Evolve<S, E>

    suspend fun projectView(repository: EventRepository<E>): S =
        repository.events().fold(domainLogic.initialState(), domainLogic.evolve)
}

interface EventSourcingDecisionExecutor<in C, S, E>: EventSourcingViewProjector<S, E> {
    override val domainLogic: Decide<C, S, E>

    suspend fun executeDecision(repository: EventRepository<E>, command: C): Result<S> {
        val currentState = projectView(repository)
        return domainLogic.decide(command, currentState)
            .mapCatching { events ->
                val result = repository.store(events)
                if (result.isSuccess) {
                    events
                } else {
                    throw result.exceptionOrNull()!!
                }
            }
            .map { events -> events.fold(currentState, domainLogic.evolve) }
    }
}

// ***** Domain Functions Infrastructure Adapters *****

interface EventRepository<E> {
    fun events(): Flow<E>
    suspend fun store(events: List<E>): Result<Unit>
}
