package com.evidentdb.examples.autonomo.transfer

import com.evidentdb.examples.autonomo.domain.*
import com.fasterxml.jackson.databind.ObjectMapper
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import io.micronaut.serde.annotation.Serdeable
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.*
import com.evidentdb.examples.autonomo.domain.ConfirmPickup as DomainConfirmPickup
import com.evidentdb.examples.autonomo.domain.EndRide as DomainEndRide
import com.evidentdb.examples.autonomo.domain.RequestRide as DomainRequestRide
import com.evidentdb.examples.autonomo.domain.RequestedRideCancelled as DomainRequestedRideCancelled
import com.evidentdb.examples.autonomo.domain.Ride as DomainRide
import com.evidentdb.examples.autonomo.domain.RideEvent as DomainRideEvent
import com.evidentdb.examples.autonomo.domain.RideRequested as DomainRideRequested
import com.evidentdb.examples.autonomo.domain.RideScheduled as DomainRideScheduled
import com.evidentdb.examples.autonomo.domain.RiderDroppedOff as DomainRiderDroppedOff
import com.evidentdb.examples.autonomo.domain.RiderPickedUp as DomainRiderPickedUp
import com.evidentdb.examples.autonomo.domain.ScheduledRideCancelled as DomainScheduledRideCancelled

// ***** Commands *****

@Serdeable
data class RequestRide(
    val rider: UserId,
    val origin: GeoCoordinates,
    val destination: GeoCoordinates,
    val pickupTime: Instant
) {
    fun toDomain() = DomainRequestRide(
        rider, origin.toDomain(), destination.toDomain(), pickupTime
    )
}

@Serdeable
data class ConfirmPickup(
    val vin: String,
    val rider: UserId,
    val pickupLocation: GeoCoordinates
) {
    fun toDomain(ride: RideId) = DomainConfirmPickup(
        ride, Vin.build(vin), rider, pickupLocation.toDomain()
    )
}

@Serdeable
data class EndRide(
    val dropOffLocation: GeoCoordinates
) {
    fun toDomain(ride: RideId) = DomainEndRide(ride, dropOffLocation.toDomain())
}

// ***** Events *****

sealed interface RideEvent {
    val ride: UUID

    fun toDomain(): DomainRideEvent
    fun toCloudEvent(): CloudEvent = cloudEventBuilder().build()

    fun cloudEventBuilder(): CloudEventBuilder =
        CloudEventBuilder.v1()
            .withId(UUID.randomUUID().toString())
            .withSubject(ride.toString())
            .withType(this::class.simpleName)
            .withData(ObjectMapper().writeValueAsBytes(this))

    companion object {
        fun fromDomain(event: DomainRideEvent): RideEvent = when (event) {
            is DomainRideRequested -> RideRequested(
                event.ride,
                event.rider,
                event.origin.toTransfer(),
                event.destination.toTransfer(),
                event.pickupTime,
                event.requestedAt
            )
            is DomainRequestedRideCancelled -> RequestedRideCancelled(
                event.ride, event.cancelledAt
            )
            is DomainRideScheduled -> RideScheduled(
                event.ride,
                event.vin.value,
                event.pickupTime,
                event.scheduledAt
            )
            is DomainScheduledRideCancelled -> ScheduledRideCancelled(
                event.ride, event.vin.value, event.cancelledAt
            )
            is DomainRiderPickedUp -> RiderPickedUp(
                event.ride,
                event.rider,
                event.vin.value,
                event.pickupLocation.toTransfer(),
                event.pickedUpAt
            )
            is DomainRiderDroppedOff -> RiderDroppedOff(
                event.ride, event.vin.value, event.dropOffLocation.toTransfer(), event.droppedOffAt
            )
        }

        fun fromCloudEvent(event: CloudEvent): RideEvent {
            val objectMapper = ObjectMapper()
            val data = event.data!!
                .toBytes()
                .toString(StandardCharsets.UTF_8)
            return when (event.type) {
                "RideRequested" -> objectMapper.readValue(data, RideRequested::class.java)
                "RequestedRideCancelled" -> objectMapper.readValue(data, RequestedRideCancelled::class.java)
                "RideScheduled" -> objectMapper.readValue(data, RideScheduled::class.java)
                "ScheduledRideCancelled" -> objectMapper.readValue(data, RequestedRideCancelled::class.java)
                "RiderPickedUp" -> objectMapper.readValue(data, RiderPickedUp::class.java)
                "RiderDroppedOff" -> objectMapper.readValue(data, RiderDroppedOff::class.java)
                else -> throw IllegalArgumentException("Unknown event type: ${event.type}")
            }
        }
    }
}

@Serdeable
data class RideRequested(
    override val ride: UUID,
    val rider: UUID,
    val origin: GeoCoordinates,
    val destination: GeoCoordinates,
    val pickupTime: Instant,
    val requestedAt: Instant
): RideEvent {
    override fun toDomain() = DomainRideRequested(
        ride, rider, origin.toDomain(), destination.toDomain(), pickupTime, requestedAt
    )
}

@Serdeable
data class RideScheduled(
    override val ride: UUID,
    val vin: String,
    val pickupTime: Instant,
    val scheduledAt: Instant
): RideEvent {
    override fun toDomain() = DomainRideScheduled(
        ride, Vin.build(vin), pickupTime, scheduledAt
    )
}

@Serdeable
data class RequestedRideCancelled(
    override val ride: UUID,
    val cancelledAt: Instant
): RideEvent {
    override fun toDomain() = DomainRequestedRideCancelled(ride, cancelledAt)
}

@Serdeable
data class ScheduledRideCancelled(
    override val ride: UUID,
    val vin: String,
    val cancelledAt: Instant
): RideEvent {
    override fun toDomain() = DomainScheduledRideCancelled(
        ride, Vin.build(vin), cancelledAt
    )
}

@Serdeable
data class RiderPickedUp(
    override val ride: UUID,
    val rider: UUID,
    val vin: String,
    val pickupLocation: GeoCoordinates,
    val pickedUpAt: Instant
): RideEvent {
    override fun toDomain() = DomainRiderPickedUp(
        ride, rider, Vin.build(vin), pickupLocation.toDomain(), pickedUpAt
    )
}

@Serdeable
data class RiderDroppedOff(
    override val ride: UUID,
    val vin: String,
    val dropOffLocation: GeoCoordinates,
    val droppedOffAt: Instant
): RideEvent {
    override fun toDomain() = DomainRiderDroppedOff(
        ride, Vin.build(vin), dropOffLocation.toDomain(), droppedOffAt
    )
}

@Serdeable
data class RideError(
    override val ride: UUID,
    val message: String,
): RideEvent {
    override fun toDomain(): DomainRideEvent {
        TODO("Not yet implemented")
    }
}

// Convenience extensions

fun DomainRideEvent.toTransfer(): RideEvent = RideEvent.fromDomain(this)
fun CloudEvent.toRideEvent(): RideEvent = RideEvent.fromCloudEvent(this)

// ***** Read Models *****

@Serdeable
enum class RideStatus {
    REQUESTED, SCHEDULED, IN_PROGRESS, COMPLETED, CANCELLED
}

@Serdeable
data class Ride(
    val id: UUID,
    val rider: UUID,
    val pickupTime: Instant,
    val pickupLocation: GeoCoordinates,
    val dropOffLocation: GeoCoordinates,
    val status: RideStatus,
    val vin: String? = null,
    val requestedAt: Instant? = null,
    val scheduledAt: Instant? = null,
    val pickedUpAt: Instant? = null,
    val droppedOffAt: Instant? = null,
    val cancelledAt: Instant? = null
) {
    fun toDomain(): DomainRide = when (status) {
        RideStatus.REQUESTED -> RequestedRide(
            id,
            rider,
            pickupTime,
            pickupLocation.toDomain(),
            dropOffLocation.toDomain(),
            requestedAt!!
        )
        RideStatus.SCHEDULED -> ScheduledRide(
            id,
            rider,
            pickupTime,
            pickupLocation.toDomain(),
            dropOffLocation.toDomain(),
            Vin.build(vin!!),
            scheduledAt!!
        )
        RideStatus.IN_PROGRESS -> InProgressRide(
            id,
            rider,
            pickupLocation.toDomain(),
            dropOffLocation.toDomain(),
            scheduledAt!!,
            Vin.build(vin!!),
            pickupTime,
            pickedUpAt!!
        )
        RideStatus.COMPLETED -> CompletedRide(
            id,
            rider,
            pickupTime,
            pickupLocation.toDomain(),
            dropOffLocation.toDomain(),
            Vin.build(vin!!),
            pickedUpAt!!,
            droppedOffAt!!
        )
        RideStatus.CANCELLED -> if (scheduledAt != null) {
            CancelledScheduledRide(
                id,
                rider,
                pickupTime,
                pickupLocation.toDomain(),
                dropOffLocation.toDomain(),
                Vin.build(vin!!),
                scheduledAt,
                cancelledAt!!
            )
        } else {
            CancelledRequestedRide(
                id,
                rider,
                pickupTime,
                pickupLocation.toDomain(),
                dropOffLocation.toDomain(),
                cancelledAt!!
            )
        }
    }

    companion object {
        fun fromDomain(ride: DomainRide): Ride? = when (ride) {
            InitialRideState -> null

            is RequestedRide -> Ride(
                ride.id,
                ride.rider,
                ride.requestedPickupTime,
                ride.pickupLocation.toTransfer(),
                ride.dropOffLocation.toTransfer(),
                RideStatus.REQUESTED,
                requestedAt = ride.requestedAt
            )

            is CancelledRequestedRide -> Ride(
                ride.id,
                ride.rider,
                ride.requestedPickupTime,
                ride.pickupLocation.toTransfer(),
                ride.dropOffLocation.toTransfer(),
                RideStatus.CANCELLED,
                cancelledAt = ride.cancelledAt
            )

            is ScheduledRide -> Ride(
                ride.id,
                ride.rider,
                ride.scheduledPickupTime,
                ride.pickupLocation.toTransfer(),
                ride.dropOffLocation.toTransfer(),
                RideStatus.SCHEDULED,
                vin = ride.vin.value,
                scheduledAt = ride.scheduledAt
            )

            is CancelledScheduledRide -> Ride(
                ride.id,
                ride.rider,
                ride.scheduledPickupTime,
                ride.pickupLocation.toTransfer(),
                ride.dropOffLocation.toTransfer(),
                RideStatus.CANCELLED,
                vin = ride.vin.value,
                scheduledAt = ride.scheduledAt,
                cancelledAt = ride.cancelledAt
            )

            is InProgressRide -> Ride(
                ride.id,
                ride.rider,
                ride.pickupTime,
                ride.pickupLocation.toTransfer(),
                ride.dropOffLocation.toTransfer(),
                RideStatus.IN_PROGRESS,
                vin = ride.vin.value,
                scheduledAt = ride.scheduledAt,
                pickedUpAt = ride.pickedUpAt
            )

            is CompletedRide -> Ride(
                ride.id,
                ride.rider,
                ride.pickupTime,
                ride.pickupLocation.toTransfer(),
                ride.dropOffLocation.toTransfer(),
                RideStatus.COMPLETED,
                vin = ride.vin.value,
                pickedUpAt = ride.pickedUpAt,
                droppedOffAt = ride.droppedOffAt
            )
        }
    }
}

fun DomainRide.toTransfer() = Ride.fromDomain(this)
