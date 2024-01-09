package com.evidentdb.examples.autonomo.transfer

import java.time.Instant
import java.util.UUID
import com.evidentdb.examples.autonomo.domain.*
import io.micronaut.serde.ObjectMapper
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import io.micronaut.serde.annotation.Serdeable
import java.nio.charset.StandardCharsets
import com.evidentdb.examples.autonomo.domain.VehicleEvent as DomainVehicleEvent
import com.evidentdb.examples.autonomo.domain.VehicleAdded as DomainVehicleAdded
import com.evidentdb.examples.autonomo.domain.VehicleAvailable as DomainVehicleAvailable
import com.evidentdb.examples.autonomo.domain.VehicleOccupied as DomainVehicleOccupied
import com.evidentdb.examples.autonomo.domain.VehicleReturnRequested as DomainVehicleReturnRequested
import com.evidentdb.examples.autonomo.domain.VehicleReturning as DomainVehicleReturning
import com.evidentdb.examples.autonomo.domain.VehicleReturned as DomainVehicleReturned
import com.evidentdb.examples.autonomo.domain.VehicleRemoved as DomainVehicleRemoved
import com.evidentdb.examples.autonomo.domain.Vehicle as DomainVehicle

// ***** Events *****

sealed interface VehicleEvent {
    val vin: String

    fun toDomain(): DomainVehicleEvent
    fun toCloudEvent(): CloudEvent = cloudEventBuilder().build()

    fun cloudEventBuilder(): CloudEventBuilder =
        CloudEventBuilder.v1()
            .withId(UUID.randomUUID().toString())
            .withSubject(vin)
            .withType(this::class.simpleName)
            .withData(ObjectMapper.getDefault().writeValueAsBytes(this))

    companion object {
        fun fromDomain(event: DomainVehicleEvent): VehicleEvent = when (event) {
            is DomainVehicleAdded -> VehicleAdded(event.owner, event.vin.toString())
            is DomainVehicleAvailable -> VehicleAvailable(event.vin.toString(), event.availableAt)
            is DomainVehicleOccupied -> VehicleOccupied(event.vin.toString(), event.occupiedAt)
            is DomainVehicleReturnRequested -> VehicleReturnRequested(event.vin.toString(), event.returnRequestedAt)
            is DomainVehicleReturning -> VehicleReturning(event.vin.toString(), event.returningAt)
            is DomainVehicleReturned -> VehicleReturned(event.vin.toString(), event.returnedAt)
            is DomainVehicleRemoved -> VehicleRemoved(event.owner, event.vin.toString(), event.removedAt)
        }

        fun fromCloudEvent(event: CloudEvent): VehicleEvent {
            val objectMapper = ObjectMapper.getDefault()
            val data = event.data!!
                .toBytes()
                .toString(StandardCharsets.UTF_8)
            return when (event.type) {
                "VehicleAdded" -> objectMapper.readValue(data, VehicleAdded::class.java)
                "VehicleAvailable" -> objectMapper.readValue(data, VehicleAvailable::class.java)
                "VehicleOccupied" -> objectMapper.readValue(data, VehicleOccupied::class.java)
                "VehicleReturnRequested" -> objectMapper.readValue(data, VehicleReturnRequested::class.java)
                "VehicleReturning" -> objectMapper.readValue(data, VehicleReturning::class.java)
                "VehicleReturned" -> objectMapper.readValue(data, VehicleReturned::class.java)
                "VehicleRemoved" -> objectMapper.readValue(data, VehicleRemoved::class.java)
                else -> throw IllegalArgumentException("Unknown event type: ${event.type}")
            }
        }
    }
}

@Serdeable
data class VehicleAdded(val owner: UUID, override val vin: String): VehicleEvent {
    override fun toDomain() = DomainVehicleAdded(owner, Vin.build(vin))
}

@Serdeable
data class VehicleAvailable(override val vin: String, val availableAt: Instant): VehicleEvent {
    override fun toDomain() = DomainVehicleAvailable(Vin.build(vin), availableAt)
}

@Serdeable
data class VehicleOccupied(override val vin: String, val occupiedAt: Instant): VehicleEvent {
    override fun toDomain() = DomainVehicleOccupied(Vin.build(vin), occupiedAt)
}

@Serdeable
data class VehicleReturnRequested(override val vin: String, val returnRequestedAt: Instant): VehicleEvent {
    override fun toDomain() = DomainVehicleReturnRequested(Vin.build(vin), returnRequestedAt)
}

@Serdeable
data class VehicleReturning(override val vin: String, val returningAt: Instant): VehicleEvent {
    override fun toDomain() = DomainVehicleReturning(Vin.build(vin), returningAt)
}

@Serdeable
data class VehicleReturned(override val vin: String, val returnedAt: Instant): VehicleEvent {
    override fun toDomain() = DomainVehicleReturned(Vin.build(vin), returnedAt)
}

@Serdeable
data class VehicleRemoved(val owner: UUID, override val vin: String, val removedAt: Instant): VehicleEvent {
    override fun toDomain() = DomainVehicleRemoved(owner, Vin.build(vin), removedAt)
}

@Serdeable
data class VehicleError(override val vin: String, val message: String): VehicleEvent {
    override fun toDomain(): DomainVehicleEvent {
        TODO("Not yet implemented")
    }
}

// Convenience extensions

fun DomainVehicleEvent.toTransfer(): VehicleEvent = VehicleEvent.fromDomain(this)
fun CloudEvent.toVehicleEvent(): VehicleEvent = VehicleEvent.fromCloudEvent(this)

// ***** Read Models *****

@Serdeable
enum class VehicleStatus {
    IN_INVENTORY, AVAILABLE, OCCUPIED, OCCUPIED_RETURNING, RETURNING
}

@Serdeable
data class Vehicle(
    val vin: String,
    val owner: UUID,
    val status: VehicleStatus
) {
    fun toDomain(): DomainVehicle = when (status) {
        VehicleStatus.IN_INVENTORY -> InventoryVehicle(Vin.build(vin), owner)
        VehicleStatus.AVAILABLE -> AvailableVehicle(Vin.build(vin), owner)
        VehicleStatus.OCCUPIED -> OccupiedVehicle(Vin.build(vin), owner)
        VehicleStatus.OCCUPIED_RETURNING -> OccupiedReturningVehicle(Vin.build(vin), owner)
        VehicleStatus.RETURNING -> ReturningVehicle(Vin.build(vin), owner)
    }
}

fun DomainVehicle.toTransfer(): Vehicle? = when (this) {
    InitialVehicleState -> null
    is InventoryVehicle -> Vehicle(vin.toString(), owner, VehicleStatus.IN_INVENTORY)
    is AvailableVehicle -> Vehicle(vin.toString(), owner, VehicleStatus.AVAILABLE)
    is OccupiedReturningVehicle -> Vehicle(vin.toString(), owner, VehicleStatus.OCCUPIED_RETURNING)
    is OccupiedVehicle -> Vehicle(vin.toString(), owner, VehicleStatus.OCCUPIED)
    is ReturningVehicle -> Vehicle(vin.toString(), owner, VehicleStatus.RETURNING)
}
