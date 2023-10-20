package com.evidentdb.examples.autonomo.transfer

import java.time.Instant
import java.util.UUID

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import com.evidentdb.examples.autonomo.domain.GeoCoordinates as DomainGeoCoordinates

object InstantSerializer : KSerializer<Instant> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("java.time.Instant", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: Instant) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): Instant = Instant.parse(decoder.decodeString())
}

object UUIDSerializer : KSerializer<UUID> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("java.util.UUID", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: UUID) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): UUID = UUID.fromString(decoder.decodeString())
}

@Serializable
data class GeoCoordinates(val lat: Double, val lng: Double) {
    fun toDomain() = DomainGeoCoordinates(lat, lng)
}

fun DomainGeoCoordinates.toTransfer() =
    GeoCoordinates(latitude, longitude)
