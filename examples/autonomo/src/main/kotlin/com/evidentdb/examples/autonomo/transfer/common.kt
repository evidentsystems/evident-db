package com.evidentdb.examples.autonomo.transfer

import io.micronaut.serde.annotation.Serdeable
import com.evidentdb.examples.autonomo.domain.GeoCoordinates as DomainGeoCoordinates

@Serdeable
data class GeoCoordinates(val lat: Double, val lng: Double) {
    fun toDomain() = DomainGeoCoordinates(lat, lng)
}

fun DomainGeoCoordinates.toTransfer() =
    GeoCoordinates(latitude, longitude)
