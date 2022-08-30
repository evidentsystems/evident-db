package com.evidentdb.domain

import arrow.core.*
import org.valiktor.ConstraintViolationException
import org.valiktor.i18n.toMessage

internal inline fun <reified T> valikate(
    validationFn: () -> T
): Validated<List<String>, T> = try {
    validationFn().valid()
} catch (ex: ConstraintViolationException){
    ex.constraintViolations
        .map {
            val message = it.toMessage()
            "\"${message.value}\" of ${T::class.simpleName}.${message.property}: ${message.message}"
        }.invalid()
}