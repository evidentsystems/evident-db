package com.evidentdb.command

import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentMap

interface InFlightCoordinator<V> {
    val inFlight: ConcurrentMap<UUID, CompletableFuture<V>>

    fun add(key: UUID): CompletableFuture<V> = inFlight.putIfAbsent(key, CompletableFuture())
        ?: throw IllegalStateException("In-flight request with key $key already exists!")

    fun complete(key: UUID, value: V): CompletableFuture<V> {
        val ret = inFlight.remove(key) ?: throw IllegalStateException("No in-flight request exists with key: $key!")
        ret.complete(value)
        return ret
    }
}