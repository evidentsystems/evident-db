package com.evidentdb.command

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentMap

interface InFlightCoordinator<K, V> {
    val inFlight: ConcurrentMap<K, CompletableFuture<V>>

    fun add(key: K): CompletableFuture<V> = inFlight.putIfAbsent(key, CompletableFuture())
        ?: throw IllegalStateException("In-flight request with key $key already exists!")

    private fun complete(key: K, value: V): CompletableFuture<V> {
        val ret = inFlight.remove(key) ?: throw IllegalStateException("No in-flight request exists with key: $key!")
        ret.complete(value)
        return ret
    }
}