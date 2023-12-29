package com.evidentdb.app

import com.evidentdb.adapter.EvidentDbAdapter
import com.evidentdb.adapter.in_memory.InMemoryAdapter
import com.evidentdb.service.EvidentDbEndpoint
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

@Factory
class Configuration {
    @Singleton
    fun endpoint(adapter: EvidentDbAdapter) =
        EvidentDbEndpoint(adapter)
}
