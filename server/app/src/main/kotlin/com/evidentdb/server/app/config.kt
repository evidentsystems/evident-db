package com.evidentdb.server.app

import com.evidentdb.server.adapter.EvidentDbAdapter
import com.evidentdb.server.service.EvidentDbEndpoint
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

@Factory
class Configuration {
    @Singleton
    fun endpoint(adapter: EvidentDbAdapter) =
        EvidentDbEndpoint(adapter)
}
