package com.evidentdb.client

import com.evidentdb.client.kotlin.EvidentDb
import com.evidentdb.client.kotlin.KotlinClient
import io.grpc.ManagedChannelBuilder

object EvidentDb {
    @JvmStatic
    fun kotlinClient(managedChannelBuilder: ManagedChannelBuilder<*>): EvidentDb =
        KotlinClient(managedChannelBuilder)
}