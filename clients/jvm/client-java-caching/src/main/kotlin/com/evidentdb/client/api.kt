package com.evidentdb.client

import com.evidentdb.client.java.EvidentDb
import com.evidentdb.client.java.caching.JavaCachingClient
import io.grpc.ManagedChannelBuilder

object EvidentDb {
    @JvmStatic
    fun javaClient(managedChannelBuilder: ManagedChannelBuilder<*>): EvidentDb =
        JavaCachingClient(managedChannelBuilder)
}