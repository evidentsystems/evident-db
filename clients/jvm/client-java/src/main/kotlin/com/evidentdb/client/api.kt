package com.evidentdb.client

import com.evidentdb.client.java.EvidentDb
import com.evidentdb.client.java.JavaSimpleClient
import io.grpc.ManagedChannelBuilder

object EvidentDb {
    @JvmStatic
    fun javaClient(managedChannelBuilder: ManagedChannelBuilder<*>): EvidentDb =
        JavaSimpleClient(managedChannelBuilder)
}