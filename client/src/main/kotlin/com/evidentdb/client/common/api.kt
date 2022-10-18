package com.evidentdb.client.common

interface GrpcLifecycle {
    /**
     * Shuts down this resource while awaiting any in-flight requests to
     * complete, and cleans up local state.
     * */
    fun shutdown()

    /**
     * Shuts down this connection immediately, not awaiting in-flight requests to
     * complete, and cleans up local state.
     * */
    fun shutdownNow()
}
