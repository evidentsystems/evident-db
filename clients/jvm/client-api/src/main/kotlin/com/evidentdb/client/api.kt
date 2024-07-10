package com.evidentdb.client

interface Shutdown {
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

interface CloseableIterator<T>: Iterator<T>, AutoCloseable