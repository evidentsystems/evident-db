package com.evidentdb.application

interface Lifecycle {
    /**
     * This must be idempotent, as some implementations will invoke this method several times.
     */
    fun setup(params: Map<String, String>)

    /**
     * This must be idempotent, as some implementations will invoke this method several times.
     */
    fun teardown()
}