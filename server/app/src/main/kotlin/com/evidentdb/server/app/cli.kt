package com.evidentdb.server.app

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import io.micronaut.runtime.Micronaut

fun main(args: Array<String>) =
    EvidentDb()
        .subcommands(Init(), Run())
        .main(args)

class EvidentDb : CliktCommand(
    name="evidentdb",
    help="The Event Sourcing, Event Streaming database"
) {
    override fun run() = Unit
}

class Init: CliktCommand(name="", help="") {
    override fun run() {
        echo("Initialing the database")
        Micronaut.build()
            .properties(mapOf())
            .environments("init")
            .start()
    }
}

class Run: CliktCommand() {
    override fun run() {
        echo("Running evidentdb")
        Micronaut.build()
            .eagerInitSingletons(true)
            .properties(mapOf())
            .environments("run")
            .start()
    }
}