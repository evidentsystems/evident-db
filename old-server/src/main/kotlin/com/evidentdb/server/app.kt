package com.evidentdb.server

import io.micronaut.runtime.Micronaut.*

fun main(args: Array<String>) {
	build()
	    .args(*args)
		.packages("com.evidentdb")
		.start()
}

