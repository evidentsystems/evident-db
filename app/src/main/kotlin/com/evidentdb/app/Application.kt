package com.evidentdb.app

import io.micronaut.runtime.Micronaut.*
fun main(args: Array<String>) {
	build()
	    .args(*args)
		.packages("com.evidentdb")
		.start()
}

