package com.evidentdb.batch

interface BatchCommandLog

interface BatchCommandHandler {
    val log: BatchCommandLog
}