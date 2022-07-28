package com.evidentdb.cloudevents

import com.evidentdb.domain.CommandId
import io.cloudevents.CloudEventExtension
import io.cloudevents.CloudEventExtensions
import io.cloudevents.core.extensions.impl.ExtensionUtils
import java.util.*

data class CommandIdExtension(var commandId: CommandId): CloudEventExtension {
    companion object {
        const val COMMAND_ID = "commandId"
        private val KEY_SET = setOf(COMMAND_ID)
    }

    override fun readFrom(extensions: CloudEventExtensions) {
        extensions.getExtension(COMMAND_ID)?.let {
            this.commandId = UUID.fromString(it.toString())
        }
    }

    override fun getValue(key: String): Any =
        if (COMMAND_ID == key) {
            commandId.toString()
        } else {
            throw ExtensionUtils.generateInvalidKeyException(this.javaClass, key)
        }

    override fun getKeys(): Set<String> =
        KEY_SET

    override fun toString(): String =
        "CommandIdExtension{commandId='$commandId'}"
}