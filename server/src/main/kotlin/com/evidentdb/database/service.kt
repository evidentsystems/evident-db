package com.evidentdb.database

import com.evidentdb.command.InFlightCoordinator
import com.evidentdb.database.domain.*
import java.util.*
import java.util.concurrent.CompletableFuture

interface CommandLog {
    fun proposeCreation(proposal: ProposedDatabase) : UUID
    fun proposeRenaming(proposal: ProposedRenaming) : UUID
}

interface DatabaseCommandHandler {
    val commandLog: CommandLog
    val inFlightCreations: InFlightCoordinator<UUID, CreationProposalOutcome>
    val inFlightRenames: InFlightCoordinator<UUID, RenamingProposalOutcome>

    fun proposeCreation(name: String) : CompletableFuture<CreationProposalOutcome> {
        val proposedDatabase = validateCreationProposal(name)
        val commandId = commandLog.proposeCreation(proposedDatabase)
        return inFlightCreations.add(commandId)
    }

    fun proposeRenaming(oldName: String, newName: String) : CompletableFuture<RenamingProposalOutcome> {
        val proposedRenaming = validateRenamingProposal(oldName, newName)
        val commandId = commandLog.proposeRenaming(proposedRenaming)
        return inFlightRenames.add(commandId)
    }

//    fun deleteDatabase(name: String) : CompletableFuture<DatabaseDeletionOutcome> {
//
//    }
}

