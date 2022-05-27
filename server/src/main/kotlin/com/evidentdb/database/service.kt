package com.evidentdb.database

import com.evidentdb.command.InFlightCoordinator
import com.evidentdb.database.domain.*
import com.evidentdb.domain.database.*
import java.util.*
import java.util.concurrent.CompletableFuture

interface DatabaseCommandLog {
    fun append(proposal: ProposedDatabase) : UUID
    fun append(proposal: ProposedRenaming) : UUID
}

interface DatabaseCommandHandler {
    val log: DatabaseCommandLog
    val inFlightCreations: InFlightCoordinator<CreationProposalOutcome>
    val inFlightRenames: InFlightCoordinator<RenamingProposalOutcome>

    fun proposeCreation(name: String) : CompletableFuture<CreationProposalOutcome> {
        val proposedDatabase = validateCreationProposal(name)
        val commandId = log.append(proposedDatabase)
        return inFlightCreations.add(commandId)
    }

    fun proposeRenaming(oldName: String, newName: String) : CompletableFuture<RenamingProposalOutcome> {
        val proposedRenaming = validateRenamingProposal(oldName, newName)
        val commandId = log.append(proposedRenaming)
        return inFlightRenames.add(commandId)
    }

//    fun deleteDatabase(name: String) : CompletableFuture<DatabaseDeletionOutcome> {
//
//    }
}

