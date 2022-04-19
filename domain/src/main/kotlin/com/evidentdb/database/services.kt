package com.evidentdb.database

import com.evidentdb.command.InFlightCoordinator
import com.evidentdb.database.workflows.*
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

interface ReadableStore {
    fun byName(name: String) : Database?
    fun catalog() : Catalog
}

interface WriteableStore : ReadableStore {
    fun create(proposal: ProposedDatabase) : Catalog
    fun rename(proposal: ProposedRenaming) : Catalog
    fun delete(name: String) : Catalog
}

interface DatabaseEventHandler {
    val catalog: Catalog

    fun creationProposed(proposedDatabase: ProposedDatabase) = processCreationProposal(catalog, proposedDatabase)
    fun renamingProposed(proposedRenaming: ProposedRenaming) = processRenamingProposal(catalog, proposedRenaming)
}