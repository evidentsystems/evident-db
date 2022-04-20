package com.evidentdb.database

import com.evidentdb.database.domain.*

interface CatalogProvider {
    fun catalog() : Catalog
}

interface DatabaseEventHandler {
    val provider: CatalogProvider

    fun creationProposed(proposedDatabase: ProposedDatabase): CreationProposalOutcome {
        val catalog = provider.catalog()
        return processCreationProposal(catalog, proposedDatabase)
    }

    fun renamingProposed(proposedRenaming: ProposedRenaming): RenamingProposalOutcome {
        val catalog = provider.catalog()
        return processRenamingProposal(catalog, proposedRenaming)
    }
}
