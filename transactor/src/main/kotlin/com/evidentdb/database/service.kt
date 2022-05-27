package com.evidentdb.database

import com.evidentdb.database.domain.*
import com.evidentdb.domain.database.*

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
