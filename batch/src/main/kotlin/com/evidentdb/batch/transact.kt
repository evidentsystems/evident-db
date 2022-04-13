package com.evidentdb.batch

import java.util.*

data class BatchProposal(val database: UUID)
data class ValidBatchProposal(val database: UUID)

fun validate_proposal(proposal: BatchProposal): ValidBatchProposal {
    return ValidBatchProposal(proposal.database)
}
