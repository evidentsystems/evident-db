package com.evidentdb.server.domain_model.test

import com.evidentdb.server.domain_model.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions

class DomainModelTests {
    fun `batch constraint conflicts`() = runTest {
        // Prevent duplicates for same type & key
        validateConstraintConflict(
            listOf(
                BatchConstraint.DatabaseMinRevision(1uL),
                BatchConstraint.DatabaseMinRevision(2uL)
            )
        )

        validateConstraintConflict(
            listOf(
                BatchConstraint.DatabaseMaxRevision(1uL),
                BatchConstraint.DatabaseMaxRevision(2uL)
            )
        )

        validateConstraintConflict(
            listOf(
                BatchConstraint.DatabaseRevisionRange(1uL, 2uL).getOrNull()!!,
                BatchConstraint.DatabaseRevisionRange(2uL, 3uL).getOrNull()!!
            )
        )

        validateConstraintConflict(
            listOf(
                BatchConstraint.StreamMinRevision(StreamName("foo").getOrNull()!!, 1uL),
                BatchConstraint.StreamMinRevision(StreamName("foo").getOrNull()!!, 2uL)
            )
        )

        validateConstraintConflict(
            listOf(
                BatchConstraint.StreamMaxRevision(StreamName("bar").getOrNull()!!, 2uL),
                BatchConstraint.StreamMaxRevision(StreamName("bar").getOrNull()!!, 100uL)
            )
        )

        validateConstraintConflict(
            listOf(
                BatchConstraint.StreamRevisionRange(
                    StreamName("event-stream").getOrNull()!!,
                    1uL,
                    1uL
                ).getOrNull()!!,
                BatchConstraint.StreamRevisionRange(
                    StreamName("event-stream").getOrNull()!!,
                    1uL,
                    1uL
                ).getOrNull()!!
            )
        )

        validateConstraintConflict(
            listOf(
                BatchConstraint.SubjectMinRevision(EventSubject("foo").getOrNull()!!, 1uL),
                BatchConstraint.SubjectMinRevision(EventSubject("foo").getOrNull()!!, 2uL)
            )
        )

        validateConstraintConflict(
            listOf(
                BatchConstraint.SubjectMaxRevision(EventSubject("bar").getOrNull()!!, 2uL),
                BatchConstraint.SubjectMaxRevision(EventSubject("bar").getOrNull()!!, 100uL)
            )
        )

        validateConstraintConflict(
            listOf(
                BatchConstraint.SubjectRevisionRange(
                    EventSubject("a nice subject").getOrNull()!!,
                    1uL,
                    1uL
                ).getOrNull()!!,
                BatchConstraint.SubjectRevisionRange(
                    EventSubject("a nice subject").getOrNull()!!,
                    1uL,
                    1uL
                ).getOrNull()!!
            )
        )

        // Prevent conflicting
        validateConstraintConflict(
            listOf(
                BatchConstraint.DatabaseMinRevision(1uL),
                BatchConstraint.DatabaseMinRevision(1uL)
            )
        )
    }

    private fun validateConstraintConflict(constraints: List<BatchConstraint>) {
        val result = validateConstraints(constraints)
        Assertions.assertTrue(result.isLeft())
        Assertions.assertInstanceOf(BatchConstraintConflicts::class.java, result.leftOrNull())
    }
}