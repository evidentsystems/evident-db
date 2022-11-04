package com.evidentdb.domain

fun streamStateFromRevisions(
    streamRevisions: Map<StreamName, StreamRevision>,
    streamName: StreamName
): StreamState =
    streamRevisions[streamName]
        ?.let {
            StreamState.AtRevision(it)
        }
        ?: StreamState.NoStream

fun streamRevisionsFromDto(
    streamRevisions: Map<String, StreamRevision>
): Map<StreamName, StreamRevision> =
    streamRevisions.mapKeys { StreamName.build(it.key) }

fun streamRevisionsToDto(
    streamRevisions: Map<StreamName, StreamRevision>
): Map<String, StreamRevision> =
    streamRevisions.mapKeys { it.key.value }