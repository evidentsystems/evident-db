package com.evidentdb.client

import arrow.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.concurrent.ThreadSafe
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

const val ITERATOR_READ_AHEAD_CACHE_SIZE = 100

internal fun <T> Flow<T>.asIterator() =
    FlowIterator(this)

@ThreadSafe
internal class FlowIterator<T>(
    private val flow: Flow<T>
): CloseableIterator<T> {
    private val asyncScope = CoroutineScope(Dispatchers.Default)
    private val blockingContext = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    private val queue = LinkedBlockingQueue<Option<T>>(ITERATOR_READ_AHEAD_CACHE_SIZE)
    private val nextItem: AtomicReference<Option<T>>

    init {
        asyncScope.launch {
            flow.collect {
                transfer(it.some())
            }
            transfer(None)
            close()
        }
        nextItem = AtomicReference(queue.take())
    }

    override fun hasNext(): Boolean =
        nextItem.get() != None

    override fun next(): T =
        if (hasNext()) {
            val currentItem = nextItem.get()
            nextItem.set(queue.take())
            when(currentItem) {
                None -> throw IndexOutOfBoundsException("Iterator bounds exceeded")
                is Some -> currentItem.value
            }
        }
        else
            throw IndexOutOfBoundsException("Iterator bounds exceeded")

    override fun close() {
        nextItem.set(None)
        asyncScope.cancel()
        blockingContext.close()
    }

    private suspend inline fun transfer(item: Option<T>) = withContext(blockingContext) {
        suspendCoroutine { continuation ->
            try {
                queue.put(item)
                continuation.resume(Unit)
            } catch (e: Exception) {
                continuation.resumeWithException(e)
            }
        }
    }
}
