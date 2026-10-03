package com.dreweaster.ddd

interface Repository<S : Any> {
    fun get(id: AggregateId): S?

    fun save(
        id: AggregateId,
        state: S,
    )
}
