package io.kotmod.support

import io.kotmod.AggregateId
import io.kotmod.Repository

class StubRepository<S : Any> : Repository<S> {
    val store = mutableMapOf<AggregateId, S>()

    override fun get(id: AggregateId): S? = store[id]

    override fun save(
        id: AggregateId,
        state: S,
    ) {
        store[id] = state
    }
}
