package io.kotmod.querty.example

import io.kotmod.AggregateId
import io.kotmod.Repository
import io.kotmod.jdbc.JdbcContext
import io.querty.jdbc.execute

class QuertyCustomerRepository(
    private val jdbc: JdbcContext,
) : Repository<RegisteredCustomer> {
    override fun get(id: AggregateId): RegisteredCustomer? =
        jdbc.withConnection { conn -> FindCustomer(CustomerId(id.value)).execute(conn) }?.let { row ->
            RegisteredCustomer(row.name, row.email, row.registeredAt)
        }

    override fun save(
        id: AggregateId,
        state: RegisteredCustomer,
    ) {
        jdbc.withConnection { conn ->
            SaveCustomer(CustomerId(id.value), state.name, state.email, state.registeredAt).execute(conn)
        }
    }
}
