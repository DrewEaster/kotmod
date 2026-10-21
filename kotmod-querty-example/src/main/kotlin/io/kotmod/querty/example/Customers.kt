package io.kotmod.querty.example

import io.kotmod.AggregateKind
import io.kotmod.AggregateState
import io.kotmod.AggregateType
import io.kotmod.DomainEvent
import io.kotmod.InitialState
import io.kotmod.Outcome
import io.kotmod.accept
import io.kotmod.reject
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import kotlinx.serialization.Serializable
import kotlin.time.Instant

@Serializable
sealed interface CustomerEvent : DomainEvent

@Serializable
data class CustomerRegistered(
    val name: String,
    val email: String,
    val registeredAt: Instant,
) : CustomerEvent

@Serializable
sealed interface CustomerCommand

@Serializable
data class RegisterCustomer(
    val name: String,
    val email: String,
    val registeredAt: Instant,
) : CustomerCommand

@Serializable
sealed interface CustomerRejection

@Serializable
data object CustomerAlreadyRegistered : CustomerRejection

object Customers : AggregateKind<CustomerCommand, CustomerEvent, CustomerRejection>(
    type = AggregateType("Customer"),
    commandSerializer = CustomerCommand.serializer(),
    eventSerialization =
        jsonDataSerializationContext<CustomerEvent> {
            +CustomerRegistered.serializer().toEventSerializer()
        },
    rejectionSerializer = CustomerRejection.serializer(),
)

typealias CustomerOutcome = Outcome<RegisteredCustomer, CustomerEvent, CustomerRejection>

object NoCustomer : InitialState<RegisteredCustomer, CustomerCommand, CustomerEvent, CustomerRejection> {
    override suspend fun handle(command: CustomerCommand): CustomerOutcome =
        when (command) {
            is RegisterCustomer ->
                accept(
                    RegisteredCustomer(command.name, command.email, command.registeredAt),
                    CustomerRegistered(command.name, command.email, command.registeredAt),
                )
        }
}

// A customer has only one stored state, so it is the aggregate's state type itself, with no sealed interface.
data class RegisteredCustomer(
    val name: String,
    val email: String,
    val registeredAt: Instant,
) : AggregateState<RegisteredCustomer, CustomerCommand, CustomerEvent, CustomerRejection> {
    override suspend fun handle(command: CustomerCommand): CustomerOutcome =
        when (command) {
            is RegisterCustomer -> reject(CustomerAlreadyRegistered)
        }
}
