package io.kotmod

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json

/**
 * Names one kind of aggregate, such as orders, together with how its commands, events and rejections are serialized.
 * Build an [AggregateManager] from it, and let use cases react to its events with `on(kind)`. Declare one per
 * aggregate type, usually as an `object`:
 *
 * ```
 * object Orders : AggregateKind<OrderCommand, OrderEvent, OrderRejection>(
 *     type = AggregateType("Order"),
 *     commandSerializer = OrderCommand.serializer(),
 *     eventSerialization = jsonDataSerializationContext<OrderEvent> { +OrderPlaced.serializer().toEventSerializer() },
 *     rejectionSerializer = OrderRejection.serializer(),
 * )
 * ```
 *
 * @param type the aggregate type its events and commands are recorded under.
 * @param commandSerializer serializes its commands, so other parts of the app can request them as data.
 * @param eventSerialization reads and writes its events; use cases listening to the kind get them typed.
 * @param rejectionSerializer serializes its rejections, which are recorded so a repeated command id gets the
 *   same answer.
 */
open class AggregateKind<C : Any, E : DomainEvent, R : Any>(
    val type: AggregateType,
    val commandSerializer: KSerializer<C>,
    val eventSerialization: DataSerializationContext<E>,
    val rejectionSerializer: KSerializer<R>,
) {
    /** Requests [command] for the aggregate [id] of this kind; a process manager runs it later, asynchronously. */
    fun command(
        id: AggregateId,
        command: C,
    ): RequestedCommand<C> = RequestedCommand(this, id, command)
}

/**
 * A command a process manager asks to be run against aggregate [targetId] of [kind]. Build it with [AggregateKind.command].
 *
 * Two requested commands are equal only if their kinds are the same instance (kinds compare by identity), so declare
 * each kind once, as an `object`, and request commands from it.
 */
data class RequestedCommand<C : Any>(
    val kind: AggregateKind<C, *, *>,
    val targetId: AggregateId,
    val command: C,
) {
    internal fun encodeCommand(): String = Json.encodeToString(kind.commandSerializer, command)
}
