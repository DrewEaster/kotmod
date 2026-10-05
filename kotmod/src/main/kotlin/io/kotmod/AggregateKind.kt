package io.kotmod

import kotlinx.serialization.KSerializer

/**
 * Names one kind of aggregate, such as orders, together with how its commands and rejections are serialized.
 * Build an [AggregateManager] from it. Declare one per aggregate type, usually as an `object`:
 *
 * ```
 * object Orders : AggregateKind<OrderCommand, OrderRejection>(
 *     type = AggregateType("Order"),
 *     commandSerializer = OrderCommand.serializer(),
 *     rejectionSerializer = OrderRejection.serializer(),
 * )
 * ```
 *
 * @param type the aggregate type its events and commands are recorded under.
 * @param commandSerializer serializes its commands, so other parts of the app can request them as data.
 * @param rejectionSerializer serializes its rejections, which are recorded so a repeated command id gets the
 *   same answer.
 */
open class AggregateKind<C : Any, R : Any>(
    val type: AggregateType,
    val commandSerializer: KSerializer<C>,
    val rejectionSerializer: KSerializer<R>,
)
