package com.dreweaster.ddd

/**
 * Marker for events published outside the bounded context.
 *
 * Public events form a deliberately stable contract with other contexts, kept separate from internal
 * [DomainEvent]s so the internal model can change without breaking consumers. They are produced by a
 * [com.dreweaster.ddd.contract.PublicEventContract], which maps internal events to public ones.
 */
interface PublicDomainEvent
