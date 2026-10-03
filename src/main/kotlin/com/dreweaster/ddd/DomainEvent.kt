package com.dreweaster.ddd

/**
 * Marker for events raised by an aggregate inside its own bounded context.
 *
 * Domain events are persisted to `ddd_domain_event` in the same transaction as the state change that
 * produced them, and are free to evolve with the domain — their stored form is versioned and migrated
 * by a [DataSerializationContext]. To share an event with other bounded contexts, map it to a
 * [PublicDomainEvent] through a [com.dreweaster.ddd.contract.PublicEventContract].
 */
interface DomainEvent
