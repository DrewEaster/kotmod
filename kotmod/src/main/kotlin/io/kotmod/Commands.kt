package io.kotmod

/**
 * What a state decides about a command: accept the command with a new state and events, or reject it with one of
 * the aggregate's own rejection types. Build one with [accept] or [reject].
 *
 * @param S the aggregate's state type.
 * @param E the aggregate's domain event type.
 * @param R the aggregate's rejection type.
 */
sealed interface Outcome<out S, out E : DomainEvent, out R> {
    /** The command is accepted: the aggregate becomes [state] and [events] are recorded, in order. */
    data class Accept<out S, out E : DomainEvent>(
        val state: S,
        val events: List<E>,
    ) : Outcome<S, E, Nothing>

    /** The command is rejected with [rejection]. Nothing changes, except that the rejection is recorded. */
    data class Reject<out R>(
        val rejection: R,
    ) : Outcome<Nothing, Nothing, R>
}

/** Accepts a command: the aggregate becomes [state] and [events] are recorded, in order. */
fun <S, E : DomainEvent> accept(
    state: S,
    vararg events: E,
): Outcome<S, E, Nothing> = Outcome.Accept(state, events.toList())

/** Rejects a command with [rejection], one of the aggregate's own rejection types. */
fun <R> reject(rejection: R): Outcome<Nothing, Nothing, R> = Outcome.Reject(rejection)

/**
 * A behavioural position of an aggregate that decides the commands it receives. Implement it on your sealed state
 * type, and decide every command in [handle]: accept it with [accept], or reject it with [reject].
 *
 * Write the `when` over your sealed command type without an `else`, so adding a command doesn't compile until every
 * state has decided what to do with it. Decisions should be pure; they may suspend, but [AggregateManager.handle]
 * runs a decision again on each conflict retry, so anything it calls out to may be called more than once.
 *
 * @param S the aggregate's state type (your sealed state type itself).
 * @param C the aggregate's command type.
 * @param E the aggregate's domain event type.
 * @param R the aggregate's rejection type.
 */
interface AggregateState<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any> {
    /** Decides [command] in this state: accept it with a new state and events, or reject it. */
    suspend fun handle(command: C): Outcome<S, E, R>
}

/**
 * Decides commands for an aggregate that doesn't exist yet. Accepting a command here creates the aggregate.
 * It is not one of the aggregate's stored states, so your [Repository] never saves or loads it.
 *
 * Its decisions should be pure too (see [AggregateState]). If a concurrent create wins, [AggregateManager.handle]
 * retries and the stored state decides the command instead.
 *
 * @param S the aggregate's state type (your sealed state type).
 * @param C the aggregate's command type.
 * @param E the aggregate's domain event type.
 * @param R the aggregate's rejection type.
 */
interface InitialState<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any> {
    /** Decides [command] for an aggregate that doesn't exist yet: accepting it creates the aggregate. */
    suspend fun handle(command: C): Outcome<S, E, R>
}

/** What [AggregateManager.handle] returns. */
sealed interface CommandResult<out S, out R> {
    /** The command was accepted; [state] is the aggregate's state (its current state, for a repeated command id). */
    data class Accepted<out S>(
        val state: S,
    ) : CommandResult<S, Nothing>

    /** The command was rejected with [rejection]. */
    data class Rejected<out R>(
        val rejection: R,
    ) : CommandResult<Nothing, R>
}
