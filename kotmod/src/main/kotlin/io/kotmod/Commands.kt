package io.kotmod

import kotlinx.serialization.KSerializer

/**
 * What a command function decides: accept the command with a new state and events, or reject it with one of
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
 * What one branch of [CommandHandlers.handler] does with the aggregate's current state (`null` if the aggregate
 * does not exist). Built with [CommandHandlers.on], [CommandHandlers.creates] or [CommandHandlers.any].
 */
class CommandHandler<S : Any, out E : DomainEvent, out R : Any>
    @PublishedApi
    internal constructor(
        internal val decide: suspend (S?) -> Outcome<S, E, R>,
    )

/**
 * The commands of one aggregate type, and the single place that routes each command to a function that decides it.
 * Decisions should be pure; they may suspend, but [AggregateManager.handle] runs a decision again on each conflict
 * retry, so anything it calls out to may be called more than once.
 *
 * Extend it with an `object` and implement [handler] as an exhaustive `when` over your sealed command type, one
 * line per command:
 *
 * ```
 * object PayoutCommands : CommandHandlers<Payout, PayoutCommand, PayoutEvent, PayoutRejection>(
 *     rejectionSerializer = PayoutRejection.serializer(),
 * ) {
 *     override fun PayoutCommand.handler() = when (this) {
 *         is Hold    -> creates(otherwise = { PayoutAlreadyExists }) { hold(amount) }
 *         is Release -> on<Held>(otherwise = { PayoutNotHeld }) { it.release(reference) }
 *     }
 * }
 * ```
 *
 * Every rejection is one of your own types, so callers can match on them exhaustively.
 *
 * @param rejectionSerializer serializes rejections, which are recorded so a repeated command id gets the same
 *   answer.
 */
abstract class CommandHandlers<S : Any, C : Any, E : DomainEvent, R : Any>(
    val rejectionSerializer: KSerializer<R>,
) {
    /** Routes this command to the function that decides it. */
    abstract fun C.handler(): CommandHandler<S, E, R>

    /**
     * Runs [block] when the current state is a [T]. Otherwise rejects with [otherwise], which receives the actual
     * state (`null` when the aggregate does not exist).
     */
    inline fun <reified T : S> on(
        noinline otherwise: (S?) -> R,
        noinline block: suspend (T) -> Outcome<S, E, R>,
    ): CommandHandler<S, E, R> =
        CommandHandler { state -> if (state is T) block(state) else Outcome.Reject(otherwise(state)) }

    /** Runs [block] only when the aggregate does not exist yet. Otherwise rejects with [otherwise]. */
    fun creates(
        otherwise: (S) -> R,
        block: suspend () -> Outcome<S, E, R>,
    ): CommandHandler<S, E, R> = CommandHandler { state -> if (state == null) block() else Outcome.Reject(otherwise(state)) }

    /** Runs [block] with the full current state (`null` when the aggregate does not exist), for commands valid in several states. */
    fun any(block: suspend (S?) -> Outcome<S, E, R>): CommandHandler<S, E, R> = CommandHandler(block)

    /**
     * Decides [command] against [state] (`null` when the aggregate does not exist) without touching the database.
     * Use it to unit-test your routing and `otherwise` mappings.
     */
    suspend fun decide(
        command: C,
        state: S?,
    ): Outcome<S, E, R> = command.handler().decide(state)
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
