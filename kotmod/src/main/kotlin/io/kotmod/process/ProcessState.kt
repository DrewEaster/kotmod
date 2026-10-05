package io.kotmod.process

import io.kotmod.DomainEvent
import io.kotmod.RequestedCommand
import kotlin.time.Instant

/**
 * A behavioural position of a process manager, deciding the inputs it receives. Implement it on your sealed process
 * state type and decide every input in [handle]: [transition] to a new state (recording facts, requesting commands and
 * scheduling inputs), or [ignore] it. Inputs are facts, so they can't be rejected.
 *
 * Write the `when` over your sealed input type without an `else`, so adding an input doesn't compile until every state
 * has decided what to do with it. Decisions should be pure; they may suspend, but a decision runs again if its write
 * loses a race.
 *
 * @param S the process's state type (your sealed state type itself).
 * @param I the process's input type.
 * @param E the process's own domain event type.
 */
interface ProcessState<S : ProcessState<S, I, E>, I : Any, E : DomainEvent> {
    /** Decides [input] in this state. */
    suspend fun handle(input: I): ProcessOutcome<S, E, I>
}

/**
 * Decides inputs for a process that doesn't exist yet: [transition] starts it, [ignore] leaves it unstarted (the input
 * is still recorded, so a redelivery is recognised). Your repository never stores it.
 */
interface ProcessInitialState<S : ProcessState<S, I, E>, I : Any, E : DomainEvent> {
    /** Decides [input] for a process that doesn't exist yet. */
    suspend fun handle(input: I): ProcessOutcome<S, E, I>
}

/** What a process state decides about an input. Build it with [transition] or [ignore]. */
sealed interface ProcessOutcome<out S, out E : DomainEvent, out I> {
    /**
     * Move to [state], record [events] in the process's own stream, request [commands] and [schedule] inputs, all in
     * one transaction. The commands and scheduled inputs are carried out later, asynchronously.
     */
    data class Transition<out S, out E : DomainEvent, out I>(
        val state: S,
        val events: List<E>,
        val commands: List<RequestedCommand<*>>,
        val schedule: List<ScheduledInput<I>>,
    ) : ProcessOutcome<S, E, I>

    /** Nothing changes; the input is recorded so a redelivery is recognised. */
    data object Ignore : ProcessOutcome<Nothing, Nothing, Nothing>
}

/** An input the process sends to itself at [at], such as a timeout. */
data class ScheduledInput<out I>(
    val input: I,
    val at: Instant,
)

/** Moves the process to [state], recording [events], requesting [commands] and scheduling [schedule]. */
fun <S, E : DomainEvent, I> transition(
    state: S,
    events: List<E> = emptyList(),
    commands: List<RequestedCommand<*>> = emptyList(),
    schedule: List<ScheduledInput<I>> = emptyList(),
): ProcessOutcome<S, E, I> = ProcessOutcome.Transition(state, events, commands, schedule)

/** Ignores the input: nothing changes. */
fun ignore(): ProcessOutcome<Nothing, Nothing, Nothing> = ProcessOutcome.Ignore

/** Schedules [input] to be delivered to this process instance at [at]. */
fun <I> schedule(
    input: I,
    at: Instant,
): ScheduledInput<I> = ScheduledInput(input, at)
