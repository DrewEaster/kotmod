# Task 4 report: use-case runtime handles triggers

## Implemented
`UseCaseRuntime`, `UseCaseItem`, `TriggerItem` exactly per the brief (timeout via withTimeout -> ReactionTimeoutException, onFailure policy, onCompletion, notBefore waiting, cancellation rethrown, undecodable trigger rethrown to queue). Test helper `ManualQueues.Published.notice()` added.

## TDD
RED: `./gradlew :kotmod:test --tests 'io.kotmod.reaction.UseCaseRuntimeTest'` failed to compile (Unresolved reference 'UseCaseRuntime', 'TriggerItem', 'start', 'publish').
GREEN: after the runtime, 23/24 of `io.kotmod.reaction.*` passed; 1 failed (see deviation); after the fix `io.kotmod.reaction.*` and `./gradlew test` both BUILD SUCCESSFUL, no new compiler warnings.

## Files
- kotmod/src/main/kotlin/io/kotmod/reaction/UseCaseRuntime.kt (new)
- kotmod/src/test/kotlin/io/kotmod/reaction/UseCaseFixtures.kt (notice())
- kotmod/src/test/kotlin/io/kotmod/reaction/UseCaseRuntimeTest.kt (new)

## Deviation
The test "onFailure returning GiveUp ..." asserted `GaveUp(boom)` by equality, but the error reaching onFailure/onCompletion is a copy of `boom` (kotlinx.coroutines stack-trace recovery under -ea re-creates exceptions crossing withTimeout), so data-class equality failed though the printed values were identical. Runtime unchanged; the test now asserts the notice, GaveUp type, exception type and message. Behaviour identical.

## Self-review / concerns
- Properties `ordering`/`timeout` are read only at runtime (constructor reads `useCase.ordering` after the use case is fully constructed, which is fine).
- Consumers who compare errors by identity in onFailure/onCompletion may see a copy in debug mode; the original is its cause-less copy with the same message. Minor.

## Fix round 1
- runTrigger now catches handle's exception inside `withTimeout` and returns it as a value; only TimeoutCancellationException (-> ReactionTimeoutException) crosses the boundary, other CancellationExceptions rethrown unchanged. Exception identity is preserved.
- Test: restored the brief's `GaveUp(boom)` equality assertion and added `assertSame(boom, failures.first().second)`.
- RED: with the old runtime, the restored assertions failed (1 of 24 failed). GREEN: after the fix `io.kotmod.reaction.*` and `:kotmod:test` pass.
- KDoc on `Reactions.timeout` notes that an inner app `withTimeout` TimeoutCancellationException escaping handle is also reported as ReactionTimeoutException.
