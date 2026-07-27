# Upstream Cancellation and H3 State Safety Design

## Problem

Hedged primary attempts are currently detached Tokio tasks. When one attempt
succeeds, the primary budget expires, or the caller drops the resolution
future, the remaining attempts continue until their own resolver timeout.
They can emit late failure logs, update upstream statistics after fallback has
answered, and continue mutating shared connection state.

H3 requests also clear the cached connection unconditionally after an error.
A request using an old connection can therefore fail after another request has
installed a healthy replacement and erase that replacement.

## Goals

- Cancel every losing hedge immediately when any primary attempt succeeds.
- Cancel every hedge when all attempts fail, the primary budget expires, or the
  caller cancels the primary resolution future.
- Prevent canceled attempts from recording late failures or changing H3 state.
- Allow an H3 request to invalidate only the connection generation it used.
- Preserve the existing hedge schedule, fallback behavior, and one reconnect
  retry within an H3 request.

## Non-Goals

- Changing configured timeout values or hedge intervals.
- Disabling hedging when only one primary member exists.
- Adding HTTP/3 phase-specific diagnostics.
- Implementing multi-address Happy Eyeballs.
- Changing H2 connection invalidation in this change.

## Hedged Attempt Lifecycle

`HedgedResolver::resolve` will own its attempts in a Tokio `JoinSet` rather
than launching untracked tasks. The first attempt starts immediately. Further
attempts start at the existing `interval` cadence, and each attempt still
re-enters the inner resolver and its weighted member selection.

The resolver retains the existing completion rules:

- Return the first successful response.
- Return the final error once no attempts remain in flight.
- Return the existing budget error when `max_wait` expires.

Before each explicit return, the resolver aborts all remaining attempts and
drains their join results. If the resolver future itself is dropped, dropping
the `JoinSet` aborts all contained tasks. This makes the parent resolution
future the owner of all primary work.

An aborted attempt is dropped while awaiting the inner resolver. It does not
reach `UpstreamGroup::try_member`'s error-recording branch, so it does not
produce an upstream failure log or failure sample. Ordinary resolver failures
that complete before cancellation retain their current logging and metrics.

## H3 Connection Generations

`H3Conn` will assign a monotonically increasing generation to each connection
installed in its cached state. `H3State` stores the generation together with
the Quinn connection handle and H3 sender. A request records the generation
when it clones the sender.

After a request-level transport error, it conditionally invalidates the cache:

- If the cached generation still equals the request's generation, clear it.
- If the cache is empty or contains a newer generation, leave it unchanged.

The request then performs its existing second attempt. On that attempt it uses
whatever state is current: a concurrently installed healthy connection can be
reused, while an empty state causes a new connection to be established.

Generation allocation and state installation occur while holding the existing
state mutex. A checked increment is unnecessary for practical operation; a
wrapping `u64` counter is sufficient because equality is only used to reject
stale invalidation, and wrapping would require installing every possible
generation during the lifetime of one still-running request.

Application-level HTTP status errors retain the existing invalidation behavior
for now. Narrowing invalidation to transport-only errors is outside this fix.

## Error Handling

- A canceled hedge is control flow, not an upstream failure.
- A task panic or unexpected join cancellation before resolver-directed abort
  is converted into an error and handled like an attempt failure. Other
  attempts may still succeed.
- Budget errors retain the current message and in-flight count.
- H3 failures retain their existing error context and reconnect-once behavior.

## Testing

Hedging tests will use instrumented resolvers and short deterministic waits to
verify:

- A successful attempt immediately drops all losing attempts.
- Reaching the budget immediately drops every in-flight attempt.
- Dropping the outer resolution future drops every in-flight attempt.
- Completed ordinary failures still return or allow another attempt to win.

H3 tests will verify these invariants:

- A stale generation cannot clear a newer cached connection.
- A failure on the current generation still clears that generation.
- Existing reconnect-after-server-close and H3 round-trip behavior remains
  intact.

Verification will include targeted upstream tests followed by `cargo fmt --all
-- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and
`cargo test --all-targets --all-features`.
