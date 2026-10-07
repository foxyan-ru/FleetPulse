# FleetPulse observability roadmap

This is a short leadership artifact, not a backlog dump: it names the next
thing worth instrumenting, the next signal worth adding and why it isn't in
the initial three, and works through one genuinely hard operational
tradeoff -- rolling out a breaking protocol change to a fleet you can't force-
update -- in enough depth to actually be useful to someone building this for
real.

## What to instrument next

The current signals catalog (`docs/signals.md`) answers "is the client
there," "is its data getting through," and "is the thing it depends on up."
The next gap is **queue/worker saturation as a leading indicator**, not just
a health check. `fleetpulse_queue_backlog` and `fleetpulse_queue_in_flight`
already exist as raw gauges, but nothing today distinguishes "backlog is 500
because the fleet just had a burst of traffic and it'll drain in 30s" from
"backlog is 500 and growing because the worker pool is undersized or the
downstream dependency is degrading enrichment throughput." The next
instrumentation step is a **backlog growth rate** (first derivative of
`fleetpulse_queue_backlog` over a rolling window) and a **worker
utilization** ratio (time spent processing vs. time spent idle-polling per
worker), so an on-call engineer can tell "transient burst" from "systemic
capacity problem" from the dashboard alone, before it turns into a
heartbeat-freshness incident three minutes later.

## The next signal to add

**Per-cohort error rate** (`error` events / total events, by cohort) is the
strongest candidate for signal #4. It wasn't included in the initial three
because it measures something qualitatively different from the others:
freshness and completeness are both about *absence* (did expected data show
up), while an error-rate signal is about the *content* of what did show up,
and content-based signals need a classification step (which errors are
expected/benign vs. which indicate a real regression) before a raw rate is
actionable -- a naive "error events per minute" metric would just as easily
fire on a benign, expected error as a real one. Doing that classification
honestly is enough work that it deserved to be scoped out of an initial
three-signal catalog rather than included half-done. The right next step is
a small taxonomy of error categories (client-recoverable vs.
requires-reinstall vs. unknown) attached to the `error_message` field, then
a completeness-style ratio per category per cohort.

## Rolling out a breaking client-protocol change without breaking installed
## versions you can't force-update

This is a real constraint of installed desktop/extension software, and it's
worth being concrete about the approach rather than waving at "versioning."

The ingestion endpoint's contract with the fleet is the `RawEvent` JSON
shape (`internal/ingest.RawEvent`) plus whatever the client considers a
successful response. Any change that isn't purely additive -- renaming a
field, changing a field's type or semantics, tightening validation that used
to be lenient -- is a breaking change to that contract, and a meaningful
fraction of the installed fleet will still be sending the old shape for
however long it takes users to update (which, for desktop software without
forced auto-update, can be measured in months, not days; the version
distribution baked into the simulator's `weightedVersion` -- most of the
fleet on the newest stable release, a real tail on the previous two -- is
exactly this situation in miniature).

The approach: **the server supports both shapes for as long as the old
shape has non-trivial traffic, and the decision to stop is data-driven, not
calendar-driven.**

1. **Version the wire contract, not just the client build.** Add an explicit
   `schema_version` field to `RawEvent` (defaulting to `1` for clients that
   don't send it, which is every client that exists today) rather than
   inferring the shape from the client's `version` field -- app version and
   wire-schema version are different axes that will drift independently
   over the product's life, and conflating them is how you end up unable to
   ship a protocol fix without also shipping an app release.
2. **`Normalize()` becomes shape-aware, not the transport layer.** The
   ingestion handler keeps accepting the same batched-JSON-array-or-object
   shape at the transport level; `internal/ingest.Normalize` dispatches on
   `schema_version` internally and maps both old and new payload shapes to
   the same canonical `Event` type. This keeps the queue, worker pool, and
   signals computer completely unaware that a migration is happening at
   all -- they only ever see `Event`, so the blast radius of the migration
   is contained to one function.
3. **New clients opt into the new shape; old clients are never forced.**
   The new schema version ships in new client builds going forward. Old
   installed clients keep sending `schema_version: 1` indefinitely -- there
   is no server-side deadline that rejects it, because rejecting it doesn't
   make the old client send anything different, it just turns "still on an
   old version" into "invisible to the fleet-health signals," which is
   worse than the problem it solves.
4. **Deprecation is driven by `fleetpulse_ingest_events_accepted_total{schema_version=...}`,
   not a calendar date.** Track schema-version share as its own metric.
   Sunset planning starts once `schema_version: 1` drops below some small
   threshold (say 1% of accepted events, sustained) of total fleet traffic
   -- at that point continuing to support it costs real engineering
   attention (every ingest change now needs testing against two shapes) for
   a shrinking, low-value tail, which is when a genuine sunset
   conversation -- support ending in an announced release, not an ingest
   rejection -- becomes the right call rather than a unilateral server-side
   cutoff.
5. **Never let "make the migration easier" justify silently coercing old
   data to look like new data.** If old and new schema versions mean
   materially different things for the same field, normalize them to
   distinct, honestly-labeled `Event` fields rather than best-effort
   coercing the old value into the new field's shape -- a coerced value that
   is subtly wrong is far more dangerous to signal accuracy than a visibly
   separate `schema_version` label, because it silently corrupts every
   signal downstream instead of being an obvious, debuggable seam.

The throughline: the fleet doesn't get to be forced onto the new contract,
so the server has to be the thing that bends, for as long as real traffic on
the old contract says it needs to. The sunset happens because the data says
it's safe, not because a migration deadline was set in advance.
