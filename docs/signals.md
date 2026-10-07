# FleetPulse signals catalog

This document defines the three signals FleetPulse computes from the
simulated fleet's telemetry, and is the artifact this project treats as more
important than any single line of code: choosing the wrong signal, the wrong
window, or the wrong denominator produces a dashboard that *looks*
authoritative while quietly lying to you. Everything below is implemented in
`internal/signals/` and exposed as Prometheus metrics at `/metrics`; this doc
is the "why", the code is the "how".

Every signal is a **ratio in `[0, 1]`**, labeled by cohort (`version`, `os`,
`region`) wherever the label makes the signal more actionable, so a
degradation in one narrow slice of the fleet is visible as *that slice*
dropping rather than being diluted into an aggregate number that barely
moves.

---

## 1. Heartbeat freshness

**What it answers:** "Is this cohort even still there?" This is the
foundational fleet-health signal -- if a client isn't heartbeating, nothing
else it reports (capture completeness, error rates) can be trusted, because
you have no way to distinguish "this cohort is healthy and just isn't doing
anything interesting" from "this cohort's networking silently broke."

**Metric name:** `fleetpulse_heartbeat_freshness_ratio{version, os, region}`

**Formula:**

```
heartbeat_freshness(cohort) = |{ clients in cohort : now - last_heartbeat_at <= 60s }|
                               ------------------------------------------------------
                               |{ clients in cohort }|
```

`last_heartbeat_at` is tracked per client in `internal/state` (in-memory map,
or DynamoDB in the AWS-shaped deployment) and updated every time a
`heartbeat` event is successfully processed by the worker pool.

**Why 60 seconds, specifically:** the simulator's default heartbeat interval
is 30 seconds (`internal/simulator.DefaultConfig`). A 60-second window is
exactly 2x that interval -- long enough to absorb one missed beat from
ordinary network jitter or a delayed queue redelivery without false-alarming,
short enough that a genuinely silent client shows up as stale within at most
two of its own heartbeat periods. A real fleet's window should be derived the
same way: `2 * p99(heartbeat_interval)` for whatever interval your actual
client SDK uses, not a round number picked for aesthetics.

**Target threshold:** **>= 99.0%** per cohort, sustained. Below that,
something is actively wrong for real clients, not just simulator noise.

- **Warning** at `< 99.0%` for 5 minutes: investigate, not yet page-worthy.
- **Page** at `< 95.0%` for 5 minutes, OR at `< 50%` for any single cohort
  representing more than 1% of the fleet for 2 minutes -- the second
  condition exists specifically so a small-but-total cohort outage (the
  silent-cohort-failure scenario this project demos, see
  `docs/runbook.md`) isn't masked by a healthy aggregate. A beta cohort that
  is 8% of the fleet going to 0% freshness would barely move an
  aggregate-only number, which is exactly the failure mode per-cohort
  labeling exists to catch.

**Known failure modes of this signal itself** (worth stating explicitly,
because a signal that can lie about its own health is worse than no signal):
a client with a badly wrong clock could report a heartbeat timestamp far in
the future, which `internal/ingest.Normalize` rejects outright (events more
than 5 minutes ahead of server receive time are dropped as malformed) so
they can't artificially inflate freshness. A client that *stops* heartbeating
correctly decays this ratio toward 0 over the 60s window, which is the
intended behavior.

---

## 2. Capture completeness

**What it answers:** "Of the data a client says it was going to send for a
session, how much of it actually arrived?" Heartbeat freshness tells you a
client is alive; this tells you whether what it's alive to say is actually
getting through -- these are genuinely different failure modes (a client can
heartbeat perfectly fine while its capture upload path is silently dropping
fragments due to, say, a flaky local buffer or a partial network
partition).

**Metric name:** `fleetpulse_capture_completeness_ratio{version, os, region}`

**Formula:**

```
capture_completeness(cohort) = mean over sessions in cohort of:
                                   |distinct sequence_numbers received for session|
                                   -----------------------------------------------
                                   expected_event_count (as declared by the client)
```

Each `capture` event carries `session_id`, `sequence_number`, and
`expected_event_count` (see `internal/ingest.RawEvent`); `internal/signals`
tracks, per session, the set of distinct sequence numbers seen so far
(duplicates from a queue redelivery don't inflate the count -- it's a set,
not a counter) and divides by the client-declared expectation once the
session is being scored. Session-level ratios are averaged per cohort on
every computation tick (default: every 10s), not accumulated forever, so a
long-running fleet's completeness reflects *recent* sessions, not a lifetime
average that a bad month can never recover from. Session progress records
older than 30 minutes are pruned.

**Why the client's own declared `expected_event_count` is the denominator,
not a server-side guess:** the alternative -- inferring how many fragments a
session "should" have from historical averages -- silently absorbs a client-
side regression (e.g. a new build that starts splitting sessions into fewer,
larger fragments) into the baseline instead of surfacing it. Trusting the
client's own declaration means a completeness drop is unambiguously "the
client said N and we got less than N," not "this session looked smaller than
usual for reasons we're inferring."

**Target threshold:** **>= 97.0%** per cohort. Some fragment loss under
imperfect networks is expected and not actionable (hence not 100%); this
project's simulator injects an 8% fragment-loss rate by default
(`CaptureLossRate` in `internal/simulator.Config`) specifically to keep this
number realistically below 100% in the demo rather than pinned at a value
that would hide a regression.

- **Warning** at `< 97.0%` for 15 minutes (capture completeness is noisier
  session-to-session than heartbeat freshness, so it gets a longer
  evaluation window before alerting).
- **Page** at `< 90.0%` for 15 minutes for any cohort above 1% of fleet
  session volume.

---

## 3. Mapping availability

**What it answers:** "Is the downstream service our processing pipeline
depends on actually up?" This is the one signal in the catalog that isn't
about the fleet at all -- it's about a dependency the fleet's data has to
flow through before it's useful (the "Workflow Mapping" step, modeled here
by the standalone `cmd/downstream` stub service that the worker pool's
enrichment step calls for every `capture` event). It's included in the same
catalog as the fleet-health signals because, in practice, a mapping-
availability incident and a capture-completeness incident look identical
from a support ticket ("data isn't showing up") and need to be
distinguishable from the dashboard in seconds, not after a code read.

**Metric name:** `fleetpulse_mapping_availability_ratio` (unlabeled --
there is exactly one downstream dependency in this scaffold; a real
deployment with multiple downstream services would label this by
`dependency`).

**Formula:**

```
mapping_availability = successful_calls / total_calls, over a trailing 5-minute window
```

Implemented as a small ring-buffer-style tracker (`internal/signals/mapping.go`)
that records a success/failure boolean for every call the worker pool's
enrichment step makes to `cmd/downstream`, and reports the ratio over the
last 5 minutes of samples. A **trailing window**, not an all-time ratio, was
chosen deliberately: an all-time success rate takes a very long time to
visibly recover after a real outage ends (a 10-minute outage after a week of
100% uptime barely dents an all-time average), which is exactly backwards
from what you want during incident response -- you want the signal to
recover promptly once the fix lands, so "is it fixed yet" has a fast, clear
answer.

**Target threshold:** **>= 99.5%** over the trailing window. `cmd/downstream`
exposes a `POST /admin/config` endpoint to inject failures/latency/hard
outages on demand specifically so this threshold can be demoed being
crossed and recovered from.

- **Warning** at `< 99.5%` for 3 minutes.
- **Page** at `< 90%` for 2 minutes, or immediately if the ratio hits `0%`
  (a hard downstream outage should never wait out a multi-minute evaluation
  window before paging).

**Edge case, stated explicitly:** with zero calls in the trailing window
(e.g. right after process startup, before any capture events have been
processed), this reports `1.0` rather than `0.0`. "No data yet" and "every
call failed" are different situations and conflating them would make a
freshly started process falsely page on startup.

---

## Why these three and not more

The planning notes behind this project explicitly call out that this catalog
"does more to prove seniority than the code does" -- the discipline here is
in stopping at three well-justified signals with real thresholds, rather
than producing a sprawling list of metrics with no stated target and no
reasoning for the window chosen. `docs/roadmap.md` names the next signal
that would be worth adding and why it isn't in this initial three.
