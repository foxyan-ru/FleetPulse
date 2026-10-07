# FleetPulse

Observability and ingestion muscle for a fleet of desktop-app /
browser-extension installs you don't control the internals of -- clients
"phone home" with heartbeats, capture events, and errors; FleetPulse ingests,
validates, queues, processes, and turns that into three concretely-defined
reliability signals with real thresholds, demoed against one fully-worked
failure scenario.

> **Scaffold status:** this repository was generated as a portfolio scaffold.
> Every file is real, substantive starter code, but **nothing in this repo
> has been built, run, or tested.** No `go build`, `go run`, `go mod tidy`,
> or `docker compose up` has been executed. Treat every command below as
> "the command that would be run next," not as something already verified to
> work. `go.sum` does not exist yet because `go mod tidy` hasn't run --
> `go.mod`'s dependency versions are hand-authored best guesses, not a
> resolved graph.

## Problem statement

A fleet of installed clients you don't control (can't force-update, can't
SSH into, can't instrument beyond what you shipped) is phoning home. The
questions that matter operationally are simple to state and hard to answer
well:

- Is a given slice of the fleet still alive?
- Is the data it's sending actually getting through completely?
- Is the service downstream of ingestion, that the data has to flow through
  to be useful, actually up?

FleetPulse answers all three with a small number of precisely-defined
signals (see `docs/signals.md`) computed from real ingested traffic, rather
than either skipping the definition work or building an unbounded
"observability platform." The fleet itself is simulated (`cmd/simulator`) --
see **Scope** below for why that's the right tradeoff for this project.

## Scope (read this before wondering "where's the real client?")

This project deliberately does **not** build a real desktop app or browser
extension. The fleet is simulated by `cmd/simulator` spinning up N concurrent
goroutines, each a fixed-cohort fake client emitting realistic traffic over
HTTP. That's a genuine, useful abstraction boundary: everything downstream of
the ingestion endpoint (validation, queueing, worker processing, signal
computation, dashboards) can't tell the difference between a simulated
client and a real one, so building the real client would add engineering
effort without adding anything the observability stack needs to prove.

Similarly, this project fully demos **one** failure scenario end-to-end (a
cohort silently going dark -- see `docs/runbook.md`) rather than
shallowly touching several. That's a deliberate scope choice: one scenario
walked all the way from "what the dashboard shows" to "how you'd trace it to
a cause" is a stronger signal than three scenarios that only get as far as
"and then an alert would fire."

## Architecture

See `docs/architecture.md` for the full request/data-flow diagram (ASCII and
Mermaid) and component table. Short version:

```
simulator --> ingestion (validate/normalize) --> queue --> worker pool (enrich, retry)
                                                              |            |
                                                       client state    signals computer --> /metrics --> Prometheus --> Grafana
                                                       (state store)         ^
                                                                             |
                                                                 downstream stub (Workflow Mapping)
```

The queue and client-state store each have two implementations behind the
same interface: an in-process one (buffered channel / in-memory map) for
zero-dependency local development, and a LocalStack/real-AWS one (SQS /
DynamoDB, in `internal/aws`) for the one real AWS-shaped deployment used to
record a demo video. See `internal/queue`'s and `internal/state`'s package
docs for the tradeoffs.

## Repository layout

```
cmd/
  server/       ingestion HTTP server + worker pool + /metrics (wires everything together)
  simulator/    fake fleet client generator
  downstream/   "Workflow Mapping" dependency stub, with on-demand failure injection
internal/
  ingest/       wire schema (RawEvent), validation/normalization, HTTP handler
  queue/        Queue interface + in-memory (buffered channel + visibility timeout) implementation
  worker/       worker pool: enrichment, retry/dead-letter, ack/nack against the queue
  signals/      computes + exposes the three cataloged signals as Prometheus metrics
  state/        per-client last-seen store, Store interface + in-memory implementation
  aws/          LocalStack/real-AWS-targeting SQS queue + DynamoDB store implementations
  simulator/    fleet simulator implementation (cohorts, per-client goroutine loop, kill-cohort timer)
  downstreamclient/  HTTP client the worker pool uses to call cmd/downstream
docs/
  signals.md        the signals catalog -- read this first
  architecture.md   data-flow diagram + LocalStack bootstrap commands
  roadmap.md        what to instrument next + a real protocol-versioning tradeoff writeup
  runbook.md        the silent-cohort-failure demo script, end to end
deploy/
  prometheus/       scrape config
  grafana/          datasource + dashboard provisioning
docker-compose.yml  server, simulator, downstream, localstack, prometheus, grafana
```

## Setup (commands that WOULD be run -- none have been executed yet)

```bash
# 1. Resolve and download dependencies (go.mod was hand-authored; this has
#    not been run, so version pins may need adjusting once it is).
go mod tidy

# 2. Sanity-check everything compiles.
go build ./...

# 3a. Run the ingestion server + worker pool locally (in-memory queue/state,
#     no external dependencies required).
go run ./cmd/server

# 3b. In another terminal, run the downstream Workflow Mapping stub.
go run ./cmd/downstream

# 3c. In another terminal, run the fake fleet against the local server.
go run ./cmd/simulator --clients=300 --ingest-url=http://localhost:8080/v1/events

# 4. Check the signals.
curl http://localhost:8080/metrics | grep fleetpulse_

# 5. Or bring up the whole stack (server, simulator, downstream, LocalStack,
#    Prometheus, Grafana) via Docker Compose -- also not run/verified yet.
docker compose up --build
```

Grafana would be reachable at `http://localhost:3000` (default provisioned
dashboard: "FleetPulse Overview") and Prometheus at
`http://localhost:9090`.

### Running against LocalStack / AWS instead of in-memory

```bash
export FLEETPULSE_QUEUE_BACKEND=sqs
export FLEETPULSE_STATE_BACKEND=dynamodb
export FLEETPULSE_SQS_QUEUE_URL=http://localhost:4566/000000000000/fleetpulse-events
export FLEETPULSE_DYNAMODB_TABLE=fleetpulse-client-state
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

go run ./cmd/server
```

This requires the LocalStack queue/table to already exist -- see
`docs/architecture.md`'s bootstrap commands. Not run as part of this
scaffold.

## The silent-cohort-failure demo

This is the project's single fully-worked demo scenario:

```bash
go run ./cmd/simulator \
  --clients=300 \
  --ingest-url=http://localhost:8080/v1/events \
  --kill-cohort=2.6.0-beta \
  --kill-cohort-after=2m
```

Two minutes in, every simulated client on version `2.6.0-beta` goes
completely silent -- no heartbeats, no captures, nothing -- exactly as a real
cohort would if a bad update broke its networking. `docs/runbook.md` walks
through what the dashboard shows, how to confirm it's a real incident rather
than a false alarm, and how you'd trace it down to a cause, step by step.

## Signals

Three signals, each with a precise formula and a stated target threshold:
**heartbeat freshness**, **capture completeness**, and **mapping
availability**. Full definitions, rationale, and thresholds are in
`docs/signals.md` -- per the project's own planning notes, that document is
meant to do more work proving seniority than any single file of code here.

## What's intentionally not built

- A real desktop app or browser extension (see **Scope** above).
- More than one fully-demoed failure scenario.
- The Temporal/Kinesis/SageMaker/ECS-scale version of this idea that a
  broader draft of this project considered -- deliberately out of scope for
  a portfolio-sized scaffold; see `docs/roadmap.md` for what a next
  iteration would prioritize instead.
- An admin HTTP endpoint to unsilence a cohort at runtime (referenced as a
  gap in `docs/runbook.md`).
