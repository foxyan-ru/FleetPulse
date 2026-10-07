| FleetPulse: Complete Architectural Analysis |  |  |  |  |
| --- | --- | --- | --- | --- |
| Real-World Grounding |  |  |  |  |
| Industry Analogy: Imagine Grammarly — a browser extension and desktop app installed on millions of endpoints across Windows, macOS, and Linux. Every install must "phone home" with heartbeats ("I'm alive"), screen-capture diagnostics ("my capture pipeline is healthy"), and error reports ("something broke"). FleetPulse is the server-side observability backbone that ingests this telemetry, processes it through a queue-backed worker pool, and distills it into three reliability signals that tell engineering "the fleet is healthy" or "version 2.6.0-beta just went dark on macOS in EU." |  |  |  |  |
| Tech Stack: Go 1.24 · net/http stdlib · AWS SQS + DynamoDB (via LocalStack) · Prometheus + Grafana · Docker Compose (6 services) · ~3,300 lines across 23 Go source files, 8 internal packages, 3 binaries. |  |  |  |  |
| System-Wide Architecture Overview |  |  |  |  |
| ┌─────────────────────────────────────────────────────────────────────────────────┐ |  |  |  |  |
| │                          FleetPulse System Boundary                            │ |  |  |  |  |
| │                                                                                 │ |  |  |  |  |
| │  ┌──────────────┐     POST /v1/events    ┌──────────────────────────────────┐   │ |  |  |  |  |
| │  │  Simulator   │ ─────────────────────► │  Ingestion Server (:8080)       │   │ |  |  |  |  |
| │  │  (300 fake   │    JSON batch          │                                  │   │ |  |  |  |  |
| │  │   clients)   │                        │  ┌────────┐    ┌─────────────┐  │   │ |  |  |  |  |
| │  └──────────────┘                        │  │ Handler │───►│  Normalize  │  │   │ |  |  |  |  |
| │        OR                                │  │ (HTTP)  │    │  & Validate │  │   │ |  |  |  |  |
| │  ┌──────────────┐                        │  └────┬───┘    └─────────────┘  │   │ |  |  |  |  |
| │  │ Real Client  │                        │       │                          │   │ |  |  |  |  |
| │  │ (Grammarly   │                        │       ▼                          │   │ |  |  |  |  |
| │  │  Extension)  │                        │  ┌─────────────────────────┐     │   │ |  |  |  |  |
| │  └──────────────┘                        │  │    Queue (Interface)    │     │   │ |  |  |  |  |
| │                                          │  │  ┌─────────┬─────────┐ │     │   │ |  |  |  |  |
| │                                          │  │  │InMemory │   SQS   │ │     │   │ |  |  |  |  |
| │                                          │  │  │ (dev)   │  (prod) │ │     │   │ |  |  |  |  |
| │                                          │  │  └─────────┴─────────┘ │     │   │ |  |  |  |  |
| │                                          │  └────────┬──────────────┘     │   │ |  |  |  |  |
| │                                          │           │                      │   │ |  |  |  |  |
| │                                          │           ▼                      │   │ |  |  |  |  |
| │                                          │  ┌─────────────────────┐        │   │ |  |  |  |  |
| │                                          │  │  Worker Pool (8x)   │        │   │ |  |  |  |  |
| │  ┌──────────────┐   POST /v1/workflow    │  │  ┌───────────────┐  │        │   │ |  |  |  |  |
| │  │  Downstream  │ ◄─────────────────────┤  │  │  Upsert State │  │        │   │ |  |  |  |  |
| │  │  Stub (:8090)│   -mapping             │  │  │  Enrich       │  │        │   │ |  |  |  |  |
| │  │  (Workflow   │                        │  │  │  Record Signal│  │        │   │ |  |  |  |  |
| │  │   Mapping)   │                        │  │  └───────────────┘  │        │   │ |  |  |  |  |
| │  └──────────────┘                        │  └─────────┬───────────┘        │   │ |  |  |  |  |
| │                                          │            │                     │   │ |  |  |  |  |
| │                                          │            ▼                     │   │ |  |  |  |  |
| │  ┌──────────────┐                        │  ┌─────────────────────┐        │   │ |  |  |  |  |
| │  │  Prometheus  │ ◄── GET /metrics ──────┤  │  Signals Computer   │        │   │ |  |  |  |  |
| │  │  (:9090)     │    (scrape 5s)         │  │  (tick every 10s)   │        │   │ |  |  |  |  |
| │  └──────┬───────┘                        │  │  · Freshness        │        │   │ |  |  |  |  |
| │         │                                │  │  · Completeness     │        │   │ |  |  |  |  |
| │         ▼                                │  │  · Mapping Avail.   │        │   │ |  |  |  |  |
| │  ┌──────────────┐                        │  └─────────────────────┘        │   │ |  |  |  |  |
| │  │   Grafana    │                        │                                  │   │ |  |  |  |  |
| │  │  (:3000)     │                        │  ┌─────────────────────────┐     │   │ |  |  |  |  |
| │  │  6-panel     │                        │  │  State Store (Interface)│     │   │ |  |  |  |  |
| │  │  dashboard   │                        │  │  ┌─────────┬──────────┐│     │   │ |  |  |  |  |
| │  └──────────────┘                        │  │  │InMemory │ DynamoDB ││     │   │ |  |  |  |  |
| │                                          │  │  │ (dev)   │  (prod)  ││     │   │ |  |  |  |  |
| │                                          │  │  └─────────┴──────────┘│     │   │ |  |  |  |  |
| │                                          │  └─────────────────────────┘     │   │ |  |  |  |  |
| │                                          └──────────────────────────────────┘   │ |  |  |  |  |
| └─────────────────────────────────────────────────────────────────────────────────┘ |  |  |  |  |
| WORKFLOW 1: Event Ingestion |  |  |  |  |
| 1. Plain-English Scenario |  |  |  |  |
| What happens in the real world: Grammarly's browser extension on a user's laptop fires a heartbeat every 30 seconds — "version 2.5.1 on macOS in EU is alive." Occasionally it sends a screen-capture diagnostic ("I started capture session ABC, sending fragment 3 of 8") or an error ("disk write failed"). These events arrive as JSON payloads at a single HTTP endpoint. |  |  |  |  |
|   |  |  |  |  |
| How FleetPulse handles it: The ingestion server receives the POST, decodes either a single event or a batch of up to 500, validates and normalizes each one (stripping v prefixes from versions, lowercasing OS names, rejecting future timestamps), publishes valid events to an internal queue, and returns {accepted: 3, rejected: 1, errors: [...]}. The HTTP response comes back immediately — the heavy processing happens later via background workers. This means even if the downstream systems are slow, the client never blocks. |  |  |  |  |
| 2. Markdown Documentation |  |  |  |  |
| Component: internal/ingest/handler.go — HTTP Handler for POST /v1/events |  |  |  |  |
| Aspect | Detail |  |  |  |
| Endpoint | POST /v1/events on :8080 |  |  |  |
| Input | Single RawEvent JSON object OR JSON array of up to 500 RawEvent objects |  |  |  |
| Output | JSON {accepted, rejected, errors} — 202 Accepted if any pass, 400 if all fail |  |  |  |
| Validation | Delegated to Normalize() in validate.go — see Workflow 2 |  |  |  |
| Queue Publish | Each valid event published individually via Publisher.Publish() |  |  |  |
| Batch Limit | 500 events per request (batchLimit constant at handler.go:46) |  |  |  |
| Content-Type | application/json response; detects single vs array input by sniffing first byte ([ vs {) |  |  |  |
| Key Design Decision: The handler returns 202 Accepted (not 200 OK) because events are enqueued for asynchronous processing, not processed synchronously. This is a deliberate HTTP semantics choice. |  |  |  |  |
| 3. System Design Diagrams |  |  |  |  |
| High-Level: Ingestion Boundary |  |  |  |  |
| ┌──────────────────────┐ |  |  |  |  |
| │    Client Fleet       │ |  |  |  |  |
| │  (300 goroutines OR   │ |  |  |  |  |
| │   real browser ext.)  │ |  |  |  |  |
| └──────────┬───────────┘ |  |  |  |  |
| │ |  |  |  |  |
| │  HTTP POST /v1/events |  |  |  |  |
| │  Content-Type: application/json |  |  |  |  |
| │  Body: [{...}, {...}] or {...} |  |  |  |  |
| ▼ |  |  |  |  |
| ┌──────────────────────────────────────────────┐ |  |  |  |  |
| │  net/http.ServeMux  (:8080)                  │ |  |  |  |  |
| │  ┌────────────────────────────────────────┐  │ |  |  |  |  |
| │  │  ingest.Handler.ServeHTTP()            │  │ |  |  |  |  |
| │  │                                        │  │ |  |  |  |  |
| │  │  1. Check method == POST               │  │ |  |  |  |  |
| │  │  2. decodeEvents(r) → []RawEvent       │  │ |  |  |  |  |
| │  │     ├─ sniff '[' → array               │  │ |  |  |  |  |
| │  │     └─ sniff '{' → single, wrap        │  │ |  |  |  |  |
| │  │  3. Reject if len==0 or len>500        │  │ |  |  |  |  |
| │  │  4. for each raw:                      │  │ |  |  |  |  |
| │  │     ├─ Normalize(raw, now())           │  │ |  |  |  |  |
| │  │     │   ├─ OK → publisher.Publish(evt) │  │ |  |  |  |  |
| │  │     │   │       metrics.IncAccepted()  │  │ |  |  |  |  |
| │  │     │   └─ Err → append to errors[]    │  │ |  |  |  |  |
| │  │     │           metrics.IncRejected()  │  │ |  |  |  |  |
| │  │  5. Return JSON {accepted, rejected}   │  │ |  |  |  |  |
| │  │     202 if accepted>0, 400 if all fail │  │ |  |  |  |  |
| │  └────────────────────────────────────────┘  │ |  |  |  |  |
| └──────────────────┬───────────────────────────┘ |  |  |  |  |
| │ |  |  |  |  |
| │ publisher.Publish(Event) |  |  |  |  |
| ▼ |  |  |  |  |
| ┌─────────────────┐ |  |  |  |  |
| │  Queue Backend  │ |  |  |  |  |
| │ (memory or SQS) │ |  |  |  |  |
| └─────────────────┘ |  |  |  |  |
| Low-Level: Object Interaction Sequence |  |  |  |  |
| Client            Handler              Normalize()          publisherAdapter      Queue |  |  |  |  |
| │                  │                     │                      │                │ |  |  |  |  |
| │  POST /v1/events │                     │                      │                │ |  |  |  |  |
| │  [{raw1},{raw2}] │                     │                      │                │ |  |  |  |  |
| │─────────────────►│                     │                      │                │ |  |  |  |  |
| │                  │                     │                      │                │ |  |  |  |  |
| │                  │ decodeEvents(r)     │                      │                │ |  |  |  |  |
| │                  │──── sniff byte ────►│                      │                │ |  |  |  |  |
| │                  │◄── []RawEvent ──────│                      │                │ |  |  |  |  |
| │                  │                     │                      │                │ |  |  |  |  |
| │                  │ Normalize(raw1, t)  │                      │                │ |  |  |  |  |
| │                  │────────────────────►│                      │                │ |  |  |  |  |
| │                  │◄──── Event ─────────│                      │                │ |  |  |  |  |
| │                  │                     │                      │                │ |  |  |  |  |
| │                  │ Publish(ctx, event) │                      │                │ |  |  |  |  |
| │                  │─────────────────────────────────────────►  │                │ |  |  |  |  |
| │                  │                     │                      │ Publish(evt)   │ |  |  |  |  |
| │                  │                     │                      │───────────────►│ |  |  |  |  |
| │                  │                     │                      │◄── nil ────────│ |  |  |  |  |
| │                  │◄──────────────────── nil ──────────────────│                │ |  |  |  |  |
| │                  │                     │                      │                │ |  |  |  |  |
| │                  │ Normalize(raw2, t)  │                      │                │ |  |  |  |  |
| │                  │────────────────────►│                      │                │ |  |  |  |  |
| │                  │◄── ValidationError ─│                      │                │ |  |  |  |  |
| │                  │                     │                      │                │ |  |  |  |  |
| │  202 Accepted    │                     │                      │                │ |  |  |  |  |
| │  {accepted:1,    │                     │                      │                │ |  |  |  |  |
| │   rejected:1,    │                     │                      │                │ |  |  |  |  |
| │   errors:[...]}  │                     │                      │                │ |  |  |  |  |
| │◄─────────────────│                     │                      │                │ |  |  |  |  |
| 4. Repository & Codebase Mapping |  |  |  |  |
| File | Key Symbol | Line | Purpose |  |
| internal/ingest/handler.go | Handler struct | 28-33 | Holds publisher, metrics, logger, now |  |
| internal/ingest/handler.go | NewHandler() | 35 | Constructor, injecting dependencies |  |
| internal/ingest/handler.go | ServeHTTP() | 53-104 | Main HTTP handler logic |  |
| internal/ingest/handler.go | decodeEvents() | 115-140 | Polymorphic JSON decode (array vs single) |  |
| internal/ingest/handler.go | Publisher interface | 15-17 | Narrow dependency: Publish(ctx, Event) error |  |
| internal/ingest/handler.go | Metrics interface | 22-25 | Counter hooks: IncAccepted, IncRejected |  |
| internal/ingest/handler.go | ingestResponse | 106-110 | Response DTO: {accepted, rejected, errors} |  |
| cmd/server/main.go | publisherAdapter | 119-125 | Wraps queue.Queue → ingest.Publisher |  |
| cmd/server/main.go | Route registration | 80 | mux.Handle("/v1/events", ingestHandler) |  |
| Where to add new features: |  |  |  |  |
| - New event types (e.g., "crash") → add to EventType constants in types.go:14-20 and Valid() at line 23-30, then add type-specific validation in validate.go:85-113. |  |  |  |  |
| - Request logging middleware → wrap mux in cmd/server/main.go:87 (currently no middleware exists). |  |  |  |  |
| - Rate limiting → add middleware wrapper before the mux at cmd/server/main.go:87. |  |  |  |  |
| 5. Underlying Architectural Principles |  |  |  |  |
| Principle | How It Manifests |  |  |  |
| Separation of Concerns | HTTP handling (handler.go) is completely separate from validation (validate.go), queueing (queue/), and processing (worker/). The handler knows nothing about SQS, DynamoDB, or signals. |  |  |  |
| Anti-Corruption Layer | RawEvent (untrusted, all strings) is transformed into Event (typed, validated) at the boundary. Everything downstream trusts Event implicitly. |  |  |  |
| Interface Segregation (SOLID - I) | Handler depends on Publisher (1 method) and Metrics (2 methods), not the full queue.Queue (4 methods). Minimal surface area. |  |  |  |
| Store-and-Forward | The queue decouples ingestion speed from processing speed. A slow downstream never blocks HTTP responses. |  |  |  |
| Idempotency-Readiness | The 202 Accepted response signals fire-and-forget semantics. Clients can safely retry on network failures — the queue provides at-least-once delivery. |  |  |  |
| 6. Gang of Four Design Pattern Analysis |  |  |  |  |
| Pattern Identified: Adapter (Structural) |  |  |  |  |
| The publisherAdapter struct at cmd/server/main.go:119-125 wraps queue.Queue (which has Publish, Receive, Metrics, Close) to satisfy ingest.Publisher (which has only Publish). This is a textbook Structural Adapter. |  |  |  |  |
| GoF 1994 vs Go 2024: The original Adapter pattern in C++ used class inheritance (class Adapter : public Target, private Adaptee). In Go, there are no classes or inheritance. Instead, the adapter is a struct embedding or wrapping the adaptee, and the target is an implicit interface. Go's implicit interface satisfaction means the adapter doesn't need to declare implements Publisher — it just needs a method with the right signature. This is duck-typed structural subtyping rather than nominal subtyping. |  |  |  |  |
| PM View: |  |  |  |  |
| Think of the Adapter like a universal power plug converter. Your laptop (the handler) has a two-prong plug (needs Publisher). The wall socket (the queue) has a three-prong outlet (exposes Queue). The adapter converts one shape to another. Why does this matter for business? It means the ingestion team can change the queue implementation (swap memory for SQS, or SQS for Kafka) without touching the HTTP handler code at all. New infrastructure integrations in days, not weeks, with zero risk to the ingestion API contract that clients depend on. |  |  |  |  |
| Junior Eng View: |  |  |  |  |
| Step-by-step execution: |  |  |  |  |
| 1. In cmd/server/main.go:80, the route is registered: mux.Handle("/v1/events", ingestHandler). |  |  |  |  |
| 2. ingestHandler was created at line 78 with ingest.NewHandler(publisherAdapter{q}, metrics, logger). |  |  |  |  |
| 3. publisherAdapter{q} is a struct that wraps the full Queue interface but only exposes Publish(). |  |  |  |  |
| 4. When Handler.ServeHTTP() calls h.publisher.Publish(ctx, evt) at line 86, it's calling publisherAdapter.Publish() at line 123, which delegates to q.Publish(ctx, evt). |  |  |  |  |
|   |  |  |  |  |
| Common mistake to avoid: Don't make Handler depend directly on queue.Queue. If you do, you'd need to stub all 4 methods in unit tests. With the narrow Publisher interface, you only stub Publish(). |  |  |  |  |
| Senior Eng View: |  |  |  |  |
| The adapter here is deliberately thin — a single struct with a single forwarding method. The trade-off is minimal: one extra type definition and one stack frame of indirection, both inlined by the Go compiler. The alternative would be making Handler accept queue.Queue directly (no adapter needed), but that would violate ISP and create a coupling between the ingestion package and the full queue lifecycle (Close, Receive, Metrics). This matters at scale because it means the ingest package can be tested, compiled, and reasoned about without importing any queue implementation. |  |  |  |  |
|   |  |  |  |  |
| A subtler point: Go's implicit interfaces mean this adapter pattern has zero ceremony cost. In Java, you'd need class PublisherAdapter implements Publisher { private final Queue queue; ... }. In Go, the adapter satisfies Publisher automatically by having a Publish method with the correct signature. This is why Go codebases use small interfaces prolifically — the cost of each adapter is near-zero. |  |  |  |  |
| WORKFLOW 2: Validation & Normalization |  |  |  |  |
| 1. Plain-English Scenario |  |  |  |  |
| What happens in the real world: Grammarly's extension on Windows 11 sends {"os": "Win32", "version": "v2.5.1", "timestamp": "2099-01-01T00:00:00Z"}. The OS name is non-standard, the version has a stray v prefix, and the timestamp is absurdly in the future (possibly a clock-skew bug or an attacker trying to poison freshness metrics). |  |  |  |  |
|   |  |  |  |  |
| How FleetPulse handles it: The Normalize() function at the ingestion boundary acts as a customs checkpoint. It lowercases and maps "Win32" → "windows", strips the v prefix from the version, and rejects the event outright because the timestamp is >5 minutes ahead of server time. This ensures that every event crossing into the system interior is clean, typed, and trustworthy. The rejection is counted as a Prometheus metric (fleetpulse_ingest_events_rejected_total{reason="timestamp"}) so the on-call team can see if a specific client version is sending garbage. |  |  |  |  |
| 2. Markdown Documentation |  |  |  |  |
| Component: internal/ingest/validate.go — Normalize() Function |  |  |  |  |
| Rule | Strictness | On Failure |  |  |
| client_id non-empty after trim | Strict | ValidationError{Field:"client_id"} |  |  |
| type must be heartbeat | capture | error	Strict	ValidationError{Field:"type"} |  |  |
| timestamp must parse RFC3339, ≤5min future | Strict | ValidationError{Field:"timestamp"} |  |  |
| version non-empty after trim + strip v/V | Strict | ValidationError{Field:"version"} |  |  |
| os mapped to known set | Lenient | Coerced to "unknown" |  |  |
| region mapped to known set | Lenient | Coerced to "unknown" |  |  |
| session_id required for captures | Conditional | ValidationError{Field:"session_id"} |  |  |
| expected_event_count > 0 for captures | Conditional | ValidationError{Field:"expected_event_count"} |  |  |
| sequence_number ∈ [0, expected) for captures | Conditional | ValidationError{Field:"sequence_number"} |  |  |
| error_message required for errors, capped 2048 | Conditional | ValidationError{Field:"error_message"} |  |  |
| Design Choice — Strict vs Lenient: Fields that affect signal correctness (timestamp, client_id) are strictly rejected. Fields that affect only labeling (os, region) are coerced to "unknown" so the event isn't lost — a mislabeled event is better than a dropped event for freshness computation. |  |  |  |  |
| 3. System Design Diagrams |  |  |  |  |
| High-Level: Validation Boundary |  |  |  |  |
| ┌─────────── UNTRUSTED ──────────┐    ┌──────── TRUSTED ───────┐ |  |  |  |  |
| │                                 │    │                        │ |  |  |  |  |
| │  RawEvent (wire JSON)           │    │  Event (canonical)     │ |  |  |  |  |
| │  ┌───────────────────────────┐  │    │  ┌──────────────────┐  │ |  |  |  |  |
| │  │ client_id: string         │  │    │  │ ClientID: string │  │ |  |  |  |  |
| │  │ type: string              │──┼───►│  │ Type: EventType  │  │ |  |  |  |  |
| │  │ version: string ("v2.5")  │  │    │  │ Cohort: {        │  │ |  |  |  |  |
| │  │ os: string ("Win32")      │  │    │  │   Version:"2.5"  │  │ |  |  |  |  |
| │  │ region: string            │  │    │  │   OS:"windows"   │  │ |  |  |  |  |
| │  │ timestamp: string (RFC3339)│  │    │  │   Region:"eu"  } │  │ |  |  |  |  |
| │  │ session_id: string         │  │    │  │ Timestamp: Time  │  │ |  |  |  |  |
| │  │ expected_event_count: int  │  │    │  │ ReceivedAt: Time │  │ |  |  |  |  |
| │  │ sequence_number: int       │  │    │  │ SessionID: ...   │  │ |  |  |  |  |
| │  │ error_message: string      │  │    │  └──────────────────┘  │ |  |  |  |  |
| │  └───────────────────────────┘  │    │                        │ |  |  |  |  |
| │                                 │    │  Zero JSON tags.       │ |  |  |  |  |
| │  Has JSON tags.                 │    │  Never serialized      │ |  |  |  |  |
| │  Deserialized from HTTP body.   │    │  directly.             │ |  |  |  |  |
| └─────────────────────────────────┘    └────────────────────────┘ |  |  |  |  |
| │ |  |  |  |  |
| │  Normalize(raw, receivedAt) |  |  |  |  |
| │  validate.go:47-116 |  |  |  |  |
| │ |  |  |  |  |
| ┌─────┴──────┐ |  |  |  |  |
| │  Success?  │ |  |  |  |  |
| ├─── YES ───►  Event (canonical) → Queue |  |  |  |  |
| └─── NO ────►  ValidationError → reject counter |  |  |  |  |
| Low-Level: Normalization Flow |  |  |  |  |
| Normalize(raw RawEvent, receivedAt time.Time) |  |  |  |  |
| │ |  |  |  |  |
| ├─► trim(client_id) ─── empty? ──► ValidationError |  |  |  |  |
| │ |  |  |  |  |
| ├─► lower(type) ─── Valid()? ──► No → ValidationError |  |  |  |  |
| │ |  |  |  |  |
| ├─► parse(timestamp, RFC3339) |  |  |  |  |
| │     └─► ts > now()+5min? ──► ValidationError |  |  |  |  |
| │ |  |  |  |  |
| ├─► normalizeVersion(version) |  |  |  |  |
| │     ├─ trim whitespace |  |  |  |  |
| │     ├─ strip leading 'v' or 'V' |  |  |  |  |
| │     └─ empty? ──► ValidationError |  |  |  |  |
| │ |  |  |  |  |
| ├─► normalizeOS(os)         ──► "windows" | "macos" | "linux" | "unknown" |  |
| │     └─ knownOS map lookup (case-insensitive) |  |  |  |  |
| │        "win32"→"windows", "darwin"/"osx"→"macos" |  |  |  |  |
| │ |  |  |  |  |
| ├─► normalizeRegion(region) ──► "us-east" | "us-west" | "eu" | "apac" | "unknown" |
| │     └─ knownRegions map lookup |  |  |  |  |
| │        "eu-west"→"eu" |  |  |  |  |
| │ |  |  |  |  |
| ├─► [if type==capture] |  |  |  |  |
| │     ├─ session_id required |  |  |  |  |
| │     ├─ expected_event_count > 0 |  |  |  |  |
| │     └─ 0 <= sequence_number < expected |  |  |  |  |
| │ |  |  |  |  |
| ├─► [if type==error] |  |  |  |  |
| │     ├─ error_message required |  |  |  |  |
| │     └─ truncate to 2048 chars |  |  |  |  |
| │ |  |  |  |  |
| └─► Assemble Event{..., ReceivedAt: receivedAt} ──► return |  |  |  |  |
| 4. Repository & Codebase Mapping |  |  |  |  |
| File | Key Symbol | Line | Purpose |  |
| internal/ingest/validate.go | Normalize() | 47-116 | Central validation + normalization function |  |
| internal/ingest/validate.go | ValidationError | 34-41 | Error type: {Field, Reason} |  |
| internal/ingest/validate.go | knownOS map | 14-21 | OS normalization lookup table |  |
| internal/ingest/validate.go | knownRegions map | 23-29 | Region normalization lookup table |  |
| internal/ingest/validate.go | normalizeVersion() | 118-128 | Strip v/V prefix + trim |  |
| internal/ingest/types.go | RawEvent struct | 52-69 | Untrusted wire format (JSON tags) |  |
| internal/ingest/types.go | Event struct | 73-84 | Trusted canonical form (no JSON tags) |  |
| internal/ingest/types.go | Cohort value object | 36-46 | {Version, OS, Region} + Key() |  |
| internal/ingest/types.go | EventType | 14-30 | String enum + Valid() |  |
| Where to add new features: |  |  |  |  |
| - New OS mapping (e.g., "chromeos") → add to knownOS map at validate.go:14-21. |  |  |  |  |
| - New event type (e.g., "crash") → add constant at types.go:20, update Valid() at types.go:23-30, add conditional validation block in Normalize() at validate.go:85-113. |  |  |  |  |
| 5. Underlying Architectural Principles |  |  |  |  |
| Principle | Manifestation |  |  |  |
| Anti-Corruption Layer (DDD) | RawEvent → Event transformation happens once at the boundary. Interior code never sees untrusted data. |  |  |  |
| Parse, Don't Validate | Normalize() doesn't just check validity — it transforms data into a richer type (string → EventType, string → time.Time). The returned Event makes invalid states unrepresentable. |  |  |  |
| Fail Fast | Invalid events are rejected at ingestion. They never enter the queue, never consume worker capacity, never corrupt state. |  |  |  |
| Lenient Reader / Tolerant Reader | OS and region fields use Postel's Law: accept what you can, normalize it, coerce unknowns rather than reject. |  |  |  |
| Single Responsibility (SOLID - S) | Validation is in its own file (validate.go), not mixed into the HTTP handler. The handler orchestrates; Normalize() decides. |  |  |  |
| 6. Gang of Four Design Pattern Analysis |  |  |  |  |
| Pattern Identified: Strategy (Behavioral) — embedded via type-specific validation branches |  |  |  |  |
| The Normalize() function contains type-conditional validation (lines 85-113): heartbeat, capture, and error events each have different required fields and validation rules. While this isn't a classic Strategy with separate classes, it's the Go-idiomatic expression of the Strategy pattern: the event type acts as a discriminator that selects a validation strategy inline. |  |  |  |  |
| GoF 1994 vs Go 2024: The original Strategy pattern uses an interface hierarchy (class ValidationStrategy { virtual validate() = 0 }) with subclasses for each variant. In Go, the pattern is collapsed into a switch on the type discriminator because Go favors explicit control flow over polymorphic dispatch for simple branching. If the validation grew much more complex per type, you'd extract to a map[EventType]func(RawEvent) error — a function-value strategy map. |  |  |  |  |
| PM View: |  |  |  |  |
| Think of this like airport security checkpoints with different lanes. Economy passengers (heartbeats) get a quick ID check. Business passengers (captures) get an extra carry-on inspection (session ID, fragment count). VIP passengers (errors) get a special screening (error message required, length-capped for safety). Same checkpoint, different rules based on ticket type. This matters for the business because adding a new event type (say, "crash reports") means adding one new lane — you don't rebuild the entire checkpoint. |  |  |  |  |
| Junior Eng View: |  |  |  |  |
| When you need to add a new event type: |  |  |  |  |
| 1. Add a constant in types.go: EventCrash EventType = "crash" |  |  |  |  |
| 2. Update Valid() in types.go to include "crash" |  |  |  |  |
| 3. Add a case EventCrash: block in Normalize() at validate.go:85 with your type-specific field requirements |  |  |  |  |
| 4. Done — the handler doesn't change, the queue doesn't change, the worker pool doesn't change |  |  |  |  |
|   |  |  |  |  |
| Antipattern to avoid: Don't add validation logic inside the HTTP handler. Keep it in Normalize(). If you scatter validation across handler and validate, you'll get inconsistent behavior when the same event arrives via different paths (HTTP vs. replay from dead-letter queue in the future). |  |  |  |  |
| Senior Eng View: |  |  |  |  |
| The current inline-switch approach is fine for 3 event types. If this grew to 10+, you'd want to extract to a registry pattern: var validators = map[EventType]func(*Event) error{...} to achieve O(1) dispatch and enable registering validators from separate packages (open-closed principle). The current design is closed for extension — adding a new type requires modifying Normalize(). This is a conscious trade-off: at 3 types, the clarity of a single function outweighs the extensibility of a registry. The moment you hit 6-7 types, refactor. |  |  |  |  |
|   |  |  |  |  |
| Note the subtle correctness choice on the timestamp check: it rejects timestamps >5 minutes in the future but accepts arbitrarily old timestamps. This is intentional — a client with clock skew backward will still produce valid freshness data (its events will have old timestamps, and the ReceivedAt field captures server time). But a client with forward clock skew would poison the freshness signal by making stale events appear fresh. |  |  |  |  |
| WORKFLOW 3: Queue-Based Decoupling |  |  |  |  |
| 1. Plain-English Scenario |  |  |  |  |
| What happens in the real world: On Black Friday, Grammarly's install base spikes as users activate corporate licenses. The extension fleet grows from 300K to 500K active endpoints in hours. Heartbeat volume doubles. The downstream Workflow Mapping service slows under load. |  |  |  |  |
|   |  |  |  |  |
| How FleetPulse handles it: The queue acts as a shock absorber. The ingestion endpoint stuffs events into the queue and returns 202 immediately — the response time stays constant regardless of downstream health. The worker pool drains the queue at its own pace. If workers are overwhelmed, the backlog grows (visible as fleetpulse_queue_backlog in Grafana). If a worker crashes mid-processing, the visibility timeout expires after 10 seconds and the message is automatically redelivered to another worker. After 5 failed deliveries, the message is dead-lettered so poison pills don't loop forever. |  |  |  |  |
| 2. Markdown Documentation |  |  |  |  |
| Component: internal/queue/ — Queue Interface + Two Implementations |  |  |  |  |
| Interface | Methods |  |  |  |
| Queue (queue.go:77-92) | Publish, Receive, Metrics, Close |  |  |  |
| Delivery (queue.go:47-58) | Message(), Ack(ctx), Nack(ctx) |  |  |  |
| Metrics (queue.go:62-72) | Backlog, InFlight, DeadLettered |  |  |  |
| Implementation | File | Backend |  |  |
| InMemoryQueue | queue/inmemory.go | Buffered Go channel + slice |  |  |
| SQSQueue | aws/sqs_queue.go | AWS SQS (via LocalStack) |  |  |
| Constants: DefaultVisibilityTimeout = 10s, DefaultMaxDeliveries = 5 |  |  |  |  |
| 3. System Design Diagrams |  |  |  |  |
| High-Level: Queue as Shock Absorber |  |  |  |  |
| ┌────────────────┐                          ┌────────────────┐ |  |  |  |  |
| │  Ingestion     │    Publish(Event)         │  Worker Pool   │ |  |  |  |  |
| │  (fast path)   │ ─────────────────►        │  (slow path)   │ |  |  |  |  |
| │                │                           │  8 goroutines  │ |  |  |  |  |
| │  Returns 202   │      ┌──────────┐         │                │ |  |  |  |  |
| │  immediately   │      │  QUEUE   │         │  Receive()     │ |  |  |  |  |
| │                │      │          │ ◄───────│  long-poll 2s  │ |  |  |  |  |
| └────────────────┘      │ ┌──────┐ │         │                │ |  |  |  |  |
| │ │ready │ │         │  Ack() → done  │ |  |  |  |  |
| │ │ msg1 │ │         │  Nack() → retry│ |  |  |  |  |
| │ │ msg2 │ │         │                │ |  |  |  |  |
| │ │ msg3 │ │         └────────────────┘ |  |  |  |  |
| │ └──────┘ │ |  |  |  |  |
| │          │ |  |  |  |  |
| │ ┌────────┴───┐ |  |  |  |  |
| │ │ in-flight  │ ← visibility timeout 10s |  |  |  |  |
| │ │  msg4      │ |  |  |  |  |
| │ │  msg5      │ |  |  |  |  |
| │ └────────────┘ |  |  |  |  |
| │          │ |  |  |  |  |
| │ ┌────────┴───┐ |  |  |  |  |
| │ │ dead-letter│ ← after 5 failed deliveries |  |  |  |  |
| │ │  msg_bad   │ |  |  |  |  |
| │ └────────────┘ |  |  |  |  |
| └──────────────┘ |  |  |  |  |
|   |  |  |  |  |
| ┌──────────────────────────────────────┐ |  |  |  |  |
| │  Reaper Goroutine (every 500ms)      │ |  |  |  |  |
| │                                      │ |  |  |  |  |
| │  for each in-flight msg:             │ |  |  |  |  |
| │    if now > msg.visibleAfter:         │ |  |  |  |  |
| │      if deliveryCount >= 5:           │ |  |  |  |  |
| │        → move to dead-letter          │ |  |  |  |  |
| │      else:                            │ |  |  |  |  |
| │        → move back to ready           │ |  |  |  |  |
| │        → increment deliveryCount      │ |  |  |  |  |
| └──────────────────────────────────────┘ |  |  |  |  |
| Low-Level: InMemoryQueue Internals |  |  |  |  |
| InMemoryQueue struct |  |  |  |  |
| ┌───────────────────────────────────────────────┐ |  |  |  |  |
| │  mu         sync.Mutex                         │ |  |  |  |  |
| │  ready      []Message          ← backlog       │ |  |  |  |  |
| │  inFlight   map[string]*inFlightMessage         │ |  |  |  |  |
| │  deadLetter []Message          ← poison pills  │ |  |  |  |  |
| │  notify     chan struct{}       ← wake waiters  │ |  |  |  |  |
| │  closed     bool                                │ |  |  |  |  |
| │  nextID     func() string      ← uuid.New()    │ |  |  |  |  |
| │                                                 │ |  |  |  |  |
| │  Methods:                                       │ |  |  |  |  |
| │  ┌─────────────────────────────────────────┐   │ |  |  |  |  |
| │  │ Publish(ctx, evt)                       │   │ |  |  |  |  |
| │  │   1. Lock mu                            │   │ |  |  |  |  |
| │  │   2. Assign UUID to msg.ID              │   │ |  |  |  |  |
| │  │   3. Set msg.EnqueuedAt = now()         │   │ |  |  |  |  |
| │  │   4. Append to ready[]                  │   │ |  |  |  |  |
| │  │   5. Non-blocking send on notify chan    │   │ |  |  |  |  |
| │  │   6. Unlock                             │   │ |  |  |  |  |
| │  └─────────────────────────────────────────┘   │ |  |  |  |  |
| │  ┌─────────────────────────────────────────┐   │ |  |  |  |  |
| │  │ Receive(ctx, max, wait)                 │   │ |  |  |  |  |
| │  │   1. Lock, check ready[]                │   │ |  |  |  |  |
| │  │   2. If messages: move up to max to     │   │ |  |  |  |  |
| │  │      inFlight map with visibleAfter     │   │ |  |  |  |  |
| │  │   3. If empty: unlock, wait on notify   │   │ |  |  |  |  |
| │  │      chan OR deadline                    │   │ |  |  |  |  |
| │  │   4. Return []Delivery wrappers         │   │ |  |  |  |  |
| │  └─────────────────────────────────────────┘   │ |  |  |  |  |
| └───────────────────────────────────────────────┘ |  |  |  |  |
| 4. Repository & Codebase Mapping |  |  |  |  |
| File | Key Symbol | Line |  |  |
| internal/queue/queue.go | Queue interface | 77-92 |  |  |
| internal/queue/queue.go | Delivery interface | 47-58 |  |  |
| internal/queue/queue.go | Message struct | 29-34 |  |  |
| internal/queue/inmemory.go | InMemoryQueue | 40-56 |  |  |
| internal/queue/inmemory.go | reapExpiredVisibility() | 164-195 |  |  |
| internal/queue/inmemory.go | ack() / nack() | 197-221 |  |  |
| internal/aws/sqs_queue.go | SQSQueue | 89-94 |  |  |
| internal/aws/sqs_queue.go | wireEvent DTO | 24-36 |  |  |
| internal/aws/sqs_queue.go | toWireEvent / fromWireEvent | 38-74 |  |  |
| cmd/server/main.go | buildQueue() | 130-151 |  |  |
| Where to add new features: |  |  |  |  |
| - Switch to Kafka → implement queue.Queue in a new internal/kafka/ package, add "kafka" case in buildQueue(). |  |  |  |  |
| - Dead-letter replay → add ReplayDeadLetter() method to the Queue interface, implement in both backends. |  |  |  |  |
| 5. Underlying Architectural Principles |  |  |  |  |
| Principle | Manifestation |  |  |  |
| Ports and Adapters (Hexagonal Architecture) | Queue is a port (interface). InMemoryQueue and SQSQueue are adapters. The worker pool is backend-agnostic. |  |  |  |
| Event-Driven Architecture | The queue decouples producers (ingestion) from consumers (workers). Neither knows nor cares about the other's lifecycle. |  |  |  |
| Visibility Timeout as Crash Recovery | No heartbeat-based liveness detection for workers. Instead, if a worker dies, its in-flight messages simply time out and are redelivered. Simpler, more robust. |  |  |  |
| Backpressure Signaling | queue.Metrics.Backlog tells Prometheus (and Grafana) when consumers can't keep up with producers. This is the foundation for autoscaling decisions. |  |  |  |
| Dependency Inversion (SOLID - D) | High-level modules (worker.Pool) depend on the Queue abstraction, not on InMemoryQueue or SQSQueue concretions. The concrete implementation is selected at boot time by buildQueue(). |  |  |  |
| 6. Gang of Four Design Pattern Analysis |  |  |  |  |
| Pattern Identified: Factory Method (Creational) + Strategy (Behavioral) |  |  |  |  |
| buildQueue() at cmd/server/main.go:130-151 is a Factory Method that reads FLEETPULSE_QUEUE_BACKEND env var and constructs either InMemoryQueue or SQSQueue. The two implementations are interchangeable Strategies behind the Queue interface. |  |  |  |  |
| GoF 1994 vs Go 2024: The original Factory Method uses an abstract creator class with a createProduct() method overridden by subclasses. In Go, there are no abstract classes. Instead, the factory is a plain function returning an interface. The "strategy" aspect is implicit — Go interfaces don't require explicit declaration, so InMemoryQueue and SQSQueue are strategies simply by implementing the same method set. |  |  |  |  |
| PM View: |  |  |  |  |
| This is like having one universal cargo dock that works with either UPS trucks or FedEx trucks. Engineering picks the carrier at deployment time via a configuration switch. In development, they use "UPS" (in-memory, fast, no infrastructure). In production, they switch to "FedEx" (SQS, durable, scalable). The warehouse workers (the worker pool) don't care which truck arrives — they just unload boxes. This means you can develop and test locally at full speed without needing an AWS account, then deploy to production with zero code changes. |  |  |  |  |
| Junior Eng View: |  |  |  |  |
| How buildQueue() works, step by step: |  |  |  |  |
| 1. Read FLEETPULSE_QUEUE_BACKEND env var (default: "memory") |  |  |  |  |
| 2. switch backend { case "memory": return queue.NewInMemoryQueue(logger) } |  |  |  |  |
| 3. case "sqs": load AWS config, return aws.NewSQSQueue(sqsClient, queueURL, ...) } |  |  |  |  |
| 4. The returned value is queue.Queue — the caller doesn't know which implementation it got |  |  |  |  |
|   |  |  |  |  |
| To add a new backend (e.g., RabbitMQ): |  |  |  |  |
| 1. Create internal/rabbitmq/queue.go |  |  |  |  |
| 2. Implement Publish, Receive, Metrics, Close, Ack, Nack |  |  |  |  |
| 3. Add case "rabbitmq": in buildQueue() |  |  |  |  |
| 4. That's it — zero changes to worker pool, ingestion, or signals |  |  |  |  |
|   |  |  |  |  |
| Common mistake: Don't import queue/inmemory from the worker pool. Import only queue (the interface package). If you import a concrete implementation, you break the abstraction. |  |  |  |  |
| Senior Eng View: |  |  |  |  |
| The factory method here uses environment-variable dispatch rather than constructor injection. This is a pragmatic Go choice — there's no DI framework, and the factory lives in main.go where all wiring happens. The trade-off: you can't easily test main.go's wiring logic itself (it reads env vars). The mitigation: the individual components are all testable via their interfaces. |  |  |  |  |
|   |  |  |  |  |
| The InMemoryQueue implementation is more interesting than it looks. It reimplements SQS-style visibility timeout semantics (reaper goroutine, delivery count tracking, dead-lettering) to ensure behavioral parity between dev and prod. This is critical — if the dev queue had different retry semantics than SQS, you'd get dev/prod behavioral divergence that only manifests under failure conditions. The 500ms reaper tick is a trade-off: faster ticks mean quicker recovery from hung workers but more mutex contention under load. |  |  |  |  |
|   |  |  |  |  |
| The wireEvent DTO in sqs_queue.go deserves attention. It exists to decouple the SQS wire format from the internal Event struct. If you serialized Event directly, you'd couple your SQS message format to your internal domain model — changing a field name in Event would break in-flight messages. The DTO ensures backward-compatible serialization. |  |  |  |  |
| WORKFLOW 4: Worker Pool Processing (Retry & Dead-Letter) |  |  |  |  |
| 1. Plain-English Scenario |  |  |  |  |
| What happens in the real world: A Grammarly extension sends a capture event — "session XYZ, fragment 5 of 8." The worker pool picks it up, updates the client's last-seen state, then tries to call the Workflow Mapping service to register this fragment. But the Mapping service is overloaded and returns a 503. The worker classifies this as a transient error and nacks the message. The queue redelivers it to another worker. On the 2nd attempt, the Mapping service responds successfully. The fragment is recorded. If the Mapping service were down for 5 straight attempts, the message would be dead-lettered — parked in a holding area so it doesn't loop forever, and a Prometheus counter increments to alert on-call. |  |  |  |  |
| 2. Markdown Documentation |  |  |  |  |
| Component: internal/worker/pool.go — Worker Pool with Retry Logic |  |  |  |  |
| Config | Default | Purpose |  |  |
| NumWorkers | 8 | Concurrent consumer goroutines |  |  |
| ReceiveBatch | 10 | Messages per Receive() call |  |  |
| ReceiveWait | 2s | Long-poll timeout per Receive() |  |  |
| MetricsInterval | 2s | Queue-health reporting frequency |  |  |
| Processing Pipeline per Event (handleEvent at pool.go:152-166): |  |  |  |  |
| 1. store.Upsert() — Update client state (last heartbeat, last event, session ID) |  |  |  |  |
| 2. enricher.Enrich() — Call downstream for captures; simulate latency; inject failures |  |  |  |  |
| 3. computer.RecordCapture() — Feed capture events to the completeness signal tracker |  |  |  |  |
| Retry Logic (process at pool.go:112-148): |  |  |  |  |
| - Success → Ack() + counter {outcome="success"} |  |  |  |  |
| - Transient error + retries remaining → Nack() + counter {outcome="retry"} |  |  |  |  |
| - Permanent error OR retries exhausted → Nack() + counter {outcome="dead_letter"} |  |  |  |  |
| 3. System Design Diagrams |  |  |  |  |
| High-Level: Worker Pool Architecture |  |  |  |  |
| ┌────────────────────────────────────────────────────────────┐ |  |  |  |  |
| │                    Worker Pool                              │ |  |  |  |  |
| │                                                            │ |  |  |  |  |
| │  ┌──────────────────────────────────────────────────────┐  │ |  |  |  |  |
| │  │  Run(ctx)                                            │  │ |  |  |  |  |
| │  │                                                      │  │ |  |  |  |  |
| │  │  ┌──────┐ ┌──────┐ ┌──────┐     ┌──────┐            │  │ |  |  |  |  |
| │  │  │ W-0  │ │ W-1  │ │ W-2  │ ... │ W-7  │  8 workers │  │ |  |  |  |  |
| │  │  │      │ │      │ │      │     │      │            │  │ |  |  |  |  |
| │  │  │ loop │ │ loop │ │ loop │     │ loop │            │  │ |  |  |  |  |
| │  │  └──┬───┘ └──┬───┘ └──┬───┘     └──┬───┘            │  │ |  |  |  |  |
| │  │     │        │        │             │                │  │ |  |  |  |  |
| │  │     └────────┴────────┴──────┬──────┘                │  │ |  |  |  |  |
| │  │                              │                        │  │ |  |  |  |  |
| │  │                    q.Receive(ctx, 10, 2s)             │  │ |  |  |  |  |
| │  │                              │                        │  │ |  |  |  |  |
| │  │                    ┌─────────▼──────────┐             │  │ |  |  |  |  |
| │  │                    │   process(d)       │             │  │ |  |  |  |  |
| │  │                    │                    │             │  │ |  |  |  |  |
| │  │                    │ ┌────────────────┐ │             │  │ |  |  |  |  |
| │  │                    │ │ handleEvent()  │ │             │  │ |  |  |  |  |
| │  │                    │ │ 1. Upsert      │ │             │  │ |  |  |  |  |
| │  │                    │ │ 2. Enrich      │ │             │  │ |  |  |  |  |
| │  │                    │ │ 3. RecordCapt. │ │             │  │ |  |  |  |  |
| │  │                    │ └───────┬────────┘ │             │  │ |  |  |  |  |
| │  │                    │         │          │             │  │ |  |  |  |  |
| │  │                    │    ┌────┴────┐     │             │  │ |  |  |  |  |
| │  │                    │    │ Result? │     │             │  │ |  |  |  |  |
| │  │                    │    └─┬───┬───┘     │             │  │ |  |  |  |  |
| │  │                    │      │   │   │     │             │  │ |  |  |  |  |
| │  │                    │   OK  Trans  Perm  │             │  │ |  |  |  |  |
| │  │                    │   │   │      │     │             │  │ |  |  |  |  |
| │  │                    │  Ack  │    Nack    │             │  │ |  |  |  |  |
| │  │                    │       │   (dead)   │             │  │ |  |  |  |  |
| │  │                    │    retries         │             │  │ |  |  |  |  |
| │  │                    │    left?           │             │  │ |  |  |  |  |
| │  │                    │    ├─Y→ Nack       │             │  │ |  |  |  |  |
| │  │                    │    │   (redeliver) │             │  │ |  |  |  |  |
| │  │                    │    └─N→ Nack       │             │  │ |  |  |  |  |
| │  │                    │       (dead)       │             │  │ |  |  |  |  |
| │  │                    └────────────────────┘             │  │ |  |  |  |  |
| │  └──────────────────────────────────────────────────────┘  │ |  |  |  |  |
| │                                                            │ |  |  |  |  |
| │  ┌──────────────────────────────────┐                      │ |  |  |  |  |
| │  │  reportQueueMetrics (every 2s)   │                      │ |  |  |  |  |
| │  │  q.Metrics() → Prometheus gauges │                      │ |  |  |  |  |
| │  └──────────────────────────────────┘                      │ |  |  |  |  |
| └────────────────────────────────────────────────────────────┘ |  |  |  |  |
| Low-Level: Retry Decision Tree |  |  |  |  |
| process(ctx, workerID, delivery) |  |  |  |  |
| │ |  |  |  |  |
| ├─► timer = time.Now() |  |  |  |  |
| │ |  |  |  |  |
| ├─► err = handleEvent(ctx, delivery.Message().Event) |  |  |  |  |
| │ |  |  |  |  |
| ├─► duration = time.Since(timer) |  |  |  |  |
| │   metrics.WorkerProcessingDuration.Observe(duration) |  |  |  |  |
| │ |  |  |  |  |
| ├─► err == nil? |  |  |  |  |
| │     │ |  |  |  |  |
| │     YES ──► delivery.Ack(ctx) |  |  |  |  |
| │             metrics.WorkerEventsProcessed.Inc("success") |  |  |  |  |
| │             return |  |  |  |  |
| │ |  |  |  |  |
| ├─► IsTransient(err)? |  |  |  |  |
| │     │ |  |  |  |  |
| │     YES ──► delivery.Message().DeliveryCount < DefaultMaxDeliveries (5)? |  |  |  |  |
| │     │         │ |  |  |  |  |
| │     │         YES ──► delivery.Nack(ctx)    ← immediate redelivery |  |  |  |  |
| │     │         │       metrics.WorkerRetries.Inc() |  |  |  |  |
| │     │         │       metrics.WorkerEventsProcessed.Inc("retry") |  |  |  |  |
| │     │         │       return |  |  |  |  |
| │     │         │ |  |  |  |  |
| │     │         NO  ──► delivery.Nack(ctx)    ← dead-lettered by queue |  |  |  |  |
| │     │                 metrics.WorkerEventsProcessed.Inc("dead_letter") |  |  |  |  |
| │     │                 log.Error("dead-lettering after max retries") |  |  |  |  |
| │     │                 return |  |  |  |  |
| │     │ |  |  |  |  |
| │     NO ──► delivery.Nack(ctx)               ← permanent failure |  |  |  |  |
| │            metrics.WorkerEventsProcessed.Inc("dead_letter") |  |  |  |  |
| │            log.Error("permanent error, dead-lettering") |  |  |  |  |
| │            return |  |  |  |  |
| 4. Repository & Codebase Mapping |  |  |  |  |
| File | Key Symbol | Line | Purpose |  |
| internal/worker/pool.go | Pool struct | 47-55 | Holds all dependencies: queue, store, enricher, computer, metrics |  |
| internal/worker/pool.go | Run() | 66-85 | Launches workers + metrics reporter, blocks until ctx cancel |  |
| internal/worker/pool.go | workerLoop() | 87-110 | Single worker's infinite receive-process loop |  |
| internal/worker/pool.go | process() | 112-148 | Retry/dead-letter decision tree |  |
| internal/worker/pool.go | handleEvent() | 152-166 | Core pipeline: upsert → enrich → record |  |
| internal/worker/errors.go | ErrTransient | 10 | Sentinel error for retryable failures |  |
| internal/worker/errors.go | IsTransient() | 13 | errors.Is() check for sentinel |  |
| internal/worker/enrich.go | Enricher interface | 21-23 | Enrich(ctx, Event) error |  |
| internal/worker/enrich.go | StubEnricher | 32-51 | Downstream caller + failure injector |  |
| internal/worker/enrich.go | mapWorkflow() | 82-96 | HTTP POST to downstream + record mapping result |  |
| 5. Underlying Architectural Principles |  |  |  |  |
| Principle | Manifestation |  |  |  |
| At-Least-Once Delivery | Visibility timeout + redelivery guarantees no message is silently lost. Trade-off: store.Upsert() must be idempotent (it is — last-write-wins on timestamps). |  |  |  |
| Circuit Breaker Semantics | Max 5 deliveries per message. Prevents poison pills from consuming infinite worker capacity. |  |  |  |
| Separation of Error Classification | IsTransient() centralizes the retry/permanent decision. All downstream HTTP failures are wrapped as ErrTransient; all state-store failures are permanent. This single function is the retry policy. |  |  |  |
| Fan-Out Concurrency | 8 goroutines process in parallel. Go's goroutine model (M:N scheduler, ~4KB stack) makes this cheap — 8 goroutines cost ~32KB total, vs. 8 OS threads costing ~8MB. |  |  |  |
| 6. Gang of Four Design Pattern Analysis |  |  |  |  |
| Pattern Identified: Template Method (Behavioral) |  |  |  |  |
| handleEvent() at pool.go:152-166 defines a fixed processing skeleton: (1) Upsert state → (2) Enrich → (3) Record capture. The enrichment step is pluggable via the Enricher interface. The process() wrapper adds the retry/ack/nack lifecycle around this template. |  |  |  |  |
| GoF 1994 vs Go 2024: The original Template Method uses an abstract class with final template methods calling abstract hook methods. Go has no abstract classes or final methods. Instead, the template is a concrete function (handleEvent) that calls an interface method (enricher.Enrich). The "hook" is the interface, not a virtual method override. |  |  |  |  |
| PM View: |  |  |  |  |
| Think of this like a car wash with 3 stations: rinse (update state), soap (enrich), wax (record signal). Every car goes through the same 3 stations in the same order. But the soap station can be swapped — maybe today it's wax-free soap, tomorrow it's premium polish. The stations are fixed; the soap recipe is pluggable. This means if Grammarly needs to add a new enrichment step (say, geo-IP lookup), they swap the soap station without redesigning the entire car wash. |  |  |  |  |
| Junior Eng View: |  |  |  |  |
| The processing pipeline is: |  |  |  |  |
| handleEvent(ctx, evt): |  |  |  |  |
| 1. store.Upsert(...)     ← always runs first |  |  |  |  |
| 2. enricher.Enrich(...)  ← pluggable via interface |  |  |  |  |
| 3. if capture: computer.RecordCapture(evt)  ← conditional |  |  |  |  |
| If Enrich() returns an error wrapping ErrTransient, the event gets retried. If it returns any other error, the event is dead-lettered. |  |  |  |  |
|   |  |  |  |  |
| To create a new enricher: implement the Enricher interface (one method: Enrich(ctx, Event) error), then wire it in cmd/server/main.go:73 where NewStubEnricher is currently called. |  |  |  |  |
|   |  |  |  |  |
| Mistake to avoid: Don't put retry logic inside the enricher. The retry decision belongs in process(). The enricher should just return errors — transient or permanent. Let the caller decide. |  |  |  |  |
| Senior Eng View: |  |  |  |  |
| The step ordering in handleEvent is significant. State is upserted before enrichment. This means if enrichment fails and the event is retried, the state is upserted again — but since Upsert is last-write-wins on timestamps, the duplicate write is harmless. If the order were reversed (enrich first, then upsert), a failure between enrich and upsert would leave the state inconsistent: the downstream would have a mapping but the state store wouldn't reflect the event. |  |  |  |  |
|   |  |  |  |  |
| A notable absence: there's no backoff on nack. Transient failures are retried at full speed (the nacked message returns to the ready queue immediately). In a real production system, this would cause a thundering herd against a degraded downstream. You'd want exponential backoff on the visibility timeout: nack should call ChangeMessageVisibility(timeout = min(base * 2^attempt, maxTimeout)) instead of making the message immediately available. |  |  |  |  |
| WORKFLOW 5: Signal Computation Engine |  |  |  |  |
| 1. Plain-English Scenario |  |  |  |  |
| What happens in the real world: It's 3 AM. The on-call engineer at Grammarly gets paged: "Heartbeat freshness for version 2.6.0-beta on macOS in EU dropped below 50%." This means more than half of that cohort's clients haven't sent a heartbeat in the last 60 seconds. Either the beta release has a crash bug, a network partition is isolating EU users, or the update server pushed a broken binary. The signal tells them which version, which OS, which region — so they can decide in seconds whether to roll back 2.6.0-beta or investigate the EU network. |  |  |  |  |
|   |  |  |  |  |
| How FleetPulse computes this: Every 10 seconds, the Signals Computer runs a tick. It loads all client state from the store, buckets clients by cohort (version | os | region), and for each cohort computes: fresh_count / total_count where "fresh" means the client's last heartbeat was within the last 60 seconds. This ratio is set as a Prometheus gauge with cohort labels. Prometheus scrapes it every 5 seconds. Grafana dashboards visualize it. Alertmanager rules (defined in docs/signals.md but not yet implemented) would fire when the ratio drops below 99% for 5 minutes or below 50% for any cohort >1% of the fleet. |  |  |
| 2. Markdown Documentation |  |  |  |  |
| Component: internal/signals/signals.go — Signals Computer |  |  |  |  |
| The Three Signals: |  |  |  |  |
| # | Signal | Metric | Formula |  |
| 1 | Heartbeat Freshness | fleetpulse_heartbeat_freshness_ratio | fresh_in_cohort / total_in_cohort |  |
| 2 | Capture Completeness | fleetpulse_capture_completeness_ratio | mean(received/expected) per cohort |  |
| 3 | Mapping Availability | fleetpulse_mapping_availability_ratio | successes / total |  |
| 3. System Design Diagrams |  |  |  |  |
| High-Level: Signal Computation Loop |  |  |  |  |
| ┌──────────────────────────────────────────────────────────────┐ |  |  |  |  |
| │  Signals Computer (goroutine, ticks every 10s)               │ |  |  |  |  |
| │                                                              │ |  |  |  |  |
| │  tick()                                                      │ |  |  |  |  |
| │    │                                                         │ |  |  |  |  |
| │    ├─► computeHeartbeatFreshness()                           │ |  |  |  |  |
| │    │     │                                                   │ |  |  |  |  |
| │    │     ├─ store.All() → []ClientInfo                       │ |  |  |  |  |
| │    │     │    (full table scan of all known clients)          │ |  |  |  |  |
| │    │     │                                                   │ |  |  |  |  |
| │    │     ├─ bucket by cohort.Key()                            │ |  |  |  |  |
| │    │     │    "2.5.1 | windows | us-east" → [client1, client2]   │ |  |  |
| │    │     │    "2.6.0-beta | macos | eu"   → [client3]            │ |  |  |
| │    │     │                                                   │ |  |  |  |  |
| │    │     ├─ for each cohort:                                  │ |  |  |  |  |
| │    │     │    fresh = count(c.LastHeartbeatAt > now - 60s)    │ |  |  |  |  |
| │    │     │    ratio = fresh / len(cohort)                     │ |  |  |  |  |
| │    │     │                                                   │ |  |  |  |  |
| │    │     └─ metrics.HeartbeatFreshness                        │ |  |  |  |  |
| │    │          .With(version, os, region)                      │ |  |  |  |  |
| │    │          .Set(ratio)                                     │ |  |  |  |  |
| │    │                                                         │ |  |  |  |  |
| │    ├─► computeCaptureCompleteness()                           │ |  |  |  |  |
| │    │     │                                                   │ |  |  |  |  |
| │    │     ├─ iterate sessions map                              │ |  |  |  |  |
| │    │     │    prune if lastUpdated > 30min ago                │ |  |  |  |  |
| │    │     │                                                   │ |  |  |  |  |
| │    │     ├─ for each session:                                 │ |  |  |  |  |
| │    │     │    completeness = len(received) / expected         │ |  |  |  |  |
| │    │     │    cap at 1.0                                      │ |  |  |  |  |
| │    │     │                                                   │ |  |  |  |  |
| │    │     ├─ average per cohort                                │ |  |  |  |  |
| │    │     │                                                   │ |  |  |  |  |
| │    │     └─ metrics.CaptureCompleteness                      │ |  |  |  |  |
| │    │          .With(version, os, region)                      │ |  |  |  |  |
| │    │          .Set(avg_ratio)                                 │ |  |  |  |  |
| │    │                                                         │ |  |  |  |  |
| │    └─► mapping health                                        │ |  |  |  |  |
| │          │                                                   │ |  |  |  |  |
| │          ├─ mappingHealth.ratio()                             │ |  |  |  |  |
| │          │    prune samples older than 5 min                  │ |  |  |  |  |
| │          │    return successes / total                        │ |  |  |  |  |
| │          │    (1.0 if no samples — no-data ≠ failure)         │ |  |  |  |  |
| │          │                                                   │ |  |  |  |  |
| │          └─ metrics.MappingAvailability.Set(ratio)            │ |  |  |  |  |
| │                                                              │ |  |  |  |  |
| └──────────────────────────────────────────────────────────────┘ |  |  |  |  |
|   |  |  |  |  |
| │                               │ |  |  |  |  |
| ▼                               ▼ |  |  |  |  |
| ┌───────────┐                 ┌───────────────┐ |  |  |  |  |
| │ Prometheus│ ◄── scrape ──── │ GET /metrics  │ |  |  |  |  |
| │  (:9090)  │    every 5s     │    (server)   │ |  |  |  |  |
| └─────┬─────┘                 └───────────────┘ |  |  |  |  |
| │ |  |  |  |  |
| ▼ |  |  |  |  |
| ┌───────────┐ |  |  |  |  |
| │  Grafana  │  6-panel dashboard |  |  |  |  |
| │  (:3000)  │  · freshness by version |  |  |  |  |
| │           │  · completeness by version |  |  |  |  |
| │           │  · mapping availability |  |  |  |  |
| │           │  · queue health |  |  |  |  |
| │           │  · ingest rates |  |  |  |  |
| │           │  · worker outcomes |  |  |  |  |
| └───────────┘ |  |  |  |  |
| Low-Level: Session Progress Tracking (Capture Completeness) |  |  |  |  |
| Worker calls computer.RecordCapture(evt) for each capture event |  |  |  |  |
|   |  |  |  |  |
| RecordCapture(evt) |  |  |  |  |
| │ |  |  |  |  |
| ├─ key = evt.SessionID |  |  |  |  |
| │ |  |  |  |  |
| ├─ sessions[key] exists? |  |  |  |  |
| │    │ |  |  |  |  |
| │    NO  → create sessionProgress{ |  |  |  |  |
| │              cohort:   evt.Cohort, |  |  |  |  |
| │              expected: evt.ExpectedEventCount, |  |  |  |  |
| │              received: map[int]struct{}{evt.SequenceNumber: {}}, |  |  |  |  |
| │              lastUpdated: now() |  |  |  |  |
| │          } |  |  |  |  |
| │    │ |  |  |  |  |
| │    YES → sessions[key].received[evt.SequenceNumber] = struct{}{} |  |  |  |  |
| │          sessions[key].lastUpdated = now() |  |  |  |  |
| │ |  |  |  |  |
|   |  |  |  |  |
| Example: Session "ABC", expected=5, fragments received over time: |  |  |  |  |
|   |  |  |  |  |
| t=0   seq=0  →  received: {0}           completeness: 1/5 = 0.20 |  |  |  |  |
| t=1   seq=1  →  received: {0,1}         completeness: 2/5 = 0.40 |  |  |  |  |
| t=2   seq=3  →  received: {0,1,3}       completeness: 3/5 = 0.60 |  |  |  |  |
| (seq=2 lost in transit!) |  |  |  |  |
| t=3   seq=4  →  received: {0,1,3,4}     completeness: 4/5 = 0.80 |  |  |  |  |
| (seq=2 never arrives) |  |  |  |  |
|   |  |  |  |  |
| After 30 min: session pruned, final completeness 0.80 for this session |  |  |  |  |
| Averaged with other sessions in the same cohort for the gauge value |  |  |  |  |
| 4. Repository & Codebase Mapping |  |  |  |  |
| File | Key Symbol | Line | Purpose |  |
| internal/signals/signals.go | Computer struct | 40-53 | Holds store, metrics, mapping health, sessions map |  |
| internal/signals/signals.go | Start() | 96-112 | Background ticker goroutine (10s interval) |  |
| internal/signals/signals.go | tick() | 114-118 | Runs all 3 signal computations |  |
| internal/signals/signals.go | computeHeartbeatFreshness() | 125-155 | Signal 1: per-cohort freshness ratio |  |
| internal/signals/signals.go | computeCaptureCompleteness() | 161-199 | Signal 2: per-cohort session completeness |  |
| internal/signals/signals.go | RecordCapture() | 71-86 | Records fragment arrival (called by worker) |  |
| internal/signals/signals.go | RecordMappingResult() | 91-93 | Records downstream success/failure |  |
| internal/signals/signals.go | FreshnessWindow | 27 | 60s constant |  |
| internal/signals/signals.go | sessionTTL | 52 | 30min constant |  |
| internal/signals/mapping.go | mappingHealth struct | 14-18 | Time-windowed success ratio tracker |  |
| internal/signals/mapping.go | ratio() | 41-56 | Signal 3: 5-min sliding window success rate |  |
| internal/signals/metrics.go | Metrics struct | 19-36 | All 11 Prometheus collectors |  |
| internal/signals/metrics.go | NewMetrics() | 38-104 | Registers all collectors with Prometheus |  |
| 5. Underlying Architectural Principles |  |  |  |  |
| Principle | Manifestation |  |  |  |
| CQRS (Command Query Responsibility Segregation) | The worker pool writes to the state store and sessions map (command side). The signals computer reads from them (query side). Different write and read patterns, different access frequencies (writes per-event, reads every 10s). |  |  |  |
| Time-Windowed Aggregation | All three signals use time windows: 60s for freshness, 30min TTL for sessions, 5min for mapping health. This prevents stale data from polluting current metrics. |  |  |  |
| Graceful Degradation | mappingHealth.ratio() returns 1.0 on empty window (startup). This prevents false alerts during cold-start when no downstream calls have been made yet. |  |  |  |
| Cohort-Based Observability | Signals are bucketed by `version |  |  |  |
| 6. Gang of Four Design Pattern Analysis |  |  |  |  |
| Pattern Identified: Observer (Behavioral) |  |  |  |  |
| The worker pool and enricher publish events to the Computer via RecordCapture() and RecordMappingResult(). The Computer observes these events and updates its internal state. On each tick, it reads the accumulated state and publishes metrics to Prometheus gauges. Prometheus then scrapes these gauges (another observer relationship). |  |  |  |  |
| GoF 1994 vs Go 2024: The classic Observer uses Subject.attach(Observer) with a list of observers and a notify() loop. In Go, the pattern is simplified: the "subject" (worker pool) calls methods directly on the "observer" (Computer) rather than going through an event bus. This is because Go's concurrency model (goroutines + mutexes) makes direct method calls safe, and the observer count is small (exactly 1 Computer). |  |  |  |  |
| PM View: |  |  |  |  |
| Think of the Signals Computer like a flight control tower monitoring all aircraft. Every plane (client) periodically checks in (heartbeat). The tower doesn't call each plane — it watches the radar (state store) every 10 seconds and counts how many planes checked in recently. If a group of planes (a cohort) goes quiet, the tower raises an alarm. The tower also watches whether the runway mapping system (downstream) is responding to requests. Three radars, three alarms, one control tower. |  |  |  |  |
| Junior Eng View: |  |  |  |  |
| The signals computer has two data input paths: |  |  |  |  |
| 1. Synchronous: RecordCapture(evt) and RecordMappingResult(success) are called directly by the worker goroutines during event processing. These update in-memory data structures (sessions map, mapping samples). |  |  |  |  |
| 2. Periodic query: computeHeartbeatFreshness() calls store.All() every 10 seconds to read all client state. This is a full scan. |  |  |  |  |
|   |  |  |  |  |
| Thread safety concern: Both worker goroutines AND the ticker goroutine access Computer.sessions. The Computer uses a sync.Mutex to protect concurrent access. mappingHealth also has its own mutex. |  |  |  |  |
|   |  |  |  |  |
| Antipattern to avoid: Don't try to compute signals inside the worker's handleEvent. The signal computation needs to see all clients/sessions at once (it computes ratios across cohorts). Per-event computation would be incomplete. |  |  |  |  |
| Senior Eng View: |  |  |  |  |
| The full table scan in computeHeartbeatFreshness() (store.All()) is the single largest scalability bottleneck. At 300 clients, it's trivial. At 300K clients, it's a DynamoDB Scan paginating through the entire table every 10 seconds. The docs/architecture.md acknowledges this and prescribes a GSI on (version, last_heartbeat_at) to enable per-cohort range queries instead of full scans. |  |  |  |  |
|   |  |  |  |  |
| The sessionProgress data structure uses map[int]struct{} for received sequence numbers (set semantics) rather than a counter. This is correct — it deduplicates. If a capture fragment is retried and processed twice, the completeness ratio doesn't over-count. The 30-minute TTL prune is essential: without it, completed sessions would accumulate forever, and the memory footprint would grow linearly with fleet lifetime. |  |  |  |  |
|   |  |  |  |  |
| The mapping health tracker uses a time-pruned slice rather than a ring buffer. This means prune() is O(n) on every call. At 5-minute window with events arriving every ~50ms (enricher latency), the slice could hold ~6,000 samples. This is fine, but if the downstream call rate increased 100x, you'd want a ring buffer with a fixed capacity and binary search for time-based pruning. |  |  |  |  |
| WORKFLOW 6: Infrastructure Portability (Pluggable Backends) |  |  |  |  |
| 1. Plain-English Scenario |  |  |  |  |
| What happens in the real world: Grammarly's platform team decides to migrate from AWS SQS to a self-hosted Kafka cluster for cost reasons. Or a junior engineer needs to run the full system on their laptop without an AWS account. |  |  |  |  |
|   |  |  |  |  |
| How FleetPulse handles it: Both the queue and the state store are behind interfaces with two implementations each. Setting FLEETPULSE_QUEUE_BACKEND=memory and FLEETPULSE_STATE_BACKEND=memory runs everything in-process — no Docker, no LocalStack, no AWS credentials needed. Setting them to sqs and dynamodb switches to the production-grade implementations. The worker pool, signals computer, and ingestion handler don't know or care which backend is active. Adding Kafka would mean implementing queue.Queue in a new package and adding one case to the factory switch. |  |  |  |  |
| 2. Markdown Documentation |  |  |  |  |
| Component: Backend Selection in cmd/server/main.go |  |  |  |  |
| Backend | Interface | Implementations |  |  |
| Queue | queue.Queue | InMemoryQueue (dev), SQSQueue (prod) |  |  |
| State | state.Store | InMemoryStore (dev), DynamoDBStore (prod) |  |  |
| Factory Functions: |  |  |  |  |
| - buildQueue() at cmd/server/main.go:130-151 |  |  |  |  |
| - buildStateStore() at cmd/server/main.go:155-174 |  |  |  |  |
| 3. System Design Diagrams |  |  |  |  |
| High-Level: Ports and Adapters (Hexagonal Architecture) |  |  |  |  |
| ┌─────────────────────────────────┐ |  |  |  |  |
| │       APPLICATION CORE          │ |  |  |  |  |
| │                                 │ |  |  |  |  |
| │   ┌────────────┐                │ |  |  |  |  |
| │   │  Worker    │                │ |  |  |  |  |
| │   │  Pool      │                │ |  |  |  |  |
| │   └──┬───┬─────┘                │ |  |  |  |  |
| │      │   │                      │ |  |  |  |  |
| ┌──────────┼──────┘   └──────┼───────────┐   │ |  |  |  |  |
| │          │                 │           │   │ |  |  |  |  |
| ┌────────▼──────┐   │    ┌────────────▼────────┐  │   │ |  |  |  |  |
| │ «port»        │   │    │  «port»             │  │   │ |  |  |  |  |
| │ queue.Queue   │   │    │  state.Store         │  │   │ |  |  |  |  |
| │               │   │    │                      │  │   │ |  |  |  |  |
| │ Publish()     │   │    │  Upsert()            │  │   │ |  |  |  |  |
| │ Receive()     │   │    │  All()               │  │   │ |  |  |  |  |
| │ Metrics()     │   │    │  Get()               │  │   │ |  |  |  |  |
| │ Close()       │   │    │                      │  │   │ |  |  |  |  |
| └──┬────────┬───┘   │    └──┬──────────────┬────┘  │   │ |  |  |  |  |
| │        │       │       │              │       │   │ |  |  |  |  |
| ┌────▼───┐ ┌──▼────┐  │  ┌───▼─────────┐ ┌──▼────┐ │   │ |  |  |  |  |
| │InMemory│ │  SQS  │  │  │  InMemory   │ │Dynamo │ │   │ |  |  |  |  |
| │ Queue  │ │ Queue │  │  │  Store      │ │ DB    │ │   │ |  |  |  |  |
| │«adapter│ │«adapt.│  │  │  «adapter»  │ │«adapt.│ │   │ |  |  |  |  |
| └────────┘ └───────┘  │  └─────────────┘ └───────┘ │   │ |  |  |  |  |
| │                             │   │ |  |  |  |  |
| │        «port»               │   │ |  |  |  |  |
| │    ┌───────────────┐        │   │ |  |  |  |  |
| │    │ Enricher      │        │   │ |  |  |  |  |
| │    │ Enrich()      │        │   │ |  |  |  |  |
| │    └───┬───────────┘        │   │ |  |  |  |  |
| │        │                    │   │ |  |  |  |  |
| │   ┌────▼────────┐           │   │ |  |  |  |  |
| │   │ StubEnricher│           │   │ |  |  |  |  |
| │   │ «adapter»   │           │   │ |  |  |  |  |
| │   └─────────────┘           │   │ |  |  |  |  |
| └─────────────────────────────────┘ |  |  |  |  |
| 4. Repository & Codebase Mapping |  |  |  |  |
| File | Key Symbol | Line |  |  |
| cmd/server/main.go | buildQueue() | 130-151 |  |  |
| cmd/server/main.go | buildStateStore() | 155-174 |  |  |
| internal/queue/queue.go | Queue interface | 77-92 |  |  |
| internal/queue/inmemory.go | InMemoryQueue | 40-234 |  |  |
| internal/aws/sqs_queue.go | SQSQueue | 89-261 |  |  |
| internal/state/state.go | Store interface | 32-50 |  |  |
| internal/state/state.go | InMemoryStore | 53-99 |  |  |
| internal/aws/dynamodb_store.go | DynamoDBStore | 48-218 |  |  |
| internal/aws/config.go | LoadLocalConfig() | 45-66 |  |  |
| 5. Underlying Architectural Principles |  |  |  |  |
| Principle | Manifestation |  |  |  |
| Hexagonal Architecture (Ports & Adapters) | Interfaces are ports. Implementations are adapters. Core business logic depends only on ports. |  |  |  |
| Configuration-Driven Strategy | One binary, two backend modes, selected by environment variable at startup. No build tags, no conditional compilation. |  |  |  |
| Dependency Inversion (SOLID - D) | worker.Pool depends on queue.Queue (abstraction), not on InMemoryQueue (concretion). The concretion is injected by cmd/server/main.go. |  |  |  |
| Behavioral Parity | The in-memory queue reimplements SQS semantics (visibility timeout, dead-lettering) so dev and prod behavior match. |  |  |  |
| 6. Gang of Four Design Pattern Analysis |  |  |  |  |
| Pattern Identified: Abstract Factory (Creational) |  |  |  |  |
| buildQueue() and buildStateStore() together form an Abstract Factory — they produce families of related objects (queue + store) that are consistent with each other. While you could mix InMemoryQueue with DynamoDBStore, the typical deployment uses matching backends. |  |  |  |  |
| GoF 1994 vs Go 2024: The original Abstract Factory uses an abstract Factory class with createProductA() and createProductB() methods, plus concrete factory subclasses. In Go, the factories are plain functions that return interfaces. There's no factory interface because there's only one factory call site (cmd/server/main.go). Go's pragmatism: if there's only one call site, a function is simpler than a factory interface + implementations. |  |  |  |  |
| PM View: |  |  |  |  |
| This is like a car manufacturer with interchangeable powertrains. The same car body (worker pool, signals computer) can run on a gasoline engine (in-memory, for the test track) or an electric motor (AWS, for the highway). Engineering doesn't rebuild the car for each powertrain — they plug in the engine at the factory and ship. This means development velocity stays high (no AWS costs or setup delays for local dev), and production confidence stays high (the production backend is battle-tested AWS infrastructure). |  |  |  |  |
| Junior Eng View: |  |  |  |  |
| The wiring happens in cmd/server/main.go, the run() function: |  |  |  |  |
| q := buildQueue(ctx, logger)           // reads FLEETPULSE_QUEUE_BACKEND |  |  |  |  |
| stateStore := buildStateStore(ctx, logger)  // reads FLEETPULSE_STATE_BACKEND |  |  |  |  |
| pool := worker.NewPool(..., q, stateStore, ...) |  |  |  |  |
| q is type queue.Queue (interface). stateStore is type state.Store (interface). The pool never knows which concrete type it received. |  |  |  |  |
|   |  |  |  |  |
| To add a Redis state backend: |  |  |  |  |
| 1. Create internal/redis/store.go |  |  |  |  |
| 2. Define RedisStore struct, implement Upsert, All, Get |  |  |  |  |
| 3. Add case "redis": in buildStateStore() at cmd/server/main.go:155 |  |  |  |  |
| 4. Add github.com/redis/go-redis/v9 to go.mod |  |  |  |  |
|   |  |  |  |  |
| Mistake to avoid: Don't type-assert back to the concrete type inside the worker pool. If you see q.(*InMemoryQueue).someInternalMethod(), you've broken the abstraction and the SQS backend will crash at runtime. |  |  |  |  |
| Senior Eng View: |  |  |  |  |
| The clean interface split pays dividends beyond testability. It enables gradual migration: you could deploy with FLEETPULSE_QUEUE_BACKEND=sqs + FLEETPULSE_STATE_BACKEND=memory to test just the SQS path in staging before enabling DynamoDB. This partial deployment capability is a direct consequence of the two backends being independently selectable. |  |  |  |  |
|   |  |  |  |  |
| The DynamoDBStore.Upsert() implementation at dynamodb_store.go:72 uses UpdateItem with SET + if_not_exists(registered_at, :now). This is a DynamoDB conditional write — it creates the record atomically on first write and preserves registered_at on subsequent writes. The in-memory store achieves the same semantics with a map lookup + conditional set. This behavioral parity is non-trivial and is the kind of detail that breaks if you're not careful. |  |  |  |  |
|   |  |  |  |  |
| Known scaling concern: DynamoDBStore.All() does a full Scan with pagination. At scale, this would need to become a GSI query per cohort, or the signal computation would need to switch to a streaming architecture where workers push to per-cohort counters rather than the computer pulling all state. |  |  |  |  |
| Cross-Cutting Concern: Observability Pipeline |  |  |  |  |
| End-to-End Metrics Flow |  |  |  |  |
| ┌──────────────────────────────────────────────────────────────────┐ |  |  |  |  |
| │                     METRICS DATA FLOW                            │ |  |  |  |  |
| │                                                                  │ |  |  |  |  |
| │  Event arrives ─────────────────────────────────────────────────►│ |  |  |  |  |
| │                                                                  │ |  |  |  |  |
| │  1. Ingest accepted?                                             │ |  |  |  |  |
| │     YES → IncAccepted(evt)  → ingest_events_accepted_total++     │ |  |  |  |  |
| │     NO  → IncRejected(rsn)  → ingest_events_rejected_total++     │ |  |  |  |  |
| │                                                                  │ |  |  |  |  |
| │  2. Queue publish                                                │ |  |  |  |  |
| │     (every 2s) → q.Metrics() → queue_backlog.Set()               │ |  |  |  |  |
| │                               → queue_in_flight.Set()            │ |  |  |  |  |
| │                               → queue_dead_lettered_total.Set()  │ |  |  |  |  |
| │                                                                  │ |  |  |  |  |
| │  3. Worker processes event                                       │ |  |  |  |  |
| │     → worker_processing_duration_seconds.Observe(duration)        │ |  |  |  |  |
| │     → worker_events_processed_total{outcome=X}.Inc()              │ |  |  |  |  |
| │     → worker_retries_total.Inc() (on transient retry)            │ |  |  |  |  |
| │                                                                  │ |  |  |  |  |
| │  4. Enricher calls downstream                                    │ |  |  |  |  |
| │     → computer.RecordMappingResult(success)                      │ |  |  |  |  |
| │       → mappingHealth.record(success)                            │ |  |  |  |  |
| │                                                                  │ |  |  |  |  |
| │  5. Signals ticker (every 10s)                                   │ |  |  |  |  |
| │     → heartbeat_freshness_ratio{v,os,r}.Set(ratio)               │ |  |  |  |  |
| │     → capture_completeness_ratio{v,os,r}.Set(ratio)              │ |  |  |  |  |
| │     → mapping_availability_ratio.Set(ratio)                      │ |  |  |  |  |
| │                                                                  │ |  |  |  |  |
| │  6. Prometheus scrapes GET /metrics (every 5s)                   │ |  |  |  |  |
| │     → all 11 metrics exposed in text format                      │ |  |  |  |  |
| │                                                                  │ |  |  |  |  |
| │  7. Grafana queries Prometheus                                   │ |  |  |  |  |
| │     → 6-panel dashboard renders live fleet health                 │ |  |  |  |  |
| │                                                                  │ |  |  |  |  |
| └──────────────────────────────────────────────────────────────────┘ |  |  |  |  |
| Alert Thresholds (from docs/signals.md) |  |  |  |  |
| Signal | Warn | Page |  |  |
| Heartbeat Freshness | <99% for 5min | <95% for 5min, OR <50% for any cohort >1% of fleet for 2min |  |  |
| Capture Completeness | <97% for 15min | <90% for 15min for cohorts >1% of session volume |  |  |
| Mapping Availability | <99.5% for 3min | <90% for 2min, OR 0% immediately |  |  |
| Complete GoF Pattern Summary |  |  |  |  |
| Workflow | Pattern | Category | GoF 1994 |  |
| Ingestion | Adapter | Structural | Class inheritance adapter |  |
| Validation | Strategy | Behavioral | Interface hierarchy + subclasses |  |
| Queue Decoupling | Factory Method + Strategy | Creational + Behavioral | Abstract creator class |  |
| Worker Processing | Template Method | Behavioral | Abstract class with hook methods |  |
| Signal Computation | Observer | Behavioral | Subject.attach(Observer) list |  |
| Backend Portability | Abstract Factory | Creational | Factory interface + factory subclasses |  |
| File Index: Complete Repository Map |  |  |  |  |
| File | Lines | Package | Responsibility |  |
| cmd/server/main.go | 181 | main | Entry point, DI wiring, backend selection, HTTP mux, graceful shutdown |  |
| cmd/simulator/main.go | 97 | main | Simulator entry point, CLI flags, env var parsing |  |
| cmd/downstream/main.go | 185 | main | Downstream stub service, failure injection admin API |  |
| internal/ingest/handler.go | 155 | ingest | HTTP handler for POST /v1/events, batch decode |  |
| internal/ingest/types.go | 84 | ingest | Wire schema, canonical schema, EventType, Cohort |  |
| internal/ingest/validate.go | 144 | ingest | Normalize(), all validation, OS/region/version normalization |  |
| internal/queue/queue.go | 92 | queue | Queue/Delivery/Message/Metrics interfaces |  |
| internal/queue/inmemory.go | 234 | queue | In-memory queue with visibility timeout reaper |  |
| internal/worker/pool.go | 186 | worker | Worker pool, retry/dead-letter logic |  |
| internal/worker/enrich.go | 105 | worker | Enricher interface + StubEnricher |  |
| internal/worker/errors.go | 15 | worker | ErrTransient sentinel + IsTransient() |  |
| internal/signals/signals.go | 200 | signals | Computer: freshness, completeness computation |  |
| internal/signals/metrics.go | 114 | signals | All 11 Prometheus collectors |  |
| internal/signals/mapping.go | 68 | signals | Time-windowed mapping health tracker |  |
| internal/state/state.go | 99 | state | Store interface + InMemoryStore |  |
| internal/downstreamclient/client.go | 88 | downstreamclient | HTTP client for downstream workflow mapping |  |
| internal/aws/config.go | 83 | aws | AWS SDK config loader, LocalStack endpoint |  |
| internal/aws/sqs_queue.go | 261 | aws | SQS-backed Queue implementation |  |
| internal/aws/dynamodb_store.go | 218 | aws | DynamoDB-backed Store implementation |  |
| internal/simulator/simulator.go | 140 | simulator | Fleet simulator orchestrator |  |
| internal/simulator/client.go | 188 | simulator | Per-client goroutine loop |  |
| internal/simulator/cohort.go | 60 | simulator | Cohort pool, weighted version distribution |  |
| internal/simulator/silence.go | 42 | simulator | Thread-safe silenced-cohort set |  |
| Total: 23 Go source files, ~3,300 lines, 8 internal packages, 3 binaries, 0 test files. |  |  |  |  |
| # Todos |  |  |  |  |
| [✓] Explore codebase structure, entry points, and config |  |  |  |  |
| [✓] Explore core domain models and data layer |  |  |  |  |
| [✓] Explore business logic, services, and handlers |  |  |  |  |
| [✓] Explore API layer, routing, middleware, and external integrations |  |  |  |  |
| [✓] Synthesize full architectural analysis document |  |  |  |  |
| + Thought: 7.2s |  |  |  |  |
| Continuing with the remaining workflows and the concluding architectural assessment. |  |  |  |  |
| WORKFLOW 7: Fleet Simulation & Kill-Cohort Demo |  |  |  |  |
| 1. Plain-English Scenario |  |  |  |  |
| What happens in the real world: Grammarly's QA team needs to reproduce a production incident where version 2.6.0-beta suddenly stopped reporting from all macOS clients in the EU region. They can't break production to test their monitoring, so they need a simulator that generates realistic fleet traffic — and a "kill switch" that silences a specific version cohort on demand to verify that the Grafana dashboard and alerting rules detect the drop. |  |  |  |  |
|   |  |  |  |  |
| How FleetPulse handles it: The simulator binary spawns 300 goroutines, each representing a unique client with a randomly assigned cohort (version | os | region). Version distribution is weighted: 55% on stable 2.5.1, 25% on 2.5.0, 12% on legacy 2.4.0, and 8% on beta 2.6.0-beta. Every 30 seconds each client sends a heartbeat. With 15% probability it also sends a capture session (3-8 fragments, 8% per-fragment loss rate). With 3% probability it sends an error. After a configurable delay (default 2 minutes), the kill-cohort timer fires and silences 2.6.0-beta — those clients stop sending anything. The heartbeat freshness gauge for 2.6.0-beta drops from ~100% to 0% over the next 60 seconds, triggering a visible cliff on the Grafana dashboard. |  |  |
| 2. Markdown Documentation |  |  |  |  |
| Component: internal/simulator/ — Fleet Simulator Package (4 files) |  |  |  |  |
| File | Responsibility |  |  |  |
| simulator.go | Orchestrator: launches N client goroutines + kill-cohort timer |  |  |  |
| client.go | Per-client loop: heartbeat, capture session, error emission |  |  |  |
| cohort.go | Version/OS/Region pools + weighted version sampling |  |  |  |
| silence.go | Thread-safe silenced-cohort set |  |  |  |
| Configuration (simulator.Config): |  |  |  |  |
| Field | Default | Purpose |  |  |
| NumClients | 300 | Total simulated endpoints |  |  |
| IngestURL | http://localhost:8080/v1/events | Target ingestion server |  |  |
| HeartbeatInterval | 30s | Time between heartbeats per client |  |  |
| CaptureProbability | 0.15 (15%) | Chance of capture session per tick |  |  |
| ErrorProbability | 0.03 (3%) | Chance of error event per tick |  |  |
| CaptureLossRate | 0.08 (8%) | Per-fragment drop probability |  |  |
| KillCohort | "" | Version string to silence (e.g., "2.6.0-beta") |  |  |
| KillCohortAfter | 2m | Delay before kill activates |  |  |
| Weighted Version Distribution: |  |  |  |  |
| Version | Weight | ~% of fleet |  |  |
| 2.5.1 | 55 | 55% |  |  |
| 2.5.0 | 25 | 25% |  |  |
| 2.4.0 | 12 | 12% |  |  |
| 2.6.0-beta | 8 | 8% |  |  |
| 3. System Design Diagrams |  |  |  |  |
| High-Level: Simulator Architecture |  |  |  |  |
| ┌──────────────────────────────────────────────────────────┐ |  |  |  |  |
| │  Simulator Binary (cmd/simulator)                        │ |  |  |  |  |
| │                                                          │ |  |  |  |  |
| │  Simulator.Run(ctx)                                      │ |  |  |  |  |
| │    │                                                     │ |  |  |  |  |
| │    ├─► for i := 0..299:                                  │ |  |  |  |  |
| │    │     cohort = randomCohort()                          │ |  |  |  |  |
| │    │       version = weightedRandom(55/25/12/8)           │ |  |  |  |  |
| │    │       os = random(windows, macos, linux)             │ |  |  |  |  |
| │    │       region = random(us-east, us-west, eu, apac)    │ |  |  |  |  |
| │    │     clientID = fmt.Sprintf("sim-client-%05d", i)     │ |  |  |  |  |
| │    │     go client.run(ctx)  ──────────────────┐          │ |  |  |  |  |
| │    │                                          │          │ |  |  |  |  |
| │    │    ┌─────────────────────────────────────┐│          │ |  |  |  |  |
| │    │    │  client.run(ctx)                    ││          │ |  |  |  |  |
| │    │    │  sleep(random 0..interval) ← stagger││          │ |  |  |  |  |
| │    │    │  ticker every 30s:                  ││          │ |  |  |  |  |
| │    │    │    if silenced(version): skip       ││          │ |  |  |  |  |
| │    │    │    else: tick()                     ││          │ |  |  |  |  |
| │    │    │      ├─ sendHeartbeat()             ││          │ |  |  |  |  |
| │    │    │      ├─ 15%: sendCaptureSession()   ││          │ |  |  |  |  |
| │    │    │      │   3-8 fragments              ││          │ |  |  |  |  |
| │    │    │      │   8% per-frag loss            ││          │ |  |  |  |  |
| │    │    │      └─ 3%: sendError()             ││          │ |  |  |  |  |
| │    │    │         random error message         ││          │ |  |  |  |  |
| │    │    │                                     ││          │ |  |  |  |  |
| │    │    │  post(events) ──► POST /v1/events   ││          │ |  |  |  |  |
| │    │    └─────────────────────────────────────┘│          │ |  |  |  |  |
| │    │                                          │          │ |  |  |  |  |
| │    └─► if KillCohort != "":                   │          │ |  |  |  |  |
| │          go killCohortTimer(ctx)              │          │ |  |  |  |  |
| │            sleep(KillCohortAfter)             │          │ |  |  |  |  |
| │            silencedCohorts.Silence("2.6.0-beta")         │ |  |  |  |  |
| │            ↓                                  │          │ |  |  |  |  |
| │          all "2.6.0-beta" clients skip tick    │          │ |  |  |  |  |
| │          heartbeat freshness drops to 0%       │          │ |  |  |  |  |
| │                                                          │ |  |  |  |  |
| └──────────────────────────────────────────────────────────┘ |  |  |  |  |
| Low-Level: Capture Session Generation |  |  |  |  |
| sendCaptureSession(ctx) |  |  |  |  |
| │ |  |  |  |  |
| ├─ sessionID = uuid.New() |  |  |  |  |
| ├─ expected = rand.Intn(6) + 3     ← 3 to 8 fragments |  |  |  |  |
| │ |  |  |  |  |
| ├─ events = []RawEvent{} |  |  |  |  |
| │ |  |  |  |  |
| ├─ for seq := 0; seq < expected; seq++: |  |  |  |  |
| │    │ |  |  |  |  |
| │    ├─ rand.Float64() < 0.08?      ← 8% loss rate |  |  |  |  |
| │    │    YES → skip this fragment (simulates network drop) |  |  |  |  |
| │    │    NO  → append RawEvent{ |  |  |  |  |
| │    │              ClientID, Type:"capture", Version, OS, Region, |  |  |  |  |
| │    │              Timestamp: now(), SessionID: sessionID, |  |  |  |  |
| │    │              ExpectedEventCount: expected, |  |  |  |  |
| │    │              SequenceNumber: seq |  |  |  |  |
| │    │           } |  |  |  |  |
| │    │ |  |  |  |  |
| │ |  |  |  |  |
| └─ post(ctx, events)   ← batch POST all surviving fragments |  |  |  |  |
|   |  |  |  |  |
| Example: expected=5, loss rate hits seq=2: |  |  |  |  |
| Sent:    [seq=0, seq=1, seq=3, seq=4] |  |  |  |  |
| Missing: [seq=2] |  |  |  |  |
| Server sees: completeness = 4/5 = 0.80 for this session |  |  |  |  |
| 4. Repository & Codebase Mapping |  |  |  |  |
| File | Key Symbol | Line | Purpose |  |
| internal/simulator/simulator.go | Simulator struct | 48-52 | Holds config, HTTP client, silenced set |  |
| internal/simulator/simulator.go | Run() | 77-127 | Launch N goroutines + kill timer, block until done |  |
| internal/simulator/simulator.go | runKillCohortTimer() | 130-140 | One-shot timer → Silence(version) |  |
| internal/simulator/client.go | client struct | 31-40 | Per-client state: ID, cohort, config refs |  |
| internal/simulator/client.go | run() | 59-88 | Staggered start + infinite tick loop |  |
| internal/simulator/client.go | tick() | 90-106 | Heartbeat + probabilistic capture/error |  |
| internal/simulator/client.go | sendHeartbeat() | 108-123 | Builds single heartbeat RawEvent |  |
| internal/simulator/client.go | sendCaptureSession() | 125-150 | Builds fragment batch with loss simulation |  |
| internal/simulator/client.go | sendError() | 152-164 | Random error message from pool |  |
| internal/simulator/client.go | post() | 166-188 | HTTP POST JSON array to ingestion |  |
| internal/simulator/cohort.go | randomCohort() | 19-25 | Generates random {version, os, region} |  |
| internal/simulator/cohort.go | weightedVersion() | 37-57 | Weighted random from version distribution |  |
| internal/simulator/silence.go | SilencedCohorts | 17-19 | sync.RWMutex-guarded map[string]bool |  |
| internal/simulator/silence.go | Silence() / IsSilenced() | 26-42 | Thread-safe add/check |  |
| cmd/simulator/main.go | main() / run() | 22-97 | CLI flags, env vars, config assembly |  |
| Where to add new features: |  |  |  |  |
| - New event types in simulation → add sendCrashReport() method to client.go, call from tick(). |  |  |  |  |
| - Gradual rollout simulation → modify weightedVersion() to shift weights over time. |  |  |  |  |
| - Admin API to unsilence → noted as a gap in docs/runbook.md; would need an HTTP endpoint in simulator.go. |  |  |  |  |
| 5. Underlying Architectural Principles |  |  |  |  |
| Principle | Manifestation |  |  |  |
| Staggered Start (Anti-Thundering-Herd) | Each goroutine sleeps random(0, interval) before its first tick (client.go:63-65). This prevents 300 heartbeats from arriving in a single burst at t=0. |  |  |  |
| Statistical Realism | Weighted version distribution mirrors real-world adoption curves (most users on stable, small % on beta). 8% loss rate on captures simulates real network conditions. |  |  |  |
| Chaos Engineering | The kill-cohort feature is a controlled chaos experiment: intentionally degrade one dimension of the system to verify observability detects it. |  |  |  |
| Separation of Concerns | The simulator is a separate binary (cmd/simulator), not a test mode within the server. It uses the same HTTP API that real clients would use. |  |  |  |
| 6. Gang of Four Design Pattern Analysis |  |  |  |  |
| Pattern Identified: Prototype (Creational) — via cohort template reuse |  |  |  |  |
| Each simulated client is constructed from a Cohort prototype — a {version, os, region} tuple randomly assembled from pools. The cohort acts as a template that's "stamped out" onto each client goroutine. The SilencedCohorts set then acts on these prototypes by version key, silencing all clients sharing a version without iterating over individual clients. |  |  |  |  |
| GoF 1994 vs Go 2024: The original Prototype pattern clones objects via a clone() method. In Go, the Cohort value type is naturally copyable (it's a struct of strings). There's no Clone() method needed — Go's value semantics handle copying implicitly. The "prototype" here is the pool of version/OS/region strings from which cohorts are assembled, rather than a clonable object. |  |  |  |  |
| PM View: |  |  |  |  |
| The simulator is like a movie set with 300 extras. Each extra wears a costume (cohort) picked from a wardrobe (version/OS/region pools). The director (kill-cohort timer) can shout "all extras in the beta costume — freeze!" and they stop moving. The cameras (Grafana dashboards) should immediately show that the beta group went silent. This validates that your monitoring works before a real incident happens. The alternative — waiting for a real production outage to test your alerts — is what separates mature SRE teams from ones that get surprised at 3 AM. |  |  |  |  |
| Junior Eng View: |  |  |  |  |
| The kill-cohort flow: |  |  |  |  |
| 1. cmd/simulator/main.go reads FLEETPULSE_SIM_KILL_COHORT=2.6.0-beta |  |  |  |  |
| 2. Simulator.Run() launches go runKillCohortTimer(ctx) at simulator.go:130 |  |  |  |  |
| 3. After 2 minutes, silencedCohorts.Silence("2.6.0-beta") is called |  |  |  |  |
| 4. In every client's run() loop at client.go:77, the check if sim.silenced.IsSilenced(c.cohort.Version) returns true |  |  |  |  |
| 5. The client skips tick() — no heartbeat, no captures, no errors |  |  |  |  |
| 6. Within 60 seconds (the freshness window), computeHeartbeatFreshness() sees zero fresh heartbeats for the 2.6.0-beta cohort |  |  |  |  |
| 7. The gauge drops to 0.0, visible in Grafana |  |  |  |  |
|   |  |  |  |  |
| Mistake to avoid: Don't silence by cohort key (e.g., "2.6.0-beta | macos | eu"). The silence is by version only, which is correct — a version-level crash bug affects all OS/region combinations for that version. |  |  |
| Senior Eng View: |  |  |  |  |
| The staggered start is subtly important. Without it, all 300 clients would fire their first heartbeat at t=0, then again at t=30s, t=60s, etc. — creating perfectly periodic traffic bursts. This would make the freshness signal artificially binary: either 100% (within the 30s window) or 0% (all heartbeats expire simultaneously). The random stagger distributes heartbeats uniformly across the 30s interval, producing realistic continuous traffic and smooth signal transitions. |  |  |  |  |
|   |  |  |  |  |
| The SilencedCohorts uses a sync.RWMutex — read-heavy pattern (300 goroutines checking IsSilenced every 30s) with rare writes (one Silence() call). RWMutex is correct here: readers don't block each other. A regular sync.Mutex would serialize all 300 goroutines on every tick, adding unnecessary contention. |  |  |  |  |
| WORKFLOW 8: Downstream Dependency & Failure Injection |  |  |  |  |
| 1. Plain-English Scenario |  |  |  |  |
| What happens in the real world: Grammarly's fleet depends on a "Workflow Mapping" microservice that registers each capture session fragment in a central workflow engine. This service is maintained by a different team and has its own availability characteristics — sometimes it's slow, sometimes it returns 500s, sometimes it's completely down for maintenance. The SRE team needs to verify that FleetPulse's mapping_availability_ratio signal correctly reflects the downstream's health and that on-call gets paged when it drops below 99.5%. |  |  |  |  |
|   |  |  |  |  |
| How FleetPulse handles it: The downstream binary is a standalone HTTP stub that mimics the real Workflow Mapping service. It has an /admin/config endpoint where you can inject failures in real-time: set failure_rate: 0.6 to make 60% of requests fail, set timeout_ms: 3000 to simulate 3-second latency, or set down: true to take it completely offline. The worker pool's enricher calls this stub for every capture event, and the success/failure outcome feeds the mappingHealth tracker, which computes the 5-minute rolling availability ratio. |  |  |  |  |
| 2. Markdown Documentation |  |  |  |  |
| Component: cmd/downstream/main.go — Downstream Stub Service |  |  |  |  |
| Endpoint | Method | Purpose |  |  |
| /v1/workflow-mapping | POST | Accept mapping requests, return {mapping_id, status} |  |  |
| /healthz | GET | Liveness probe — returns "ok" |  |  |
| /admin/config | GET | View current failure injection config |  |  |
| /admin/config | POST | Set failure injection: {failure_rate, timeout_ms, down} |  |  |
| Failure Injection Config (adminConfig): |  |  |  |  |
| Field | Type | Range | Effect |  |
| failure_rate | float64 | 0.0–1.0 | Probability of returning HTTP 500 |  |
| timeout_ms | int | ≥0 | Artificial latency injected before response |  |
| down | bool | true/false | If true, all requests return HTTP 503 immediately |  |
| Processing Order for /v1/workflow-mapping (lines 108-126): |  |  |  |  |
| 1. Apply timeout_ms sleep (always, if >0) |  |  |  |  |
| 2. Check down flag → 503 Service Unavailable |  |  |  |  |
| 3. Roll against failure_rate → 500 Internal Server Error |  |  |  |  |
| 4. Success → 200 with {mapping_id: uuid, status: "mapped"} |  |  |  |  |
| 3. System Design Diagrams |  |  |  |  |
| High-Level: Downstream Failure Injection Flow |  |  |  |  |
| ┌───────────────────────────┐ |  |  |  |  |
| │  Worker Pool              │ |  |  |  |  |
| │                           │ |  |  |  |  |
| │  StubEnricher.Enrich()    │ |  |  |  |  |
| │    │                      │ |  |  |  |  |
| │    ├─ [capture event]     │ |  |  |  |  |
| │    │   mapWorkflow()      │ |  |  |  |  |
| │    │     │                │ |  |  |  |  |
| │    │     ▼                │ |  |  |  |  |
| │    │   downstreamclient   │ |  |  |  |  |
| │    │   .Map(MapRequest)   │ |  |  |  |  |
| │    │     │                │ |  |  |  |  |
| └────┼─────┼────────────────┘ |  |  |  |  |
| │     │ |  |  |  |  |
| │     │ POST /v1/workflow-mapping |  |  |  |  |
| │     │ {client_id, session_id, sequence_number} |  |  |  |  |
| │     ▼ |  |  |  |  |
| ┌────────────────────────────────────────────────┐ |  |  |  |  |
| │  Downstream Stub (:8090)                       │ |  |  |  |  |
| │                                                │ |  |  |  |  |
| │  handleMap(w, r)                               │ |  |  |  |  |
| │    │                                           │ |  |  |  |  |
| │    ├─► cfg = atomic.Load(adminConfig)           │ |  |  |  |  |
| │    │                                           │ |  |  |  |  |
| │    ├─► timeout_ms > 0?                          │ |  |  |  |  |
| │    │     YES → sleep(timeout_ms)                │ |  |  |  |  |
| │    │                                           │ |  |  |  |  |
| │    ├─► cfg.Down?                                │ |  |  |  |  |
| │    │     YES → 503 "service unavailable"        │ |  |  |  |  |
| │    │                                           │ |  |  |  |  |
| │    ├─► rand < cfg.FailureRate?                  │ |  |  |  |  |
| │    │     YES → 500 "internal error"             │ |  |  |  |  |
| │    │                                           │ |  |  |  |  |
| │    └─► 200 {mapping_id: uuid, status: "mapped"} │ |  |  |  |  |
| │                                                │ |  |  |  |  |
| │  ┌─────────────────────────────────────┐       │ |  |  |  |  |
| │  │  /admin/config                      │       │ |  |  |  |  |
| │  │  GET  → view {failure_rate, ...}    │       │ |  |  |  |  |
| │  │  POST → set  {failure_rate: 0.6,    │       │ |  |  |  |  |
| │  │                timeout_ms: 3000,    │       │ |  |  |  |  |
| │  │                down: false}         │       │ |  |  |  |  |
| │  └─────────────────────────────────────┘       │ |  |  |  |  |
| └────────────────────────────────────────────────┘ |  |  |  |  |
| │ |  |  |  |  |
| │ result |  |  |  |  |
| ▼ |  |  |  |  |
| ┌───────────────────────────┐ |  |  |  |  |
| │  StubEnricher             │ |  |  |  |  |
| │  mapWorkflow() result:    │ |  |  |  |  |
| │    │                      │ |  |  |  |  |
| │    ├─ success              │ |  |  |  |  |
| │    │   computer            │ |  |  |  |  |
| │    │   .RecordMappingResult│ |  |  |  |  |
| │    │   (true)              │ |  |  |  |  |
| │    │                      │ |  |  |  |  |
| │    └─ failure              │ |  |  |  |  |
| │        computer            │ |  |  |  |  |
| │        .RecordMappingResult│ |  |  |  |  |
| │        (false)             │ |  |  |  |  |
| │        return ErrTransient │ |  |  |  |  |
| │        → triggers retry    │ |  |  |  |  |
| └───────────────────────────┘ |  |  |  |  |
| Low-Level: Mapping Health Rolling Window |  |  |  |  |
| mappingHealth struct |  |  |  |  |
| ┌──────────────────────────────────────────────────────┐ |  |  |  |  |
| │  window: 5 minutes                                   │ |  |  |  |  |
| │  samples: []sample                                   │ |  |  |  |  |
| │                                                      │ |  |  |  |  |
| │  Timeline example (each ✓ = success, ✗ = failure):   │ |  |  |  |  |
| │                                                      │ |  |  |  |  |
| │  t=0     t=1m    t=2m    t=3m    t=4m    t=5m       │ |  |  |  |  |
| │  ├───────┼───────┼───────┼───────┼───────┤          │ |  |  |  |  |
| │  │ ✓✓✓✓✓ │ ✓✓✓✓✓ │ ✓✓✗✓✓ │ ✗✗✗✗✗ │ ✓✓✓✓✓ │          │ |  |  |  |  |
| │  │       │       │       │       │       │          │ |  |  |  |  |
| │  └───────┴───────┴───────┴───────┴───────┘          │ |  |  |  |  |
| │                                                      │ |  |  |  |  |
| │  At t=5m, ratio() computes:                          │ |  |  |  |  |
| │    total = 25 samples                                │ |  |  |  |  |
| │    successes = 20                                     │ |  |  |  |  |
| │    ratio = 20/25 = 0.80                              │ |  |  |  |  |
| │    → below 99.5% target → WARN                       │ |  |  |  |  |
| │    → below 90% threshold → PAGE                      │ |  |  |  |  |
| │                                                      │ |  |  |  |  |
| │  At t=6m (window slides, t=0..1m samples pruned):    │ |  |  |  |  |
| │    total = 20 samples                                │ |  |  |  |  |
| │    successes = 15                                     │ |  |  |  |  |
| │    ratio = 15/20 = 0.75                              │ |  |  |  |  |
| │    → still below threshold                           │ |  |  |  |  |
| │                                                      │ |  |  |  |  |
| │  Edge case: empty window (startup):                  │ |  |  |  |  |
| │    ratio() returns 1.0  ← no data ≠ failure          │ |  |  |  |  |
| └──────────────────────────────────────────────────────┘ |  |  |  |  |
| 4. Repository & Codebase Mapping |  |  |  |  |
| File | Key Symbol | Line |  |  |
| cmd/downstream/main.go | server struct | 28-31 |  |  |
| cmd/downstream/main.go | adminConfig struct | 33-46 |  |  |
| cmd/downstream/main.go | handleMap() | 102-146 |  |  |
| cmd/downstream/main.go | handleAdminConfig() | 149-172 |  |  |
| cmd/downstream/main.go | mapRequest / mapResponse | 91-100 |  |  |
| internal/downstreamclient/client.go | Client struct | 21-24 |  |  |
| internal/downstreamclient/client.go | New() | 27-36 |  |  |
| internal/downstreamclient/client.go | Map() | 60-88 |  |  |
| internal/downstreamclient/client.go | ErrDownstreamUnavailable | 55 |  |  |
| internal/downstreamclient/client.go | MapRequest / MapResponse | 38-48 |  |  |
| internal/worker/enrich.go | mapWorkflow() | 82-96 |  |  |
| internal/signals/mapping.go | mappingHealth | 14-18 |  |  |
| internal/signals/mapping.go | record() | 30-35 |  |  |
| internal/signals/mapping.go | ratio() | 41-56 |  |  |
| 5. Underlying Architectural Principles |  |  |  |  |
| Principle | Manifestation |  |  |  |
| Chaos Engineering | The admin config endpoint lets operators inject degradation (latency, partial failure, full outage) into a running system to test observability. This is Netflix Chaos Monkey philosophy at the service level. |  |  |  |
| Circuit Breaker Readiness | The system measures downstream health (mappingHealth.ratio()) but doesn't yet act on it. A natural extension would be a circuit breaker that stops calling the downstream when ratio < threshold, reducing cascading load. |  |  |  |
| Failure Hierarchy | Timeout is applied before the down/failure checks (line 108-126). This means even "successful" responses can be slow, exercising the downstream client's timeout logic independently from error-rate testing. |  |  |  |
| Independent Deployability | The downstream stub is a separate binary, separate Docker container, separate port. It can be restarted, reconfigured, or replaced without touching the ingestion server. |  |  |  |
| 6. Gang of Four Design Pattern Analysis |  |  |  |  |
| Pattern Identified: Proxy (Structural) |  |  |  |  |
| The downstreamclient.Client at internal/downstreamclient/client.go is a Proxy for the remote downstream service. It encapsulates the HTTP transport details (URL construction, JSON serialization, timeout handling, error wrapping) behind a clean Map(ctx, MapRequest) (MapResponse, error) interface. The worker pool never constructs HTTP requests directly — it goes through the proxy. |  |  |  |  |
| GoF 1994 vs Go 2024: The original Proxy pattern in C++ creates a local representative for a remote object, implementing the same interface as the real subject. In Go, the proxy is a struct with methods that match the expected call pattern. There's no formal interface that both the proxy and the real service implement (the real service is remote HTTP, not a Go type). Instead, the proxy provides a typed Go API over an untyped HTTP boundary — it's a Remote Proxy in GoF taxonomy. |  |  |  |  |
| PM View: |  |  |  |  |
| The downstream client is like a travel agent for the Workflow Mapping service. Instead of each worker calling the airline (HTTP service) directly and dealing with booking codes, confirmations, and cancellations, they go through the travel agent who handles all the logistics and returns a simple result: "booked" or "failed." If the airline changes its booking system, only the travel agent needs to update — the workers never know. This protects engineering velocity: when the real Workflow Mapping team changes their API, you update one file (client.go) instead of every worker. |  |  |  |  |
| Junior Eng View: |  |  |  |  |
| The call chain is: |  |  |  |  |
| 1. StubEnricher.mapWorkflow() at enrich.go:82 calls e.downstream.Map(ctx, req) |  |  |  |  |
| 2. Client.Map() at client.go:60 builds the HTTP request, POSTs JSON, reads response |  |  |  |  |
| 3. On non-200: returns ErrDownstreamUnavailable (the sentinel) |  |  |  |  |
| 4. mapWorkflow() catches the error, calls computer.RecordMappingResult(err == nil) |  |  |  |  |
| 5. If error: wraps it as ErrTransient → triggers retry in process() |  |  |  |  |
|   |  |  |  |  |
| To add a new downstream dependency (e.g., a geo-IP lookup): |  |  |  |  |
| 1. Create internal/geoipclient/client.go with a struct and Lookup(ctx, ip) (Location, error) method |  |  |  |  |
| 2. Add it as a field on StubEnricher |  |  |  |  |
| 3. Call it from Enrich() for relevant event types |  |  |  |  |
| 4. Wire the client in cmd/server/main.go alongside the downstream client |  |  |  |  |
| Senior Eng View: |  |  |  |  |
| The downstream client has a hardcoded 3-second timeout (cmd/server/main.go:72). In a production system, this timeout interacts with the SQS visibility timeout (10s) and the retry count (5). If every attempt takes 3s and there are 5 retries, a single event could take 15s before dead-lettering. With 8 workers, 8 concurrent slow requests could bottleneck the pool. A circuit breaker (e.g., sony/gobreaker) would solve this by fast-failing after detecting sustained downstream degradation, preserving worker capacity for events that don't need the downstream. |  |  |  |  |
|   |  |  |  |  |
| The ErrDownstreamUnavailable sentinel at client.go:55 is wrapped as ErrTransient in enrich.go:93. This is a deliberate classification decision: downstream HTTP failures are always retryable. But consider: a persistent 404 (wrong URL) would also be classified as transient and retried 5 times. A more nuanced approach would classify 4xx errors as permanent and 5xx/timeout as transient. |  |  |  |  |
| Architectural Assessment: Trade-Offs & Recommendations |  |  |  |  |
| What FleetPulse Gets Right |  |  |  |  |
| Strength | Evidence |  |  |  |
| Clean interface boundaries | 5 core interfaces (Queue, Delivery, Store, Publisher, Enricher) with 2 implementations each. The core business logic is infrastructure-agnostic. |  |  |  |
| Anti-corruption layer | RawEvent → Event transformation at the boundary. Interior code never handles untrusted data. SQS uses a separate wireEvent DTO. DynamoDB uses a separate clientRecord DTO. |  |  |  |
| Behavioral parity between backends | In-memory queue reimplements SQS semantics (visibility timeout, dead-lettering) to prevent dev/prod behavioral divergence. |  |  |  |
| Observable by design | 11 Prometheus metrics covering every pipeline stage. Pre-built Grafana dashboard. Signal thresholds documented in docs/signals.md. |  |  |  |
| Graceful shutdown | cmd/server/main.go handles SIGINT/SIGTERM with context cancellation, propagating cleanly through worker goroutines. |  |  |  |
| Known Gaps & Scaling Concerns |  |  |  |  |
| Gap | Location | Risk | Recommendation |  |
| No auth on any endpoint | All 3 binaries | Anyone can POST fake events or inject failures | Add API key middleware on /v1/events; restrict /admin/config to internal network or require auth |  |
| Full table scan in All() | dynamodb_store.go:129 | O(N) on client count, runs every 10s | Add GSI (version, last_heartbeat_at), query per-cohort |  |
| No retry backoff | worker/pool.go:137 | Thundering herd against degraded downstream | Use exponential backoff on nack visibility timeout |  |
| No circuit breaker | worker/enrich.go:82-96 | Workers block on dead downstream | Add sony/gobreaker wrapping downstream calls |  |
| No MaxBytesReader | ingest/handler.go:59 | Memory exhaustion via huge payloads | Add http.MaxBytesReader(w, r.Body, 1<<20) |  |
| No dead-letter replay | queue/inmemory.go | Dead-lettered events are lost | Add admin endpoint to replay or export dead-letter queue |  |
| No tests | Entire codebase | Regression risk on any change | Start with integration tests for the ingestion→queue→worker→signal pipeline |  |
| go.sum missing | Root directory | go mod tidy never run, builds may fail | Run go mod tidy before first build |  |
| Scaling Path |  |  |  |  |
| Current State                   Medium Scale (100K clients)       Large Scale (1M+ clients) |  |  |  |  |
| ─────────────                   ──────────────────────────        ────────────────────────── |  |  |  |  |
| 300 clients                     GSI on DynamoDB                   Event streaming (Kafka) |  |  |  |  |
| In-memory everything            SQS in production                 Per-cohort signal workers |  |  |  |  |
| Full table scan                 Circuit breaker                   Horizontal server scaling |  |  |  |  |
| 10s signal tick                 Structured logging                Pre-aggregated metrics |  |  |  |  |
| No auth                        API key auth                      mTLS + OAuth2 |  |  |  |  |
| No tests                       Integration test suite            Property-based tests |  |  |  |  |
| Single server                  Blue-green deploys                Multi-region federation |  |  |  |  |
| This concludes the complete architectural analysis of FleetPulse. The system is a well-structured portfolio-grade observability pipeline demonstrating hexagonal architecture, queue-based decoupling, pluggable backends, and signal-driven fleet health monitoring — with clearly documented growth paths for each known limitation. |  |  |  |  |