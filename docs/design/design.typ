#import ".render/designlib.typ": *
#let title = [Durable jobs]
#let accent = "amber"
#let body = [
  #section(title: "Foundation", lead: [Grind admits typed work durably and fences each execution attempt. Applications own the meaning and safety of external effects.], body: [
    #goal(title: "Admit typed work without executing it")[An application defines a #term("term-worker"), starts one runtime, and submits a #term("term-job") with immediate or delayed availability. The durable receipt and typed handle support later recovery.]
    #goal(title: "Preserve uncertainty across process and database failures")[Lost replies require reconciliation against durable commands. Expired ownership cannot acknowledge work. Safe delivery after an unknown effect requires an explicit application replay policy or an operator decision.]
    #goal(title: "Retain the full job-system scope")[Ordinary queues, uniqueness, retries, schedules, administration, observations and testing compose independently. Broader queue controls, dependency workflows, batches and other retained capabilities remain specified below with explicit gaps.]
    #no-goal(title: "Guarantee exactly-once external effects")[PostgreSQL transitions are atomic. An external payment, message or HTTP call is outside that transaction unless the application supplies its own durable protocol.]
    #no-goal(title: "Own application authentication or compensation")[Operator attribution is evidence, not authorization. Saga owns typed workflow progress and compensation; a Grind job invoking a local Saga does not make each Saga step durable.]
    #invariant(title: "Admission and execution are separate", enforcement: "mechanism")[Input and key encoding precede storage. Admission never invokes the handler. Exact deployed worker and codec versions are required to execute stored input.]
    #invariant(title: "A retry counter never grants ownership", enforcement: "mechanism")[All execution writes require the complete #term("term-attempt-fence"). Refunding an #term("term-attempt-ordinal") does not restore an expired epoch.]
    #invariant(title: "Runtime failure never invents a business error", enforcement: "mechanism")[Typed errors retain their codec or remain explicitly unrecorded. Operational failure, contract mismatch and uncertainty have separate representations.]
    #principle(title: "Use durable rows as authority")[An event, a returned handler value, or an elapsed timeout cannot establish a committed job outcome. Recovery reads the matching durable receipt or current job.]
    #principle(title: "Make resource ownership explicit")[Grind owns its supervised processes, main pool lifetime and reserved renewal pools. Applications may borrow the main pool and own outer business transactions.]
    #points([Scope and oracle boundaries are recorded in #adr(1). Historical changes and their provenance are recorded in #adr(11).])
  ])

  #pending-ledger(
    pending-entry(title: "Define a hard bound for queued storage checkout", kind: "ruling", adr: [#adr(8)])[The absolute deadline is calculated before pgo checkout, but the pinned pool does not arm a receive timeout while queued. Pool overload shedding or connection transfer ends that wait. Decide whether the public deadline must include a hard queue-wait bound.],
    pending-entry(title: "Complete retained queue and schedule capabilities", kind: "build", adr: [#adr(1)])[Priority, live controls, cluster/rate/partition limits, recurring schedules, notification infrastructure, plugin contracts and richer administration need executable contracts and implementations. Local concurrency and delayed admission implement narrower behavior.],
    pending-entry(title: "Complete dependency workflows, batches and chunks", kind: "build", adr: [#adr(10)])[Durable dependency edges, named step results, batch callbacks and homogeneous chunk dispatch retain separate contracts. They must preserve admission ambiguity, attempt fencing and per-item outcomes without importing Saga compensation into the queue core.],
    pending-entry(title: "Extend uniqueness beyond the conservative profile", kind: "build", adr: [#adr(3)])[Cross-worker matching, arbitrary state sets, broader replacement, backfill and bulk uniqueness need concrete equality, locking and reconciliation contracts. Ordinary bulk insertion cannot imply unique admission parity.],
    pending-entry(title: "Establish fresh qualification for the chosen release", kind: "verify", adr: [#adr(9)])[Retained benchmark and resilience summaries describe historical source and dependencies. A new qualification must record source, dependencies, hardware, budgets, validity verdicts and failed runs; existing scenario coverage does not prove unconditional liveness or full Oban/Pro parity.],
    pending-entry(title: "Decide fallback installation identity strength", kind: "ruling", adr: [#adr(8)])[When either endpoint lacks a PostgreSQL cluster identifier, binding compares only database OID and schema. Distinct clusters may collide and compatibility is nontransitive. A stricter identity or constrained deployment contract needs an explicit decision.],
  )

  #section(title: "System at a glance", lead: [One Durable jobs context owns admission, persisted outcomes, attempt authority and queue runtime.], body: [
    #diagram(altitude: "L1", viewpoint: "context-ownership", title: "Application and queue ownership", caption: [Dependency arrows do not confer authorization or shared transaction ownership.], flow: "top-to-bottom", nodes: (
      (id: "app", label: "Application", kind: "actor", tint: "slate"),
      (id: "grind", label: "Durable jobs", sub: "Grind", kind: "bounded-context", tint: "amber"),
      (id: "pg", label: "PostgreSQL / pog", kind: "external-system", tint: "slate"),
      (id: "sinal", label: "Sinal", kind: "external-system", tint: "slate"),
      (id: "effect", label: "External effect system", kind: "external-system", tint: "slate"),
    ), edges: (
      (from: "app", to: "grind", relation: "dependency", label: "workers / admission / recovery"),
      (from: "grind", to: "pg", relation: "dependency", label: "durable rows and fenced writes"),
      (from: "grind", to: "sinal", relation: "dependency", label: "best-effort typed observations"),
      (from: "app", to: "effect", relation: "call", label: "handler-owned effects"),
    ))
    #points([The runtime targets Erlang/OTP. Direct ecosystem dependency is Sinal. Standard Gleam JSON supplies ordinary codecs; schema-aware conversion is an application choice.])
    #points([The main pool is configured by the caller's pog.Config. Each used queue adds one consumer, a renewer and a reserved one-connection renewal pool. Pruning is optional; processing can be disabled while admission and reads remain available.])
    #diagram(altitude: "L3", viewpoint: "runtime", title: "Runtime responsibilities", caption: [The coordinator keeps capacity occupied until an attempt and its acknowledgement settle. Renewal has an independent actor and pool.], flow: "top-to-bottom", nodes: (
      (id: "runtime", label: "Runtime / supervision", kind: "component", tint: "amber"),
      (id: "consumer", label: "Queue coordinator", kind: "component", tint: "amber"),
      (id: "attempt", label: "Temporary attempt actor", kind: "component", tint: "amber"),
      (id: "handler", label: "Linked handler process", kind: "component", tint: "slate"),
      (id: "renewer", label: "Independent renewer", kind: "component", tint: "amber"),
      (id: "store", label: "PostgreSQL commands", kind: "component", tint: "amber"),
    ), edges: (
      (from: "runtime", to: "consumer", relation: "dependency", label: "one per used queue"),
      (from: "consumer", to: "attempt", relation: "call", label: "claim / capacity"),
      (from: "attempt", to: "handler", relation: "call", label: "one handler invocation"),
      (from: "attempt", to: "store", relation: "call", label: "retained ACK proposal"),
      (from: "consumer", to: "renewer", relation: "call", label: "running and pending fences"),
      (from: "renewer", to: "store", relation: "call", label: "reserved-pool batch renewal"),
    ))
    #points([Local units are Worker definitions, Admission and uniqueness, Storage and migrations, Attempts and acknowledgements, Cancellation and recovery, Runtime capacity, Reads and retention, Observations, and Verification and extensions.])
  ])

  #section(title: "Whole model", lead: [Persistent job identity, command identity and execution authority have different lifetimes.], body: [
    #state-type(id: "job-state", title: "Job state", variants: (
      (id: "queued", description: [Ready ordinary work.]),
      (id: "scheduled", description: [Delayed work awaiting availability.]),
      (id: "retryable", description: [Observed failure scheduled for another attempt.]),
      (id: "executing", description: [Owned by a live attempt fence.]),
      (id: "succeeded", description: [Committed typed output.]),
      (id: "business_failed", description: [Terminal business failure or scheduling budget exhaustion.]),
      (id: "runtime_failed", description: [Terminal operational failure.]),
      (id: "contract_mismatch", description: [Stored/deployed contract refused.]),
      (id: "uncertain", description: [Effect outcome requires investigation; nonterminal.]),
      (id: "discarded", description: [Explicitly discarded with reason.]),
      (id: "cancelled", description: [Committed cancellation; not proof of absent effects.]),
    ))
    #entity(id: "job", title: "Job", description: [Retained record of a submitted versioned worker contract.], kind: "entity", owner: "Durable jobs", domain: "Durable jobs", lifecycle: "stateful", tint: "amber")[
      #attribute(name: "Identity", type: "Installation × Int", provenance: "derived")[Database-generated job ID is installation-local and never substitutes for a submission key.]
      #attribute(name: "Worker contract", type: "Worker ID × worker version × codec versions", provenance: "authored")[Input/output and optional error versions are checked at execution and read attachment.]
      #attribute(name: "Scheduling", type: "Queue × availability × limits", provenance: "authored")[Relative availability is resolved by PostgreSQL. Attempts, snoozes and replays have separate counters.]
      #attribute(id: "state", name: "State", type: "Job state", provenance: "derived", state-type: "job-state", state-machine: "job-lifecycle")[Only committed storage commands change lifecycle state.]
      #attribute(name: "Execution evidence", type: "Optional attempt fence / cancellation / uncertainty", provenance: "derived")[Uncertain retains effect evidence and ownership evidence. Cancellation intent remains independent until attributed terminal resolution settles the job.]
      #attribute(name: "Outcome and retention", type: "Optional encoded output/error/reason/cause/finished time", provenance: "derived")[finished_at exists for six terminal states and never for Uncertain.]
      #relates(cardinality: "1 : 0..n")[Admission, acknowledgement and operator-resolution receipts refer to the job and cascade when it is pruned.]
    ]
    #state-machine(id: "job-lifecycle", subject: "job", state-field: "state", state-type: "job-state", states: ("queued", "scheduled", "retryable", "executing", "succeeded", "business_failed", "runtime_failed", "contract_mismatch", "uncertain", "discarded", "cancelled"), initial: "queued", accepting: ("succeeded", "business_failed", "runtime_failed", "contract_mismatch", "discarded", "cancelled"), transitions: (
      ("queued", "executing", "claim"), ("scheduled", "executing", "due claim"), ("retryable", "executing", "due claim"),
      ("queued", "cancelled", "cancel"), ("scheduled", "cancelled", "cancel"), ("retryable", "cancelled", "cancel"),
      ("executing", "succeeded", "success ACK"), ("executing", "business_failed", "failure / exhaustion"),
      ("executing", "runtime_failed", "unencodable outcome"), ("executing", "contract_mismatch", "contract refusal"),
      ("executing", "retryable", "retry ACK"), ("executing", "scheduled", "snooze ACK"),
      ("executing", "discarded", "discard ACK"), ("executing", "cancelled", "cancel precedence"),
      ("executing", "uncertain", "unknown effect / expiry"), ("executing", "queued", "unstarted release / replay-safe expiry"),
      ("uncertain", "succeeded", "operator confirms"), ("uncertain", "business_failed", "operator confirms"), ("uncertain", "queued", "operator authorizes replay"),
    ))
    #points([Immediate admission chooses Queued; delayed admission chooses Scheduled. The single diagram initial node represents ordinary admission. Eligibility also requires queue, exact worker version, due database time, remaining budget and current row locks; an arrow alone grants no execution authority.])
    #entity(id: "receipt", title: "Admission receipt", description: [Original command decision retained independently of later job state.], kind: "entity", owner: "Admission", domain: "Durable jobs", lifecycle: "immutable", tint: "amber")[
      #attribute(name: "Identity", type: "Installation × submission key", provenance: "authored")[All submissions retain a supplied or generated identity.]
      #attribute(name: "Fingerprint", type: "SHA256 prepared command", provenance: "derived")[Plain and unique commands use different domain tags. Exact encoded input, worker/codecs, queue, limit, schedule and uniqueness action participate; correlation and maximum replay count do not.]
      #attribute(name: "Decision", type: "Inserted | Existing | Rescheduled", provenance: "derived")[Original job, actual conflict queue/state and decision time survive matching-window and job-state changes.]
    ]
    #entity(id: "fence", title: "Attempt fence", description: [Authority to write one executing attempt.], kind: "value-object", owner: "Attempt execution", domain: "Durable jobs", lifecycle: "immutable", tint: "amber")[
      #attribute(name: "Identity", type: "Job ID × attempt ID × epoch × owner", provenance: "derived")[Attempt ID and epoch distinguish claims even when the retry ordinal repeats. Owner includes runtime incarnation.]
      #attribute(name: "Validity", type: "Executing state × live database lease × contract match", provenance: "derived")[Exact lease expiry is invalid. Renewing the same owner changes lease expiry without changing the authority identity.]
    ]
    #entity(id: "unique-policy", title: "Uniqueness policy", description: [Typed candidate selection and conservative conflict action.], kind: "value-object", owner: "Admission", domain: "Durable jobs", lifecycle: "immutable", tint: "amber")[
      #attribute(name: "Key and scope", type: "Full input | selected key; WithinQueue | AcrossQueues", provenance: "authored")[Selected keys retain a nonempty name/version and typed encoder. Worker identity/version always participate.]
      #attribute(name: "Period", type: "Within(positive duration, origin) | WhileRetained", provenance: "authored")[Origin is insertion or schedule; supported millisecond bounds are checked by the constructor.]
      #attribute(name: "Eligible states", type: "Incomplete | ScheduledOnly | IncompleteOrSucceeded | AllRetained", provenance: "authored")[These profiles filter current rows without changing key identity.]
      #attribute(name: "Action", type: "KeepExisting | RescheduleTo(timestamp)", provenance: "authored")[Only a scheduled candidate may change availability. Input, worker/version and identity remain unchanged.]
    ]
    #md-table(3, ([Refinement], [Validation owner], [Failure contract],
      [Worker/codec/queue identity and positive source limits], [Source-owned builders], [Panic at definition time; no deployment starts],
      [Runtime schema, deadlines, used queue tuning, duplicate workers], [Config.check and start], [Typed configuration error before resources start],
      [Encoded input/key and payload byte limit], [Preparation], [InvalidInput before checkout or writes],
      [Output/error codec acceptance and byte limit], [After handler returns], [Terminal runtime failure; no synthetic business error],
      [Nonempty resolution identity/by/details], [Operator command construction], [Rejected attribution/command before resolution],
      [List limit and quarantine/prune batch], [Administration], [Positive bounded page/batch; maximum ten thousand],
    ))
    #md-table(3, ([Axis], [Owner and identity], [What it proves],
      [Submission], [Caller/generated key and exact command], [One admission decision, while receipt remains],
      [Uniqueness], [Worker/key contract and incoming eligibility], [An equivalent retained candidate was selected],
      [Execution], [Attempt fence and database time], [Authority for a current storage write],
      [Acknowledgement], [Command ID and exact proposal/fence], [The actual committed disposition],
      [Resolution], [Resolution ID, decision and attribution], [An operator command was applied],
      [Correlation], [Application tracing value], [No authority or idempotency guarantee],
    ))
    #points([The typed JobHandle retains installation, queue, worker contract and output/error codecs. A Conflict includes its real route and compatible handle. PendingSubmission retains the prepared fingerprint; it is not a handle proving admission.])
  ])

  #section(title: "Worker definitions and codecs", lead: [Typed application values cross storage through explicit versioned JSON contracts.], body: [
    #answers(title: "Worker definition", responsibility: [Retain typed handlers and all policies needed to interpret stored input.], interface: [worker.new wraps Result; worker.responding receives Context and returns Response. Builders select queue, version, codecs, attempts, timeout, snoozes, retry and abandonment.], interactions: [Configuration erases heterogeneous workers through retained typed closures. Storage and reads check declared versions before decoding.], invariants: [Duplicate worker ID/version is rejected across the runtime. Multiple versions may coexist; there is no fallback. Source-owned invalid constructors panic; runtime Config.check returns typed validation errors.], failure: [Invalid input/key encoding refuses admission before storage. Stored contract mismatch prevents handler execution. Output/error refusal after effects becomes terminal RuntimeFailed, with no retry.])
    #md-table(3, ([Response], [Scheduling/accounting], [Outcome ownership],
      [Succeeded(output)], [Terminal; output encoded once], [Output codec refusal is operational failure],
      [Failed(error)], [Retry only below limit and when policy permits], [Optional error codec preserves typed error; otherwise explicitly unrecorded],
      [Snoozed(delay, reason)], [Refund attempt; increment separate snooze count], [Snooze exhaustion is business failure with cause],
      [Discarded / Cancelled], [Terminal reason], [Does not prove earlier effects absent],
      [Uncertain(evidence)], [Bypasses retry policy], [Operator recovery or explicit replay safety required],
    ))
    #points([Defaults: queue default; worker/codecs version 1; maximum attempts 20; timeout 15 minutes; maximum snoozes 100; HoldUncertain. Retry uses 15-second exponential delay capped at one day with up to ten percent jitter. Delays are clamped to supported nonnegative bounds.])
    #points([Context retains job/attempt/queue, correlation, attempts/snoozes, optional main-pool connection and a handler-owned cooperative cancellation selector. A forged local testing context grants no storage authority.])
    #behavior(title: "An encoding failure follows an executed effect", level: "boundary", area: "Worker outcomes")[#given[The handler returned a value after executing application code.] #when[The output or configured error encoder rejects it or exceeds the payload limit.] #then[The acknowledgement proposes RuntimeFailed and does not request another attempt. The failure does not undo effects.]]
    #points([The default payload bound is one MiB. No codec version string proves equivalent implementations; applications change versions and retain compatible deployed code. See #adr(2), #lnk("../../src/grind/worker.gleam")[worker contracts] and #lnk("../../consumer/test/grind_consumer/codec_test.gleam")[external codec consumer].])
  ])

  #section(title: "Admission and uniqueness", lead: [One atomic admission command combines receipt lookup, candidate selection and any permitted mutation.], body: [
    #answers(title: "Admission", responsibility: [Prepare exact typed input and durably choose Inserted, Existing or Rescheduled.], interface: [job.new and setters; grind.submit; submit_in; reconcile_submission; Admission and SubmitError.], interactions: [Prepares input/key before checkout; PostgreSQL locks the receipt/domain and candidate rows; typed handles retain the selected actual route.], invariants: [All submissions have receipt identities. Matching receipt is checked before a new decision. Same identity/different fingerprint is IdConflict. Admission never runs the handler.], failure: [InvalidInput and known pre-commit refusal differ from CommitUnknown. An absent receipt during reconciliation remains unknown; it is not proof that the original transaction cannot still commit.])
    #points([with_id supplies a submission key, not the durable integer job ID. Retrying ordinary job.new without preserving a key creates another command. A PendingSubmission is the preferred retained recovery token after CommitUnknown.])
    #points([after resolves its duration against database time at admission. at retains an absolute timestamp, with past values made immediately eligible. Correlation and replay policy are retained on insertion but excluded from receipt equality; changing either under an existing key returns the original decision.])
    #behavior(title: "A lost admission reply is reconciled", level: "boundary", area: "Admission")[#given[A prepared command returned CommitUnknown and its original PendingSubmission remains available.] #when[Reconciliation finds its matching durable receipt.] #then[It returns the original admission decision without rerunning uniqueness or executing work. A conflicting receipt returns IdConflict; missing receipt remains unknown.]]
    #subsection(title: "Conservative uniqueness profile")[
      #points([full_input uses input JSON and its codec version. selected uses a named key, key codec version and typed projection. Jobs without matching retained key material do not participate; arbitrary ordinary rows cannot be reconstructed from a new projection closure.])
      #points([Equality hashes PostgreSQL jsonb text: object order is normalized; repeated fields use PostgreSQL last-wins semantics; array order matters; exponent notation normalizes; 1 and 1.0 remain distinct. This differs from exact prepared-request byte equality and from Oban Basic containment.])
      #points([Worker ID/version always scope the domain. WithinQueue is default; AcrossQueues stays inside the installation. Defaults are Incomplete states and KeepExisting. Incomplete includes Uncertain; other profiles are ScheduledOnly, IncompleteOrSucceeded and AllRetained.])
      #points([within uses a positive bounded duration from insertion or scheduled time with an inclusive cutoff. while_retained has no time cutoff; pruning ends participation. Incoming period and state filter are predicates, not extra namespaces.])
      #points([A transaction advisory lock hashes schema, worker ID/version, key contract and PostgreSQL key digest; queue is excluded, so same-key admission serializes even across queue-local policies; FOR KEY SHARE protects candidates against pruning. The default lock wait is two seconds and must clear the storage deadline by at least one second. Lock timeout is UniquenessContended, not Existing.])
      #points([Reschedule changes availability only on eligible scheduled work, preserving identity, payload and contract. Other states return Existing. Across-queue conflict handles retain the selected queue, not the submitting queue.])
    ]
    #subsection(title: "Caller-owned business transaction")[
      #points([submit_in accepts a checked SingleConnection inside an open READ COMMITTED transaction for the same database. It issues no BEGIN or COMMIT, temporarily scopes search_path and lock_timeout, and restores them. A failed statement may abort the caller transaction.])
      #points([Its reply means staged admission, not commit. No lifecycle admitted event is emitted. The borrowed call inherits the outer transaction timeout rather than acquiring another Grind deadline. SERIALIZABLE and REPEATABLE READ are refused without weakening the caller transaction. Applications retaining those isolation levels may commit a business outbox and dispatch it separately under stable submission identity.])
      #points([After a lost outer COMMIT reply, reconciling an admission receipt proves that job admission occurred. An older matching receipt cannot prove that this retry's surrounding business writes committed. The application needs its own business-command reconciliation.])
    ]
    #points([Concrete authority and alternatives are recorded in #adr(3) and #adr(4). Executable consumers: #lnk("../../consumer/test/grind_consumer/admission_test.gleam")[admission] and #lnk("../../test/grind/unique")[concurrent uniqueness tests].])
  ])

  #section(title: "Storage and migrations", lead: [The adapter owns one exact schema and forward-only transactions. Unknown commit does not authorize blind repetition of effects.], body: [
    #answers(title: "PostgreSQL adapter", responsibility: [Scope SQL to one installation, preserve physical schema contracts and distinguish known refusal from uncertain reply.], interface: [grind.Config from pog.Config; start/migrate/connection; internal bounded checkout and generated or dynamic queries.], interactions: [Main pool is shared with application queries. Borrowed admission uses caller transaction settings; migrations use independent step budgets.], invariants: [Grind-owned transactions pin READ COMMITTED and restore application search_path. A failed cleanup retires the connection. The bounded wrapper runs its callback at most once and never retries its SQL body after stale-holder replacement.], failure: [Unavailable pools, connection loss and uncertain COMMIT can deny a reliable reply. Deadline and cleanup cannot prove rollback. Installation binding is a misuse guard, not authentication.])
    #points([Client installation identity uses database OID, schema and optional pg_control_system cluster ID. Cluster comparison is enforced when both values exist; fallback OID/schema comparison has collision and nontransitivity limits. Schema identifiers are quoted and validated for PostgreSQL length and reserved/unsafe names.])
    #points([Existing schema lookup avoids demanding database CREATE permission from an installed role. Concurrent creation is rechecked. The original creation error is retained if that recheck fails.])
    #points([Current source migrations are versions 11–13. Version 11 is the baseline; 12 adds finished-time retention and dependent receipt lifecycle; 13 adds correlation and replay accounting, uncertainty indexing and snooze cause. The hand-maintained source list is authoritative. priv migrations mirror exact step SQL for Cigogne consumers.])
    #points([Each step takes a schema-keyed transaction advisory lock first, rereads markers, applies DDL, writes the marker and checks physical shape before COMMIT. Earlier committed steps survive a later failure. Unknown step reply can be safely rechecked/rerun because lock and marker decide application.])
    #points([Shape validation rejects unsupported marker versions, wrong relations/columns/indexes/sequences/foreign keys, obsolete storage_owner and stray Grind-prefixed objects. It does not quietly adopt an Oban schema. Experimental version 10 needs an explicit reinstall; no automatic destructive reset is performed.])
    #points([Default migration budget is thirty seconds per step and lock wait two seconds. Large table work can exhaust it; default finalization and foreign-key validation acquire real locks. Operators quiesce/drain and budget migration rather than infer a zero-downtime guarantee.])
    #points([Static SQL is Squirrel generated; dynamic admission/ACK/resolution predicates remain hand-maintained. Regeneration replaces generated files as a whole. See #adr(7), #adr(8), #lnk("../../priv/migrations")[migration mirrors] and #lnk("../OPERATIONS.md")[operator procedure].])
  ])

  #section(title: "Attempts and acknowledgements", lead: [Each admitted execution obtains a new authority identity. A handler result becomes an outcome only after a fenced command commits.], body: [
    #answers(title: "Claim and attempt execution", responsibility: [Claim due work, decode the exact deployed contract, run one handler, and retain its result until settlement.], interface: [Internal claim, lease, attempt and acknowledgement commands; testing.drain for explicit manual processing.], interactions: [The coordinator claims one slot at a time; a temporary actor owns the linked handler and retained ACK proposal; the renewer maintains live leases.], invariants: [Every new write checks executing state, attempt ID, epoch, owner, codec contract and a live database-time lease. Same-owner renewal is not takeover. Handler invocation is not repeated by ACK recovery.], failure: [Contract mismatch parks the row before handler execution. Child startup refusal before StartAttempt releases/refunds its exact claim. Handler/attempt death leaves lease-based recovery, not synthetic success or business failure.])
    #state-type(id: "attempt-phase", title: "Attempt actor phase", variants: (
      (id: "ready", description: [Claim acquired; StartAttempt not yet received.]),
      (id: "running", description: [One linked handler is active.]),
      (id: "waiting", description: [Handler result retained for ACK/reconciliation.]),
      (id: "finished", description: [Completion reported; actor has no further execution authority.]),
    ))
    #entity(id: "attempt-actor", title: "Attempt actor", description: [Temporary local owner of one handler invocation and its acknowledgement proposal.], kind: "entity", owner: "Attempt execution", domain: "Durable jobs", lifecycle: "stateful", tint: "amber")[
      #attribute(name: "Request", type: "ClaimedJob × runtime owner × coordinator/renewer", provenance: "derived")[Retains the attempt fence, deployment and local completion callbacks.]
      #attribute(id: "phase", name: "Phase", type: "Attempt actor phase", provenance: "derived", state-type: "attempt-phase", state-machine: "attempt-progress")[Local phase does not replace the persistent job state.]
      #attribute(name: "Pending completion", type: "Optional Execution × stable ACK command × retry count", provenance: "derived")[Only one handler result is retained. Retry diagnostics count ACK attempts, not business attempts.]
    ]
    #state-machine(id: "attempt-progress", subject: "attempt-actor", state-field: "phase", state-type: "attempt-phase", states: ("ready", "running", "waiting", "finished"), initial: "ready", accepting: ("finished",), transitions: (
      ("ready", "running", "StartAttempt"),
      ("running", "waiting", "ACK retry retains result"), ("running", "finished", "ACK settled / replay-safe timeout"),
      ("waiting", "waiting", "same ACK retry"), ("waiting", "finished", "ACK settled"),
    ))
    #points([Actor death from linked handler failure is destruction of this local entity, not a committed transition to Finished. The durable job remains fenced until lease recovery. StopWorker destroys the actor from any phase; it does not create a Finished state or resolve storage. A successful first ACK can go directly from Running to Finished.])
    #points([Claims increment the attempt ordinal. Snooze refunds one attempt and increments snooze_count. Explicitly replay-safe expiry refunds and increments replay_count, bounded independently. Fresh attempt ID/epoch fence old processes even when the ordinal repeats.])
    #points([Due claims select the earliest available_at then job ID among exact deployed worker versions, using FOR NO KEY UPDATE SKIP LOCKED. This is a selection order, not a starvation/fairness guarantee. Each claim poll first quarantines at most one expired row in the queue, including versions absent from the deployed registry. Admin quarantine can cover unpolled queues in bounded batches. HoldUncertain preserves evidence; bounded ReplayAfterLeaseExpiry clears old ownership and queues work only without pending cancellation.])
    #points([The handler is linked to the attempt actor. Its finite timeout stops/unlinks it. HoldUncertain proposes uncertainty; explicit replay-safe policy stops renewal and lets lease expiry deliver again. Runtime failure alone is no proof of replay safety.])
    #answers(title: "Acknowledgement recovery", responsibility: [Commit one actual disposition and reconcile lost replies without reinvoking the handler.], interface: [Stable command ID, exact proposal hash and durable ACK receipt; admin.reconcile_acknowledgement for manual recovery.], interactions: [The attempt actor retries both known rollback and ambiguous ACK replies. The consumer keeps its slot occupied; reserved renewal covers running and pending attempts.], invariants: [A matching receipt returns the original committed state/cause. Conflicting command/proposal is refused. Receipt lookup is allowed after expiry; a fresh write is not. Cancellation can change a proposed non-uncertain state before commit. An explicit Uncertain proposal preserves its evidence and cancellation intent.], failure: [If no receipt is found and ownership has expired, ACK cannot commit. Lease recovery preserves uncertainty. Reconciliation lacks availability detail present only in a fresh UPDATE RETURNING reply.])
    #behavior(title: "The first acknowledgement rolls back", level: "boundary", area: "Attempt recovery")[#given[The handler already returned and the attempt actor retains its proposal.] #when[The first ACK fails with a known rollback.] #then[The actor retries that same command and proposal while its fence permits it. It does not run the handler again and it continues occupying local capacity.]]
    #points([Completion-pending renewal has a finite lifetime of one lease duration after completion notice. Once that window ends, receipt reconciliation may continue but cannot create a write under an expired fence. A slow ACK row cannot stall healthy siblings because renewal skips locked rows.])
    #points([ACK receipts fingerprint the full proposal and fence. Matching receipts are checked before writes and after a zero-row update. Renewing a lease cannot invalidate that command. See #adr(5) and #lnk("../../test/grind/queue/ack_failure_test.gleam")[ACK fault tests].])
  ])

  #section(title: "Cancellation and operator recovery", lead: [A request to stop and evidence of an unknown effect answer different questions.], body: [
    #answers(title: "Cancellation", responsibility: [Record inactive cancellation or cooperative executing intent and arbitrate races atomically.], interface: [grind.cancel; CancellationResult; worker cancellation selector.], interactions: [Committed intent is observed by renewal and delivered to the handler once. ACK checks cancellation under the same fenced row transition.], invariants: [Completion first preserves the finished result. Cancellation intent first wins non-uncertain ACK precedence. Explicit Uncertain retains its evidence and cancellation intent in either ordering. Expired ownership cannot overwrite recovery. Repeating an unknown cancellation is safe; Uncertain is not implicitly replayed.], failure: [A live handler may ignore intent or already have performed effects. Database/scheduler progress is required for observation; there is no unconditional cancellation latency. Cancellation intent cannot certify that an effect was prevented or completed.])
    #behavior(title: "Cancellation precedes an explicit uncertain response", level: "boundary", area: "Cancellation precedence")[#given[Cancellation intent committed while the handler was executing.] #when[The handler returns Uncertain and its ACK wins with a live fence.] #then[Storage commits Uncertain with the exact effect evidence and retained attempt fence. Cancellation intent remains recorded. Operator listing includes the row and ordinary pruning excludes it. AuthorizeReplay is refused. An attributed terminal confirmation can settle the effect.]]
    #behavior(title: "Cancellation follows an uncertain acknowledgement", level: "boundary", area: "Cancellation precedence")[#given[An uncertain acknowledgement already committed.] #when[The caller requests cancellation.] #then[The result is AlreadyUncertain. Storage retains the evidence and records cancellation intent. AuthorizeReplay is refused; attributed terminal confirmation remains available.]]
    #points([Lease quarantine after pending cancellation also preserves Uncertain and suppresses automatic replay. Matching acknowledgement retries return the original committed Uncertain disposition without reinvoking the handler. #adr(12) records the rationale and historical evidence limits.])
    #answers(title: "Operator resolution", responsibility: [Settle or authorize a new attempt after application investigation of uncertain work.], interface: [admin.list; resolution(id, by, details); ConfirmSuccess(output), ConfirmFailure(error), AuthorizeReplay; resolve_uncertain; resolve_uncertain_in returning Staged(Resolved).], interactions: [Encodes the selected typed value before storage, locks the uncertain row and commits an attributed durable resolution receipt.], invariants: [Nonempty command/attribution fields; exact full command equality; AlreadyApplied returns original state. AuthorizeReplay is refused with pending cancellation; terminal confirmations can settle it. ConfirmFailure requires an error codec.], failure: [Value rejection writes nothing. Different reuse is a conflict; lost reply is resolved by repeating the identical command. Authorizing replay cannot certify an external effect did not occur.])
    #answers(title: "Borrowed operator resolution", responsibility: [Stage the job transition and its exact attributed receipt alongside caller writes.], interface: [admin.resolve_uncertain_in accepts the caller's open READ COMMITTED transaction for the same database and returns Staged(Applied or AlreadyApplied).], interactions: [The caller commits the resolution with its application acknowledgment. Grind scopes search_path and caps lock_timeout and statement_timeout by the configured storage deadline while preserving stricter caller limits; successful statements restore all three settings.], invariants: [The ordinary typed command validation, receipt equality, attempt fence and cancellation rules remain shared. There is no new transaction owner or committed resolved observation. A prior matching receipt cannot prove the surrounding application's new commit.], failure: [A pool or unsupported isolation is refused. A statement error may abort the transaction; callers propagate errors and roll back. The caller owns checkout, network waits and the outer transaction lifetime. Row locks remain until that transaction ends.])
    #behavior(title: "Resolution and application acknowledgment share a commit", level: "boundary", area: "Operator recovery")[#given[The caller stages an attributed operator resolution together with its application acknowledgment.] #when[The enclosing transaction commits or rolls back.] #then[Both changes become durable together or neither does.] #then[A lost commit reply requires durable application readback; the staged result alone proves no commit.]]
    #points([Investigation and external calls precede the borrowed transaction. An application acknowledgment committed with resolution remains available under application retention after job pruning. This capability does not reconstruct proof already lost under an older two-commit procedure. See #adr(13) and #lnk("../../consumer/test/grind_consumer/resolution_transaction_test.gleam")[transactional resolution consumer].])
    #points([Applications own operator permissions, evidence gathering, external status queries and replay/idempotency decisions. Resolution IDs and by/details fields do not authenticate anyone. Durable retention bounds command recovery.])
    #points([See #lnk("../../consumer/test/grind_consumer/recovery_test.gleam")[public recovery consumer] and #lnk("../../src/grind/admin.gleam")[operator contracts].])
  ])

  #section(title: "Runtime capacity and deadlines", lead: [Independent renewal protects lease progress from ordinary main-pool saturation. It does not eliminate infrastructure stalls.], body: [
    #answers(title: "Supervised runtime", responsibility: [Own process and pool lifetimes, queue capacity, timers and shutdown.], interface: [check/start/supervised/named/stop; queue tuning; without_consumers/without_pruner.], interactions: [RestForOne supervision orders pool/deadline ownership, forwarder, runtime, per-queue consumers and pruner. Each consumer owns its reserved renewal pool.], invariants: [Current schema is required before consumer start. Named runtime lookup survives restart. Failed startup unwinds all acquired resources, registry and caches. Running and ACK-pending attempts count against local capacity.], failure: [Stop invalidates retained connections/lifetime tokens. Shutdown grace bounds cooperative drain; remaining workers leave fenced lease recovery. Node crash, database outage and OS suspension have no liveness guarantee.])
    #points([Queue defaults: ten local slots, 250-millisecond polling, thirty-second lease, fifteen-second shutdown grace. Queues are derived from registered workers; duplicate queue tuning and tuning unused queues are refused. Local slots do not cap all nodes or serialize equivalent job effects.])
    #points([Claim/refill is serialized one free slot per mailbox message. The coordinator yields between slots and owns at most one poll timer. Polling is the current wakeup mechanism; fairness, LISTEN/NOTIFY and live pause/scale are retained gaps.])
    #points([Storage deadline D defaults to four seconds. Lease L must satisfy L ≥ 4D. Renewal arms its L/3 timer before the query; the reserve isolates it from admission/ACK checkout. The timing argument assumes an established progressing connection, bounded storage work and actor progress; initial reconnect, same-row contention and OS scheduling remain outside it.])
    #md-table(3, ([Boundary], [Owned budget], [Practical limit],
      [Ordinary storage], [Absolute deadline calculated before checkout; callback executes at most once], [Queued pgo checkout has no unconditional receive timeout; late transferred candidates are refused before SQL],
      [Migration step], [Separate thirty-second default transaction budget], [Large DDL and lock acquisition may fail; earlier steps remain committed],
      [Borrowed transaction], [Caller transaction timeout], [No new Grind BEGIN/COMMIT or checkout budget],
      [Handler], [Fifteen-minute default execution timeout], [Stopping a process cannot retract an already executed effect],
      [await within], [Monotonic wait checks after reads; adaptive polls], [Final read may extend beyond within; pool queue wait may exceed D],
    ))
    #points([The FFI retires stale holders/reconnection candidates within one absolute deadline, probes sockets under a separate watchdog and preserves unrelated Erlang exceptions. A narrow pog conversion exception is mapped; broad catches never forge rollback. See #adr(5), #adr(8) and #lnk("../../src/grind_postgres_ffi.erl")[deadline boundary].])
  ])

  #section(title: "Reads, administration and retention", lead: [Typed read attachment checks stored contracts. Retention deliberately limits recovery.], body: [
    #answers(title: "Reads and administration", responsibility: [Return committed typed outcomes and bounded payload-free operator views.], interface: [bind/arguments/state/outcome/await; admin.list, admin.statistics, quarantine_expired, prune_finished and receipt reconciliation.], interactions: [Reads check installation, queue, worker and codec versions. List uses ascending job ID, optional queue/state filters and a bounded page size.], invariants: [Native values are decoded only by the retained compatible codecs. Uncertain is nonterminal. Administrative attribution is not access control.], failure: [Missing job, stale installation, incompatible contract and invalid stored payload remain read errors. Timeout does not cancel work or prove absence of a commit.])
    #points([Outcome is Pending(state), Succeeded(output), Failed(failure, optional terminal cause, description), Discarded(reason), Cancelled(reason) or Uncertain(evidence). Business(error), BusinessUnrecorded and RuntimeFailure retain distinct causes. await starts at twenty milliseconds, doubles to a five-hundred-millisecond polling cap and reads committed state.])
    #answers(title: "Queue statistics", responsibility: [Return a content-free committed view of one explicitly selected queue.], interface: [admin.statistics returns Statistics(sampled_at_ms, states), with one StateStatistics for each known state: count, oldest_job_age_ms, due_count and oldest_due_age_ms.], interactions: [One statement uses one database snapshot and one clock sample through the same installation-scoped storage path as other administration reads. It reads committed rows without claiming, acknowledging, pruning or changing jobs.], invariants: [All eleven states remain distinct and zero groups are present. Ages are optional exactly when their corresponding count is zero. Insertion age is time since admission, not time in the current state. Due includes only Queued, Scheduled and Retryable rows whose availability has arrived; it does not establish deployed compatibility, retry budget, capacity or authority to execute.], failure: [A stopped runtime returns NotRunning; storage failure returns Unavailable; an unrecognized stored state returns RecordMismatch. No failure is reported as an empty queue. The existing checkout/deadline limitations remain.])
    #points([Job and due ages are elapsed milliseconds clamped at zero. The result contains no payloads, identifiers, keys, correlation or failure descriptions. Explicit queue selection bounds result cardinality, not storage work: cost grows with retained rows in that queue. State order is unspecified. Applications own authorization, export, sampling frequency and business-state interpretation; independently sampled installations or libraries do not form one atomic snapshot.])
    #answers(title: "Retention", responsibility: [Delete old terminal jobs and dependent receipt rows in bounded concurrent-safe batches.], interface: [Default pruner: seven-day retention, thirty-second interval, batch ten thousand; explicit admin.prune_finished.], interactions: [Selects finished rows older than database-time cutoff using FOR UPDATE SKIP LOCKED, ordered by finished_at/ID; cascades receipt foreign keys. Admission candidate locks prevent deleting a selected retained conflict.], invariants: [Only six terminal states with finished_at are pruned. Uncertain work remains retained. No leader election is needed for concurrent bounded pruning.], failure: [Pruning permanently ends admission/ACK/resolution reconciliation and while_retained uniqueness. Runtime/schema outages can postpone deletion; configured retention is not an exact deletion instant.])
    #points([An application requiring permanent deduplication archives its own durable business identity. A Grind submission receipt is neither an eternal tombstone nor an effect ledger. See #adr(7).])
  ])

  #section(title: "Observations", lead: [Durable rows establish outcomes; events explain transitions and runtime pressure.], body: [
    #answers(title: "Typed Sinal observations", responsibility: [Publish payload-free lifecycle evidence and bounded operational diagnostics.], interface: [Sixteen grind.telemetry descriptors, typed metadata codecs and measurement builders.], interactions: [A supervised forwarder receives bounded producer messages and delivers through Sinal outside database callbacks.], invariants: [Lifecycle events require commit reply or matching receipt. submit_in and pure reconcile_submission emit no admitted event; resolve_uncertain_in emits no resolved event. Diagnostics do not grant commit authority. Metadata excludes inputs/outputs, raw SQL/settings/exception terms.], failure: [Default forwarder capacity is 1024; events may drop or duplicate. Ordering is per producer only, monotonic time is local, subscriber failure cannot block queue correctness. Missing event does not imply missing job.])
    #md-table(2, ([Family], [Descriptors],
      [Job lifecycle (8)], [admitted, claimed, quarantined, acknowledged, resolved, cancellation_decided, released, contract_mismatch_recorded],
      [Pruning (2)], [completed, failed],
      [Diagnostics (6)], [renewal, acknowledgement, acknowledgement_retry, checkout, claim_failed, capacity],
    ))
    #points([Acknowledgement/resolution observations distinguish Replied and Reconciled and report the actual receipt disposition. A retry may publish another observation of the same commit. Consume stable identities when deduplicating operational counts.])
    #points([Renewal reports signed database lease headroom at measurement, not delivery. Checkout reports actual unclamped wait; nested storage wrappers do not double-count checkout. Capacity includes pending ACK occupancy. Native diagnostic enum codecs reject unknown stored labels.])
    #points([Handler cancellation observation requires progressing renewal and is best effort within its cadence, not a global delivery guarantee. See #adr(9) and #lnk("../../src/grind/telemetry.gleam")[descriptor schemas].])
  ])

  #section(title: "Verification and retained extensions", lead: [Independent consumers, PostgreSQL faults and a frozen oracle answer different questions.], body: [
    #answers(title: "Verification boundary", responsibility: [Prove public typing, codec behavior, fenced PostgreSQL transitions and scoped comparison independently.], interface: [testing.perform/perform_with/drain; consumer package; root tests; oracle manifests; benchmark and resilience harnesses.], interactions: [Inline helpers round-trip input/output/configured error codecs without a database. Drain drives real manual attempts; oracle runs Oban separately against classified shared scenarios.], invariants: [Inline helpers do not simulate lease, timeout, retry or persistence. External consumers cannot use internal modules. Passing a paired scenario proves only its declared equivalent configuration.], failure: [Database-free helpers execute real application code in the calling process. Drain timeout kills its runner but leaves lease recovery for claimed work. Missing/invalid benchmark audit cannot count as a pass.])
    #points([testing.perform_with supplies synthetic Context overrides, including already-fired cancellation and an optional connection. Without an error codec, direct typed Failed(error) remains visible. drain handles one real job at a time up to its bound; disable automatic consumers to avoid competing processing.])
    #points([The primary behavioral oracle is Oban 2.24.1 open source. Grind default backoff, exact encoded uniqueness, opt-in matching, bounded attempt ownership and durable command receipts have deliberate differences. Pro families require original explicit contracts or a licensed oracle; source availability cannot be assumed.])
    #points([Keep root regression tests, separate consumer tests, migration conformance mirrors, fault proxies, scenario manifests, audit invariants and machine-readable catalogs. Historical measured summaries and command provenance belong to evidence/ADRs; they do not qualify arbitrary future dependencies.])
    #subsection(title: "Complete capability inventory")[
      #md-table(3, ([Family], [Current contract], [Retained intended contract / missing surface],
        [Job records and attempts], [Typed input/output/error, correlation, eleven states and counters], [Priority, tags, metadata, full attempt audit and suspension with explicit state/retention rules],
        [Admission], [Immediate/absolute/relative; receipts; borrowed transaction], [Bulk commands with per-item atomicity, conflicts and ambiguity; uniqueness bulk needs independent contract],
        [Queue control], [Static local concurrency/poll/lease/grace], [Pause/resume/scale and registration management; cluster/global/rate/partition limits with durable authority and fairness],
        [Uniqueness], [Exact key profile, incoming state/time filter, scheduled reschedule], [Cross-worker domains, arbitrary states, broader replacements, key backfill and bulk selection],
        [Schedules], [One-off database availability], [Cron/interval/timezones, DST, missed runs, clock jumps, durable tick identity and elected schedule ownership],
        [Infrastructure/plugins], [Concurrent pruner, polling and expiry quarantine], [DynamicCron, Lifeline, Stager, Reindexer, leader/notifier contracts, PostgreSQL LISTEN/NOTIFY with loss/poll fallback],
        [Dependency workflows], [Ordinary jobs compose in application code], [Named durable DAG steps, dependency values/codecs, waits and ignored cancelled/discarded upstream rules; distinct from Saga compensation],
        [Batches], [No batch coordinator], [Membership and completed/attempted/discarded/cancelled callbacks; callback admission deduplication without exactly-once effect claim],
        [Chunks], [One input per handler], [Homogeneous count/time flush, retained item IDs, one result per item, missing/duplicate result refusal and partial-failure accounting],
        [Recorder and relay], [Committed typed job output and bounded polling await], [Named step result retrieval, reusable composition, notification optimization and recovery semantics beyond single-job outputs],
        [Testing], [Explicit inline helpers and manual drain], [Typed enqueue inspection and broader modes without silently changing runtime authority],
        [Engine and storage], [PostgreSQL implementation only], [SQLite/engine adapter only after a real consumer establishes minimum port and isolation/clock/fencing semantics],
        [Management], [Payload-free list, resolution, prune/quarantine and sixteen events], [Richer read/filter APIs, per-node/queue/plugin metrics and explicit operational permissions],
      ))
    ]
    #points([A local Saga inside a job may restart as a whole job under a declared replay-safe policy; it has no durable intermediate compensation journal. Durable Saga uses its own operation identities, codecs and storage, even when sharing Grind's pool. Fabric and Relay compose typed boundaries without a universal retry/error/runtime abstraction.])
    #points([See #adr(1), #adr(9), #adr(10), #lnk("../COVERAGE.md")[source coverage], #lnk("../evidence/qualification.md")[evidence limits] and #lnk("../../oracle/ORACLE-LEDGER.md")[oracle provenance].])
  ])
]
