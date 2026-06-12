# Short-Job Penalty Service Implementation Plan (Revision 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract short-job-penalty accounting into a dedicated in-memory service driven by job-lifecycle events, so the scheduler stops scanning every job in jobdb each cycle to recompute the penalty and jobdb can immediately GC terminal short jobs.

**Architecture:** A new `ShortJobPenaltyService` owns penalty state keyed by `(pool, queue) -> ResourceList`. Jobs are reported into it (idempotently, dedup by job id) from **two feed points** whose roles are complementary:

1. **Same-cycle leader feed** (today-parity charge timing): in `cycle()`, after `generateUpdateMessages` and `expireJobsIfNecessary` have set job-level terminal flags and upserted the affected jobs into the txn, and **before** `schedulingAlgo.Schedule` — i.e. at exactly the point where today's per-job scan would observe those flags. It iterates `updatedJobs` and reports the **live** txn job (`txn.GetById`), so the `Schedule` call in the same cycle sees identical penalties to today's scan.
2. **syncState feed** (durability + failover state): on **every node (leader and followers)**, inside `syncState` immediately after `ReconcileDifferences` derives the cycle's `jsts` and **before** the (now unconditional) terminal-job GC in the same function. On the leader this is normally a dedup no-op (the job was charged by feed 1 a round-trip earlier); on followers it builds the state that makes failover lossless; and it guarantees no job is ever GC'd from jobdb without having been offered to the service on that node first.

The candidate check at both call sites is the job-level `InTerminalState()` — NOT the run-level `jst.Succeeded/Failed/Cancelled` transition flags (see "Lifecycle timing facts" below for why the flags are wrong). State self-expires lazily via a deadline min-heap evaluated against a pinned cycle-start `now` that is threaded as an explicit parameter into the write path (`cycle` -> feeds -> `ReportFinishedJob`) and the read path (`Schedule` -> `calculateJobSchedulingInfo` -> `GetPenaltiesForPool`). The old per-job `ShouldApplyPenalty` predicate is reused verbatim as the service's internal qualification gate (it is load-bearing for both today-parity and post-expiry dedup). jobdb GC becomes unconditional, the `fullJobGc` special case and the `GetAllTerminalJobs` fetch in the scheduling round are removed, and the now-dead `terminalJobs` jobdb index is deleted.

**Tech Stack:** Go, `container/heap` (stdlib), existing `internaltypes.ResourceList` value type, testify.

---

## Revision 2 changelog (review findings — read before executing)

Revision 1 was reviewed against the codebase at `9c8126f`. The following defects were found and are corrected throughout this revision:

1. **FATAL (fixed): the v1 feed never charged succeeded or failed jobs.** v1 fed the service leader-only from the `cycle()` leader block, pre-filtered on `jst.Succeeded || jst.Failed || jst.Cancelled`. Those are **run-level** transition flags; the qualification gate requires **job-level** `Job.InTerminalState()`. For the success and failure paths these are never true in the same cycle's jst (see "Lifecycle timing facts"), so the penalty would silently never apply to the two most common terminal states. **Fix:** feed from `syncState` using `jst.Job.InTerminalState()` as the candidate check; never consult the jst flags.
2. **Feed moved from leader-only to all nodes** (inside `syncState`, which every node runs). This (a) closes the leader-failover gap v1 had accepted — today followers retain terminal short jobs in their jobdb mirror via the GC skip, so today's failover has *no* gap and leader-only feeding would have been a regression; (b) makes feed-then-GC atomic within one function with no error return between them, eliminating a lost-penalty race v1 had (cycle error between the committed `syncState` GC and the leader-block feed permanently lost the charge, because the re-fetched job row recreates the job without runs — `reconciliation.go:162-166` — which fails the gate); and (c) removes any dependency on `updatedJobs`, making the `updateAll`-failover `txn.GetAll()` hazard structurally impossible rather than merely avoided.
3. **Missed compile-site (fixed):** `Schedule` is called through the `SchedulingAlgo` interface (`scheduling_algo.go:38-41`), implemented also by `testSchedulingAlgo` (`scheduler_test.go:2138`, `:2150`). Both must gain the `now` parameter (Task 3).
4. **Missed test call sites (fixed):** `syncState` is called directly by tests at `scheduler_test.go:1779` and `:2002` (Task 4).
5. **Unported assertion (fixed):** the old `TestTimeNotSetReturnsFalse` (zero `now` -> false) now has a counterpart (Task 1).
6. **Dead code v1 left behind (fixed):** after the `GetAllTerminalJobs` fetch is removed, the jobdb `terminalJobs` index is maintained on every upsert/delete for zero readers. Task 7 deletes the method, the index, and its tests.
7. **Benchmark compile errors (fixed):** missing `jobdb` import; `ptr` vs `ptrTime` helper inconsistency (now `ptrTime` everywhere, defined once in the service test file).
8. **`penaltyEntry.heapIndex` removed:** it was written by the heap but never read (no `heap.Fix`/`heap.Remove` use; entries are immutable and removed only via `Pop`).
9. ~~Accepted, documented behavior deviation — charge start timing.~~ **Superseded by Revision 3.**

Design-decision bullets below have been rewritten where their v1 premises were factually wrong; the changes were made at the plan owner's request ("update the plan according to your findings").

## Revision 3 changelog

The Revision 2 design charged the penalty one Pulsar round-trip later than today (at cycle N+k, when the job-level terminal flag returns from the database, instead of cycle N, when the run-terminal row arrives). The plan owner rejected that deviation: **charge timing must be identical to today.**

Fix: add a **same-cycle leader feed** in `cycle()` at the exact point where today's behavior is produced — after `generateUpdateMessages`/`expireJobsIfNecessary` have set job-level terminal flags and upserted the jobs, before `Schedule` runs. Today's per-job scan inside `Schedule` reads those freshly-upserted flags; the new feed reports the same live txn jobs to the service immediately before `Schedule`, so `GetPenaltiesForPool` returns exactly what today's scan would have computed, in the same cycle, against the same pinned `now`. The all-nodes `syncState` feed from Revision 2 is **kept** (it provides follower/failover state and the GC-safety guarantee); dedup by job id makes the two feeds reporting the same job harmless.

A consequence worth noting: the same-cycle feed iterates `updatedJobs`, which on the `updateAll` failover path is `txn.GetAll()`. In v1 this was treated as a hazard ("would re-charge every job"); with the dedup + gate inside `ReportFinishedJob` it is not only safe but **required** for failover parity — a freshly promoted leader processes all jobs through `generateUpdateMessages` on that cycle and the full pass charges, in that same cycle, any short job that went terminal around the failover, exactly as today's scan of the retained jobdb would have. The two-phase tests and a dedicated failover test in Task 4 pin all of this.

---

## Lifecycle timing facts (verified against the code — the feed design depends on these)

These were verified at `9c8126f` and are the reason the feed looks the way it does. Re-verify the cited lines if the base has moved.

- **(A) Run-level and job-level terminal flags arrive in different cycles.** When a run succeeds, the executor-side event updates only the **runs** table. In that cycle (call it N), `reconcileRunDifferences` sets `rst.Succeeded` -> `jst.Succeeded = true` (`jobdb/reconciliation.go:272-274`), but reconciliation never propagates run state to the job-level flag, so `jst.Job.InTerminalState()` is still false during `syncState`. The job-level flag is set later **in the same cycle, after syncState**, by `generateUpdateMessagesFromJob` (`scheduler.go:957` for success, `:1010` for failure), which upserts the modified job into the cycle txn (`scheduler.go:1057-1060`) and publishes `JobSucceeded`/`JobErrors`.
- **(B) The round-trip job-row update carries no run rows.** `JobSucceeded` maps to `MarkJobsSucceeded` and terminal `JobErrors` to `MarkJobsFailed` — **jobs-table-only** writes (`scheduleringester/instructions.go:300-312`). So at the cycle where the job row lands back (call it N+k), the fetch window contains the job row but no run row: every run-level jst flag is false. A jst is still generated for the job (every job id in the fetch window gets one — `jobdb/reconciliation.go:92, 114-116`), and `jst.Job` is the current jobdb job, which IS job-level terminal (on the leader it was upserted terminal in cycle N; on followers the job-row reconcile at N+k sets it via `reconciliation.go:185-186`).
- **(C) Consequence:** `jst.Succeeded || jst.Failed || jst.Cancelled` and `jst.Job.InTerminalState()` are **disjoint in time** for the success/failure paths. Only `jst.Job.InTerminalState()` identifies the cycle where the gate can pass. (Cancellation and lease-expiry happen to set both at N+k because their round-trips also touch the run row and `enforceTerminalStateExclusivity` re-raises the rst flag on any run-row touch — `reconciliation.go:284, 291-302` — but the feed must not rely on that.)
- **(D) How same-cycle charge parity is preserved (Revision 3).** Today the penalty starts in cycle N: the per-job scan runs during `Schedule`, after `generateUpdateMessagesFromJob` has set the job-level flag and upserted the job (`scheduler.go:957, :1010, :1057-1060`), and after `expireJobsIfNecessary` has failed lease-expired jobs the same way (`scheduler.go:1116`). The same-cycle leader feed is inserted at precisely that point — after both of those, before the `Schedule` call at :353-362 — and reports the **live** txn job (`txn.GetById(job.Id())`, because the `updatedJobs` slice holds pre-update snapshots). The gate evaluates the same pinned cycle-start `now` that `SetNow(start)` provides today. Result: `GetPenaltiesForPool` inside `Schedule` returns, in cycle N, exactly the set and values today's scan computes in cycle N. The syncState feed at N+k is then a dedup no-op on the leader.
- **(E) GC timing under the new design.** In cycle N the jst's job is not yet job-level terminal at syncState time, so the (unconditional) GC does not delete it — `generateUpdateMessagesFromJob` still finds it, and the same-cycle feed charges it. At N+k the jst's job is terminal: the syncState feed re-offers it (dedup no-op on the leader; the real charge on followers) and the GC deletes it, in that order, inside the same `syncState` call with no error return between. jobdb retention for terminal jobs drops from "cutoff + up to 10 cycles" to "one Pulsar round-trip".

### Failure-mode walk-through (why there are no lost or double charges)

The service is in-memory and the jobdb txn is transactional; the DB cursors (`jobsSerial`/`runsSerial`) only advance after a successful publish on the leader (`scheduler.go:391-392`) or at the non-leader early-out (`scheduler.go:287-288`). Walking every failure point:

1. **Error in `syncState` before its feed** (fetch, reconcile, upsert): txn aborted, cursors not advanced, nothing charged. Next cycle re-fetches the same rows and retries from scratch.
2. **Error in `syncState` after its feed, before commit** (`BatchDelete`): service charged, txn aborted, cursors not advanced. Next cycle re-derives the same jsts; `ReportFinishedJob` dedups by job id (`byId`), so the re-report is a no-op. No double charge, no loss.
3. **Error in the leader block before the same-cycle feed** (run-error fetch at :316, `generateUpdateMessages`, `submitCheck`, `expireJobsIfNecessary`): the cycle txn is aborted (job-level flags revert), cursors not advanced, nothing extra charged. Next cycle re-fetches the same run rows, re-derives the transitions, and the same-cycle feed charges on the retry — the same recovery today's scan gets (it would also not have run, because `Schedule` is after all those error returns).
4. **Error after the same-cycle feed, before commit/publish** (scheduling, publish): service charged; the cycle txn may abort (job-level flags revert in jobdb) but cursors are also not advanced, so the transitions are re-derived next cycle and dedup makes the re-report a no-op. Today's equivalent: the scan recomputes from re-derived jobdb state next cycle — same observable penalties from the next successful `Schedule` onward.
5. **Error anywhere after `syncState` committed at N+k:** the charge happened at N (same-cycle feed) or at this `syncState` (its feed, before GC). Cursors not advanced -> the job row is re-fetched next cycle; even though the job was GC'd and its run rows' serials are already consumed (so the row-recreated job has no runs and would fail the gate — `reconciliation.go:162-166`), the `byId` dedup short-circuits first. No loss, no double charge.
6. **Leader failover:** two complementary mechanisms. (a) Followers run the same `syncState` feed against the same DB stream, so the new leader's service already holds every penalty whose job row round-tripped before the failover. (b) For jobs that went terminal *around* the failover (run row arrived, `JobSucceeded` not yet round-tripped), the new leader's first cycle runs with `updateAll` -> `updatedJobs = txn.GetAll()` -> `generateUpdateMessages` sets their job-level flags -> the same-cycle feed charges them **in that same cycle**, exactly as today's scan of the retained jobdb would. No failover gap, no timing regression.
7. **Cold restart:** in-memory state is lost and `FetchInitialJobs` filters terminated jobs, so jobs already job-level terminal in the DB are never re-fetched -> not penalized for up to one cutoff window. **This matches today exactly** (a restarted scheduler's jobdb also has no terminal jobs for the same reason); it is not a new gap.
8. **Publish-failure replay:** cursors not advanced -> same rows re-fetched -> same jsts/updatedJobs -> `byId` dedup at both feeds. Post-expiry replay can never re-charge because the expiry deadline (`runStart + cutoff`) and the gate (`now - runStart < cutoff`) are the same inequality over the same immutable `runStart` — an expired entry can never re-qualify, so no tombstones are needed.

---

## Design decisions locked in (updated in Revision 2)

- **No `ReportDeletedJob`.** Finish/terminal event + lazy time-based expiry fully reproduces today's behavior. Deletion events are not added (they reintroduce double-count/ordering hazards for no benefit). *(Unchanged from v1.)*
- **Terminal candidate check = `jst.Job.InTerminalState()`** (job-level `Succeeded || Failed || Cancelled`), evaluated at the feed call site. **Never use the jst run-level transition flags** (`jst.Succeeded` etc.) — they are false on the only cycle where the job qualifies (Lifecycle facts A-C). Cancel-while-running short jobs ARE penalized today; the job-level check preserves that (the job-level cancelled flag round-trips like the others).
- **The candidate check is only a cheap pre-filter.** The real gate is `shouldApplyPenalty(job, now)`, ported verbatim from `ShortJobPenalty.ShouldApplyPenalty`. It does double duty: today-parity filter AND post-expiry dedup (failure-mode point 6).
- **One pinned cycle-start `now`,** threaded as an explicit parameter: `Run`'s per-tick `start` -> `cycle(..., now)` -> both feeds and `syncState(..., now)` (write path) and `Schedule(ctx, txn, now)` (read path). No `SetNow` setter. No `time.Now()` at call sites. `initialise` passes `s.clock.Now()` to its `syncState` call (harmless: initial jobs are non-terminal, the gate rejects everything).
- **Two feed points, both idempotent (dedup by job id):** (1) the same-cycle leader feed in `cycle()` after `generateUpdateMessages`/`expireJobsIfNecessary`, before `Schedule`, iterating `updatedJobs` and reporting the live `txn.GetById` job — this is what makes charge timing identical to today; (2) the `syncState` feed, every cycle, on every node, after `ReconcileDifferences`/upsert and before GC, iterating `jsts` — this is what makes failover lossless and guarantees nothing is GC'd unreported. Neither feed consults the run-level jst flags.
- **Iterating `updatedJobs` (= `txn.GetAll()` on the `updateAll` failover path) in the same-cycle feed is deliberate.** v1 treated this as a re-charge hazard; with the gate + dedup inside `ReportFinishedJob` it is a no-op for already-counted jobs and is exactly what gives a freshly promoted leader same-cycle charge parity for jobs that went terminal around the failover (failure-mode point 6b).
- **GC becomes unconditional in `syncState`.** It no longer consults the penalty. The `fullJobGc` parameter and the `cycleNumber%10==0` full-sweep exist solely to eventually collect short jobs the per-cycle GC skipped; both are removed. The `terminalJobs` jobdb index then has zero readers and is removed too (Task 7).
- **Cold-restart gap accepted; failover gap eliminated.** See failure-mode points 6-7. The cold-restart behavior is identical to today's; no restart-rebuild DB query.
- **Charge timing is identical to today** (Revision 3): the penalty becomes visible to `Schedule` in the same cycle the run-terminal row arrives, against the same pinned `now`, with the same expiry deadline. Pinned by Task 4's tests.
- **Cutoff is keyed per-pool** (`cutoffs[pool]`). Config is restart-only; no hot reload. An unconfigured pool yields cutoff `0`, so `now - runStart < 0` is always false -> never penalized (matches today, short_job_penalty.go:52).
- **No mutex.** Verified single-goroutine access: zero goroutines in `scheduling_algo.go`, `queue_scheduler.go`, `preempting_queue_scheduler.go`, `scheduler.go`. The cycle loop is the only writer/reader; Prometheus reads the copied `queueContext.ShortJobPenalty` snapshot (`metrics/cycle_metrics.go:573, :828`), never the live service. Do not add a lock for future-proofing.
- **Known harmless parity nuance:** the old per-job scan skipped jobs whose queue no longer exists (`scheduling_algo.go:590-594`); the service will briefly hold sums for such queues. Downstream consumption is keyed by existing queues (`AddQueueSchedulingContext`), so the extra entries are never read, and they expire within one cutoff window. Do not add queue-existence filtering to the service.

---

## File Structure

**Create:**
- `internal/scheduler/scheduling/short_job_penalty_service.go` - the new service: `ShortJobPenaltyService`, `penaltyEntry`, the deadline min-heap, `ReportFinishedJob`, `GetPenaltiesForPool`, internal `expireUpTo`. The existing `ShouldApplyPenalty` qualification logic folds in here as an internal method taking `(job, now)`.
- `internal/scheduler/scheduling/short_job_penalty_service_test.go` - unit tests for the service (qualification parity incl. zero-`now`, accumulation, lazy expiry, dedup, multi-pool isolation, per-pool cutoff, post-expiry non-re-qualification).
- `internal/scheduler/scheduling/short_job_penalty_service_bench_test.go` - the 50K-short-job benchmark for the perf AC.

**Modify:**
- `internal/scheduler/scheduling/short_job_penalty.go` - DELETE this file (the old `ShortJobPenalty` type, `NewShortJobPenalty`, `SetNow`, `ShouldApplyPenalty`). Its logic moves into the service file.
- `internal/scheduler/scheduling/short_job_penalty_test.go` - DELETE (every assertion ported into the service test, including the zero-`now` case).
- `internal/scheduler/scheduling/scheduling_algo.go` - **`SchedulingAlgo` interface gains `now time.Time` (:38-41)**; struct field type change (:62); constructor param type (:74); thread `now` through `Schedule` (:109) -> `runPoolSchedulingRound` (:203) -> `newFairSchedulingAlgoContext` (:411) -> `calculateJobSchedulingInfo` (:579); replace the per-job penalty branch (:596-602) with a `GetPenaltiesForPool` call; drop `GetAllTerminalJobs` from the scheduling round (:441, :445) and its comment bullet (:436-437).
- `internal/scheduler/scheduler.go` - thread `now` into `cycle` (:266) and `syncState` (:426); add the syncState feed (all nodes, before GC) and the same-cycle leader feed (after `expireJobsIfNecessary` :344-349, before the `Schedule` block :351-362); remove `SetNow` calls (:153, :172); make `syncState` GC unconditional and drop the `fullJobGc` param + full-sweep; update call sites (:211, :273, :362, :1263); `Scheduler` struct field (:71) and `NewScheduler` param (:109) type change.
- `internal/scheduler/schedulerapp.go` - construct `NewShortJobPenaltyService` instead of `NewShortJobPenalty` (:350).
- `internal/scheduler/scheduling/scheduling_algo_test.go` - update the three `Schedule(ctx, txn)` calls (:73, :216, :980) and, where penalty behavior is exercised, the three `NewFairSchedulingAlgo` calls (:55, :196, :922).
- `internal/scheduler/scheduler_test.go` - update `testSchedulingAlgo.Schedule` (:2150), the `cycle(...)` calls (:1058, :1225, :1232, :1297, :3328), the direct `syncState(...)` calls (:1779, :2002); add the two-phase lifecycle test.
- `internal/scheduler/jobdb/jobdb.go` - DELETE `GetAllTerminalJobs` (:960-963) and the `terminalJobs` index + maintenance (:75, :140, :148, :181, :356, :379, :402, :447-448, :476, :618-619, :801-815, :1037-1038).
- `internal/scheduler/jobdb/jobdb_test.go` - DELETE the three terminal-index tests (`TestJobDb_TestGetTerminalJobs` :150, `TestJobDb_TerminalJobs_Lifecycle` :170, `TestJobDb_TerminalJobs_Deleted` :195).

---

## Task 1: Create the service skeleton + fold in the qualification gate

**Files:**
- Create: `internal/scheduler/scheduling/short_job_penalty_service.go`
- Create: `internal/scheduler/scheduling/short_job_penalty_service_test.go`
- Reference (will delete in Task 6): `internal/scheduler/scheduling/short_job_penalty.go`

The qualification gate is ported verbatim from `short_job_penalty.go:29-53`, changed only to take `now` as a parameter instead of reading `sjp.now`, and to nil-guard the service.

- [ ] **Step 1: Write the failing test (qualification parity)**

Create `internal/scheduler/scheduling/short_job_penalty_service_test.go`. This ports every assertion from the old `short_job_penalty_test.go` to the new service surface, replacing `SetNow(now)` + `ShouldApplyPenalty(job)` with `shouldApplyPenalty(job, now)`. Note `TestService_ZeroNowReturnsFalse` ports the old `TestTimeNotSetReturnsFalse`.

```go
package scheduling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestService_NilServiceQualificationReturnsFalse(t *testing.T) {
	var nilSvc *ShortJobPenaltyService = nil
	job := shortServiceTestJob(time.Now()).WithSucceeded(true)
	assert.False(t, nilSvc.shouldApplyPenalty(job, time.Now()))
}

func TestService_ZeroNowReturnsFalse(t *testing.T) {
	job := shortServiceTestJob(time.Now()).WithSucceeded(true)
	assert.False(t, makeServiceSut().shouldApplyPenalty(job, time.Time{}))
}

func TestService_LongSucceededJobReturnsFalse(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()
	job := longServiceTestJob(now).WithSucceeded(true)
	assert.False(t, svc.shouldApplyPenalty(job, now))
}

func TestService_ShortRunningJobReturnsFalse(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()
	job := shortServiceTestJob(now)
	assert.False(t, svc.shouldApplyPenalty(job, now))
}

func TestService_ShortSucceededJobReturnsTrue(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()
	job := shortServiceTestJob(now).WithSucceeded(true)
	assert.True(t, svc.shouldApplyPenalty(job, now))
}

func TestService_ShortCancelledJobReturnsTrue(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()
	job := shortServiceTestJob(now).WithCancelled(true)
	assert.True(t, svc.shouldApplyPenalty(job, now))
}

func TestService_ShortFailedJobReturnsTrue(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()
	job := shortServiceTestJob(now).WithFailed(true)
	assert.True(t, svc.shouldApplyPenalty(job, now))
}

func TestService_ShortPreemptedJobReturnsFalse(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()
	job := shortServiceTestJob(now).WithSucceeded(true)
	job = job.WithUpdatedRun(job.LatestRun().WithPreempted(true))
	assert.False(t, svc.shouldApplyPenalty(job, now))
}

func TestService_ShortJobWithPreemptRequestedReturnsFalse(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()
	job := shortServiceTestJob(now).WithSucceeded(true)
	job = job.WithUpdatedRun(job.LatestRun().WithPreemptRequested(true))
	assert.False(t, svc.shouldApplyPenalty(job, now))
}

func TestService_ShortJobWithPreemptedTimeSetReturnsFalse(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()
	job := shortServiceTestJob(now).WithSucceeded(true)
	job = job.WithUpdatedRun(job.LatestRun().WithPreemptedTime(&now))
	assert.False(t, svc.shouldApplyPenalty(job, now))
}

func makeServiceSut() *ShortJobPenaltyService {
	return NewShortJobPenaltyService(map[string]time.Duration{testfixtures.TestPool: time.Minute})
}

func shortServiceTestJob(now time.Time) *jobdb.Job {
	return serviceTestJob(now.Add(time.Second * -30))
}

func longServiceTestJob(now time.Time) *jobdb.Job {
	return serviceTestJob(now.Add(-time.Hour))
}

func serviceTestJob(runningTime time.Time) *jobdb.Job {
	return serviceTestJobForPool("q", testfixtures.TestPool, runningTime)
}

func serviceTestJobForQueue(queue string, runningTime time.Time) *jobdb.Job {
	return serviceTestJobForPool(queue, testfixtures.TestPool, runningTime)
}

func serviceTestJobForPool(queue string, pool string, runningTime time.Time) *jobdb.Job {
	job := testfixtures.Test32Cpu256GiJob(queue, testfixtures.PriorityClass2).WithNewRun("testExecutor", "test-node", "node", pool, 5)
	run := job.LatestRun()
	return job.WithUpdatedRun(run.WithRunningTime(&runningTime))
}

func ptrTime(t time.Time) *time.Time { return &t }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/scheduler/scheduling/ -run TestService_ -v`
Expected: FAIL - compile error, `NewShortJobPenaltyService` / `ShortJobPenaltyService` undefined.

- [ ] **Step 3: Write minimal implementation (struct + qualification gate)**

Create `internal/scheduler/scheduling/short_job_penalty_service.go`:

```go
package scheduling

import (
	"container/heap"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

// penaltyEntry is one reported terminal short job: the minimal record needed to
// charge the penalty, know when to un-charge it, and recognize a re-report.
type penaltyEntry struct {
	jobId     string
	pool      string
	queue     string
	resources internaltypes.ResourceList
	// deadline is runStart + cutoff[pool], fixed at insert from the immutable run
	// start time. It never changes, which is what makes lazy heap-expiry clean.
	deadline time.Time
}

// ShortJobPenaltyService owns short-job-penalty state keyed by (pool, queue).
// It is fed terminal short jobs once each via ReportFinishedJob and read via
// GetPenaltiesForPool. State self-expires lazily against the now passed at call
// time. Not safe for concurrent use: only ever touched by the scheduler cycle
// goroutine.
type ShortJobPenaltyService struct {
	cutoffs map[string]time.Duration

	byId   map[string]*penaltyEntry
	expiry *entryHeap
	// sums is the maintained pool -> queue -> ResourceList cache returned by
	// GetPenaltiesForPool; kept in sync on every insert and expiry.
	sums map[string]map[string]internaltypes.ResourceList
}

func NewShortJobPenaltyService(cutoffs map[string]time.Duration) *ShortJobPenaltyService {
	return &ShortJobPenaltyService{
		cutoffs: cutoffs,
		byId:    map[string]*penaltyEntry{},
		expiry:  &entryHeap{},
		sums:    map[string]map[string]internaltypes.ResourceList{},
	}
}

// shouldApplyPenalty is the qualification gate, ported verbatim from the old
// ShortJobPenalty.ShouldApplyPenalty, taking now as a parameter. It is the
// single source of truth for today-parity AND post-expiry dedup: deadline
// (runStart + cutoff) and this predicate (now - runStart < cutoff) are the same
// inequality against the same immutable runStart.
func (s *ShortJobPenaltyService) shouldApplyPenalty(job *jobdb.Job, now time.Time) bool {
	if s == nil || now.IsZero() {
		return false
	}
	if !job.InTerminalState() {
		return false
	}
	jobRun := job.LatestRun()
	if jobRun == nil {
		return false
	}
	if jobRun.Preempted() || jobRun.PreemptRequested() || jobRun.PreemptedTime() != nil {
		return false
	}
	jobStart := jobRun.RunningTime()
	if jobStart == nil {
		return false
	}
	return now.Sub(*jobStart) < s.cutoffs[jobRun.Pool()]
}

// entryHeap is a min-heap of penaltyEntry ordered by deadline. Implements
// container/heap.Interface. Entries are immutable and only ever removed via
// Pop (no heap.Fix/heap.Remove), so no back-index is maintained.
type entryHeap []*penaltyEntry

func (h entryHeap) Len() int           { return len(h) }
func (h entryHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h entryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *entryHeap) Push(x any) {
	*h = append(*h, x.(*penaltyEntry))
}

func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

func (h entryHeap) peek() *penaltyEntry {
	return h[0]
}

var _ heap.Interface = (*entryHeap)(nil)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/scheduler/scheduling/ -run TestService_ -v`
Expected: PASS (all qualification tests).

- [ ] **Step 5: Commit**

```bash
git add internal/scheduler/scheduling/short_job_penalty_service.go internal/scheduler/scheduling/short_job_penalty_service_test.go
git commit -m "feat(scheduler): add ShortJobPenaltyService skeleton with qualification gate"
```

---

## Task 2: Implement ReportFinishedJob, GetPenaltiesForPool, and lazy expiry

**Files:**
- Modify: `internal/scheduler/scheduling/short_job_penalty_service.go`
- Modify: `internal/scheduler/scheduling/short_job_penalty_service_test.go`

- [ ] **Step 1: Write the failing tests (accumulation, expiry, dedup, multi-pool, per-pool cutoff)**

Append to `internal/scheduler/scheduling/short_job_penalty_service_test.go`:

```go
func TestService_AccumulatesPerQueue(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()

	jobA := serviceTestJobForQueue("q1", now.Add(-30*time.Second)).WithSucceeded(true)
	jobB := serviceTestJobForQueue("q1", now.Add(-20*time.Second)).WithSucceeded(true)
	jobC := serviceTestJobForQueue("q2", now.Add(-20*time.Second)).WithSucceeded(true)

	svc.ReportFinishedJob(jobA, now)
	svc.ReportFinishedJob(jobB, now)
	svc.ReportFinishedJob(jobC, now)

	penalties := svc.GetPenaltiesForPool(testfixtures.TestPool, now)
	expectedTwoJobs := jobA.AllResourceRequirements().Add(jobB.AllResourceRequirements())
	assert.True(t, penalties["q1"].Equal(expectedTwoJobs))
	assert.True(t, penalties["q2"].Equal(jobC.AllResourceRequirements()))
}

func TestService_NonQualifyingJobIsNotCharged(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()

	longJob := longServiceTestJob(now).WithSucceeded(true) // ran > cutoff
	svc.ReportFinishedJob(longJob, now)

	penalties := svc.GetPenaltiesForPool(testfixtures.TestPool, now)
	assert.Empty(t, penalties)
}

func TestService_NonTerminalJobIsNotCharged(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()

	runningJob := shortServiceTestJob(now) // short but still running
	svc.ReportFinishedJob(runningJob, now)

	assert.Empty(t, svc.GetPenaltiesForPool(testfixtures.TestPool, now))
}

func TestService_DedupSameJobReportedTwice(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()

	job := serviceTestJobForQueue("q1", now.Add(-30*time.Second)).WithSucceeded(true)
	svc.ReportFinishedJob(job, now)
	svc.ReportFinishedJob(job, now) // second report must be a no-op

	penalties := svc.GetPenaltiesForPool(testfixtures.TestPool, now)
	assert.True(t, penalties["q1"].Equal(job.AllResourceRequirements()))
}

func TestService_EntryExpiresExactlyAtDeadline(t *testing.T) {
	start := time.Now()
	svc := makeServiceSut() // cutoff = 1 minute
	// runStart = start; deadline = start + 1m.
	job := serviceTestJobForQueue("q1", start).WithSucceeded(true)

	reportNow := start.Add(30 * time.Second) // 30s in, still within window
	svc.ReportFinishedJob(job, reportNow)
	assert.True(t, svc.GetPenaltiesForPool(testfixtures.TestPool, reportNow)["q1"].Equal(job.AllResourceRequirements()))

	// Exactly at deadline (start + 1m): now - runStart == cutoff, expired (strict <).
	atDeadline := start.Add(time.Minute)
	assert.Empty(t, svc.GetPenaltiesForPool(testfixtures.TestPool, atDeadline))
}

func TestService_PostExpiryReReportNeverReQualifies(t *testing.T) {
	start := time.Now()
	svc := makeServiceSut() // cutoff = 1 minute
	job := serviceTestJobForQueue("q1", start).WithSucceeded(true)

	svc.ReportFinishedJob(job, start.Add(10*time.Second))
	// Read past the deadline: entry expires.
	assert.Empty(t, svc.GetPenaltiesForPool(testfixtures.TestPool, start.Add(2*time.Minute)))
	// A late re-report after expiry: now - runStart >= cutoff, so it fails
	// qualification and is never re-charged. No tombstone required.
	svc.ReportFinishedJob(job, start.Add(2*time.Minute))
	assert.Empty(t, svc.GetPenaltiesForPool(testfixtures.TestPool, start.Add(2*time.Minute)))
}

func TestService_PerPoolCutoffAndPoolIsolation(t *testing.T) {
	now := time.Now()
	svc := NewShortJobPenaltyService(map[string]time.Duration{
		"poolA": time.Minute,
		"poolB": time.Hour,
		// "poolC" intentionally absent -> cutoff 0 -> never penalized.
	})

	// Ran 30m ago: short for poolB (1h cutoff), long for poolA (1m cutoff).
	jobA := serviceTestJobForPool("q1", "poolA", now.Add(-30*time.Minute)).WithSucceeded(true)
	jobB := serviceTestJobForPool("q1", "poolB", now.Add(-30*time.Minute)).WithSucceeded(true)
	jobC := serviceTestJobForPool("q1", "poolC", now.Add(-1*time.Second)).WithSucceeded(true)

	svc.ReportFinishedJob(jobA, now)
	svc.ReportFinishedJob(jobB, now)
	svc.ReportFinishedJob(jobC, now)

	assert.Empty(t, svc.GetPenaltiesForPool("poolA", now))
	assert.True(t, svc.GetPenaltiesForPool("poolB", now)["q1"].Equal(jobB.AllResourceRequirements()))
	assert.Empty(t, svc.GetPenaltiesForPool("poolC", now))
}

func TestService_GetPenaltiesForUnknownPoolIsEmpty(t *testing.T) {
	now := time.Now()
	svc := makeServiceSut()
	assert.Empty(t, svc.GetPenaltiesForPool("does-not-exist", now))
}
```

Note: `Test32Cpu256GiJob(queue, ...)` sets the job's queue from the first argument, generates a unique ULID job id per call, and `AllResourceRequirements()` is non-empty for this fixture (32-CPU job).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/scheduler/scheduling/ -run TestService_ -v`
Expected: FAIL - `ReportFinishedJob` and `GetPenaltiesForPool` undefined.

- [ ] **Step 3: Implement the three methods**

Append to `internal/scheduler/scheduling/short_job_penalty_service.go`:

```go
// ReportFinishedJob is idempotent and is called from two feed points: the
// same-cycle leader feed in cycle() (live txn jobs, after generateUpdateMessages
// sets job-level terminal flags - this gives today-parity charge timing) and
// the syncState feed on every node (jst jobs, before GC - this gives failover
// durability). shouldApplyPenalty is the authoritative gate; callers may
// pre-filter on job.InTerminalState() as an optimization but must NOT
// pre-filter on the jst run-level transition flags (see plan: "Lifecycle
// timing facts"). now must be the pinned cycle-start time. Re-reports of an
// already-counted job are no-ops (dedup by job id); reports after the entry
// expired can never re-qualify (the gate and the expiry deadline are the same
// inequality over the immutable run start).
func (s *ShortJobPenaltyService) ReportFinishedJob(job *jobdb.Job, now time.Time) {
	if s == nil {
		return
	}
	s.expireUpTo(now)

	if _, alreadyCounted := s.byId[job.Id()]; alreadyCounted {
		return
	}
	if !s.shouldApplyPenalty(job, now) {
		return
	}

	run := job.LatestRun()
	pool := run.Pool()
	queue := job.Queue()
	resources := job.AllResourceRequirements()
	// deadline == runStart + cutoff[pool]. shouldApplyPenalty already guaranteed
	// run, RunningTime, and a positive remaining window.
	deadline := run.RunningTime().Add(s.cutoffs[pool])

	e := &penaltyEntry{
		jobId:     job.Id(),
		pool:      pool,
		queue:     queue,
		resources: resources,
		deadline:  deadline,
	}
	s.byId[job.Id()] = e
	heap.Push(s.expiry, e)
	s.addToSums(pool, queue, resources)
}

// GetPenaltiesForPool returns a copy of the per-queue penalty for the given pool,
// after expiring anything whose deadline has passed at now. Queues whose summed
// penalty is fully zero are omitted, matching today's "absent active queue"
// output.
func (s *ShortJobPenaltyService) GetPenaltiesForPool(pool string, now time.Time) map[string]internaltypes.ResourceList {
	if s == nil {
		return nil
	}
	s.expireUpTo(now)

	poolSums := s.sums[pool]
	if len(poolSums) == 0 {
		return nil
	}
	out := make(map[string]internaltypes.ResourceList, len(poolSums))
	for queue, rl := range poolSums {
		out[queue] = rl
	}
	return out
}

// expireUpTo pops every entry whose deadline is at or before now, subtracting its
// resources from the running sum and dropping its dedup record. Each entry is
// pushed once and popped once over its lifetime: O(log n) amortized, no scan.
func (s *ShortJobPenaltyService) expireUpTo(now time.Time) {
	for s.expiry.Len() > 0 && !s.expiry.peek().deadline.After(now) {
		e := heap.Pop(s.expiry).(*penaltyEntry)
		s.subtractFromSums(e.pool, e.queue, e.resources)
		delete(s.byId, e.jobId)
	}
}

func (s *ShortJobPenaltyService) addToSums(pool, queue string, resources internaltypes.ResourceList) {
	queueSums, ok := s.sums[pool]
	if !ok {
		queueSums = map[string]internaltypes.ResourceList{}
		s.sums[pool] = queueSums
	}
	queueSums[queue] = queueSums[queue].Add(resources)
}

func (s *ShortJobPenaltyService) subtractFromSums(pool, queue string, resources internaltypes.ResourceList) {
	queueSums := s.sums[pool]
	remaining := queueSums[queue].Subtract(resources)
	if remaining.AllZero() {
		delete(queueSums, queue)
		if len(queueSums) == 0 {
			delete(s.sums, pool)
		}
		return
	}
	queueSums[queue] = remaining
}
```

Note on `AllZero`: `internaltypes.ResourceList.AllZero()` exists (resource_list.go:108) and reports "all resource quantities are zero". After subtracting the last contribution the value is non-empty-factory but all-zero, so `AllZero()` is the correct prune check. Do NOT use `IsEmpty()` here - that only checks the factory is nil (resource_list.go:145), which is never true for a summed value.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/scheduler/scheduling/ -run TestService_ -v`
Expected: PASS (all service tests, including accumulation, expiry, dedup, multi-pool).

- [ ] **Step 5: Commit**

```bash
git add internal/scheduler/scheduling/short_job_penalty_service.go internal/scheduler/scheduling/short_job_penalty_service_test.go
git commit -m "feat(scheduler): implement ShortJobPenaltyService report/get/lazy-expiry"
```

---

## Task 3: Thread `now` through the read path (interface + impl + mock) and call the service in calculateJobSchedulingInfo

**Files:**
- Modify: `internal/scheduler/scheduling/scheduling_algo.go`
  - **`SchedulingAlgo` interface (:38-41): `Schedule(*armadacontext.Context, *jobdb.Txn, time.Time) (*SchedulerResult, error)`**
  - struct field `shortJobPenalty *ShortJobPenalty` (:62) -> `shortJobPenalty *ShortJobPenaltyService`
  - `NewFairSchedulingAlgo` param `shortJobPenalty *ShortJobPenalty` (:74) -> `*ShortJobPenaltyService`
  - `Schedule` (:109) gains a `now time.Time` parameter
  - `runPoolSchedulingRound` (:203) gains `now time.Time`
  - `newFairSchedulingAlgoContext` (:411) gains `now time.Time`
  - `calculateJobSchedulingInfo` (:579) gains `now time.Time`; replace the per-job penalty branch (:596-602) with a single `GetPenaltiesForPool` call
  - drop `terminalJobs := txn.GetAllTerminalJobs()` (:441) and its append (:445)
- Modify: `internal/scheduler/scheduler_test.go` - **`testSchedulingAlgo.Schedule` (:2150) gains the `now` parameter** (it can ignore the value). Do this in the same commit as the interface change or the package will not compile.
- Modify: `internal/scheduler/scheduling/scheduling_algo_test.go` - update the three `Schedule(ctx, txn)` calls (:73, :216, :980) and the three `NewFairSchedulingAlgo` calls (:55, :196, :922)

Note: `internal/scheduler/scheduler.go:362` (`s.schedulingAlgo.Schedule(ctx, txn)`) also calls through the interface; it is updated in Task 4 together with the `cycle` signature change. To keep every commit compiling, Tasks 3 and 4 may be landed as one commit if needed; otherwise pass `s.clock.Now()` temporarily at :362 in this task and replace it with the threaded `now` in Task 4. **Do not forget to replace it** - Task 4 Step 3a includes the replacement.

The `now` plumbing chain is: `Schedule(ctx, txn, now)` -> `runPoolSchedulingRound(ctx, pool, txn, executors, now)` -> `newFairSchedulingAlgoContext(ctx, txn, executors, pool, now)` -> `calculateJobSchedulingInfo(..., now)`.

- [ ] **Step 1: Write the failing test (service-driven penalty in scheduling info)**

Add to `internal/scheduler/scheduling/scheduling_algo_test.go` a test that builds a `FairSchedulingAlgo` with a populated `ShortJobPenaltyService` and asserts the penalty reaches the queue scheduling context. Because the full `Schedule` path needs executors/nodes, copy the harness setup from an existing `TestSchedule_*` case and inject the service:

```go
func TestSchedule_ShortJobPenaltyComesFromService(t *testing.T) {
	// Build the standard single-pool scheduling harness used by the other
	// TestSchedule_* cases, but inject a service pre-populated with one short
	// job for queue "A".
	now := time.Now()
	svc := NewShortJobPenaltyService(map[string]time.Duration{testfixtures.TestPool: time.Minute})

	shortJob := testfixtures.Test32Cpu256GiJob("A", testfixtures.PriorityClass2).
		WithNewRun("testExecutor", "test-node", "node", testfixtures.TestPool, 5).
		WithSucceeded(true)
	shortJob = shortJob.WithUpdatedRun(shortJob.LatestRun().WithRunningTime(ptrTime(now.Add(-10 * time.Second))))
	svc.ReportFinishedJob(shortJob, now)

	sch, err := NewFairSchedulingAlgo(
		schedulingConfig,
		0,
		mockExecutorRepo,
		mockQueueCache,
		reports.NewSchedulingContextRepository(),
		testfixtures.TestResourceListFactory,
		testfixtures.TestEmptyFloatingResources,
		priorityoverride.NewNoOpProvider(),
		svc,
		&testRunReconciler{},
	)
	require.NoError(t, err)

	// ... upsert at least one active job for queue "A" so the queue is active,
	// then:
	result, err := sch.Schedule(ctx, txn, now)
	require.NoError(t, err)

	sctx := result.PoolResults[0].GetSchedulingContext()
	require.NotNil(t, sctx)
	qctx := sctx.QueueSchedulingContexts["A"]
	require.NotNil(t, qctx)
	assert.True(t, qctx.ShortJobPenalty.Equal(shortJob.AllResourceRequirements()))
}
```

(`ptrTime` is defined in `short_job_penalty_service_test.go`, same package - reuse it; do not define a second helper.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/scheduler/scheduling/ -run TestSchedule_ShortJobPenaltyComesFromService -v`
Expected: FAIL - `Schedule` does not take a `now` argument / `NewFairSchedulingAlgo` type mismatch.

- [ ] **Step 3a: Change the interface, struct field, and constructor type**

In `internal/scheduler/scheduling/scheduling_algo.go`:

Lines 38-41, change the interface:
```go
type SchedulingAlgo interface {
	// Schedule should assign jobs to nodes.
	// Any jobs that are scheduled should be marked as such in the JobDb using the transaction provided.
	// now is the pinned cycle-start time used for short-job-penalty reads.
	Schedule(*armadacontext.Context, *jobdb.Txn, time.Time) (*SchedulerResult, error)
}
```

Line 62, change:
```go
	shortJobPenalty       *ShortJobPenalty
```
to:
```go
	shortJobPenalty       *ShortJobPenaltyService
```

Line 74 (constructor param), change:
```go
	shortJobPenalty *ShortJobPenalty,
```
to:
```go
	shortJobPenalty *ShortJobPenaltyService,
```

(The assignment at :96 `shortJobPenalty: shortJobPenalty,` is unchanged.)

In `internal/scheduler/scheduler_test.go` line 2150, change the mock:
```go
func (t *testSchedulingAlgo) Schedule(_ *armadacontext.Context, txn *jobdb.Txn, _ time.Time) (*scheduling.SchedulerResult, error) {
```

- [ ] **Step 3b: Thread `now` through Schedule -> runPoolSchedulingRound -> newFairSchedulingAlgoContext**

`Schedule` signature (:109):
```go
func (l *FairSchedulingAlgo) Schedule(
	ctx *armadacontext.Context,
	txn *jobdb.Txn,
	now time.Time,
) (*SchedulerResult, error) {
```

At :155, pass `now`:
```go
			outcome, schedulingResult, err = l.runPoolSchedulingRound(ctx, pool, txn, executors, now)
```

`runPoolSchedulingRound` signature (:203):
```go
func (l *FairSchedulingAlgo) runPoolSchedulingRound(
	ctx *armadacontext.Context,
	pool configuration.PoolConfig,
	txn *jobdb.Txn,
	executors []*schedulerobjects.Executor,
	now time.Time,
) (*PoolSchedulingOutcome, *SchedulingResult, error) {
```

At :218, pass `now`:
```go
	fsctx, err := l.newFairSchedulingAlgoContext(ctx, txn, executors, pool, now)
```

`newFairSchedulingAlgoContext` signature (:411):
```go
func (l *FairSchedulingAlgo) newFairSchedulingAlgoContext(ctx *armadacontext.Context, txn *jobdb.Txn, executors []*schedulerobjects.Executor, currentPool configuration.PoolConfig, now time.Time) (*FairSchedulingAlgoContext, error) {
```

- [ ] **Step 3c: Drop the terminal-jobs fetch (perf win) and pass `now` to calculateJobSchedulingInfo**

In `newFairSchedulingAlgoContext`, the block at :440-446 currently fetches terminal jobs solely for the penalty. Replace:
```go
	leasedJobs := txn.GetAllLeasedJobs()
	terminalJobs := txn.GetAllTerminalJobs()
	queuedJobs := getQueuedJobs(txn, allPools)
	allJobs := make([]*jobdb.Job, 0, len(leasedJobs)+len(terminalJobs)+len(queuedJobs))
	allJobs = append(allJobs, leasedJobs...)
	allJobs = append(allJobs, terminalJobs...)
	allJobs = append(allJobs, queuedJobs...)
```
with:
```go
	leasedJobs := txn.GetAllLeasedJobs()
	queuedJobs := getQueuedJobs(txn, allPools)
	allJobs := make([]*jobdb.Job, 0, len(leasedJobs)+len(queuedJobs))
	allJobs = append(allJobs, leasedJobs...)
	allJobs = append(allJobs, queuedJobs...)
```

Also update the comment block at :432-439 to drop the "Terminal jobs of this pool - For calculating short job penalty" bullet, since terminal jobs are no longer fetched here.

At the `calculateJobSchedulingInfo` call (:448-456), append `now`:
```go
	jobSchedulingInfo, err := l.calculateJobSchedulingInfo(ctx,
		armadamaps.FromSlice(executors,
			func(ex *schedulerobjects.Executor) string { return ex.Id },
			func(_ *schedulerobjects.Executor) bool { return true }),
		queueByName,
		allJobs,
		currentPool.Name,
		awayAllocationPools,
		allPools,
		now)
```

- [ ] **Step 3d: Replace the per-job penalty branch with a service lookup**

`calculateJobSchedulingInfo` signature (:579):
```go
func (l *FairSchedulingAlgo) calculateJobSchedulingInfo(ctx *armadacontext.Context, activeExecutorsSet map[string]bool,
	queues map[string]*api.Queue, jobs []*jobdb.Job, currentPool string, awayAllocationPools []string, allPools []string, now time.Time,
) (*jobSchedulingInfo, error) {
```

Remove the local accumulation (:587):
```go
	shortJobPenaltyByQueue := make(map[string]internaltypes.ResourceList)
```

Remove the per-job branch (:596-602):
```go
		if l.shortJobPenalty.ShouldApplyPenalty(job) {
			jobPool := job.LatestRun().Pool()
			jobRequirements := job.AllResourceRequirements()
			if jobPool == currentPool {
				shortJobPenaltyByQueue[queue.Name] = shortJobPenaltyByQueue[queue.Name].Add(jobRequirements)
			}
		}
```

Because terminal jobs are no longer in `jobs`, the `if job.InTerminalState() { continue }` at :604 now only ever skips jobs that became terminal mid-loop via reconcile within the leased set; leave it in place (harmless, preserves the existing guard).

Just before the `return` at :681, populate from the service:
```go
	shortJobPenaltyByQueue := l.shortJobPenalty.GetPenaltiesForPool(currentPool, now)
```

The returned struct literal (:681-688) is unchanged - it still references `shortJobPenaltyByQueue`. (The service may include entries for queues that no longer exist; that is fine - `AddQueueSchedulingContext` consumption is keyed by existing queues, see Design decisions.)

- [ ] **Step 3e: Do NOT touch the internal optimal-scheduler Schedule call**

At :864 there is `result, err := scheduler.Schedule(ctx)` - this is a *different* `Schedule` (the queue scheduler / optimal scheduler), NOT `FairSchedulingAlgo.Schedule`. Confirm by inspecting the receiver type at that line. Do NOT add `now` to it. (Verified: the `SchedulingAlgo.Schedule` callers are scheduler.go:362, the mock at scheduler_test.go:2150, and the three algo tests.)

- [ ] **Step 3f: Update the algo test call sites**

In `internal/scheduler/scheduling/scheduling_algo_test.go`:
- The three `NewFairSchedulingAlgo(...)` calls (:55, :196, :922) currently pass `nil` for the penalty arg (verified at :64). `nil` is still valid for the `*ShortJobPenaltyService` typed parameter, so they compile unchanged - but for the cases that exercise penalty behavior, pass a real `NewShortJobPenaltyService(...)`. The skeleton cases can keep `nil`.
- The three `sch.Schedule(ctx, txn)` calls (:73, :216, :980) become `sch.Schedule(ctx, txn, time.Now())` (or a fixed test clock value where determinism matters).

- [ ] **Step 3g: Keep scheduler.go compiling**

If landing Task 3 as its own commit, change `internal/scheduler/scheduler.go:362` to `s.schedulingAlgo.Schedule(ctx, txn, s.clock.Now())` as a stopgap (replaced by the threaded `now` in Task 4 Step 3a). Note the `Scheduler` struct field `shortJobPenalty *scheduling.ShortJobPenalty` (scheduler.go:71) and `NewScheduler` param (:109) do not need to change yet - they change in Task 5 - but if the compiler complains after the constructor type change, move that type change forward into this commit; both orderings are acceptable as long as every commit builds.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/scheduler/scheduling/ -v` and `go build ./internal/scheduler/...`
Expected: PASS / builds (existing `TestSchedule_*` plus the new `TestSchedule_ShortJobPenaltyComesFromService`).

- [ ] **Step 5: Commit**

```bash
git add internal/scheduler/scheduling/scheduling_algo.go internal/scheduler/scheduling/scheduling_algo_test.go internal/scheduler/scheduler.go internal/scheduler/scheduler_test.go
git commit -m "feat(scheduler): read short-job penalty from service; drop terminal-job scan"
```

---

## Task 4: Add both feeds, thread `now` into cycle/syncState, make GC unconditional

**Files:**
- Modify: `internal/scheduler/scheduler.go`
  - `cycle` (:266) gains a `now time.Time` parameter; pass it to `syncState` (:273) and `schedulingAlgo.Schedule` (:362)
  - add the **same-cycle leader feed** in `cycle()` after `expireJobsIfNecessary` (:344-349) and before the `Schedule` block (:351-353)
  - `syncState` (:426): drop the `fullJobGc` param, gain `now time.Time`; add the **syncState feed** after the upsert block and before GC; GC unconditionally; remove the `ShouldApplyPenalty` skip
  - remove `SetNow` calls (:153, :172)
  - `Run` passes its pinned `start` into `cycle` (:211)
  - the initialise-path `syncState` call (:1263) passes `s.clock.Now()`
- Modify: `internal/scheduler/scheduler_test.go` - update `cycle(...)` calls (:1058, :1225, :1232, :1297, :3328) and the direct `syncState(...)` calls (:1779, :2002); add the lifecycle tests

- [ ] **Step 1: Write the failing tests (two-phase terminal flow, follower feed, failover updateAll)**

**The two-phase test exists to catch exactly the bug that killed Revision 1** (Lifecycle facts A-C). It MUST deliver the run-terminal row and the job-terminal row in *different* cycles - do not seed both in one fetch window, or the test will pass against broken feed designs.

Add to `internal/scheduler/scheduler_test.go`, modeled on the existing cycle harness (see :1000-1065 for construction; `testJobRepository` at :2064 returns its `updatedJobs`/`updatedRuns` fields verbatim on each fetch, so mutate those fields between cycles; the multi-cycle pattern at :1225-1232 shows two sequential `cycle` calls):

```go
func TestCycle_ShortJobPenalty_TwoPhaseTerminalFlow(t *testing.T) {
	// Harness: real ShortJobPenaltyService (cutoff 1m on the test pool) injected
	// into NewScheduler; standalone (always-leader) controller; fake clock.
	// Seed jobdb with a leased, running job (run started 10s before the fake
	// clock's now) and the matching DB rows in testJobRepository.initialJobs/
	// initialRuns so serials line up.
	//
	// Phase A - cycle 1: deliver ONLY the run row with Succeeded=true (bump its
	// Serial); leave the job row untouched. Run sched.cycle(..., now1). Assert:
	//   - the penalty IS charged in THIS cycle (same-cycle leader feed; this is
	//     the today-parity property the plan owner requires - Lifecycle fact D):
	//     sched.shortJobPenalty.GetPenaltiesForPool(pool, now1)[queue] equals
	//     the job's AllResourceRequirements()
	//   - the job is still in jobdb (not GC'd) and is now job-level terminal
	//     (set by generateUpdateMessagesFromJob): txn.GetById(jobId) != nil and
	//     .InTerminalState() == true
	//   - a JobSucceeded event was published (existing publisher assertions).
	//
	// Phase B - cycle 2: deliver ONLY the job row with Succeeded=true (bump its
	// Serial); set updatedRuns to nil. Run sched.cycle(..., now2). Assert:
	//   - the penalty total is UNCHANGED (the syncState feed's re-offer is a
	//     dedup no-op on the leader)
	//   - the job has been deleted from jobdb by the unconditional GC:
	//     txn.GetById(jobId) == nil
	//
	// Phase C - re-deliver the same job row a third cycle (publish-failure
	// replay simulation). Assert the penalty total is still unchanged (dedup).
}

func TestCycle_ShortJobPenalty_FollowerFeedsViaSyncState(t *testing.T) {
	// Same two-phase flow, but run the cycles with a non-leader token (see the
	// failover-style tests around :3300 for obtaining one; cycle exits before
	// the leader block, so neither generateUpdateMessages nor the same-cycle
	// feed runs). Assert:
	//   - after phase A: NOT charged (job-level flag has not arrived; followers
	//     have no same-cycle feed - their state only matters at failover, where
	//     the updateAll test below provides same-cycle parity)
	//   - after phase B: charged (the job-row reconcile sets the job-level flag
	//     via reconciliation.go:185-186, so the syncState feed sees it), and
	//     the job is GC'd from the follower's jobdb.
	// This pins the failover-durability property (failure-mode point 6a).
}

func TestCycle_ShortJobPenalty_FailoverUpdateAllChargesSameCycle(t *testing.T) {
	// Pins failure-mode point 6b: a freshly promoted leader charges, in its
	// FIRST cycle, short jobs that went terminal around the failover - exactly
	// as today's scan of the retained jobdb would.
	// Seed jobdb the way a follower's would look mid-flow: job present, run
	// succeeded (run-level), job-level flag NOT set, no rows pending in the
	// repo. Run sched.cycle(ctx, true /* updateAll */, leaderToken, ...).
	// Assert:
	//   - the penalty IS charged in this same cycle (updatedJobs = txn.GetAll()
	//     -> generateUpdateMessages sets the job-level flag -> same-cycle feed
	//     reports the live job)
	//   - a JobSucceeded event was published.
	// Then run a second updateAll cycle and assert the total is unchanged
	// (dedup makes the GetAll pass idempotent - the v1 "re-charge" hazard).
}
```

(Fill the bodies using the established harness; the commented assertions are the load-bearing checks. If wiring a real service into `NewScheduler` requires the Task 5 type change first, land Tasks 4 and 5 in one commit - every commit must build.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/scheduler/ -run 'TestCycle_ShortJobPenalty' -v`
Expected: FAIL - `cycle`/`syncState` signature mismatch and/or service not fed.

- [ ] **Step 3a: Thread `now` into cycle and remove SetNow**

In `internal/scheduler/scheduler.go` `Run`:

Delete :153:
```go
	s.shortJobPenalty.SetNow(start)
```
Delete :172:
```go
					s.shortJobPenalty.SetNow(start)
```

At :211, pass the pinned cycle-start `start` (defined at :171) into `cycle`:
```go
					err := s.cycle(ctx, fullUpdate, leaderToken, shouldSchedule, cycleNumber, start)
```

`cycle` signature (:266):
```go
func (s *Scheduler) cycle(ctx *armadacontext.Context, updateAll bool, leaderToken leaderelection.LeaderToken, shouldSchedule bool, cycleNumber int, now time.Time) error {
```

At the syncState call (:273):
```go
	updatedJobs, jsts, newJobsSerial, newRunsSerial, err := s.syncState(ctx, false, now)
```

At the scheduling call (:362), pass `now` (replacing the Task 3 stopgap if one was used):
```go
		result, err = s.schedulingAlgo.Schedule(ctx, txn, now)
```

- [ ] **Step 3b: Add the same-cycle leader feed in cycle()**

Insert after the `expireJobsIfNecessary` block (:344-349, i.e. after `events = append(events, expirationEvents...)`) and before `schedulerResult := scheduling.SchedulerResult{}` (:351):

```go
	// Charge the short-job penalty for jobs that went terminal this cycle.
	// generateUpdateMessages and expireJobsIfNecessary above have just set
	// job-level terminal flags and upserted the affected jobs into txn; reporting
	// them here, before the Schedule call below, means GetPenaltiesForPool sees
	// exactly what the old per-job scan saw in the same cycle. We look up the
	// live job in txn because the updatedJobs slice holds pre-update snapshots.
	// On the updateAll failover path updatedJobs is txn.GetAll(); iterating it
	// here is deliberate - ReportFinishedJob dedups by job id and applies the
	// qualification gate, and the full pass gives a freshly promoted leader
	// same-cycle parity for jobs that went terminal around the failover.
	for _, job := range updatedJobs {
		if live := txn.GetById(job.Id()); live != nil && live.InTerminalState() {
			s.shortJobPenalty.ReportFinishedJob(live, now)
		}
	}
```

This placement is load-bearing: it must be after every code path that sets job-level terminal flags in the txn (`generateUpdateMessagesFromJob` for success/failure/cancel, `expireJobsIfNecessary` for lease expiry) and before `schedulingAlgo.Schedule`. Do not move it earlier "for tidiness" - that reintroduces the one-cycle charge delay the plan owner rejected.

- [ ] **Step 3c: Add the feed + unconditional GC in syncState**

`syncState` signature (:426), drop `fullJobGc`, add `now`:
```go
func (s *Scheduler) syncState(ctx *armadacontext.Context, initial bool, now time.Time) ([]*jobdb.Job, []jobdb.JobStateTransitions, int64, int64, error) {
```

Immediately after the upsert block (:478-485, `txn.Upsert(jobDbJobs)`), insert the feed - **before** the GC block so feed-then-GC is atomic with no error return between them (failure-mode points 2-3):

```go
	// Offer terminal jobs to the short-job-penalty service before deleting them.
	// This runs on every node (leader and followers) so the in-memory penalty
	// state survives leader failover; on the leader it is normally a dedup no-op
	// because the same-cycle feed in cycle() already charged the job when it went
	// terminal. ReportFinishedJob dedups by job id, so replays after a failed
	// publish (cursors not advanced) are no-ops too. The candidate check is the
	// job-level InTerminalState - the same condition the GC below uses. Do NOT
	// filter on jst.Succeeded/Failed/Cancelled: those are run-level transition
	// flags and are false on the cycle where the job-level terminal flag arrives
	// from the database, which is the only cycle where the job qualifies here.
	for _, jst := range jsts {
		if jst.Job.InTerminalState() {
			s.shortJobPenalty.ReportFinishedJob(jst.Job, now)
		}
	}
```

Then replace the GC block (:487-510):
```go
	// Delete jobs in a terminal state.
	idsOfJobsToDelete := make([]string, 0)
	deletionCandidates := jobDbJobs
	if fullJobGc {
		// Occasional full gc so jobs that were not deleted
		// earlier as ShortJobPenalty was being applied
		// eventually get deleted.
		deletionCandidates = txn.GetAll()
	}
	shortJobCount := 0
	for _, j := range deletionCandidates {
		if !j.InTerminalState() {
			continue
		}
		if s.shortJobPenalty.ShouldApplyPenalty(j) {
			shortJobCount++
			continue
		}
		idsOfJobsToDelete = append(idsOfJobsToDelete, j.Id())
	}
	ctx.Logger().Infof("Deleting %d jobs out of %d considered for deletion (%d short jobs, full job gc=%t)", len(idsOfJobsToDelete), len(deletionCandidates), shortJobCount, fullJobGc)
	if err := txn.BatchDelete(idsOfJobsToDelete); err != nil {
		return nil, nil, 0, 0, err
	}
```
with:
```go
	// Delete jobs in a terminal state. Short-job-penalty state is owned by the
	// ShortJobPenaltyService (fed above, before this delete), so terminal jobs -
	// including short ones - are deleted on the normal cadence and never retained
	// in jobdb.
	idsOfJobsToDelete := make([]string, 0)
	for _, j := range jobDbJobs {
		if j.InTerminalState() {
			idsOfJobsToDelete = append(idsOfJobsToDelete, j.Id())
		}
	}
	ctx.Logger().Infof("Deleting %d terminal jobs out of %d updated jobs", len(idsOfJobsToDelete), len(jobDbJobs))
	if err := txn.BatchDelete(idsOfJobsToDelete); err != nil {
		return nil, nil, 0, 0, err
	}
```

- [ ] **Step 3d: Update remaining syncState call sites**

At :1263 (the initialise path; initial jobs are non-terminal so the gate rejects everything - the `now` value is immaterial but must be non-zero for clarity):
```go
		if _, _, newJobsSerial, newRunsSerial, err := s.syncState(ctx, true, s.clock.Now()); err != nil {
```

In `internal/scheduler/scheduler_test.go`, the direct calls:
- :1779: `sched.syncState(ctx, true, sched.clock.Now())`
- :2002: `sched.syncState(ctx, false, sched.clock.Now())`

- [ ] **Step 3e: Update cycle test call sites**

In `internal/scheduler/scheduler_test.go`, add the `now` argument to each `cycle(...)` call (:1058, :1225, :1232, :1297, :3328). Use the same clock the test's scheduler uses. Example for :1058:
```go
			err = sched.cycle(ctx, false, sched.leaderController.GetToken(), true, 1, sched.clock.Now())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/scheduler/ -run 'TestCycle_ShortJobPenalty' -v`
Then: `go test ./internal/scheduler/ -run TestScheduler -v` (regression on the cycle harness)
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/scheduler/scheduler.go internal/scheduler/scheduler_test.go
git commit -m "feat(scheduler): feed penalty service same-cycle on leader and via syncState on all nodes; unconditional terminal-job GC"
```

---

## Task 5: Update schedulerapp wiring + Scheduler struct types

**Files:**
- Modify: `internal/scheduler/schedulerapp.go:350` (construction) - both injection points (:361 into the algo, :407 into the scheduler) already pass the same `shortJobPenalty` variable, so only the constructor call changes.
- Modify: `internal/scheduler/scheduler.go` struct field (:71) and `NewScheduler` param (:109) - if not already moved forward into Task 3/4 to keep commits building.

- [ ] **Step 1: Change the construction**

In `internal/scheduler/schedulerapp.go`, line 350, change:
```go
	shortJobPenalty := scheduling.NewShortJobPenalty(config.Scheduling.GetShortJobPenaltyCutoffs())
```
to:
```go
	shortJobPenalty := scheduling.NewShortJobPenaltyService(config.Scheduling.GetShortJobPenaltyCutoffs())
```

The variable is still passed at :361 (`NewFairSchedulingAlgo` read side) and :407 (`NewScheduler` write side) unchanged - same instance shared, same topology as today. **The shared single instance is required**: the algo reads the same state syncState writes.

- [ ] **Step 2: Update the Scheduler struct + constructor type (if not already done)**

In `internal/scheduler/scheduler.go`:

Line 71:
```go
	shortJobPenalty *scheduling.ShortJobPenaltyService
```
Line 109 (`NewScheduler` param):
```go
	shortJobPenalty *scheduling.ShortJobPenaltyService,
```
(Assignment at :131 unchanged. Test call sites passing `nil` for this param compile unchanged.)

- [ ] **Step 3: Build the whole scheduler module**

Run: `go build ./internal/scheduler/...`
Expected: success.

- [ ] **Step 4: Commit**

```bash
git add internal/scheduler/schedulerapp.go internal/scheduler/scheduler.go
git commit -m "refactor(scheduler): wire NewShortJobPenaltyService into app"
```

---

## Task 6: Delete the dead old type and its test

**Files:**
- Delete: `internal/scheduler/scheduling/short_job_penalty.go`
- Delete: `internal/scheduler/scheduling/short_job_penalty_test.go`

- [ ] **Step 1: Confirm no remaining references**

Run:
```bash
grep -rn "ShortJobPenalty\b\|NewShortJobPenalty\b\|\.SetNow(\|ShouldApplyPenalty" internal/ --include="*.go" | grep -v "ShortJobPenaltyService\|ShortJobPenaltyByQueue\|ShortJobPenaltyCutoff\|shortJobPenaltyByQueue\|qctx.ShortJobPenalty\|queueContext.ShortJobPenalty\|qCtx.ShortJobPenalty\|GetAllocationInclShortJobPenalty\|\.shortJobPenalty\b"
```
Expected: no output (every reference is to the service, the per-queue field, the config cutoff, or the context accessor - none to the deleted type/methods). If `shouldApplyPenalty` (lowercase, the service's internal method) appears, that's fine - it lives in the service file.

Note: `s.shortJobPenalty` and `l.shortJobPenalty` (the struct fields) remain valid - they now hold `*ShortJobPenaltyService`. The grep above excludes them.

- [ ] **Step 2: Delete the files**

```bash
git rm internal/scheduler/scheduling/short_job_penalty.go internal/scheduler/scheduling/short_job_penalty_test.go
```

- [ ] **Step 3: Build and test the scheduling package**

Run: `go build ./internal/scheduler/... && go test ./internal/scheduler/scheduling/ -count=1`
Expected: success / PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(scheduler): remove old ShortJobPenalty type superseded by service"
```

---

## Task 7: Remove the now-dead jobdb terminalJobs index

After Task 3, `txn.GetAllTerminalJobs()` has zero production callers, and with prompt GC the `terminalJobs` set is maintained on every upsert/delete for nothing. Removing it is part of the perf win.

**Files:**
- Modify: `internal/scheduler/jobdb/jobdb.go` - delete `GetAllTerminalJobs` (:960-963) and the `terminalJobs` field/initialisation/copies/maintenance (:75, :140, :148, :181, :356, :379, :402, :447-448, :476, :618-619, :801-815, :1037-1038 - re-locate by grepping `terminalJobs`, line numbers shift as you edit).
- Modify: `internal/scheduler/jobdb/jobdb_test.go` - delete `TestJobDb_TestGetTerminalJobs` (:150), `TestJobDb_TerminalJobs_Lifecycle` (:170), `TestJobDb_TerminalJobs_Deleted` (:195).

- [ ] **Step 1: Confirm the only callers are jobdb-internal**

Run:
```bash
grep -rn "GetAllTerminalJobs\|terminalJobs" internal/ --include="*.go"
```
Expected: hits only in `internal/scheduler/jobdb/jobdb.go` and `internal/scheduler/jobdb/jobdb_test.go`. If anything else appears, STOP and resolve it first - do not delete an index with live readers.

- [ ] **Step 2: Delete the method, the field, every maintenance site, and the three tests**

Grep-driven: remove every `terminalJobs` line and its enclosing dead statements (the upsert-path add/replace blocks at ~:801-815, the delete-path removal at ~:1037-1038, the txn copy lines, the struct fields, the constructor init).

- [ ] **Step 3: Build and test jobdb**

Run: `go build ./internal/scheduler/... && go test ./internal/scheduler/jobdb/ -count=1`
Expected: success / PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "perf(scheduler): remove unused jobdb terminalJobs index"
```

---

## Task 8: Benchmark for the 50K-short-job perf AC

**Files:**
- Create: `internal/scheduler/scheduling/short_job_penalty_service_bench_test.go`

The perf AC is "no regression in scheduler cycle time when a queue has 50K+ recent short jobs." The real win is structural (terminal short jobs leave jobdb promptly, so the `calculateJobSchedulingInfo` loop and the dropped `GetAllTerminalJobs` fetch no longer traverse them, and jobdb upserts no longer maintain the terminal index). The service itself must be cheap at 50K live entries: report and get must stay sub-millisecond and allocation-light. This benchmark measures the service in isolation, which is the component this change introduces.

- [ ] **Step 1: Write the benchmark**

```go
package scheduling

import (
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func BenchmarkShortJobPenaltyService_50kReports(b *testing.B) {
	now := time.Now()
	jobs := make([]*jobdb.Job, 50000)
	for i := range jobs {
		j := testfixtures.Test32Cpu256GiJob("q", testfixtures.PriorityClass2).
			WithNewRun("testExecutor", "test-node", "node", testfixtures.TestPool, 5).
			WithSucceeded(true)
		jobs[i] = j.WithUpdatedRun(j.LatestRun().WithRunningTime(ptrTime(now.Add(-10 * time.Second))))
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		svc := NewShortJobPenaltyService(map[string]time.Duration{testfixtures.TestPool: time.Minute})
		for _, j := range jobs {
			svc.ReportFinishedJob(j, now)
		}
		_ = svc.GetPenaltiesForPool(testfixtures.TestPool, now)
	}
}

func BenchmarkShortJobPenaltyService_GetWith50kLiveEntries(b *testing.B) {
	now := time.Now()
	svc := NewShortJobPenaltyService(map[string]time.Duration{testfixtures.TestPool: time.Minute})
	for i := 0; i < 50000; i++ {
		j := testfixtures.Test32Cpu256GiJob("q", testfixtures.PriorityClass2).
			WithNewRun("testExecutor", "test-node", "node", testfixtures.TestPool, 5).
			WithSucceeded(true)
		j = j.WithUpdatedRun(j.LatestRun().WithRunningTime(ptrTime(now.Add(-10 * time.Second))))
		svc.ReportFinishedJob(j, now)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = svc.GetPenaltiesForPool(testfixtures.TestPool, now)
	}
}
```

(`ptrTime` is defined in `short_job_penalty_service_test.go`, same package.) All 50K jobs use queue "q", so they accumulate into one `(pool, "q")` sum - exactly the AC scenario. `Test32Cpu256GiJob` generates a unique ULID job id per call, so dedup does not collapse them.

- [ ] **Step 2: Run the benchmark**

Run: `go test ./internal/scheduler/scheduling/ -bench BenchmarkShortJobPenaltyService -benchmem -run '^$'`
Expected: completes; record ns/op and allocs/op. `Get` with 50K live entries should be O(queues) for the copy (one queue here) plus the no-op `expireUpTo` peek. Report is O(n log n) for 50K heap inserts. There is no hard pass/fail threshold; the artifact is the recorded numbers demonstrating the service is not a new bottleneck.

- [ ] **Step 3: Commit**

```bash
git add internal/scheduler/scheduling/short_job_penalty_service_bench_test.go
git commit -m "test(scheduler): benchmark ShortJobPenaltyService at 50k short jobs"
```

---

## Task 9: Full-suite verification

- [ ] **Step 1: Build everything**

Run: `go build ./...`
Expected: success.

- [ ] **Step 2: Run the affected package tests**

Postgres must be running (see CLAUDE.md - `armada-dev deps` / `armada-dev init` if not). Run with `TZ=UTC` per the project memory note about non-UTC hosts:

Run:
```bash
TZ=UTC go test -count=1 ./internal/scheduler/scheduling/... ./internal/scheduler/ ./internal/scheduler/jobdb/
```
Expected: PASS.

- [ ] **Step 3: Run the local CI equivalent**

Invoke the `armada-local-ci` skill if available in your environment. If it is not, run the equivalent directly:

```bash
go mod tidy
golangci-lint run ./internal/scheduler/... --timeout 5m
go build ./...
```

Expected: clean.

- [ ] **Step 4: Final commit (if CI made fixups)**

```bash
git add -A
git commit -m "chore(scheduler): lint/format fixups for short-job penalty service"
```

---

## Self-Review notes (verified during planning + Revision 2 review)

- **Spec coverage:**
  - "New service owns penalty state, wired into lifecycle path" -> Tasks 1, 2, 4.
  - "FairSchedulingAlgo no longer iterates jobs to compute penalty; queries service" -> Task 3 (per-job branch removed, `GetPenaltiesForPool` added; terminal-job fetch dropped; `SchedulingAlgo` interface + mock updated).
  - "Terminal short jobs eligible for deletion on normal path" -> Task 4 (unconditional GC, `fullJobGc` removed) + Task 7 (dead index removed).
  - "Penalty entries expire correctly without jobdb retention (tests)" -> Task 2 (`TestService_EntryExpiresExactlyAtDeadline`, `TestService_PostExpiryReReportNeverReQualifies`).
  - "Existing behavior preserved end-to-end" -> Task 1 ports every old qualification assertion (incl. zero-`now`); Task 3 asserts the service value reaches the queue context; Task 4's tests pin the real run-row/job-row lifecycle on both leader and follower, the dedup-on-replay property, and - per Revision 3 - **same-cycle charge timing identical to today**, including on the failover/updateAll path.
  - "No regression at 50K+ short jobs (benchmark)" -> Task 8.
- **Correctness invariants the tests pin:** (1) the feeds must key off job-level `InTerminalState`, never the run-level jst flags (two-phase test fails otherwise); (2) the same-cycle leader feed sits after `generateUpdateMessages`/`expireJobsIfNecessary` and before `Schedule`, reporting live `txn.GetById` jobs - that exact placement is what reproduces today's charge timing (phase A assertion fails otherwise); (3) syncState feed-before-GC in one function with no intervening error return (failure-mode points 2 and 5); (4) dedup by job id makes cursor replays and the double-feed no-ops; (5) expiry-deadline/gate equivalence makes post-expiry re-reports impossible without tombstones; (6) followers accumulate state via syncState and a promoted leader's first `updateAll` cycle charges in-cycle via the `GetAll` pass, so failover loses neither charges nor timing.
- **Type consistency:** `ShortJobPenaltyService`, `NewShortJobPenaltyService`, `ReportFinishedJob(job, now)`, `GetPenaltiesForPool(pool, now)`, `shouldApplyPenalty(job, now)`, `penaltyEntry`, `entryHeap`, `ptrTime` used consistently across all tasks. The struct field name `shortJobPenalty` is retained on both `Scheduler` and `FairSchedulingAlgo`; only its type changes.
- **`ResourceList` methods confirmed:** `Equal` (resource_list.go:17), `AllZero` (:108), `IsEmpty` (:145 - explicitly NOT the prune check), `Add` (:221), `Subtract` (:236).
- **Compile-site inventory for the `Schedule` signature change:** interface (scheduling_algo.go:38-41), `FairSchedulingAlgo.Schedule` (:109), mock `testSchedulingAlgo.Schedule` (scheduler_test.go:2150), callers at scheduler.go:362 and scheduling_algo_test.go:73/:216/:980. The `scheduler.Schedule(ctx)` at scheduling_algo.go:864 is a different type and is untouched.
- **Compile-site inventory for the `syncState` signature change:** definition (scheduler.go:426), callers at scheduler.go:273, scheduler.go:1263, scheduler_test.go:1779, scheduler_test.go:2002.
