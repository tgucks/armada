# Short-Job Penalty Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract short-job-penalty accounting into a dedicated in-memory service driven by job-lifecycle events, so the scheduler stops scanning every job in jobdb each cycle to recompute the penalty and jobdb can immediately GC terminal short jobs.

**Architecture:** A new `ShortJobPenaltyService` owns penalty state keyed by `(pool, queue) -> ResourceList`. Terminal short jobs are reported into it once, leader-only, from the per-cycle reconcile loop in `cycle()` (riding alongside the existing `ReportStateTransitions` loop, iterating `jsts` not `updatedJobs`). State self-expires lazily via a deadline min-heap evaluated against a pinned cycle-start `now` that is threaded as an explicit parameter into both the write path (`ReportFinishedJob`) and the read path (`Schedule` -> `calculateJobSchedulingInfo` -> `GetPenaltiesForPool`). The old per-job `ShouldApplyPenalty` predicate is reused verbatim as the service's internal qualification gate (it is load-bearing for both today-parity and post-expiry dedup). jobdb GC becomes unconditional, and the `fullJobGc` special case plus the `GetAllTerminalJobs` fetch in the scheduling round are removed.

**Tech Stack:** Go, `container/heap` (stdlib), `k8s.io/utils/clock`, existing `internaltypes.ResourceList` value type, testify.

---

## Design decisions locked in (from brainstorming Q&A)

These were resolved with the requesting engineer and must not be re-litigated during execution:

- **No `ReportDeletedJob`.** Finish/terminal event + lazy time-based expiry fully reproduces today's behavior. Deletion events are not added (they reintroduce double-count/ordering hazards for no benefit).
- **Terminal tap set = `Succeeded || Failed || Cancelled`** (exactly `Job.InTerminalState()`). Cancel-while-running short jobs ARE penalized today; excluding cancelled would be a regression.
- **The JST flags are only a cheap pre-filter.** The real gate is `ShouldApplyPenalty(job, now)`, reused verbatim. It does double duty: today-parity filter AND post-expiry dedup (expiry deadline `runStart + cutoff` is algebraically identical to the qualification cutoff `now - runStart < cutoff`, so an expired entry can never re-qualify -> no tombstone needed).
- **One pinned cycle-start `now`,** threaded as an explicit parameter into both `ReportFinishedJob` and `GetPenaltiesForPool`. No `SetNow` setter. No `time.Now()` at call sites.
- **Feed runs every cycle but leader-only.** Tap `jsts` (genuinely-transitioned jobs), NOT `updatedJobs` (which becomes `txn.GetAll()` on the `updateAll` failover path - iterating it would re-charge every job). Place the feed inside the leader block of `cycle()` after `ValidateToken`, beside the existing `ReportStateTransitions` loop.
- **GC becomes unconditional in `syncState`.** It no longer consults the penalty. Followers GC their own jobdb mirror too (desirable - smaller jobdb everywhere). The `fullJobGc` parameter and the `cycleNumber%10==0` full-sweep exist solely to eventually collect short jobs the per-cycle GC skipped; both are removed.
- **Cold-restart / failover gaps are acceptable.** Penalties may be empty for up to one cutoff window after a full restart or leader failover. No restart-rebuild DB query. This is no worse than today (`SelectInitialJobs` already filters `terminated = false`).
- **Cutoff is keyed per-pool** (`cutoffs[pool]`). Config is restart-only; no hot reload. An unconfigured pool yields cutoff `0`, so `now - runStart < 0` is always false -> never penalized (matches today, short_job_penalty.go:52).
- **No mutex.** Verified single-goroutine access: zero goroutines in `scheduling_algo.go`, `queue_scheduler.go`, `preempting_queue_scheduler.go`, `scheduler.go`. The cycle loop is the only writer/reader; Prometheus reads the copied `queueContext.ShortJobPenalty` snapshot, never the live service. Do not add a lock for future-proofing.

---

## File Structure

**Create:**
- `internal/scheduler/scheduling/short_job_penalty_service.go` - the new service: `ShortJobPenaltyService`, `penaltyEntry`, the deadline min-heap, `ReportFinishedJob`, `GetPenaltiesForPool`, internal `expireUpTo`. The existing `ShouldApplyPenalty` qualification logic folds in here as an internal method taking `(job, now)`.
- `internal/scheduler/scheduling/short_job_penalty_service_test.go` - unit tests for the service (qualification parity, accumulation, lazy expiry, dedup, multi-pool isolation, per-pool cutoff, post-expiry non-re-qualification).
- `internal/scheduler/scheduling/short_job_penalty_service_bench_test.go` - the 50K-short-job benchmark for the perf AC.

**Modify:**
- `internal/scheduler/scheduling/short_job_penalty.go` - DELETE this file (the old `ShortJobPenalty` type, `NewShortJobPenalty`, `SetNow`, `ShouldApplyPenalty`). Its logic moves into the service file. (Keep the old test file's parity assertions by porting them to the service test.)
- `internal/scheduler/scheduling/short_job_penalty_test.go` - DELETE (ported into the service test).
- `internal/scheduler/scheduling/scheduling_algo.go` - struct field type change; thread `now` through `Schedule` -> `runPoolSchedulingRound` -> `newFairSchedulingAlgoContext` -> `calculateJobSchedulingInfo`; replace the per-job penalty branch with a `GetPenaltiesForPool` call; drop `GetAllTerminalJobs` from the scheduling round; remove the terminal-job penalty branch.
- `internal/scheduler/scheduler.go` - thread `now` into `cycle`; add the leader-only penalty feed beside the `ReportStateTransitions` loop; remove `SetNow` calls; make `syncState` GC unconditional and drop the `fullJobGc` param + full-sweep.
- `internal/scheduler/schedulerapp.go` - construct `NewShortJobPenaltyService` instead of `NewShortJobPenalty`.
- `internal/scheduler/scheduling/scheduling_algo_test.go` - update `Schedule(ctx, txn)` calls to the new signature; pass a service (or `nil`) to `NewFairSchedulingAlgo`.
- `internal/scheduler/scheduler_test.go` - update `cycle(...)` calls to the new signature.

---

## Task 1: Create the service skeleton + fold in the qualification gate

**Files:**
- Create: `internal/scheduler/scheduling/short_job_penalty_service.go`
- Create: `internal/scheduler/scheduling/short_job_penalty_service_test.go`
- Reference (will delete in Task 6): `internal/scheduler/scheduling/short_job_penalty.go`

The qualification gate is ported verbatim from `short_job_penalty.go:29-53`, changed only to take `now` as a parameter instead of reading `sjp.now`, and to nil-guard the service.

- [ ] **Step 1: Write the failing test (qualification parity)**

Create `internal/scheduler/scheduling/short_job_penalty_service_test.go`. This ports every assertion from the old `short_job_penalty_test.go` to the new service surface, replacing `SetNow(now)` + `ShouldApplyPenalty(job)` with `shouldApplyPenalty(job, now)`.

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
	job := testfixtures.Test32Cpu256GiJob("q", testfixtures.PriorityClass2).WithNewRun("testExecutor", "test-node", "node", testfixtures.TestPool, 5)
	run := job.LatestRun()
	return job.WithUpdatedRun(run.WithRunningTime(&runningTime))
}
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
	// heapIndex is maintained by the heap implementation; unused outside it.
	heapIndex int
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
// container/heap.Interface.
type entryHeap []*penaltyEntry

func (h entryHeap) Len() int            { return len(h) }
func (h entryHeap) Less(i, j int) bool  { return h[i].deadline.Before(h[j].deadline) }
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *entryHeap) Push(x any) {
	e := x.(*penaltyEntry)
	e.heapIndex = len(*h)
	*h = append(*h, e)
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
import (
	// add to the existing import block:
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

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

func serviceTestJobForQueue(queue string, runningTime time.Time) *jobdb.Job {
	return serviceTestJobForPool(queue, testfixtures.TestPool, runningTime)
}

func serviceTestJobForPool(queue string, pool string, runningTime time.Time) *jobdb.Job {
	job := testfixtures.Test32Cpu256GiJob(queue, testfixtures.PriorityClass2).WithNewRun("testExecutor", "test-node", "node", pool, 5)
	run := job.LatestRun()
	return job.WithUpdatedRun(run.WithRunningTime(&runningTime))
}
```

Note: `Test32Cpu256GiJob(queue, ...)` sets the job's queue from the first argument; confirm via `testfixtures` if `AllResourceRequirements()` is non-empty for this fixture (it is - it's a 32-CPU job).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/scheduler/scheduling/ -run TestService_ -v`
Expected: FAIL - `ReportFinishedJob` and `GetPenaltiesForPool` undefined.

- [ ] **Step 3: Implement the three methods**

Append to `internal/scheduler/scheduling/short_job_penalty_service.go`:

```go
// ReportFinishedJob is called every cycle, leader-only, for each terminal-state
// transition (Succeeded || Failed || Cancelled). The JST flag is only a cheap
// pre-filter; shouldApplyPenalty is the authoritative gate. now must be the
// pinned cycle-start time.
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

Note on `AllZero`: `internaltypes.ResourceList.AllZero()` exists (resource_list.go:108) and reports "all resource quantities are zero". After subtracting the last contribution the value is non-empty-factory but all-zero, so `AllZero()` is the correct prune check. Do NOT use `IsEmpty()` here - that only checks the factory is nil, which is never true for a summed value.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/scheduler/scheduling/ -run TestService_ -v`
Expected: PASS (all service tests, including accumulation, expiry, dedup, multi-pool).

- [ ] **Step 5: Commit**

```bash
git add internal/scheduler/scheduling/short_job_penalty_service.go internal/scheduler/scheduling/short_job_penalty_service_test.go
git commit -m "feat(scheduler): implement ShortJobPenaltyService report/get/lazy-expiry"
```

---

## Task 3: Thread `now` through the read path and call the service in calculateJobSchedulingInfo

**Files:**
- Modify: `internal/scheduler/scheduling/scheduling_algo.go`
  - struct field `shortJobPenalty *ShortJobPenalty` (:62) -> `shortJobPenalty *ShortJobPenaltyService`
  - `NewFairSchedulingAlgo` param `shortJobPenalty *ShortJobPenalty` (:74) -> `*ShortJobPenaltyService`
  - `Schedule` (:109) gains a `now time.Time` parameter
  - `runPoolSchedulingRound` (:203) gains `now time.Time`
  - `newFairSchedulingAlgoContext` (:411) gains `now time.Time`
  - `calculateJobSchedulingInfo` (:579) gains `now time.Time`; replace the per-job penalty branch (:596-602) with a single `GetPenaltiesForPool` call
  - drop `terminalJobs := txn.GetAllTerminalJobs()` (:441) and its append (:445)
- Modify: `internal/scheduler/scheduling/scheduling_algo_test.go` - update the three `Schedule(ctx, txn)` calls (:73, :216, :980) and the three `NewFairSchedulingAlgo` calls (:55, :196, :922)

This is the most mechanically invasive task. The `now` plumbing chain is: `Schedule(ctx, txn, now)` -> `runPoolSchedulingRound(ctx, pool, txn, executors, now)` -> `newFairSchedulingAlgoContext(ctx, txn, executors, pool, now)` -> `calculateJobSchedulingInfo(..., now)`.

- [ ] **Step 1: Write the failing test (service-driven penalty in scheduling info)**

Add to `internal/scheduler/scheduling/scheduling_algo_test.go` a test that builds a `FairSchedulingAlgo` with a populated `ShortJobPenaltyService` and asserts the penalty reaches the queue scheduling context. Because the full `Schedule` path needs executors/nodes, scope this to `calculateJobSchedulingInfo` directly if the existing harness supports it; otherwise assert via the existing `Schedule` harness that `QueueSchedulingContexts[q].ShortJobPenalty` equals the service's reported value.

```go
func TestSchedule_ShortJobPenaltyComesFromService(t *testing.T) {
	// Build the standard single-pool scheduling harness used by the other
	// TestSchedule_* cases (copy the setup from TestSchedule_PoolFailureIsolation),
	// but inject a service pre-populated with one short job for queue "A".
	now := time.Now()
	svc := NewShortJobPenaltyService(map[string]time.Duration{testfixtures.TestPool: time.Minute})

	shortJob := testfixtures.Test32Cpu256GiJob("A", testfixtures.PriorityClass2).
		WithNewRun("testExecutor", "test-node", "node", testfixtures.TestPool, 5).
		WithSucceeded(true)
	shortJob = shortJob.WithUpdatedRun(shortJob.LatestRun().WithRunningTime(ptr(now.Add(-10 * time.Second))))
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

	sctx := result.PoolResults[0].SchedulingResult.SchedulingContext
	qctx := sctx.QueueSchedulingContexts["A"]
	require.NotNil(t, qctx)
	assert.True(t, qctx.ShortJobPenalty.Equal(shortJob.AllResourceRequirements()))
}
```

(`ptr` is a local test helper for taking the address of a value; if the test file lacks one, use a `tmp := ...; &tmp` pattern matching the file's existing style.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/scheduler/scheduling/ -run TestSchedule_ShortJobPenaltyComesFromService -v`
Expected: FAIL - `Schedule` does not take a `now` argument / `NewFairSchedulingAlgo` type mismatch.

- [ ] **Step 3a: Change the struct field and constructor type**

In `internal/scheduler/scheduling/scheduling_algo.go`:

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

The returned struct literal (:681-688) is unchanged - it still references `shortJobPenaltyByQueue`.

- [ ] **Step 3e: Update the internal optimal-scheduler Schedule call**

At :864 there is `result, err := scheduler.Schedule(ctx)` - this is a *different* `Schedule` (the queue scheduler / optimal scheduler), NOT `FairSchedulingAlgo.Schedule`. Confirm by inspecting the receiver type at that line. Do NOT add `now` to it. (Verified during planning: the `FairSchedulingAlgo.Schedule` callers are scheduler.go:362 and the three algo tests.)

- [ ] **Step 3f: Update the algo test call sites**

In `internal/scheduler/scheduling/scheduling_algo_test.go`:
- The three `NewFairSchedulingAlgo(...)` calls (:55, :196, :922) currently pass `nil` for the penalty arg (verified at :64). `nil` is still valid for the `*ShortJobPenaltyService` typed parameter, so they compile unchanged - but for the cases that exercise penalty behavior, pass a real `NewShortJobPenaltyService(...)`. The skeleton cases can keep `nil`.
- The three `sch.Schedule(ctx, txn)` calls (:73, :216, :980) become `sch.Schedule(ctx, txn, time.Now())` (or a fixed test clock value where determinism matters).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/scheduler/scheduling/ -v`
Expected: PASS (existing `TestSchedule_*` plus the new `TestSchedule_ShortJobPenaltyComesFromService`).

- [ ] **Step 5: Commit**

```bash
git add internal/scheduler/scheduling/scheduling_algo.go internal/scheduler/scheduling/scheduling_algo_test.go
git commit -m "feat(scheduler): read short-job penalty from service; drop terminal-job scan"
```

---

## Task 4: Wire the leader-only feed + thread `now` into cycle; simplify syncState GC

**Files:**
- Modify: `internal/scheduler/scheduler.go`
  - `cycle` (:266) gains a `now time.Time` parameter; pass it to `schedulingAlgo.Schedule` (:362)
  - add the leader-only penalty feed in the leader block (after :324 `ReportStateTransitions`), iterating `jsts`
  - remove `SetNow` calls (:153, :172)
  - `Run` passes its pinned `start` into `cycle` (:211)
  - `syncState` (:426): drop the `fullJobGc` param, GC unconditionally, remove the `ShouldApplyPenalty` skip
  - the `syncState` call at :273 drops the `cycleNumber%10 == 0` arg; the initial call at :1263 drops its `false` arg
- Modify: `internal/scheduler/scheduler_test.go` - update `cycle(...)` calls (:1058, :1225, :1232, :1297, :3328)

- [ ] **Step 1: Write the failing test (terminal short job feeds the service via cycle, GC deletes it)**

Add to `internal/scheduler/scheduler_test.go` a test asserting that after a cycle in which a short job goes terminal: (a) the job is deleted from jobdb (GC no longer skips it), and (b) the penalty service has charged it. Use the existing scheduler test harness (`schedulingConfig`, `NewScheduler`, leader token). Model it on the existing cycle tests around :1058.

```go
func TestCycle_TerminalShortJobIsGcdAndPenaltyCharged(t *testing.T) {
	// Build a scheduler via the standard test harness with a real
	// ShortJobPenaltyService injected (cutoff 1m on the test pool).
	// Seed jobdb with a job that transitions to Succeeded this cycle, with a
	// run that started 10s ago on the test pool.
	// After sched.cycle(ctx, false, leaderToken, true, 1, now):
	//   - assert txn.GetById(jobId) == nil (deleted by unconditional GC)
	//   - assert sched.shortJobPenalty.GetPenaltiesForPool(pool, now)[queue] equals the job's requirements
	// Exact harness setup mirrors the existing TestScheduler_* cycle cases.
}
```

(Fill the body using the established harness in `scheduler_test.go`; the existing cycle tests show how to construct the scheduler, leader controller, and seed jobs. The two assertions above are the load-bearing checks.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/scheduler/ -run TestCycle_TerminalShortJobIsGcdAndPenaltyCharged -v`
Expected: FAIL - `cycle` signature mismatch (no `now` param) and/or service not fed.

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

At the scheduling call (:362), pass `now`:
```go
		result, err = s.schedulingAlgo.Schedule(ctx, txn, now)
```

- [ ] **Step 3b: Add the leader-only penalty feed**

The leader block begins after the `ValidateToken` gate (:281) returns leader. The existing `ReportStateTransitions` loop is at :324 inside `if s.metrics.LeaderMetricsEnabled()`. Add the feed immediately after that metrics block (still inside the leader path, unconditional on metrics-enabled), iterating `jsts` (NOT `updatedJobs`):

```go
	// Feed terminal short jobs into the penalty service. Leader-only (we are past
	// the ValidateToken gate) and every cycle (jsts is populated every cycle). We
	// iterate jsts, the genuinely-transitioned jobs - never updatedJobs, which is
	// txn.GetAll() on the updateAll failover path and would re-charge everything.
	// The JST terminal flags are only a pre-filter; ReportFinishedJob applies the
	// authoritative qualification gate.
	for _, jst := range jsts {
		if jst.Succeeded || jst.Failed || jst.Cancelled {
			s.shortJobPenalty.ReportFinishedJob(jst.Job, now)
		}
	}
```

Place this after the `ReportStateTransitions` metrics block (:322-325) and before `generateUpdateMessages` (:328), so it runs on every leader cycle regardless of `shouldSchedule`.

- [ ] **Step 3c: Make syncState GC unconditional**

`syncState` signature (:426), drop `fullJobGc`:
```go
func (s *Scheduler) syncState(ctx *armadacontext.Context, initial bool) ([]*jobdb.Job, []jobdb.JobStateTransitions, int64, int64, error) {
```

Replace the GC block (:487-510):
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
	// Delete jobs in a terminal state. Short-job-penalty state is now owned by the
	// ShortJobPenaltyService, so terminal jobs - including short ones - are deleted
	// on the normal cadence and never retained in jobdb.
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

- [ ] **Step 3d: Update syncState call sites**

At :273:
```go
	updatedJobs, jsts, newJobsSerial, newRunsSerial, err := s.syncState(ctx, false)
```
At :1263 (the initialise path):
```go
		if _, _, newJobsSerial, newRunsSerial, err := s.syncState(ctx, true); err != nil {
```
(Verify the surrounding code at :1263 - adapt the assignment to whatever the call expects; only the trailing `fullJobGc` arg is removed.)

- [ ] **Step 3e: Update cycle test call sites**

In `internal/scheduler/scheduler_test.go`, add the `now` argument to each `cycle(...)` call (:1058, :1225, :1232, :1297, :3328). Use the same clock value the test's scheduler uses (e.g. `sched.clock.Now()` or a fixed test time). Example for :1058:
```go
			err = sched.cycle(ctx, false, sched.leaderController.GetToken(), true, 1, sched.clock.Now())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/scheduler/ -run TestCycle_TerminalShortJobIsGcdAndPenaltyCharged -v`
Then: `go test ./internal/scheduler/ -run TestScheduler -v` (regression on the cycle harness)
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/scheduler/scheduler.go internal/scheduler/scheduler_test.go
git commit -m "feat(scheduler): feed penalty service from cycle; unconditional terminal-job GC"
```

---

## Task 5: Update schedulerapp wiring

**Files:**
- Modify: `internal/scheduler/schedulerapp.go:350` (construction) - both injection points (:361 into the algo, :407 into the scheduler) already pass the same `shortJobPenalty` variable, so only the constructor call changes.

- [ ] **Step 1: Change the construction**

In `internal/scheduler/schedulerapp.go`, line 350, change:
```go
	shortJobPenalty := scheduling.NewShortJobPenalty(config.Scheduling.GetShortJobPenaltyCutoffs())
```
to:
```go
	shortJobPenalty := scheduling.NewShortJobPenaltyService(config.Scheduling.GetShortJobPenaltyCutoffs())
```

The variable is still passed at :361 (`NewFairSchedulingAlgo` read side) and :407 (`NewScheduler` write side) unchanged - same instance shared, same topology as today.

- [ ] **Step 2: Verify the package builds**

Run: `go build ./internal/scheduler/...`
Expected: success, no compile errors. (If `NewScheduler`'s parameter is typed `*scheduling.ShortJobPenalty`, update that field/param in `scheduler.go` struct at :71 and constructor at :109 to `*scheduling.ShortJobPenaltyService` - verified during planning that scheduler.go:71 holds `shortJobPenalty *scheduling.ShortJobPenalty`.)

- [ ] **Step 3: Update the Scheduler struct + constructor type**

In `internal/scheduler/scheduler.go`:

Line 71:
```go
	shortJobPenalty *scheduling.ShortJobPenaltyService
```
Line 109 (`NewScheduler` param):
```go
	shortJobPenalty *scheduling.ShortJobPenaltyService,
```
(Assignment at :131 unchanged.)

- [ ] **Step 4: Build the whole scheduler module**

Run: `go build ./internal/scheduler/...`
Expected: success.

- [ ] **Step 5: Commit**

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
grep -rn "ShortJobPenalty\b\|NewShortJobPenalty\b\|\.SetNow(\|ShouldApplyPenalty" internal/ --include="*.go" | grep -v "ShortJobPenaltyService\|ShortJobPenaltyByQueue\|ShortJobPenaltyCutoff\|shortJobPenaltyByQueue\|qctx.ShortJobPenalty\|queueContext.ShortJobPenalty\|GetAllocationInclShortJobPenalty\|\.shortJobPenalty\b"
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

## Task 7: Benchmark for the 50K-short-job perf AC

**Files:**
- Create: `internal/scheduler/scheduling/short_job_penalty_service_bench_test.go`

The perf AC is "no regression in scheduler cycle time when a queue has 50K+ recent short jobs." The real win is structural (terminal short jobs leave jobdb, so the `calculateJobSchedulingInfo` loop and the dropped `GetAllTerminalJobs` fetch no longer traverse them). The service itself must be cheap at 50K live entries: report and get must stay sub-millisecond and allocation-light. This benchmark measures the service in isolation, which is the component this change introduces.

- [ ] **Step 1: Write the benchmark**

```go
package scheduling

import (
	"testing"
	"time"

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

func ptrTime(t time.Time) *time.Time { return &t }
```

Note: each of the 50K jobs above uses queue "q", so they all accumulate into one `(pool, "q")` sum - which is exactly the AC scenario ("a queue has 50K+ recent short jobs"). `ReportFinishedJob` dedups by `job.Id()`; confirm `Test32Cpu256GiJob` generates a unique job id per call (it does - ids are ULID-generated per construction). If it does not, vary the queue or assign ids explicitly so all 50K are distinct.

- [ ] **Step 2: Run the benchmark**

Run: `go test ./internal/scheduler/scheduling/ -bench BenchmarkShortJobPenaltyService -benchmem -run '^$'`
Expected: completes; record ns/op and allocs/op. `Get` with 50K live entries should be O(queues) for the copy (one queue here) plus the no-op `expireUpTo` peek - sub-microsecond aside from the map copy. Report is O(n log n) for 50K heap inserts. There is no hard pass/fail threshold; the artifact is the recorded numbers demonstrating the service is not a new bottleneck.

- [ ] **Step 3: Commit**

```bash
git add internal/scheduler/scheduling/short_job_penalty_service_bench_test.go
git commit -m "test(scheduler): benchmark ShortJobPenaltyService at 50k short jobs"
```

---

## Task 8: Full-suite verification

- [ ] **Step 1: Build everything**

Run: `go build ./...`
Expected: success.

- [ ] **Step 2: Run the affected package tests**

Postgres must be running (see CLAUDE.md - `armada-dev deps` / `armada-dev init` if not). Run with `TZ=UTC` per the project memory note about non-UTC hosts:

Run:
```bash
TZ=UTC go test -count=1 ./internal/scheduler/scheduling/... ./internal/scheduler/
```
Expected: PASS.

- [ ] **Step 3: Run the local CI equivalent**

Invoke the `armada-local-ci` skill (golangci-lint, go build, go mod tidy, focused tests on changed packages) to catch lint/format issues before pushing.

Expected: clean.

- [ ] **Step 4: Final commit (if CI made fixups)**

```bash
git add -A
git commit -m "chore(scheduler): lint/format fixups for short-job penalty service"
```

---

## Self-Review notes (verified during planning)

- **Spec coverage:**
  - "New service owns penalty state, wired into lifecycle path" -> Tasks 1, 2, 4.
  - "FairSchedulingAlgo no longer iterates jobs to compute penalty; queries service" -> Task 3 (per-job branch removed, `GetPenaltiesForPool` added; terminal-job fetch dropped).
  - "Terminal short jobs eligible for deletion on normal path" -> Task 4 (unconditional GC, `fullJobGc` removed).
  - "Penalty entries expire correctly without jobdb retention (tests)" -> Task 2 (`TestService_EntryExpiresExactlyAtDeadline`, `TestService_PostExpiryReReportNeverReQualifies`).
  - "Existing behavior preserved end-to-end" -> Task 1 ports every old qualification assertion; Task 3 asserts the service value reaches the queue context; `Cancelled` is in the tap set (parity for cancel-while-running).
  - "No regression at 50K+ short jobs (benchmark)" -> Task 7.
- **Type consistency:** `ShortJobPenaltyService`, `NewShortJobPenaltyService`, `ReportFinishedJob(job, now)`, `GetPenaltiesForPool(pool, now)`, `shouldApplyPenalty(job, now)`, `penaltyEntry`, `entryHeap` used consistently across all tasks. The struct field name `shortJobPenalty` is retained on both `Scheduler` and `FairSchedulingAlgo`; only its type changes.
- **`ResourceList.AllZero()` confirmed** (resource_list.go:108) - the prune check in `subtractFromSums`. `IsEmpty()` is explicitly NOT the right check for a summed value (it only tests `factory == nil`).
