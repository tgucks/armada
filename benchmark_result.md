The benchmark deliberately stacks the deck in the worst possible way: every job shares **one** deadline, so a single `GetPenaltiesForPool` call expires the entire set at once.

| Scenario                                        | N       | Time per `Get` |
| ----------------------------------------------- | ------- | -------------- |
| **Thundering herd** (all N expire in one cycle) | 1,000   | 0.12 ms        |
|                                                 | 10,000  | 0.85 ms        |
|                                                 | 100,000 | 9.8 ms         |
|                                                 | 500,000 | 73 ms          |
| **Steady state** (nothing expired)              | 10,000  | 2.7 µs         |
|                                                 | 100,000 | 2.8 µs         |

**1. The steady-state cost is independent of N, and it's ~3 µs.**

This is the number that actually matters per cycle. When nothing has crossed its deadline, `expireUpTo` does a single `peek()` + one `time.Before` comparison and bails. The cost is then purely the `O(Q)` snapshot copy (Q = queues with active penalties), which is why 10K and 100K live entries both clock ~2.8 µs. The size of the backlog doesn't touch the loop. That flat line is the whole point of the heap + incrementally-maintained sums: **you never re-scan or re-sum live entries.**

**2. `expireUpTo` touches `E`, not `N`.**

The cost is proportional to entries *crossing their deadline this cycle* (`E`), not entries alive (`N`). In steady state `E ≈ short-job completion rate × cycle period`. At ~100 ns/entry (measured: 73 ms / 500K), you'd need ~50,000 jobs to hit their deadline *in a single cycle* to add even 5 ms. To sustain that you'd need a completion rate of 50K short-jobs-per-cycle, every cycle - at which point the NodeDb scheduling work for those same jobs dominates the loop by orders of magnitude.

**3. The "all at once" submission burst does *not* produce an "all at once" expiry.**

This is the key rebuttal to the worry. The deadline is `runStart + cutoff` - anchored to when each job **started running**, not when it was submitted. Armada starts a burst of thousands of gangs/jobs *across many cycles* as capacity frees up, so their start times - and therefore their deadlines - are naturally spread out. The single-deadline herd in row 4 of the table is physically unreachable unless 500K jobs start running in the same instant and all finish short. Real bursts amortize expiry across the cutoff window into the µs-to-low-ms range.

**4. Even the unreachable worst case is bounded and one-time.**

500K simultaneous expirations = 73 ms, once, under the lock - and those entries are then gone. It's `O(E log N)`, and it's amortized-optimal: each finished short job is pushed once and popped once over its entire life. You cannot account for a finished short job without touching it at least once; the heap does it in `O(log N)`. No design avoids `Ω(E)` here.

**5. If it ever *did* get hot, the fix is allocation, not algorithm.**

Note the `1 alloc/op` per expired entry - that's `ResourceList.Subtract` returning a fresh `[]int64`. The 73 ms is GC pressure, not heap traversal. An in-place subtract or a pooled buffer would cut it substantially. But there's no reason to do that work now: the steady-state path doesn't allocate per entry, and the burst path is already a non-issue.

**Bottom line for a reviewer:** the per-cycle addition is ~3 µs in normal operation and stays sub-millisecond for any realistic per-cycle expiry volume. The scary linear number only appears under a synthetic single-deadline herd that Armada's `runStart`-anchored deadlines make physically impossible. This is dwarfed by the scheduling loop's actual NodeDb / preemption work.

I left the benchmark at `internal/scheduler/scheduling/short_job_penalty_bench_test.go` - it's a useful regression guard and documents exactly this concern. Want me to keep it (and maybe trim it to the two most illustrative cases), or remove it?
