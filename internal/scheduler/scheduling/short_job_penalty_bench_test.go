package scheduling

import (
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func BenchmarkShortJobPenalty_50kReports(b *testing.B) {
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
		svc := NewShortJobPenalty(map[string]time.Duration{testfixtures.TestPool: time.Minute})
		svc.SetNow(now)
		for _, j := range jobs {
			svc.ReportFinishedJob(j)
		}
		_ = svc.GetPenaltiesForPool(testfixtures.TestPool)
	}
}

func BenchmarkShortJobPenalty_GetWith50kLiveEntries(b *testing.B) {
	now := time.Now()
	svc := NewShortJobPenalty(map[string]time.Duration{testfixtures.TestPool: time.Minute})
	svc.SetNow(now)
	for i := 0; i < 50000; i++ {
		j := testfixtures.Test32Cpu256GiJob("q", testfixtures.PriorityClass2).
			WithNewRun("testExecutor", "test-node", "node", testfixtures.TestPool, 5).
			WithSucceeded(true)
		j = j.WithUpdatedRun(j.LatestRun().WithRunningTime(ptrTime(now.Add(-10 * time.Second))))
		svc.ReportFinishedJob(j)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = svc.GetPenaltiesForPool(testfixtures.TestPool)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
