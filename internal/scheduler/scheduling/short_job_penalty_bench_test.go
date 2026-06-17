package scheduling

import (
	"strconv"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func benchJobs(n int, queues int, runningTime time.Time) []*jobdb.Job {
	jobs := make([]*jobdb.Job, n)
	for i := range jobs {
		q := "q" + strconv.Itoa(i%queues)
		jobs[i] = testJobForQueue(q, runningTime).WithSucceeded(true)
	}
	return jobs
}

// Worst case: N short jobs share one deadline, then all expire in a single
// GetPenaltiesForPool call (one scheduling cycle crosses the deadline).
func BenchmarkExpireThunderingHerd(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000, 500_000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			start := time.Now()
			jobs := benchJobs(n, 100, start)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				sut := makeSut()
				sut.SetNow(start.Add(time.Second))
				for _, j := range jobs {
					sut.ReportFinishedJob(j)
				}
				sut.SetNow(start.Add(2 * time.Minute))
				b.StartTimer()
				sut.GetPenaltiesForPool(testfixtures.TestPool)
			}
		})
	}
}

// Steady state: per-cycle Get with nothing expiring (the common case).
func BenchmarkGetNothingExpired(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			start := time.Now()
			sut := makeSut()
			sut.SetNow(start.Add(time.Second))
			for _, j := range benchJobs(n, 100, start) {
				sut.ReportFinishedJob(j)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sut.GetPenaltiesForPool(testfixtures.TestPool)
			}
		})
	}
}
