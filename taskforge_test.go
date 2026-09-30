package taskforge_test

import (
	"context"
	"errors"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/radityajayantara/taskforge"
	"github.com/redis/go-redis/v9"
)

func setupClient(t *testing.T) (*taskforge.Client, context.Context) {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()

	// Verify Redis is reachable.
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available at %s: %v", addr, err)
	}

	client := taskforge.NewClient(rdb, taskforge.WithLogger(log.New(os.Stderr, "[test] ", log.LstdFlags)))

	// Clean up taskforge keys before each test.
	client.Broker().Flush(ctx)

	t.Cleanup(func() {
		client.StopAll()
		client.Broker().Flush(ctx)
		rdb.Close()
	})

	return client, ctx
}

func TestEnqueueAndProcess(t *testing.T) {
	client, ctx := setupClient(t)

	var processed atomic.Int32

	client.Register("test_job", func(ctx context.Context, job *taskforge.Job) error {
		processed.Add(1)
		return nil
	})

	// Enqueue 5 jobs.
	for i := 0; i < 5; i++ {
		_, err := client.Enqueue(ctx, "test", "test_job", map[string]interface{}{"i": i}, 3)
		if err != nil {
			t.Fatalf("enqueue error: %v", err)
		}
	}

	// Start workers.
	client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
		Queue:        "test",
		Concurrency:  2,
		PollInterval: 100 * time.Millisecond,
	})

	// Wait for processing.
	deadline := time.After(5 * time.Second)
	for {
		if processed.Load() == 5 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout: only processed %d/5 jobs", processed.Load())
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestJobStatusTracking(t *testing.T) {
	client, ctx := setupClient(t)

	done := make(chan struct{})
	client.Register("track_job", func(ctx context.Context, job *taskforge.Job) error {
		defer close(done)
		return nil
	})

	job, err := client.Enqueue(ctx, "test", "track_job", map[string]interface{}{}, 1)
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Check pending status.
	fetched, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get job error: %v", err)
	}
	if fetched.Status != taskforge.StatusPending {
		t.Errorf("expected status pending, got %s", fetched.Status)
	}

	// Start worker and wait for completion.
	client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
		Queue:        "test",
		Concurrency:  1,
		PollInterval: 100 * time.Millisecond,
	})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for job")
	}

	// Small delay for status update to persist.
	time.Sleep(100 * time.Millisecond)

	fetched, err = client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get job error: %v", err)
	}
	if fetched.Status != taskforge.StatusSuccess {
		t.Errorf("expected status success, got %s", fetched.Status)
	}
}

func TestRetryAndDLQ(t *testing.T) {
	client, ctx := setupClient(t)

	var attempts atomic.Int32

	client.Register("fail_job", func(ctx context.Context, job *taskforge.Job) error {
		attempts.Add(1)
		return errors.New("intentional failure")
	})

	job, err := client.Enqueue(ctx, "test", "fail_job", map[string]interface{}{}, 3)
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
		Queue:        "test",
		Concurrency:  1,
		PollInterval: 100 * time.Millisecond,
		BaseDelay:    50 * time.Millisecond, // fast backoff for tests
	})

	// Wait for all retries to exhaust.
	deadline := time.After(10 * time.Second)
	for {
		if attempts.Load() >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout: only %d attempts", attempts.Load())
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Small delay for DLQ write.
	time.Sleep(500 * time.Millisecond)

	// Job should be in DLQ.
	dlq, err := client.ListDLQ(ctx, "test")
	if err != nil {
		t.Fatalf("list dlq error: %v", err)
	}
	if len(dlq) != 1 || dlq[0] != job.ID {
		t.Errorf("expected job %s in DLQ, got %v", job.ID, dlq)
	}

	// Check job status is dead.
	fetched, _ := client.GetJob(ctx, job.ID)
	if fetched.Status != taskforge.StatusDead {
		t.Errorf("expected status dead, got %s", fetched.Status)
	}
}

func TestMultipleQueues(t *testing.T) {
	client, ctx := setupClient(t)

	var emailCount, imageCount atomic.Int32

	client.Register("send_email", func(ctx context.Context, job *taskforge.Job) error {
		emailCount.Add(1)
		return nil
	})
	client.Register("resize_image", func(ctx context.Context, job *taskforge.Job) error {
		imageCount.Add(1)
		return nil
	})

	// Enqueue to different queues.
	for i := 0; i < 3; i++ {
		client.Enqueue(ctx, "email", "send_email", map[string]interface{}{}, 1)
	}
	for i := 0; i < 2; i++ {
		client.Enqueue(ctx, "image", "resize_image", map[string]interface{}{}, 1)
	}

	// Start separate pools.
	client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
		Queue: "email", Concurrency: 2, PollInterval: 100 * time.Millisecond,
	})
	client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
		Queue: "image", Concurrency: 1, PollInterval: 100 * time.Millisecond,
	})

	deadline := time.After(5 * time.Second)
	for {
		if emailCount.Load() == 3 && imageCount.Load() == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout: email=%d/3 image=%d/2", emailCount.Load(), imageCount.Load())
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestMetrics(t *testing.T) {
	client, ctx := setupClient(t)

	client.Register("metric_job", func(ctx context.Context, job *taskforge.Job) error {
		return nil
	})

	for i := 0; i < 3; i++ {
		client.Enqueue(ctx, "test", "metric_job", map[string]interface{}{}, 1)
	}

	client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
		Queue: "test", Concurrency: 2, PollInterval: 100 * time.Millisecond,
	})

	time.Sleep(2 * time.Second)

	metrics, err := client.GetMetrics(ctx, "test")
	if err != nil {
		t.Fatalf("get metrics error: %v", err)
	}

	if metrics["enqueued"] != 3 {
		t.Errorf("expected enqueued=3, got %d", metrics["enqueued"])
	}
	if metrics["processed"] != 3 {
		t.Errorf("expected processed=3, got %d", metrics["processed"])
	}
}

func TestGracefulShutdown(t *testing.T) {
	client, ctx := setupClient(t)

	started := make(chan struct{})
	var completed atomic.Bool

	client.Register("slow_job", func(ctx context.Context, job *taskforge.Job) error {
		close(started)
		time.Sleep(1 * time.Second) // simulate slow work
		completed.Store(true)
		return nil
	})

	client.Enqueue(ctx, "test", "slow_job", map[string]interface{}{}, 1)

	client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
		Queue: "test", Concurrency: 1, PollInterval: 100 * time.Millisecond,
	})

	// Wait for job to start processing.
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for job to start")
	}

	// Stop should wait for the in-flight job to finish.
	client.StopAll()

	if !completed.Load() {
		t.Error("expected in-flight job to complete before shutdown")
	}
}

func TestScheduledJob(t *testing.T) {
	client, ctx := setupClient(t)

	var processed atomic.Int32

	client.Register("scheduled_job", func(ctx context.Context, job *taskforge.Job) error {
		processed.Add(1)
		return nil
	})

	// Schedule a job to be processed in 1 second.
	_, err := client.EnqueueIn(ctx, "test", "scheduled_job", map[string]interface{}{"key": "val"}, 1, 1*time.Second)
	if err != nil {
		t.Fatalf("enqueue in error: %v", err)
	}

	// Verify it's in the scheduled set, not the queue.
	metrics, _ := client.GetMetrics(ctx, "test")
	if metrics["scheduled"] != 1 {
		t.Errorf("expected 1 scheduled, got %d", metrics["scheduled"])
	}
	if metrics["pending"] != 0 {
		t.Errorf("expected 0 pending, got %d", metrics["pending"])
	}

	// Start workers — the scheduler goroutine will promote the job when it's due.
	client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
		Queue:        "test",
		Concurrency:  1,
		PollInterval: 500 * time.Millisecond,
	})

	// Wait for the scheduled job to be promoted and processed.
	deadline := time.After(10 * time.Second)
	for {
		if processed.Load() == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout: job not processed, processed=%d", processed.Load())
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func TestRetryDLQ(t *testing.T) {
	client, ctx := setupClient(t)

	var attempts atomic.Int32

	client.Register("retry_dlq_job", func(ctx context.Context, job *taskforge.Job) error {
		count := attempts.Add(1)
		if count <= 1 {
			return errors.New("fail first time")
		}
		return nil // succeed on retry from DLQ
	})

	// Enqueue with max 1 retry — will fail and go to DLQ immediately.
	job, _ := client.Enqueue(ctx, "test", "retry_dlq_job", map[string]interface{}{}, 1)

	client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
		Queue:        "test",
		Concurrency:  1,
		PollInterval: 500 * time.Millisecond,
		BaseDelay:    50 * time.Millisecond,
	})

	// Wait for job to hit DLQ.
	deadline := time.After(5 * time.Second)
	for {
		dlq, _ := client.ListDLQ(ctx, "test")
		if len(dlq) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for job to reach DLQ")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Retry from DLQ.
	err := client.RetryDLQ(ctx, "test", job.ID)
	if err != nil {
		t.Fatalf("retry dlq error: %v", err)
	}

	// DLQ should be empty now.
	dlq, _ := client.ListDLQ(ctx, "test")
	if len(dlq) != 0 {
		t.Errorf("expected empty DLQ, got %v", dlq)
	}

	// Wait for job to be processed successfully.
	deadline = time.After(5 * time.Second)
	for {
		fetched, _ := client.GetJob(ctx, job.ID)
		if fetched.Status == taskforge.StatusSuccess {
			break
		}
		select {
		case <-deadline:
			fetched, _ := client.GetJob(ctx, job.ID)
			t.Fatalf("timeout: job status=%s, attempts=%d", fetched.Status, attempts.Load())
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}
