package taskforge

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)

const (
	defaultPollInterval = 1 * time.Second
	defaultLockTTL      = 5 * time.Minute
	defaultBaseDelay    = 1 * time.Second
)

// WorkerPoolConfig configures a worker pool for a specific queue.
type WorkerPoolConfig struct {
	Queue       string
	Concurrency int
	PollInterval time.Duration
	LockTTL      time.Duration
	BaseDelay    time.Duration // base delay for exponential backoff
}

func (c *WorkerPoolConfig) applyDefaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.PollInterval == 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.LockTTL == 0 {
		c.LockTTL = defaultLockTTL
	}
	if c.BaseDelay == 0 {
		c.BaseDelay = defaultBaseDelay
	}
}

// WorkerPool manages a pool of goroutines that process jobs from a queue.
type WorkerPool struct {
	config   WorkerPoolConfig
	broker   *Broker
	registry *HandlerRegistry
	wg       sync.WaitGroup
	cancel   context.CancelFunc
	logger   *log.Logger
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(broker *Broker, registry *HandlerRegistry, config WorkerPoolConfig, logger *log.Logger) *WorkerPool {
	config.applyDefaults()
	if logger == nil {
		logger = log.Default()
	}
	return &WorkerPool{
		config:   config,
		broker:   broker,
		registry: registry,
		logger:   logger,
	}
}

// Start launches the worker goroutines. Call Stop() to shut down gracefully.
func (wp *WorkerPool) Start(ctx context.Context) {
	ctx, wp.cancel = context.WithCancel(ctx)

	for i := 0; i < wp.config.Concurrency; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}

	wp.logger.Printf("[taskforge] pool started: queue=%s workers=%d", wp.config.Queue, wp.config.Concurrency)
}

// Stop signals all workers to stop and waits for in-flight jobs to complete.
func (wp *WorkerPool) Stop() {
	wp.logger.Printf("[taskforge] pool stopping: queue=%s", wp.config.Queue)
	wp.cancel()
	wp.wg.Wait()
	wp.logger.Printf("[taskforge] pool stopped: queue=%s", wp.config.Queue)
}

func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			wp.poll(ctx, id)
		}
	}
}

func (wp *WorkerPool) poll(ctx context.Context, workerID int) {
	// Use a short timeout so we can check ctx.Done() frequently.
	job, err := wp.broker.Dequeue(ctx, wp.config.Queue, wp.config.PollInterval)
	if err != nil {
		// Context cancelled during blocking pop is expected during shutdown.
		if ctx.Err() != nil {
			return
		}
		wp.logger.Printf("[taskforge] worker %d dequeue error: %v", workerID, err)
		return
	}
	if job == nil {
		return // no job available, loop back
	}

	wp.process(ctx, workerID, job)
}

func (wp *WorkerPool) process(ctx context.Context, workerID int, job *Job) {
	// Acquire distributed lock to prevent double processing.
	acquired, err := wp.broker.AcquireLock(ctx, job.ID, wp.config.LockTTL)
	if err != nil {
		wp.logger.Printf("[taskforge] worker %d lock error for job %s: %v", workerID, job.ID, err)
		return
	}
	if !acquired {
		wp.logger.Printf("[taskforge] worker %d skipped locked job %s", workerID, job.ID)
		return
	}
	defer wp.broker.ReleaseLock(ctx, job.ID)

	// Mark as running. Use background context so this persists even during shutdown.
	job.Status = StatusRunning
	if err := wp.broker.UpdateJob(context.Background(), job); err != nil {
		wp.logger.Printf("[taskforge] worker %d update error for job %s: %v", workerID, job.ID, err)
		return
	}

	// Find handler.
	handler := wp.registry.Get(job.Type)
	if handler == nil {
		job.Status = StatusFailed
		job.Error = fmt.Sprintf("no handler registered for job type: %s", job.Type)
		wp.broker.UpdateJob(ctx, job)
		wp.broker.SendToDLQ(ctx, job)
		wp.logger.Printf("[taskforge] worker %d no handler for job %s type=%s, sent to DLQ", workerID, job.ID, job.Type)
		return
	}

	// Execute handler.
	if err := handler(ctx, job); err != nil {
		wp.handleFailure(ctx, workerID, job, err)
		return
	}

	// Use background context for post-handler updates so they complete even during shutdown.
	cleanupCtx := context.Background()

	// Success.
	job.Status = StatusSuccess
	job.Error = ""
	if err := wp.broker.UpdateJob(cleanupCtx, job); err != nil {
		wp.logger.Printf("[taskforge] worker %d update error for job %s: %v", workerID, job.ID, err)
		return
	}

	wp.broker.client.Incr(cleanupCtx, wp.broker.metricsKey(job.Queue, "processed"))

	wp.logger.Printf("[taskforge] worker %d completed job %s", workerID, job.ID)
}

func (wp *WorkerPool) handleFailure(ctx context.Context, workerID int, job *Job, jobErr error) {
	job.Retry++
	job.Error = jobErr.Error()

	// Use background context for cleanup operations so they complete even during shutdown.
	cleanupCtx := context.Background()

	wp.broker.client.Incr(cleanupCtx, wp.broker.metricsKey(job.Queue, "failed"))

	if job.Retry >= job.MaxRetry {
		// Exhausted retries — send to DLQ.
		wp.broker.SendToDLQ(cleanupCtx, job)
		wp.logger.Printf("[taskforge] worker %d job %s exhausted retries (%d/%d), sent to DLQ: %v",
			workerID, job.ID, job.Retry, job.MaxRetry, jobErr)
		return
	}

	// Schedule retry with exponential backoff.
	delay := wp.backoffDelay(job.Retry)
	wp.logger.Printf("[taskforge] worker %d job %s failed (attempt %d/%d), retrying in %s: %v",
		workerID, job.ID, job.Retry, job.MaxRetry, delay, jobErr)

	time.Sleep(delay)
	if err := wp.broker.RequeueForRetry(cleanupCtx, job); err != nil {
		wp.logger.Printf("[taskforge] worker %d requeue error for job %s: %v", workerID, job.ID, err)
	}
}

// backoffDelay calculates exponential backoff: baseDelay * 2^(attempt-1)
// Capped at 5 minutes.
func (wp *WorkerPool) backoffDelay(attempt int) time.Duration {
	delay := wp.config.BaseDelay * time.Duration(math.Pow(2, float64(attempt-1)))
	maxDelay := 5 * time.Minute
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}
