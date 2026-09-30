package taskforge

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Broker handles all Redis operations for the task queue.
type Broker struct {
	client *redis.Client
	prefix string
}

// NewBroker creates a new Redis-backed broker.
func NewBroker(client *redis.Client) *Broker {
	return &Broker{
		client: client,
		prefix: "taskforge",
	}
}

// redis key helpers

func (b *Broker) queueKey(queue string) string {
	return fmt.Sprintf("%s:queue:%s", b.prefix, queue)
}

func (b *Broker) jobKey(jobID string) string {
	return fmt.Sprintf("%s:job:%s", b.prefix, jobID)
}

func (b *Broker) dlqKey(queue string) string {
	return fmt.Sprintf("%s:dlq:%s", b.prefix, queue)
}

func (b *Broker) lockKey(jobID string) string {
	return fmt.Sprintf("%s:lock:%s", b.prefix, jobID)
}

func (b *Broker) metricsKey(queue string, field string) string {
	return fmt.Sprintf("%s:metrics:%s:%s", b.prefix, queue, field)
}

// Enqueue adds a job to the queue.
func (b *Broker) Enqueue(ctx context.Context, job *Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("taskforge: marshal job: %w", err)
	}

	pipe := b.client.Pipeline()
	pipe.Set(ctx, b.jobKey(job.ID), data, 0)
	pipe.LPush(ctx, b.queueKey(job.Queue), job.ID)
	pipe.Incr(ctx, b.metricsKey(job.Queue, "enqueued"))
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("taskforge: enqueue: %w", err)
	}
	return nil
}

// Dequeue retrieves the next job from the queue.
// Blocks up to timeout. Returns nil if no job is available.
func (b *Broker) Dequeue(ctx context.Context, queue string, timeout time.Duration) (*Job, error) {
	result, err := b.client.BRPop(ctx, timeout, b.queueKey(queue)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("taskforge: dequeue: %w", err)
	}

	jobID := result[1]
	return b.GetJob(ctx, jobID)
}

// GetJob fetches a job by ID.
func (b *Broker) GetJob(ctx context.Context, jobID string) (*Job, error) {
	data, err := b.client.Get(ctx, b.jobKey(jobID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("taskforge: job %s not found", jobID)
		}
		return nil, fmt.Errorf("taskforge: get job: %w", err)
	}

	var job Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("taskforge: unmarshal job: %w", err)
	}
	return &job, nil
}

// UpdateJob persists the current state of a job.
func (b *Broker) UpdateJob(ctx context.Context, job *Job) error {
	job.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("taskforge: marshal job: %w", err)
	}
	return b.client.Set(ctx, b.jobKey(job.ID), data, 0).Err()
}

// SendToDLQ moves a job to the dead-letter queue.
func (b *Broker) SendToDLQ(ctx context.Context, job *Job) error {
	job.Status = StatusDead
	if err := b.UpdateJob(ctx, job); err != nil {
		return err
	}

	pipe := b.client.Pipeline()
	pipe.LPush(ctx, b.dlqKey(job.Queue), job.ID)
	pipe.Incr(ctx, b.metricsKey(job.Queue, "dead"))
	_, err := pipe.Exec(ctx)
	return err
}

// RequeueForRetry puts a job back on the queue for retry.
func (b *Broker) RequeueForRetry(ctx context.Context, job *Job) error {
	job.Status = StatusPending
	if err := b.UpdateJob(ctx, job); err != nil {
		return err
	}
	return b.client.LPush(ctx, b.queueKey(job.Queue), job.ID).Err()
}

// AcquireLock tries to acquire a distributed lock for a job.
// Returns true if the lock was acquired.
func (b *Broker) AcquireLock(ctx context.Context, jobID string, ttl time.Duration) (bool, error) {
	ok, err := b.client.SetNX(ctx, b.lockKey(jobID), "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("taskforge: acquire lock: %w", err)
	}
	return ok, nil
}

// ReleaseLock releases the distributed lock for a job.
func (b *Broker) ReleaseLock(ctx context.Context, jobID string) error {
	return b.client.Del(ctx, b.lockKey(jobID)).Err()
}

// ListDLQ returns all job IDs in the dead-letter queue for a given queue.
func (b *Broker) ListDLQ(ctx context.Context, queue string) ([]string, error) {
	return b.client.LRange(ctx, b.dlqKey(queue), 0, -1).Result()
}

// RetryDLQ moves a job from the dead-letter queue back to the main queue.
// Resets the job's retry counter and status to pending.
func (b *Broker) RetryDLQ(ctx context.Context, queue, jobID string) error {
	// Remove from DLQ.
	removed, err := b.client.LRem(ctx, b.dlqKey(queue), 1, jobID).Result()
	if err != nil {
		return fmt.Errorf("taskforge: retry dlq remove: %w", err)
	}
	if removed == 0 {
		return fmt.Errorf("taskforge: job %s not found in DLQ for queue %s", jobID, queue)
	}

	// Reset job state.
	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	job.Retry = 0
	job.Status = StatusPending
	job.Error = ""
	if err := b.UpdateJob(ctx, job); err != nil {
		return err
	}

	// Re-enqueue.
	return b.client.LPush(ctx, b.queueKey(queue), jobID).Err()
}

// RetryAllDLQ moves all jobs from the dead-letter queue back to the main queue.
// Returns the number of jobs retried.
func (b *Broker) RetryAllDLQ(ctx context.Context, queue string) (int64, error) {
	jobIDs, err := b.ListDLQ(ctx, queue)
	if err != nil {
		return 0, err
	}

	var count int64
	for _, jobID := range jobIDs {
		if err := b.RetryDLQ(ctx, queue, jobID); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// GetMetrics returns metrics for a queue.
func (b *Broker) GetMetrics(ctx context.Context, queue string) (map[string]int64, error) {
	metrics := make(map[string]int64)

	fields := []string{"enqueued", "processed", "failed", "dead"}
	for _, field := range fields {
		val, err := b.client.Get(ctx, b.metricsKey(queue, field)).Int64()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		metrics[field] = val
	}

	// pending = queue length
	qLen, err := b.client.LLen(ctx, b.queueKey(queue)).Result()
	if err != nil {
		return nil, err
	}
	metrics["pending"] = qLen

	// dlq length
	dlqLen, err := b.client.LLen(ctx, b.dlqKey(queue)).Result()
	if err != nil {
		return nil, err
	}
	metrics["dlq_size"] = dlqLen

	// scheduled count
	schedLen, err := b.ScheduledCount(ctx, queue)
	if err != nil {
		return nil, err
	}
	metrics["scheduled"] = schedLen

	return metrics, nil
}

// Flush removes all taskforge keys from Redis. Useful for testing.
func (b *Broker) Flush(ctx context.Context) error {
	iter := b.client.Scan(ctx, 0, b.prefix+":*", 100).Iterator()
	for iter.Next(ctx) {
		b.client.Del(ctx, iter.Val())
	}
	return iter.Err()
}
