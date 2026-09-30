package taskforge

import (
	"context"
	"fmt"
	"time"
)

// scheduledKey returns the Redis key for the scheduled job sorted set.
func (b *Broker) scheduledKey(queue string) string {
	return fmt.Sprintf("%s:scheduled:%s", b.prefix, queue)
}

// EnqueueAt schedules a job to be processed at a specific time.
// The job will be moved to the queue when its scheduled time arrives.
func (b *Broker) EnqueueAt(ctx context.Context, job *Job, processAt time.Time) error {
	data, err := marshalJob(job)
	if err != nil {
		return err
	}

	pipe := b.client.Pipeline()
	pipe.Set(ctx, b.jobKey(job.ID), data, 0)
	pipe.ZAdd(ctx, b.scheduledKey(job.Queue), redisZ(job.ID, float64(processAt.Unix())))
	pipe.Incr(ctx, b.metricsKey(job.Queue, "enqueued"))
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("taskforge: enqueue scheduled: %w", err)
	}
	return nil
}

// EnqueueIn schedules a job to be processed after a delay.
func (b *Broker) EnqueueIn(ctx context.Context, job *Job, delay time.Duration) error {
	return b.EnqueueAt(ctx, job, time.Now().Add(delay))
}

// PromoteScheduled moves scheduled jobs that are due into the queue.
// Returns the number of jobs promoted.
func (b *Broker) PromoteScheduled(ctx context.Context, queue string) (int64, error) {
	now := float64(time.Now().Unix())
	key := b.scheduledKey(queue)

	// Get all jobs with score <= now (i.e., due or overdue).
	jobIDs, err := b.client.ZRangeByScore(ctx, key, rangeByScore("-inf", fmt.Sprintf("%f", now))).Result()
	if err != nil {
		return 0, fmt.Errorf("taskforge: promote scheduled: %w", err)
	}
	if len(jobIDs) == 0 {
		return 0, nil
	}

	pipe := b.client.Pipeline()
	for _, jobID := range jobIDs {
		pipe.LPush(ctx, b.queueKey(queue), jobID)
		pipe.ZRem(ctx, key, jobID)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("taskforge: promote scheduled: %w", err)
	}

	return int64(len(jobIDs)), nil
}

// ScheduledCount returns the number of scheduled (not yet due) jobs for a queue.
func (b *Broker) ScheduledCount(ctx context.Context, queue string) (int64, error) {
	return b.client.ZCard(ctx, b.scheduledKey(queue)).Result()
}
