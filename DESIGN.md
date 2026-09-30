# Design Decisions

This document explains the engineering tradeoffs behind taskforge's architecture. It's written for anyone reviewing this project — whether you're evaluating the code, learning from it, or considering contributing.

## Why BRPOP Instead of Polling or Pub/Sub?

There are three common patterns for consuming messages from Redis:

1. **Polling (RPOP in a loop)** — Simple, but wastes CPU and Redis connections when the queue is empty. You'd need to add `time.Sleep` between polls, which introduces latency.

2. **Pub/Sub (SUBSCRIBE)** — Real-time, but messages are fire-and-forget. If a worker is down when a message is published, that job is lost forever. No persistence, no replay.

3. **BRPOP (blocking pop)** — Blocks the connection until a message arrives or a timeout is reached. Zero CPU waste while idle, yet responds instantly when a job lands. The message is atomically removed from the list, so only one consumer gets it.

taskforge uses **BRPOP** because:
- Jobs must not be lost (rules out Pub/Sub)
- Workers should respond immediately to new jobs (rules out naive polling)
- The blocking nature naturally provides backpressure — workers only pull what they can handle

The `PollInterval` config controls the BRPOP timeout, which determines how often workers check for shutdown signals.

## Why SETNX for Distributed Locking?

In a multi-worker setup, there's a race condition: two workers could pop different copies of the same job (e.g., after a retry requeue) and process it simultaneously. We need a distributed lock.

Options considered:

1. **Redlock (multi-node)** — The gold standard for distributed locking across Redis clusters. Overkill for a library targeting single-node Redis, and adds significant complexity.

2. **SETNX with TTL** — Simple, atomic, and good enough for single-node Redis. `SET key value NX EX ttl` either acquires the lock or fails, in one round trip.

3. **Lua script** — More flexible but harder to read and debug. The extra power isn't needed here.

taskforge uses **SETNX with TTL** because:
- One Redis command = atomic, no race conditions
- TTL prevents deadlocks if a worker crashes while holding a lock
- Simple to understand and debug
- Sufficient for the single-node Redis target

The lock TTL defaults to 5 minutes (`LockTTL` config). If your jobs run longer than that, increase it — otherwise the lock expires and another worker might pick up the same job.

## Why Exponential Backoff with a Cap?

When a job fails, retrying immediately is usually wrong — the downstream service is likely still down. But waiting too long wastes time if it was a transient glitch.

**Exponential backoff** (`delay = baseDelay × 2^attempt`) strikes the right balance:
- First retry: 1s (fast recovery for transient errors)
- Second retry: 2s
- Third retry: 4s
- And so on...

The **5-minute cap** prevents absurd delays. Without it, attempt 10 would wait ~17 minutes, which is almost never what you want for a background job.

```
Attempt 1: 1s
Attempt 2: 2s
Attempt 3: 4s
Attempt 4: 8s
Attempt 5: 16s
...
Attempt N: min(baseDelay × 2^(N-1), 5m)
```

The `BaseDelay` is configurable per pool, so latency-sensitive queues can retry faster.

## Why a Dead-Letter Queue?

After max retries are exhausted, you have two choices:
1. **Drop the job** — Simple, but you lose data. You'll never know what failed or why.
2. **Move to a DLQ** — Preserves the job for inspection, debugging, and manual retry.

taskforge uses **DLQ** because in production, you *always* want to know what failed. The DLQ is a separate Redis list (`dlq:<queue>`) that stores job IDs. You can:
- List dead jobs: `client.ListDLQ(ctx, "email")`
- Retry one: `client.RetryDLQ(ctx, "email", jobID)`
- Retry all: `client.RetryAllDLQ(ctx, "email")`

This mirrors how Sidekiq's "Dead" tab works — and it's a feature ops teams immediately ask for.

## Why Sorted Sets for Scheduled Jobs?

Scheduled jobs need to be stored with a "process at" timestamp and efficiently queried for "what's due now?"

Options:
1. **Regular list + polling** — Store scheduled time in the job payload, pop from list, check if due, re-push if not. Terrible O(n) on every poll.
2. **Redis sorted set (ZSET)** — Score = Unix timestamp. `ZRANGEBYSCORE -inf <now>` returns all due jobs in O(log(n) + k). Perfect fit.

The scheduler goroutine runs alongside workers, checking every `PollInterval` for due jobs and promoting them to the main queue with `LPUSH`.

## Why context.Background() for Cleanup?

You'll notice that post-handler operations (status updates, metrics, DLQ writes) use `context.Background()` instead of the worker's context:

```go
cleanupCtx := context.Background()
wp.broker.UpdateJob(cleanupCtx, job)
```

This is intentional. During graceful shutdown, the worker context is cancelled. But if a job just finished successfully, we *must* persist that result — otherwise the job appears stuck in "running" forever. Using `context.Background()` ensures cleanup completes even when the pool is shutting down.

The tradeoff: cleanup operations won't respect shutdown cancellation. In practice, these are single Redis commands that complete in milliseconds, so the delay is negligible.

## Why Fixed Worker Pool Instead of Dynamic Scaling?

Dynamic auto-scaling (spawn more goroutines when queue is deep, scale down when idle) sounds appealing but adds significant complexity:
- How do you measure "queue depth" without polling overhead?
- What's the max? Without a cap you risk OOM.
- How do you gracefully drain goroutines during scale-down?

A **fixed pool** with configurable concurrency is simpler, more predictable, and easier to reason about in production. If you need 10 workers for email and 5 for images, just configure it:

```go
client.StartWorkers(ctx, WorkerPoolConfig{Queue: "email", Concurrency: 10})
client.StartWorkers(ctx, WorkerPoolConfig{Queue: "image", Concurrency: 5})
```

This matches how Sidekiq works (fixed thread count) and is the right default for most workloads.

## Job Timeout: Context Deadline

Job timeout wraps the handler in a `context.WithTimeout`. When the deadline is exceeded, the context is cancelled and well-behaved handlers return `ctx.Err()`. This:
- Prevents stuck workers from blocking the pool forever
- Gives handlers a clean signal to abort (via `ctx.Done()`)
- Treats timeout as a failure, triggering normal retry/DLQ logic

Handlers that ignore context cancellation will still run to completion — Go can't forcefully kill a goroutine. But the job will be marked as failed regardless, and the worker moves on.

## What This Library Doesn't Do (and Why)

| Feature | Why Not |
|---|---|
| Job priorities | Adds sorted set complexity; named queues achieve similar result |
| Cron/recurring jobs | Different problem domain; better served by a scheduler layer on top |
| Multi-node Redis (Cluster) | Targets single-node for simplicity; Redlock would be needed for cluster |
| Web dashboard | This is a library, not a service; users can build their own UI on the metrics API |
| Persistence beyond Redis | Redis is the single source of truth; adding DB would double the complexity |
