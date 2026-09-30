# taskforge

[![CI](https://github.com/radityajayantara/taskforge/actions/workflows/ci.yml/badge.svg)](https://github.com/radityajayantara/taskforge/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/radityajayantara/taskforge.svg)](https://pkg.go.dev/github.com/radityajayantara/taskforge)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/github/go-mod/go-version/radityajayantara/taskforge)](https://go.dev/)

A distributed task queue library for Go, backed by Redis. Built from scratch to demonstrate deep understanding of background processing, concurrency, and message queue management.

## Features

- **Worker Pool** — fixed-size goroutine pool with configurable concurrency per queue
- **Named Queues** — multiple independent queues (`email`, `image`, etc.) with separate worker pools
- **Job Lifecycle Tracking** — status progression: `pending` → `running` → `success` / `failed` / `dead`
- **Retry with Exponential Backoff** — configurable max retries per job type, backoff capped at 5 minutes
- **Dead-Letter Queue (DLQ)** — permanently failed jobs are preserved for inspection
- **Distributed Locking** — Redis-based lock prevents double processing across workers
- **Graceful Shutdown** — in-flight jobs complete before workers exit
- **Metrics** — query-able counters: enqueued, processed, failed, dead, pending, DLQ size

## Installation

```bash
go get github.com/radityajayantara/taskforge
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"

    "github.com/radityajayantara/taskforge"
    "github.com/redis/go-redis/v9"
)

func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    client := taskforge.NewClient(rdb)

    // Register a handler
    client.Register("send_email", func(ctx context.Context, job *taskforge.Job) error {
        to := job.Payload["to"].(string)
        fmt.Printf("Sending email to %s\n", to)
        return nil
    })

    ctx := context.Background()

    // Start workers
    client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
        Queue:       "email",
        Concurrency: 5,
    })

    // Enqueue a job (max 3 retries)
    job, _ := client.Enqueue(ctx, "email", "send_email", map[string]interface{}{
        "to":      "user@example.com",
        "subject": "Welcome!",
    }, 3)

    fmt.Printf("Enqueued: %s\n", job.ID)

    // Query metrics
    metrics, _ := client.GetMetrics(ctx, "email")
    fmt.Printf("Metrics: %+v\n", metrics)

    // Graceful shutdown
    client.StopAll()
}
```

## Architecture

```
Producer                         Redis                          Workers
   │                               │                               │
   ├── Enqueue(job) ──────────────►│ LPUSH queue:<name>            │
   │                               │ SET job:<id> (JSON)           │
   │                               │                               │
   │                               │ BRPOP queue:<name> ◄──────────┤
   │                               │                               ├── AcquireLock
   │                               │                               ├── Handler(job)
   │                               │                               ├── UpdateStatus
   │                               │                               └── ReleaseLock
   │                               │                               │
   │                               │ (on failure, retry < max)     │
   │                               │ LPUSH queue:<name> ◄──────────┤ (requeue)
   │                               │                               │
   │                               │ (on failure, retry >= max)    │
   │                               │ LPUSH dlq:<name> ◄────────────┤ (dead letter)
```

## Configuration

### WorkerPoolConfig

| Field | Default | Description |
|-------|---------|-------------|
| `Queue` | (required) | Queue name to consume from |
| `Concurrency` | 1 | Number of worker goroutines |
| `PollInterval` | 1s | How long BRPOP blocks before checking shutdown |
| `LockTTL` | 5m | Distributed lock expiry per job |
| `BaseDelay` | 1s | Base delay for exponential backoff (delay = base × 2^attempt) |

## Testing

Requires a running Redis instance:

```bash
# Start Redis
docker run -d -p 6379:6379 redis:7-alpine

# Run tests
go test -v -race ./...
```

## License

MIT
