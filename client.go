package taskforge

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

// Client is the main entry point for the taskforge library.
// It provides a high-level API to enqueue jobs, start worker pools,
// and query metrics.
type Client struct {
	broker   *Broker
	registry *HandlerRegistry
	pools    []*WorkerPool
	logger   *log.Logger
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithLogger sets a custom logger for the client and its worker pools.
func WithLogger(logger *log.Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

// NewClient creates a new taskforge client backed by the given Redis client.
func NewClient(redisClient *redis.Client, opts ...ClientOption) *Client {
	c := &Client{
		broker:   NewBroker(redisClient),
		registry: NewHandlerRegistry(),
		logger:   log.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Register associates a job type with a handler function.
func (c *Client) Register(jobType string, handler Handler) {
	c.registry.Register(jobType, handler)
}

// Enqueue creates and enqueues a new job.
func (c *Client) Enqueue(ctx context.Context, queue, jobType string, payload map[string]interface{}, maxRetry int) (*Job, error) {
	job := NewJob(queue, jobType, payload, maxRetry)
	if err := c.broker.Enqueue(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

// StartWorkers starts a worker pool for the given configuration.
func (c *Client) StartWorkers(ctx context.Context, config WorkerPoolConfig) *WorkerPool {
	pool := NewWorkerPool(c.broker, c.registry, config, c.logger)
	pool.Start(ctx)
	c.pools = append(c.pools, pool)
	return pool
}

// StopAll gracefully stops all worker pools.
func (c *Client) StopAll() {
	for _, pool := range c.pools {
		pool.Stop()
	}
}

// GetJob retrieves a job by ID to check its status.
func (c *Client) GetJob(ctx context.Context, jobID string) (*Job, error) {
	return c.broker.GetJob(ctx, jobID)
}

// GetMetrics returns metrics for a specific queue.
func (c *Client) GetMetrics(ctx context.Context, queue string) (map[string]int64, error) {
	return c.broker.GetMetrics(ctx, queue)
}

// ListDLQ returns all job IDs in the dead-letter queue for a given queue.
func (c *Client) ListDLQ(ctx context.Context, queue string) ([]string, error) {
	return c.broker.ListDLQ(ctx, queue)
}

// Broker returns the underlying broker for advanced usage.
func (c *Client) Broker() *Broker {
	return c.broker
}
