package taskforge

import (
	"time"

	"github.com/google/uuid"
)

// Status represents the current state of a job in its lifecycle.
type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"
	StatusDead    Status = "dead" // moved to dead-letter queue
)

// Job represents a unit of work to be processed by a worker.
type Job struct {
	ID        string                 `json:"id"`
	Queue     string                 `json:"queue"`
	Type      string                 `json:"type"`
	Payload   map[string]interface{} `json:"payload"`
	Status    Status                 `json:"status"`
	Retry     int                    `json:"retry"`
	MaxRetry  int                    `json:"max_retry"`
	Error     string                 `json:"error,omitempty"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`
}

// NewJob creates a new job with the given type, payload, and options.
func NewJob(queue, jobType string, payload map[string]interface{}, maxRetry int) *Job {
	now := time.Now().UTC()
	return &Job{
		ID:        uuid.New().String(),
		Queue:     queue,
		Type:      jobType,
		Payload:   payload,
		Status:    StatusPending,
		Retry:     0,
		MaxRetry:  maxRetry,
		CreatedAt: now,
		UpdatedAt: now,
	}
}
