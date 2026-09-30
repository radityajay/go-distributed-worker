package taskforge

import "context"

// Handler processes a job. Implementations should return an error
// if the job fails and should be retried.
type Handler func(ctx context.Context, job *Job) error

// HandlerRegistry maps job types to their handlers.
type HandlerRegistry struct {
	handlers map[string]Handler
}

// NewHandlerRegistry creates a new empty handler registry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string]Handler),
	}
}

// Register associates a job type with a handler function.
func (r *HandlerRegistry) Register(jobType string, handler Handler) {
	r.handlers[jobType] = handler
}

// Get returns the handler for the given job type.
// Returns nil if no handler is registered.
func (r *HandlerRegistry) Get(jobType string) Handler {
	return r.handlers[jobType]
}
