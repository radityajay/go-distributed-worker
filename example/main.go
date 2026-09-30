package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/radityajayantara/taskforge"
	"github.com/redis/go-redis/v9"
)

func main() {
	// Connect to Redis.
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// Create taskforge client.
	logger := log.New(os.Stdout, "", log.LstdFlags)
	client := taskforge.NewClient(rdb, taskforge.WithLogger(logger))

	// Register handlers for different job types.
	client.Register("send_email", func(ctx context.Context, job *taskforge.Job) error {
		to := job.Payload["to"].(string)
		subject := job.Payload["subject"].(string)
		fmt.Printf("📧 Sending email to=%s subject=%q\n", to, subject)
		time.Sleep(500 * time.Millisecond) // simulate work
		return nil
	})

	client.Register("resize_image", func(ctx context.Context, job *taskforge.Job) error {
		path := job.Payload["path"].(string)
		width := job.Payload["width"].(float64)
		fmt.Printf("🖼️  Resizing image path=%s width=%.0f\n", path, width)
		time.Sleep(1 * time.Second) // simulate work
		return nil
	})

	ctx := context.Background()

	// Start worker pools for different queues.
	client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
		Queue:       "email",
		Concurrency: 3,
	})
	client.StartWorkers(ctx, taskforge.WorkerPoolConfig{
		Queue:       "image",
		Concurrency: 2,
	})

	// Enqueue some jobs.
	for i := 0; i < 5; i++ {
		job, err := client.Enqueue(ctx, "email", "send_email", map[string]interface{}{
			"to":      fmt.Sprintf("user%d@example.com", i),
			"subject": fmt.Sprintf("Welcome #%d", i),
		}, 3)
		if err != nil {
			log.Fatalf("enqueue error: %v", err)
		}
		fmt.Printf("Enqueued email job: %s\n", job.ID)
	}

	for i := 0; i < 3; i++ {
		job, err := client.Enqueue(ctx, "image", "resize_image", map[string]interface{}{
			"path":  fmt.Sprintf("/uploads/photo%d.jpg", i),
			"width": float64(800),
		}, 5)
		if err != nil {
			log.Fatalf("enqueue error: %v", err)
		}
		fmt.Printf("Enqueued image job: %s\n", job.ID)
	}

	// Wait a moment then show metrics.
	time.Sleep(5 * time.Second)

	for _, queue := range []string{"email", "image"} {
		metrics, _ := client.GetMetrics(ctx, queue)
		fmt.Printf("\n📊 Metrics [%s]: %+v\n", queue, metrics)
	}

	// Wait for interrupt signal for graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	client.StopAll()
	fmt.Println("Done.")
}
