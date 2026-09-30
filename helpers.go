package taskforge

import (
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// marshalJob serializes a job to JSON bytes.
func marshalJob(job *Job) ([]byte, error) {
	data, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("taskforge: marshal job: %w", err)
	}
	return data, nil
}

// redisZ creates a redis.Z member for sorted set operations.
func redisZ(member string, score float64) redis.Z {
	return redis.Z{
		Score:  score,
		Member: member,
	}
}

// rangeByScore creates a redis.ZRangeBy for sorted set range queries.
func rangeByScore(min, max string) *redis.ZRangeBy {
	return &redis.ZRangeBy{
		Min: min,
		Max: max,
	}
}
