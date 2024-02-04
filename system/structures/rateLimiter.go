package structures

import "time"

type RateLimiter struct {
	maxTokens       int   // maximum number of tokens the bucket can hold
	availableTokens int   // number of tokens currently available in the bucket
	fillRate        int64 // time interval required to replenish the bucket (in seconds)
	lastRefill      int64 // time of the last bucket refill (in seconds)
}

func NewRateLimiter(fillRate int64, maxTokens int) *RateLimiter {
	return &RateLimiter{
		maxTokens:       maxTokens,
		availableTokens: maxTokens,
		fillRate:        fillRate,
		lastRefill:      time.Now().Unix(),
	}
}
