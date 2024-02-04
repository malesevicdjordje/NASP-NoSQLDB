package structures

import "time"

// RateLimiter is a token bucket rate-limiting implementation.
type RateLimiter struct {
	maxTokens       int   // maximum number of tokens the bucket can hold
	availableTokens int   // number of tokens currently available in the bucket
	fillRate        int64 // time interval required to replenish the bucket (in seconds)
	lastRefill      int64 // time of the last bucket refill (in seconds)
}

// NewRateLimiter creates and returns a new RateLimiter instance with the specified fill rate and maximum number of tokens.
func NewRateLimiter(fillRate int64, maxTokens int) *RateLimiter {
	return &RateLimiter{
		maxTokens:       maxTokens,
		availableTokens: maxTokens,
		fillRate:        fillRate,
		lastRefill:      time.Now().Unix(),
	}
}

// AllowRequest checks if a request can be allowed based on the rate limit.
func (rl *RateLimiter) AllowRequest() bool {
	// If enough time has passed since the last refill, reset the available tokens to the maximum.
	if time.Now().Unix()-rl.lastRefill > rl.fillRate {
		rl.lastRefill = time.Now().Unix()
		rl.availableTokens = rl.maxTokens
	}

	// If there are no available tokens, deny the request.
	if rl.availableTokens <= 0 {
		return false
	}

	// Decrement the available tokens by one and allow the request.
	rl.availableTokens--
	return true
}
