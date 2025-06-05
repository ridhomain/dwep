// internal/cache/onboarding_cache.go
package cache

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/bits-and-blooms/bloom/v3"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
)

// OnboardingCache uses dual bloom filters for ultra-fast onboarding checks
type OnboardingCache struct {
	onboardedFilter *bloom.BloomFilter // Tracks contacts that are already onboarded
	nonExistFilter  *bloom.BloomFilter // Tracks phone+agent combinations that don't exist
	mu              sync.RWMutex
	hits            atomic.Int64
	misses          atomic.Int64
	falsePositives  atomic.Int64
	companyID       string
}

// NewOnboardingCache creates a new dual bloom filter cache
func NewOnboardingCache(companyID string, expectedOnboarded, expectedNonExist uint, fpRate float64) *OnboardingCache {
	return &OnboardingCache{
		onboardedFilter: bloom.NewWithEstimates(expectedOnboarded, fpRate),
		nonExistFilter:  bloom.NewWithEstimates(expectedNonExist, fpRate),
		companyID:       companyID,
	}
}

// generateKey creates a cache key from phone and agent ID using FNV-1a hash
func (c *OnboardingCache) generateKey(phoneNumber, agentID string) string {
	h := fnv.New64a()
	h.Write([]byte(phoneNumber + ":" + agentID))
	return fmt.Sprintf("%x", h.Sum64())
}

// CheckOnboardingStatus performs ultra-fast check of contact onboarding status
func (c *OnboardingCache) CheckOnboardingStatus(phoneNumber, agentID string) (status OnboardingStatus) {
	key := c.generateKey(phoneNumber, agentID)

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check if already onboarded
	if c.onboardedFilter.TestString(key) {
		// Might be onboarded (possible false positive)
		observer.IncCacheCheck(c.companyID, "bloom_onboarded", "possible_hit")
		return StatusMaybeOnboarded
	}

	// Definitely not onboarded, check if exists
	if c.nonExistFilter.TestString(key) {
		// Might not exist (possible false positive)
		observer.IncCacheCheck(c.companyID, "bloom_nonexist", "possible_hit")
		return StatusMaybeNotExist
	}

	// Not in either filter - unknown status
	c.misses.Add(1)
	observer.IncCacheCheck(c.companyID, "bloom", "miss")
	return StatusUnknown
}

// MarkOnboarded marks a contact as onboarded
func (c *OnboardingCache) MarkOnboarded(phoneNumber, agentID string) {
	key := c.generateKey(phoneNumber, agentID)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.onboardedFilter.AddString(key)
	c.hits.Add(1)
}

// MarkNonExistent marks a phone+agent combination as non-existent
func (c *OnboardingCache) MarkNonExistent(phoneNumber, agentID string) {
	key := c.generateKey(phoneNumber, agentID)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.nonExistFilter.AddString(key)
}

// RecordFalsePositive tracks when bloom filter gave incorrect positive
func (c *OnboardingCache) RecordFalsePositive(filterType string) {
	c.falsePositives.Add(1)
	observer.IncCacheCheck(c.companyID, "bloom_"+filterType, "false_positive")
}

// GetStats returns cache statistics
func (c *OnboardingCache) GetStats() OnboardingCacheStats {
	hits := c.hits.Load()
	misses := c.misses.Load()
	fps := c.falsePositives.Load()
	total := hits + misses

	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	fpRate := float64(0)
	if total > 0 {
		fpRate = float64(fps) / float64(total)
	}

	c.mu.RLock()
	onboardedSize := c.onboardedFilter.ApproximatedSize()
	nonExistSize := c.nonExistFilter.ApproximatedSize()
	c.mu.RUnlock()

	return OnboardingCacheStats{
		Hits:              hits,
		Misses:            misses,
		HitRate:           hitRate,
		FalsePositives:    fps,
		FalsePositiveRate: fpRate,
		OnboardedSize:     onboardedSize,
		NonExistSize:      nonExistSize,
	}
}

// OnboardingStatus represents the cache check result
type OnboardingStatus int

const (
	StatusUnknown OnboardingStatus = iota
	StatusMaybeOnboarded
	StatusMaybeNotExist
)

type OnboardingCacheStats struct {
	Hits              int64
	Misses            int64
	HitRate           float64
	FalsePositives    int64
	FalsePositiveRate float64
	OnboardedSize     uint64
	NonExistSize      uint64
}
