// internal/usecase/onboarding_bloom.go
package usecase

import (
	"fmt"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
	"go.uber.org/zap"
)

// OnboardingBloomFilter manages bloom filters for tracking onboarded contacts per agent
type OnboardingBloomFilter struct {
	filters map[string]*bloom.BloomFilter // key: "company_id:agent_id"
	mutex   sync.RWMutex

	// Bloom filter parameters
	expectedItems     uint
	falsePositiveRate float64
	logger            *zap.Logger
}

// NewOnboardingBloomFilter creates a new bloom filter manager
func NewOnboardingBloomFilter(logger *zap.Logger) *OnboardingBloomFilter {
	return &OnboardingBloomFilter{
		filters:           make(map[string]*bloom.BloomFilter),
		expectedItems:     100000, // Adjust based on expected contacts per agent
		falsePositiveRate: 0.01,   // 1% false positive rate
		logger:            logger.Named("bloom_filter"),
	}
}

// getOrCreateFilter returns existing filter or creates a new one
func (f *OnboardingBloomFilter) getOrCreateFilter(companyID, agentID string) *bloom.BloomFilter {
	key := fmt.Sprintf("%s:%s", companyID, agentID)

	f.mutex.RLock()
	filter, exists := f.filters[key]
	f.mutex.RUnlock()

	if exists {
		return filter
	}

	// Create new filter
	f.mutex.Lock()
	defer f.mutex.Unlock()

	// Double-check after acquiring write lock
	if filter, exists = f.filters[key]; exists {
		return filter
	}

	filter = bloom.NewWithEstimates(f.expectedItems, f.falsePositiveRate)
	f.filters[key] = filter

	f.logger.Info("Created new bloom filter",
		zap.String("company_id", companyID),
		zap.String("agent_id", agentID),
		zap.Uint("expected_items", f.expectedItems),
		zap.Float64("false_positive_rate", f.falsePositiveRate))

	return filter
}

// Add adds a phone number to the bloom filter for a specific agent
func (f *OnboardingBloomFilter) Add(companyID, agentID, phoneNumber string) {
	filter := f.getOrCreateFilter(companyID, agentID)
	filter.AddString(phoneNumber)
}

// MightExist checks if a phone number might be onboarded for a specific agent
func (f *OnboardingBloomFilter) MightExist(companyID, agentID, phoneNumber string) bool {
	filter := f.getOrCreateFilter(companyID, agentID)
	return filter.TestString(phoneNumber)
}

// AddBatch adds multiple phone numbers at once (for warming up)
func (f *OnboardingBloomFilter) AddBatch(companyID, agentID string, phoneNumbers []string) {
	filter := f.getOrCreateFilter(companyID, agentID)
	for _, phone := range phoneNumbers {
		filter.AddString(phone)
	}
}

// GetStats returns statistics about the bloom filters
func (f *OnboardingBloomFilter) GetStats() map[string]interface{} {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	stats := make(map[string]interface{})
	stats["total_filters"] = len(f.filters)

	filterStats := make(map[string]map[string]interface{})
	for key, filter := range f.filters {
		filterStats[key] = map[string]interface{}{
			"approx_items": filter.ApproximatedSize(),
			"capacity":     filter.Cap(),
		}
	}
	stats["filters"] = filterStats

	return stats
}
