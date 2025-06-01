package utils

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestStreamConfigEqual(t *testing.T) {
	tests := []struct {
		name     string
		configA  nats.StreamConfig
		configB  nats.StreamConfig
		expected bool
	}{
		{
			name: "identical configs",
			configA: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
				Subjects:  []string{"subject.test"},
			},
			configB: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
				Subjects:  []string{"subject.test"},
			},
			expected: true,
		},
		{
			name: "different subjects",
			configA: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
				Subjects:  []string{"subject.test"}, // Not compared
			},
			configB: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
				Subjects:  []string{"subject.test", "subject.test2"}, // Different but not compared
			},
			expected: false,
		},
		{
			name: "different names",
			configA: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
			},
			configB: nats.StreamConfig{
				Name:      "different-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
			},
			expected: false,
		},
		{
			name: "different retention",
			configA: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
			},
			configB: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.InterestPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
			},
			expected: false,
		},
		{
			name: "different MaxMsgs",
			configA: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
			},
			configB: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   2000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
			},
			expected: false,
		},
		{
			name: "different MaxAge",
			configA: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
			},
			configB: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    7200,
				Storage:   nats.FileStorage,
			},
			expected: false,
		},
		{
			name: "different Storage",
			configA: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.FileStorage,
			},
			configB: nats.StreamConfig{
				Name:      "test-stream",
				Retention: nats.LimitsPolicy,
				MaxMsgs:   1000,
				MaxAge:    3600,
				Storage:   nats.MemoryStorage,
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := StreamConfigEqual(tc.configA, tc.configB)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestConsumerConfigEqual(t *testing.T) {
	tests := []struct {
		name     string
		configA  nats.ConsumerConfig
		configB  nats.ConsumerConfig
		expected bool
	}{
		{
			name: "identical configs",
			configA: nats.ConsumerConfig{
				Durable:       "consumer-1",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "subject.test",
				MaxDeliver:    10,
				Description:   "test consumer", // Not compared
			},
			configB: nats.ConsumerConfig{
				Durable:       "consumer-1",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "subject.test",
				MaxDeliver:    10,
				Description:   "different description", // Different but not compared
			},
			expected: true,
		},
		{
			name: "different Durable",
			configA: nats.ConsumerConfig{
				Durable:       "consumer-1",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "subject.test",
				MaxDeliver:    10,
			},
			configB: nats.ConsumerConfig{
				Durable:       "consumer-2",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "subject.test",
				MaxDeliver:    10,
			},
			expected: false,
		},
		{
			name: "different AckPolicy",
			configA: nats.ConsumerConfig{
				Durable:       "consumer-1",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "subject.test",
				MaxDeliver:    10,
			},
			configB: nats.ConsumerConfig{
				Durable:       "consumer-1",
				AckPolicy:     nats.AckAllPolicy,
				FilterSubject: "subject.test",
				MaxDeliver:    10,
			},
			expected: false,
		},
		{
			name: "different FilterSubject",
			configA: nats.ConsumerConfig{
				Durable:       "consumer-1",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "subject.test",
				MaxDeliver:    10,
			},
			configB: nats.ConsumerConfig{
				Durable:       "consumer-1",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "subject.different",
				MaxDeliver:    10,
			},
			expected: false,
		},
		{
			name: "different MaxDeliver",
			configA: nats.ConsumerConfig{
				Durable:       "consumer-1",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "subject.test",
				MaxDeliver:    10,
			},
			configB: nats.ConsumerConfig{
				Durable:       "consumer-1",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "subject.test",
				MaxDeliver:    20,
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ConsumerConfigEqual(tc.configA, tc.configB)
			assert.Equal(t, tc.expected, result)
		})
	}
}
