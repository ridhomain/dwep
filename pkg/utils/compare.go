package utils

import (
	"github.com/nats-io/nats.go"
)

// StreamConfigEqual compares two NATS stream configurations for equality
// Focuses on core properties only
func StreamConfigEqual(a, b nats.StreamConfig) bool {
	isCfgSame := a.Name == b.Name &&
		a.Retention == b.Retention &&
		a.MaxMsgs == b.MaxMsgs &&
		a.MaxAge == b.MaxAge &&
		a.Storage == b.Storage

	isSubjectsSame := func() bool {
		if len(a.Subjects) != len(b.Subjects) {
			return false
		}
		for i, subject := range a.Subjects {
			if subject != b.Subjects[i] {
				return false
			}
		}
		return true
	}

	return isCfgSame && isSubjectsSame()
}

// ConsumerConfigEqual compares two NATS consumer configurations for equality
// Focuses on core properties only
func ConsumerConfigEqual(a, b nats.ConsumerConfig) bool {
	return a.Durable == b.Durable &&
		a.AckPolicy == b.AckPolicy &&
		a.FilterSubject == b.FilterSubject &&
		a.MaxDeliver == b.MaxDeliver
}
