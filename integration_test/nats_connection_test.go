package integration_test

import (
	"context"
	"fmt"
	"log"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// --- Helper Functions --- (Some might be duplicates from other files - consolidate later)

// deleteStream deletes a NATS stream
func deleteStream(ctx context.Context, natsURL string, streamName string) error {
	nc, err := natsgo.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("deleteStream: failed to connect to NATS: %w", err)
	}
	defer nc.Close()

	jsCtx, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("deleteStream: failed to create JetStream context: %w", err)
	}

	// Delete stream if it exists
	err = jsCtx.DeleteStream(ctx, streamName)
	if err != nil && err != jetstream.ErrStreamNotFound {
		return fmt.Errorf("deleteStream: failed to delete stream '%s': %w", streamName, err)
	}
	log.Printf("deleteStream: Deleted or confirmed stream '%s' not found.", streamName)
	return nil
}

// createStream creates a NATS stream with standard settings
func createStream(ctx context.Context, natsURL string, streamName string, subjects string) error {
	nc, err := natsgo.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("createStream: failed to connect to NATS: %w", err)
	}
	defer nc.Close()

	jsCtx, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("createStream: failed to create JetStream context: %w", err)
	}

	// Create the stream
	cfg := jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subjects},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    24 * time.Hour,
	}
	_, err = jsCtx.CreateStream(ctx, cfg)
	if err != nil && err != jetstream.ErrStreamNameAlreadyInUse {
		return fmt.Errorf("createStream: failed to create stream '%s': %w", streamName, err)
	}
	log.Printf("createStream: Created or confirmed stream '%s' exists.", streamName)
	return nil
}

// createStreamWithFlowControl creates a NATS stream with specific max bytes for flow control testing
func createStreamWithFlowControl(ctx context.Context, natsURL string, streamName string, subjects string, maxBytes int64) error {
	nc, err := natsgo.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("createStreamWithFlowControl: failed to connect: %w", err)
	}
	defer nc.Close()

	jsCtx, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("createStreamWithFlowControl: failed to get JetStream context: %w", err)
	}

	// Create the stream with flow control settings
	cfg := jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subjects},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    24 * time.Hour,
		MaxBytes:  maxBytes,
	}
	_, err = jsCtx.CreateStream(ctx, cfg)
	if err != nil && err != jetstream.ErrStreamNameAlreadyInUse {
		return fmt.Errorf("createStreamWithFlowControl: failed to create stream '%s': %w", streamName, err)
	}
	log.Printf("createStreamWithFlowControl: Created/Updated stream '%s' with MaxBytes=%d.", streamName, maxBytes)
	return nil
}

// createStreamIfNotExists creates a stream if it doesn't already exist
func createStreamIfNotExists(ctx context.Context, natsURL string, streamName string, subjects string) error {
	nc, err := natsgo.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("createStreamIfNotExists: failed to connect: %w", err)
	}
	defer nc.Close()

	jsCtx, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("createStreamIfNotExists: failed to get JetStream context: %w", err)
	}

	_, err = jsCtx.Stream(ctx, streamName)
	if err == nil {
		log.Printf("createStreamIfNotExists: Stream '%s' already exists.", streamName)
		return nil // Stream already exists
	}
	if err != jetstream.ErrStreamNotFound {
		return fmt.Errorf("createStreamIfNotExists: unexpected error checking stream '%s': %w", streamName, err)
	}

	// Stream not found, create it
	log.Printf("createStreamIfNotExists: Stream '%s' not found, creating...", streamName)
	cfg := jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subjects},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    24 * time.Hour,
	}
	_, err = jsCtx.CreateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("createStreamIfNotExists: failed to create stream '%s': %w", streamName, err)
	}
	log.Printf("createStreamIfNotExists: Created stream '%s'.", streamName)
	return nil
}

// deleteAllConsumers deletes all consumers in a stream
func deleteAllConsumers(ctx context.Context, natsURL string, streamName string) error {
	nc, err := natsgo.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("deleteAllConsumers: failed to connect: %w", err)
	}
	defer nc.Close()

	jsCtx, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("deleteAllConsumers: failed to get JetStream context: %w", err)
	}

	// Get the stream first
	stream, err := jsCtx.Stream(ctx, streamName)
	if err != nil {
		// If stream doesn't exist, no consumers to delete
		if err == jetstream.ErrStreamNotFound {
			log.Printf("deleteAllConsumers: Stream '%s' not found, nothing to delete.", streamName)
			return nil
		}
		return fmt.Errorf("deleteAllConsumers: failed to get stream '%s': %w", streamName, err)
	}

	consumers := stream.ListConsumers(ctx) // Use ListConsumers on the Stream interface
	deletedCount := 0
	for info := range consumers.Info() {
		err = jsCtx.DeleteConsumer(ctx, streamName, info.Name)
		if err != nil {
			// Log error but continue trying to delete others
			log.Printf("deleteAllConsumers: Failed to delete consumer '%s' from stream '%s': %v", info.Name, streamName, err)
		} else {
			deletedCount++
		}
	}
	if consumers.Err() != nil {
		// ErrStreamNotFound is acceptable if the stream itself is gone
		if consumers.Err() != jetstream.ErrStreamNotFound {
			return fmt.Errorf("deleteAllConsumers: error listing consumers for stream '%s': %w", streamName, consumers.Err())
		}
	}
	log.Printf("deleteAllConsumers: Deleted %d consumers from stream '%s'.", deletedCount, streamName)
	return nil
}

// listConsumers lists all consumers in a stream
func listConsumers(ctx context.Context, natsURL string, streamName string) ([]string, error) {
	nc, err := natsgo.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("listConsumers: failed to connect: %w", err)
	}
	defer nc.Close()

	jsCtx, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("listConsumers: failed to get JetStream context: %w", err)
	}

	// Get the stream first
	stream, err := jsCtx.Stream(ctx, streamName)
	if err != nil {
		if err == jetstream.ErrStreamNotFound {
			return []string{}, nil // No stream means no consumers
		}
		return nil, fmt.Errorf("listConsumers: failed to get stream '%s': %w", streamName, err)
	}

	var consumerList []string
	consumers := stream.ListConsumers(ctx) // Use ListConsumers on the Stream interface
	for info := range consumers.Info() {
		consumerList = append(consumerList, info.Name)
	}
	if consumers.Err() != nil {
		if consumers.Err() == jetstream.ErrStreamNotFound {
			return []string{}, nil // No stream means no consumers
		}
		return nil, fmt.Errorf("listConsumers: error listing consumers for stream '%s': %w", streamName, consumers.Err())
	}
	return consumerList, nil
}

// checkStreamExists checks if a NATS stream exists.
func checkStreamExists(ctx context.Context, natsURL string, streamName string) (bool, error) {
	nc, err := natsgo.Connect(natsURL)
	if err != nil {
		return false, fmt.Errorf("checkStreamExists: failed to connect: %w", err)
	}
	defer nc.Close()

	jsCtx, err := jetstream.New(nc)
	if err != nil {
		return false, fmt.Errorf("checkStreamExists: failed to get JetStream context: %w", err)
	}

	_, err = jsCtx.Stream(ctx, streamName)
	if err == nil {
		return true, nil // Stream exists
	}
	if err == jetstream.ErrStreamNotFound {
		return false, nil // Stream does not exist
	}
	// Other error
	return false, fmt.Errorf("checkStreamExists: unexpected error checking stream '%s': %w", streamName, err)
}

// WaitUntilNatsReady waits for NATS server to be connectable.
func WaitUntilNatsReady(ctx context.Context, natsURL string) error {
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for NATS server at %s to become ready", natsURL)
		case <-ticker.C:
			nc, err := natsgo.Connect(natsURL, natsgo.Timeout(1*time.Second))
			if err == nil {
				nc.Close()
				return nil // Connected successfully
			}
			log.Printf("WaitUntilNatsReady: NATS ping failed: %v. Retrying...", err)
		}
	}
}
