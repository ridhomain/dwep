package integration_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go/network"

	natsgo "github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
)

// startNATSContainer starts a NATS container with JetStream enabled and returns it along with its URL.
// This uses the newer TestContainers NATS module approach.
func startNATSContainer(ctx context.Context, networkName string, nwr *testcontainers.DockerNetwork) (testcontainers.Container, string, error) {
	// Start a NATS container with JetStream enabled
	natsContainer, err := tcnats.Run(ctx,
		"nats:2.11-alpine",
		tcnats.WithArgument("name", "test-nats-server"),
		tcnats.WithArgument("http_port", "8222"),
		tcnats.WithArgument("store_dir", "/data"),
		network.WithNetwork([]string{"nats", networkName}, nwr),
	)
	if err != nil {
		return nil, "", fmt.Errorf("failed to start NATS container: %w", err)
	}

	// Get the connection string
	natsURL, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		return natsContainer, "", fmt.Errorf("failed to get NATS connection string: %w", err)
	}

	// Setup streams and consumers for the test
	err = setupJetStreamNew(ctx, natsContainer)
	if err != nil {
		return natsContainer, natsURL, fmt.Errorf("failed to setup JetStream: %w", err)
	}

	return natsContainer, natsURL, nil
}

// Not used because stream and consumer will created from apps, but kept for reference
// setupJetStreamNew creates the required streams and consumers for the tests.
func setupJetStreamNew(ctx context.Context, natsContainer testcontainers.Container) error {
	// Create a stream for message events
	//createStreamCmd := []string{
	//	"nats",
	//	"stream",
	//	"add",
	//	"message_events_stream",
	//	"--subjects=v1.>",
	//	"--storage=file",
	//	"--retention=limits",
	//	"--discard=old",
	//	"--max-msgs=-1",
	//	"--max-bytes=-1",
	//	"--max-age=24h",
	//	"--dupe-window=1h",
	//	"--max-msg-size=-1",
	//}
	//
	//// Using the container's Exec method
	//exitCode, _, err := natsContainer.Exec(ctx, createStreamCmd)
	//if err != nil {
	//	return fmt.Errorf("failed to execute create stream command: %w", err)
	//}
	//if exitCode != 0 {
	//	return fmt.Errorf("create stream command failed with exit code %d", exitCode)
	//}
	//
	//// Create a consumer for the stream
	//createConsumerCmd := []string{
	//	"nats",
	//	"consumer",
	//	"add",
	//	"message_events_stream",
	//	"events_consumer",
	//	"--pull",
	//	"--filter-subject=v1.>",
	//	"--durable=events_consumer",
	//	"--deliver=all",
	//	"--replay=instant",
	//	"--ack=explicit",
	//	"--max-deliver=-1",
	//	"--max-pending=1000",
	//}
	//
	//exitCode, _, err = natsContainer.Exec(ctx, createConsumerCmd)
	//if err != nil {
	//	return fmt.Errorf("failed to execute create consumer command: %w", err)
	//}
	//if exitCode != 0 {
	//	return fmt.Errorf("create consumer command failed with exit code %d", exitCode)
	//}

	return nil
}

// publishTestMessageNew publishes a test message to NATS JetStream after validating its payload against the corresponding JSON schema.
// make sure the subject pass here are the complete subject with tenant/company ID
func publishTestMessageNew(ctx context.Context, natsURL string, subject string, payload []byte) error {
	// Connect to NATS
	nc, err := natsgo.Connect(natsURL, natsgo.Name("Integration Test Publisher"))
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	defer nc.Close()

	// --- Payload Validation Start ---
	// Determine the base subject (remove tenant suffix if present)
	baseSubject := subject
	parts := strings.Split(subject, ".")
	if len(parts) > 1 {
		// Check if the last part looks like a tenant ID (e.g., _tenant_dev)
		// A more robust check might be needed depending on actual tenant ID formats
		lastPart := parts[len(parts)-1]
		if strings.Contains(lastPart, "_tenant_") || strings.Contains(lastPart, "_company_") { // Basic check
			baseSubject = strings.Join(parts[:len(parts)-1], ".")
		}
	}

	schemaName, ok := SubjectToSchemaMap[baseSubject]
	if !ok {
		// If no specific schema is mapped, maybe skip validation or log a warning?
		// For now, let's return an error to enforce mapping.
		return fmt.Errorf("no schema mapping found for base subject: %s (derived from %s)", baseSubject, subject)
	}

	err = validatePayload(payload, schemaName)
	if err != nil {
		return fmt.Errorf("payload validation failed for subject %s (schema %s): %w", subject, schemaName, err)
	}
	// --- Payload Validation End ---

	// Access JetStream context
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to access JetStream: %w", err)
	}

	// Publish message
	pubAck, err := js.Publish(subject, payload)
	if err != nil {
		return fmt.Errorf("failed to publish message to subject %s: %w", subject, err)
	}

	// Optional: Log successful publish details
	fmt.Printf("Published to %s (Seq: %d)\n", subject, pubAck.Sequence)

	// Wait a moment to ensure message is processed - Adjust as needed, consider alternatives
	time.Sleep(200 * time.Millisecond) // Increased slightly

	return nil
}

func publishTestHistoricalPayload(ctx context.Context, natsURL, payloadType, companyID string, payloads *TestPayloads) error {
	switch payloadType {
	case "history_chat":
		subject := SubjectMapping["history_chat"]
		if companyID != "" {
			subject = fmt.Sprintf("%s.%s", subject, companyID)
		}
		if err := publishTestMessageNew(ctx, natsURL, subject, payloads.ChatHistory); err != nil {
			return fmt.Errorf("failed to publish history chat payload: %w", err)
		}
	case "history_message":
		subject := SubjectMapping["history_message"]
		if companyID != "" {
			subject = fmt.Sprintf("%s.%s", subject, companyID)
		}
		if err := publishTestMessageNew(ctx, natsURL, SubjectMapping["history_message"], payloads.MessageHistory); err != nil {
			return fmt.Errorf("failed to publish history message payload: %w", err)
		}
	case "history_contact":
		subject := SubjectMapping["history_contact"]
		if companyID != "" {
			subject = fmt.Sprintf("%s.%s", subject, companyID)
		}
		if err := publishTestMessageNew(ctx, natsURL, SubjectMapping["history_contact"], payloads.ContactHistory); err != nil {
			return fmt.Errorf("failed to publish history contact payload: %w", err)
		}
	default:
		return fmt.Errorf("unsupported payload type: %s", payloadType)
	}
	return nil
}

// publishTestPayloadsNew publishes all test payloads to their respective NATS subjects.
func publishTestPayloadsNew(ctx context.Context, natsURL string, payloads *TestPayloads) error {
	// Publish chat upsert payload
	if len(payloads.ChatUpsert) > 0 {
		if err := publishTestMessageNew(ctx, natsURL, SubjectMapping["chat_created"], payloads.ChatUpsert); err != nil {
			return fmt.Errorf("failed to publish chat upsert payload: %w", err)
		}
	}

	// Publish chat update payload
	if len(payloads.ChatUpdate) > 0 {
		if err := publishTestMessageNew(ctx, natsURL, SubjectMapping["chat_updated"], payloads.ChatUpdate); err != nil {
			return fmt.Errorf("failed to publish chat update payload: %w", err)
		}
	}

	// Publish chat history payload
	if len(payloads.ChatHistory) > 0 {
		if err := publishTestMessageNew(ctx, natsURL, SubjectMapping["history_chat"], payloads.ChatHistory); err != nil {
			return fmt.Errorf("failed to publish chat history payload: %w", err)
		}
	}

	// Publish message upsert payload
	if len(payloads.MessageUpsert) > 0 {
		if err := publishTestMessageNew(ctx, natsURL, SubjectMapping["message_created"], payloads.MessageUpsert); err != nil {
			return fmt.Errorf("failed to publish message upsert payload: %w", err)
		}
	}

	// Publish message update payload
	if len(payloads.MessageUpdate) > 0 {
		if err := publishTestMessageNew(ctx, natsURL, SubjectMapping["message_updated"], payloads.MessageUpdate); err != nil {
			return fmt.Errorf("failed to publish message update payload: %w", err)
		}
	}

	// Publish message history payload
	if len(payloads.MessageHistory) > 0 {
		if err := publishTestMessageNew(ctx, natsURL, SubjectMapping["history_message"], payloads.MessageHistory); err != nil {
			return fmt.Errorf("failed to publish message history payload: %w", err)
		}
	}

	return nil
}
