package jetstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
	"go.uber.org/zap"
)

// Client wraps NATS JetStream functionality
type Client struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

// Ensure Client implements ClientInterface
var _ ClientInterface = (*Client)(nil)

// NewClient creates a new NATS JetStream client
func NewClient(url string) (*Client, error) {
	// Connect to NATS server
	nc, err := nats.Connect(url,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				logger.Log.Warn("NATS disconnected", zap.Error(err))
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Log.Info("NATS reconnected", zap.String("url", nc.ConnectedUrl()))
		}),
		nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
			logger.Log.Error("NATS error", zap.Error(err))
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Stream config is no longer defined here

	return &Client{
		nc: nc,
		js: js,
	}, nil
}

// SetupStream ensures the stream exists with the given configuration
func (c *Client) SetupStream(ctx context.Context, streamConfig *nats.StreamConfig) error {
	log := logger.FromContext(ctx)

	// Use the provided streamConfig
	log.Info("SetupStream...", zap.String("config", fmt.Sprintf("%+v", streamConfig)))

	// Check if the stream already exists
	stream, err := c.js.StreamInfo(streamConfig.Name)
	if err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		return fmt.Errorf("failed to get stream info for '%s': %w", streamConfig.Name, err)
	}

	if stream == nil {
		// Create the stream
		_, err = c.js.AddStream(streamConfig)
		if err != nil {
			return fmt.Errorf("failed to add stream '%s': %w", streamConfig.Name, err)
		}
		log.Info(
			"Created stream", zap.String("name", streamConfig.Name),
			zap.Any("subjects", streamConfig.Subjects),
		)
	} else {
		// Update the stream if needed (compare provided config with existing)
		if !utils.StreamConfigEqual(stream.Config, *streamConfig) {
			_, err = c.js.UpdateStream(streamConfig)
			if err != nil {
				return fmt.Errorf("failed to update stream '%s': %w", streamConfig.Name, err)
			}
			log.Info(
				"Updated stream", zap.String("name", streamConfig.Name),
				zap.Any("subjects", streamConfig.Subjects),
			)
		} else {
			log.Info(
				"stream no need update", zap.String("name", streamConfig.Name),
				zap.String("provided_cfg", fmt.Sprintf("%+v", streamConfig)),
				zap.String("current_cfg", fmt.Sprintf("%+v", stream.Config)),
			)
		}
	}

	return nil
}

// SetupConsumer ensures the consumer exists with the given configuration for a specific stream
func (c *Client) SetupConsumer(ctx context.Context, streamName string, consumerConfig *nats.ConsumerConfig) error {
	log := logger.FromContext(ctx).With(zap.String("stream", streamName), zap.String("consumer", consumerConfig.Durable))

	log.Info("SetupConsumer...", zap.String("config", fmt.Sprintf("%+v", consumerConfig)))

	// Check if the consumer already exists on the specified stream
	consumer, err := c.js.ConsumerInfo(streamName, consumerConfig.Durable)
	if err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
		return fmt.Errorf("failed to get consumer info for stream '%s', consumer '%s': %w", streamName, consumerConfig.Durable, err)
	}

	if consumer == nil {
		// Create the consumer on the specified stream
		_, err = c.js.AddConsumer(streamName, consumerConfig)
		if err != nil {
			return fmt.Errorf("failed to add consumer '%s' to stream '%s': %w", consumerConfig.Durable, streamName, err)
		}
		log.Info("Created consumer",
			zap.String("deliver_subject", consumerConfig.DeliverSubject),
			zap.String("queue_group", consumerConfig.DeliverGroup),
			zap.Any("filter_subjects", consumerConfig.FilterSubjects), // Use FilterSubjects if applicable
		)
	} else {
		// Update the consumer if needed (compare provided config with existing)
		if !utils.ConsumerConfigEqual(consumer.Config, *consumerConfig) {
			log.Warn("Consumer config mismatch, attempting update by delete/add",
				zap.String("provided_cfg", fmt.Sprintf("%+v", consumerConfig)),
				zap.String("current_cfg", fmt.Sprintf("%+v", consumer.Config)),
			)
			err = c.js.DeleteConsumer(streamName, consumerConfig.Durable)
			if err != nil {
				return fmt.Errorf("failed to delete existing consumer '%s' from stream '%s' for update: %w", consumerConfig.Durable, streamName, err)
			}
			_, err = c.js.AddConsumer(streamName, consumerConfig)
			if err != nil {
				return fmt.Errorf("failed to re-add consumer '%s' to stream '%s' during update: %w", consumerConfig.Durable, streamName, err)
			}
			log.Info("Updated consumer",
				zap.String("deliver_subject", consumerConfig.DeliverSubject),
				zap.String("queue_group", consumerConfig.DeliverGroup),
				zap.Any("filter_subjects", consumerConfig.FilterSubjects),
			)
		} else {
			log.Info("consumer no need update")
		}
	}

	return nil
}

// Subscribe subscribes to a subject with a durable consumer
func (c *Client) Subscribe(subject, consumer, group string, handler nats.MsgHandler) (*nats.Subscription, error) {
	sub, err := c.js.QueueSubscribe(
		subject,
		group,
		handler,
		nats.Durable(consumer),
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.DeliverAll(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	return sub, nil
}

// SubscribePush creates a push-based consumer subscription
func (c *Client) SubscribePush(subject, consumer, group, stream string, handler nats.MsgHandler) (*nats.Subscription, error) {
	sub, err := c.js.QueueSubscribe(
		subject,
		group,
		handler,
		nats.Durable(consumer),
		nats.ManualAck(),
		nats.BindStream(stream),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	return sub, nil
}

// SubscribePull creates a pull-based consumer subscription
func (c *Client) SubscribePull(streamName, subject, consumer string) (*nats.Subscription, error) {
	sub, err := c.js.PullSubscribe(
		subject,
		consumer,
		nats.Bind(streamName, consumer),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create pull subscription for stream '%s', consumer '%s': %w", streamName, consumer, err)
	}

	return sub, nil
}

// Publish publishes a message to a subject with optional headers
func (c *Client) Publish(subject string, data []byte, headers map[string]string) error {
	msg := nats.NewMsg(subject)
	msg.Data = data

	// Add headers if provided
	if headers != nil {
		for k, v := range headers {
			msg.Header.Add(k, v)
		}
	}

	_, err := c.js.PublishMsg(msg)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// NatsConn returns the underlying *nats.Conn
func (c *Client) NatsConn() *nats.Conn {
	return c.nc
}

// Close closes the NATS connection
func (c *Client) Close() {
	if c.nc != nil {
		c.nc.Close()
	}
}
