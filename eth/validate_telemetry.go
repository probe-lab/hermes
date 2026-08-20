package eth

import (
	"context"
	"log/slog"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/probe-lab/hermes/tele"
)

// Nil until initMsgValidationMetrics runs, so every record site is nil-checked.
type msgValidationMetrics struct {
	results  metric.Int64Counter
	degraded metric.Int64Counter
	duration metric.Float64Histogram
}

func (p *PubSub) initMsgValidationMetrics(meter metric.Meter) error {
	if meter == nil {
		return nil
	}

	results, err := meter.Int64Counter(
		"validation_result_total",
		metric.WithDescription("Gossip validation outcomes by topic, result and reason"),
	)
	if err != nil {
		return err
	}

	degraded, err := meter.Int64Counter(
		"validation_degraded_total",
		metric.WithDescription("Messages that could not be fully validated, usually a cold proposer duty cache"),
	)
	if err != nil {
		return err
	}

	duration, err := meter.Float64Histogram(
		"validation_duration_seconds",
		metric.WithDescription("Time spent in the gossip validator"),
		metric.WithUnit("s"),
		// The SDK defaults jump from 0 to 5 seconds, which collapses every
		// sub-millisecond observation into one bucket.
		metric.WithExplicitBucketBoundaries(
			0.00005, 0.0001, 0.00025, 0.0005,
			0.001, 0.0025, 0.005, 0.01,
			0.025, 0.05, 0.1, 0.25,
		),
	)
	if err != nil {
		return err
	}

	p.msgValidationMetrics = &msgValidationMetrics{
		results:  results,
		degraded: degraded,
		duration: duration,
	}
	return nil
}

func validationResultLabel(result pubsub.ValidationResult) string {
	switch result {
	case pubsub.ValidationAccept:
		return "accept"
	case pubsub.ValidationReject:
		return "reject"
	case pubsub.ValidationIgnore:
		return "ignore"
	default:
		return "unknown"
	}
}

// finishValidation records the outcome. Deliberately no data-stream write: the
// validator holds the topic's validation slot, and gossipsub already traces every
// non-accepted message as REJECT_MESSAGE.
func (p *PubSub) finishValidation(
	ctx context.Context,
	msg *pubsub.Message,
	result pubsub.ValidationResult,
	reason string,
	cause error,
	start time.Time,
) pubsub.ValidationResult {
	label := validationResultLabel(result)
	topic := msg.GetTopic()

	if p.msgValidationMetrics != nil {
		p.msgValidationMetrics.results.Add(ctx, 1, metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("result", label),
			attribute.String("reason", reason),
		))
		p.msgValidationMetrics.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("result", label),
		))
	}

	// The counter carries the reason; this is where the underlying error survives.
	if result != pubsub.ValidationAccept {
		slog.Debug("not forwarding gossip message",
			"topic", topic, "result", label, "reason", reason, tele.LogAttrError(cause))
	}

	return result
}

// recordUnverifiedMsg counts a message that could not be fully verified. Not
// invalid: with fail-open these are still forwarded.
func (p *PubSub) recordUnverifiedMsg(ctx context.Context, topic, reason string) {
	if p.msgValidationMetrics == nil {
		return
	}
	p.msgValidationMetrics.degraded.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("reason", reason),
	))
}
