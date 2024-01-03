// Copyright 2022-2023 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otelconnect

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	// Attributes keys
	grpcMethodKey   = "grpc.method"
	grpcStatusKey   = "grpc.status"
	grpcTargetKey   = "grpc.target"
	grpcProtocolKey = "grpc.protocol" // Custom key for protocol.

	// Client attempts
	// https://github.com/grpc/proposal/blob/master/A66-otel-stats.md#client-per-attempt-instruments
	grpcClientAttempStartedName    = "grpc.client.attempt.started"
	grpcClientAttemptDurationName  = "grpc.client.attempt.duration"
	grpcClientAttemptSentTotalName = "grpc.client.attempt.sent_total_compressed_message_size"
	grpcClientAttemptRecvTotalName = "grpc.client.attempt.rcvd_total_compressed_message_size"

	// Client per-call
	// https://github.com/grpc/proposal/blob/master/A66-otel-stats.md#client-per-call-instruments
	grpcClientCallDurationName = "grpc.client.call.duration"

	// Server instruments
	// https://github.com/grpc/proposal/blob/master/A66-otel-stats.md#server-instruments
	grpcServerCallStartedName   = "grpc.server.call.started"
	grpcServerCallSentTotalName = "grpc.server.call.sent_total_compressed_message_size"
	grpcServerCallRcvdTotalName = "grpc.server.call.rcvd_total_compressed_message_size"
	grpcServerCallDurationName  = "grpc.server.call.duration"
)

type Observability struct {
	serverMetrics *serverMetrics
	clientMetrics *clientMetrics
	config        config
}

func NewObservability(options ...Option) (Observability, error) {
	cfg := makeConfig(options)
	serverMetrics, err := newServerMetrics(cfg.meter)
	if err != nil {
		return Observability{}, err
	}
	clientMetrics, err := newClientMetrics(cfg.meter)
	if err != nil {
		return Observability{}, err
	}
	return Observability{
		serverMetrics: serverMetrics,
		clientMetrics: clientMetrics,
		config:        cfg,
	}, nil
}

func (o Observability) Observe(ctx context.Context, spec connect.Spec, peer connect.Peer, header http.Header) (context.Context, connect.Observer) {
	if o.config.filter != nil {
		if !o.config.filter(ctx, spec) {
			return ctx, nil
		}
	}
	if spec.IsClient {
		return o.observeClient(ctx, spec, peer, header)
	} else {
		return o.observeHandler(ctx, spec, peer, header)
	}
}

func (o Observability) observeClient(ctx context.Context, spec connect.Spec, peer connect.Peer, header http.Header) (context.Context, connect.Observer) {
	startAt := o.config.now()
	metrics := o.clientMetrics
	opts := []trace.SpanStartOption{
		// TODO: request key attributes.
		trace.WithSpanKind(trace.SpanKindServer),
	}
	if !o.config.trustRemote {
		opts = append(opts, trace.WithNewRoot())
		// Linking incoming span context if any for public endpoint.
		if span := trace.SpanContextFromContext(ctx); span.IsValid() && span.IsRemote() {
			opts = append(
				opts,
				trace.WithLinks(trace.Link{SpanContext: span}),
			)
		}
	}
	methodFullName := spec.Procedure[1:]
	ctx, span := o.config.tracer.Start(ctx, methodFullName, opts...)
	defer span.End()
	carrier := propagation.HeaderCarrier(header)
	o.config.propagator.Inject(ctx, carrier)
	attrSet := attribute.NewSet(
		attribute.String(grpcMethodKey, methodFullName),
		attribute.String(grpcTargetKey, peer.Addr),
		attribute.String(grpcProtocolKey, peer.Protocol),
	)
	var (
		attempts  int64
		sentTotal int64
		rcvdTotal int64
	)
	return ctx, func(ctx context.Context, event connect.ObserverEvent) {
		switch event := event.(type) {
		case *connect.ObserverEventSendHeader:
			metrics.started.Add(
				ctx, 1,
				metric.WithAttributeSet(attrSet),
			)
			if attempts > 0 {
				duration := time.Since(startAt)
				metrics.attemptRcvdTotal.Record(
					ctx, rcvdTotal,
					metric.WithAttributeSet(attrSet),
				)
				metrics.attemptSentTotal.Record(
					ctx, sentTotal,
					metric.WithAttributeSet(attrSet),
				)
				metrics.attemptDuration.Record(
					ctx, time.Since(startAt).Seconds()
					metric.WithAttributeSet(attrSet),
				)
			}
			attempts++
			rcvdTotal = 0
			sentTotal = 0
		case *connect.ObserverEventReceiveMessage:
			rcvdTotal += int64(event.Size)
		case *connect.ObserverEventSendMessage:
			sentTotal += int64(event.Size)
		case *connect.ObserverEventEnd:
			var code connect.Code
			if event.Err != nil {
				code = event.Err.Code()
			}
			endAttrSet := attribute.NewSet(
				attribute.String(grpcStatusKey, grpcCanonicalStatusString(code)),
			)
			duration := time.Since(startAt)
			metrics.duration.Record(
				ctx, duration.Seconds(),
				metric.WithAttributeSet(attrSet),
				metric.WithAttributeSet(endAttrSet),
			)
			metrics.attemptDuration.Record(
				ctx, duration.Seconds(),
				metric.WithAttributeSet(attrSet),
				metric.WithAttributeSet(endAttrSet),
			)
			metrics.attemptRcvdTotal.Record(
				ctx, rcvdTotal,
				metric.WithAttributeSet(attrSet),
				metric.WithAttributeSet(endAttrSet),
			)
			metrics.attemptSentTotal.Record(
				ctx, sentTotal,
				metric.WithAttributeSet(attrSet),
				metric.WithAttributeSet(endAttrSet),
			)
			span.End()
		}
	}
}

func (o Observability) observeHandler(ctx context.Context, spec connect.Spec, peer connect.Peer, header http.Header) (context.Context, connect.Observer) {
	startAt := o.config.now()
	metrics := o.serverMetrics
	ctx = o.config.propagator.Extract(ctx, propagation.HeaderCarrier(header))
	methodFullName := spec.Procedure[1:]
	spanOpts := []trace.SpanStartOption{
		// TODO: request key attributes.
		trace.WithSpanKind(trace.SpanKindServer),
	}
	ctx, span := o.config.tracer.Start(ctx, methodFullName, spanOpts...)
	attrSet := attribute.NewSet(
		attribute.String(grpcMethodKey, spec.Procedure[1:]),
		attribute.String(grpcProtocolKey, peer.Protocol),
	)
	metrics.started.Add(
		ctx, 1,
		metric.WithAttributeSet(attrSet),
	)
	var (
		sentTotal int64
		rcvdTotal int64
	)
	return ctx, func(ctx context.Context, event connect.ObserverEvent) {
		switch event := event.(type) {
		case *connect.ObserverEventSendHeader:
			// nothing
		case *connect.ObserverEventReceiveMessage:
			rcvdTotal += int64(event.Size)
		case *connect.ObserverEventSendMessage:
			sentTotal += int64(event.Size)
		case *connect.ObserverEventEnd:
			var code connect.Code
			if event.Err != nil {
				code = event.Err.Code()
			}
			endAttrSet := attribute.NewSet(
				attribute.String(grpcStatusKey, grpcCanonicalStatusString(code)),
			)
			duration := time.Since(startAt)
			metrics.duration.Record(
				ctx, duration.Seconds(),
				metric.WithAttributeSet(attrSet),
				metric.WithAttributeSet(endAttrSet),
			)
			metrics.rcvdTotal.Record(
				ctx, rcvdTotal,
				metric.WithAttributeSet(attrSet),
				metric.WithAttributeSet(endAttrSet),
			)
			metrics.sentTotal.Record(
				ctx, sentTotal,
				metric.WithAttributeSet(attrSet),
				metric.WithAttributeSet(endAttrSet),
			)
			span.End()
		}
	}
}

type clientMetrics struct {
	attemptStarted   metric.Int64Counter     // grpc.client.attempt.started
	attemptDuration  metric.Float64Histogram // grpc.client.attempt.duration
	attemptSentTotal metric.Int64Histogram   // grpc.client.attempt.sent_total_compressed_message_size
	attemptRcvdTotal metric.Int64Histogram   // grpc.client.attempt.rcvd_total_compressed_message_size
	duration         metric.Float64Histogram // grpc.client.duration
}

func newClientMetrics(meter metric.Meter) (*clientMetrics, error) {
	// TODO: descriptions?
	started, err := meter.Int64Counter(
		grpcServerCallStartedName,
	)
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram(
		grpcServerCallDurationName,
		metric.WithUnit(unitSeconds),
	)
	if err != nil {
		return nil, err
	}
	rcvdTotal, err := meter.Int64Histogram(
		grpcServerCallDurationName,
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return nil, err
	}
	sendTotal, err := meter.Int64Histogram(
		grpcServerCallDurationName,
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return nil, err
	}
	return &clientMetrics{
		attemptStarted:   started,
		attemptDuration:  duration,
		attemptRcvdTotal: rcvdTotal,
		attemptSentTotal: sendTotal,
		duration:         duration,
	}, nil

}

type serverMetrics struct {
	started   metric.Int64Counter     // grpc.server.started
	sentTotal metric.Int64Histogram   // grpc.server.sent_total_compressed_message_size
	rcvdTotal metric.Int64Histogram   // grpc.server.rcvd_total_compressed_message_size
	duration  metric.Float64Histogram // grpc.server.duration
}

func newServerMetrics(meter metric.Meter) (*serverMetrics, error) {
	// TODO: descriptions?
	started, err := meter.Int64Counter(
		grpcServerCallStartedName,
	)
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram(
		grpcServerCallDurationName,
		metric.WithUnit(unitSeconds),
	)
	if err != nil {
		return nil, err
	}
	rcvdTotal, err := meter.Int64Histogram(
		grpcServerCallDurationName,
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return nil, err
	}
	sendTotal, err := meter.Int64Histogram(
		grpcServerCallDurationName,
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return nil, err
	}
	return &serverMetrics{
		started:   started,
		duration:  duration,
		rcvdTotal: rcvdTotal,
		sentTotal: sendTotal,
	}, nil
}

func grpcCanonicalStatusString(code connect.Code) string {
	switch code {
	case 0:
		return "OK"
	case connect.CodeCanceled:
		return "CANCELLED"
	case connect.CodeUnknown:
		return "UNKNOWN"
	case connect.CodeInvalidArgument:
		return "INVALID_ARGUMENT"
	case connect.CodeDeadlineExceeded:
		return "DEADLINE_EXCEEDED"
	case connect.CodeNotFound:
		return "NOT_FOUND"
	case connect.CodeAlreadyExists:
		return "ALREADY_EXISTS"
	case connect.CodePermissionDenied:
		return "PERMISSION_DENIED"
	case connect.CodeResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case connect.CodeFailedPrecondition:
		return "FAILED_PRECONDITION"
	case connect.CodeAborted:
		return "ABORTED"
	case connect.CodeOutOfRange:
		return "OUT_OF_RANGE"
	case connect.CodeUnimplemented:
		return "UNIMPLEMENTED"
	case connect.CodeInternal:
		return "INTERNAL"
	case connect.CodeUnavailable:
		return "UNAVAILABLE"
	case connect.CodeDataLoss:
		return "DATA_LOSS"
	case connect.CodeUnauthenticated:
		return "UNAUTHENTICATED"
	default:
		return "CODE(" + strconv.FormatInt(int64(code), 10) + ")"
	}
}
