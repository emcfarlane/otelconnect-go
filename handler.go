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
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// protocolType is one of the supported RPC protocols.
type protocolType uint8

func (s protocolType) String() string {
	switch s {
	case grpcStreamProtocol:
		return connect.ProtocolGRPC
	case grpcWebProtocol:
		return connect.ProtocolGRPCWeb
	case connectUnaryProtocol, connectGetProtocol, connectStreamProtocol:
		return connect.ProtocolConnect
	default:
		return "unknown"
	}
}

func (s protocolType) isEnvelopeFraming() bool {
	switch s {
	case connectUnaryProtocol, connectGetProtocol:
		return false
	default:
		return true
	}
}

const (
	unknownProtocol protocolType = iota
	connectUnaryProtocol
	connectGetProtocol
	connectStreamProtocol
	grpcStreamProtocol
	grpcWebProtocol
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

func NewMiddleware(next http.Handler, options ...Option) (http.Handler, error) {

	cfg := makeConfig(options)
	instruments, err := makeServerInstruments(cfg.meter)
	if err != nil {
		return nil, fmt.Errorf("failed to create server instruments: %w", err)
	}
	return &handler{
		config:      cfg,
		instruments: instruments,
		base:        next,
	}, nil
}

func NewTransport(base http.RoundTripper, options ...Option) (http.RoundTripper, error) {
	cfg := makeConfig(options)
	instruments, err := createInstruments(cfg.meter, clientKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create client instruments: %w", err)
	}
	return &transport{
		config:      cfg,
		instruments: instruments,
		base:        base,
	}, nil
}

type clientInstruments struct {
}

func makeClientInstruments(meter metric.Meter) (clientInstruments, error) {

	return clientInstruments{}, nil
}

type serverInstruments struct {
	started   metric.Int64Counter     // grpc.server.started
	sentTotal metric.Int64Histogram   // grpc.server.sent_total_compressed_message_size
	rcvdTotal metric.Int64Histogram   // grpc.server.rcvd_total_compressed_message_size
	duration  metric.Float64Histogram // grpc.server.duration
}

func makeServerInstruments(meter metric.Meter) (serverInstruments, error) {
	// TODO: descriptions?
	started, err := meter.Int64Counter(
		grpcServerCallStartedName,
	)
	if err != nil {
		return serverInstruments{}, err
	}
	duration, err := meter.Float64Histogram(
		grpcServerCallDurationName,
		metric.WithUnit(unitSeconds),
	)
	if err != nil {
		return serverInstruments{}, err
	}
	rcvdTotal, err := meter.Int64Histogram(
		grpcServerCallDurationName,
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return serverInstruments{}, err
	}
	sendTotal, err := meter.Int64Histogram(
		grpcServerCallDurationName,
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return serverInstruments{}, err
	}
	return serverInstruments{
		started:   started,
		duration:  duration,
		rcvdTotal: rcvdTotal,
		sentTotal: sendTotal,
	}, nil
}

type handler struct {
	base        http.Handler
	config      config
	instruments serverInstruments
}

func (h *handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	startAt := h.config.now()
	protocol := classifyRequest(request)
	//  TODO: call filter option
	if protocol == unknownProtocol {
		h.base.ServeHTTP(response, request)
		return
	}
	// ...
	methodFullName := strings.TrimPrefix(request.URL.Path, "/")
	startAttrSet := attribute.NewSet(
		attribute.String(grpcMethodKey, methodFullName),
		attribute.String(grpcProtocolKey, protocol.String()),
	)

	h.instruments.started.Add(
		request.Context(), 1,
		metric.WithAttributeSet(startAttrSet),
	)

	// TODO: attributes filter
	// TODO: request attributes

	ctx := h.config.propagator.Extract(request.Context(), propagation.HeaderCarrier(request.Header))
	opts := []trace.SpanStartOption{
		// TODO: request key attributes.
		//trace.WithAttributes(attributes...),
		//trace.WithAttributes(headerAttributes(protocol, requestKey, request.Header(), i.config.requestHeaderKeys)...),
		trace.WithSpanKind(trace.SpanKindServer),
	}
	if !h.config.trustRemote {
		opts = append(opts, trace.WithNewRoot())
		// Linking incoming span context if any for public endpoint.
		if span := trace.SpanContextFromContext(ctx); span.IsValid() && span.IsRemote() {
			opts = append(
				opts,
				trace.WithLinks(trace.Link{SpanContext: span}),
			)
		}
	}
	ctx, span := h.config.tracer.Start(ctx, methodFullName, opts...)
	defer span.End()

	bw := bodyWrapper{
		protocol: protocol,
	}
	if protocol == connectGetProtocol {
		bw.total = int64(getMessageSize(request.URL.Query()))
	} else if request.Body != nil && request.Body != http.NoBody {
		bw.base = request.Body
		request.Body = &bw
		if request.GetBody != nil {
			gb := getBody{base: request.GetBody}
			request.GetBody = gb.GetBody
		}
	}

	rw := responseWrapper{
		base:       response,
		statusCode: http.StatusOK,
	}
	h.base.ServeHTTP(&rw, request.WithContext(ctx))
	//
	endAt := h.config.now()
	duration := endAt.Sub(startAt)
	fmt.Println("duration: ", duration)

	endAttrSet := attribute.NewSet(
		attribute.String(grpcStatusKey, grpcCanonicalStatusString(rw.code)),
	)
	h.instruments.duration.Record(
		ctx, duration.Seconds(),
		metric.WithAttributeSet(startAttrSet),
		metric.WithAttributeSet(endAttrSet),
	)
	h.instruments.rcvdTotal.Record(
		ctx, bw.total,
		metric.WithAttributeSet(startAttrSet),
		metric.WithAttributeSet(endAttrSet),
	)
	h.instruments.sentTotal.Record(
		ctx, rw.total,
		metric.WithAttributeSet(startAttrSet),
		metric.WithAttributeSet(endAttrSet),
	)
}

type transport struct {
	config      config
	instruments instruments
	base        http.RoundTripper
}

func (t *transport) RoundTrip(request *http.Request) (*http.Response, error) {
	protocol := classifyRequest(request)
	//  TODO: call filter option
	if protocol == unknownProtocol {
		return t.base.RoundTrip(request)
	}

	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	// TODO: wrap the response...
	return response, nil
}

// classifyRequest returns the protocol of the request.
// TODO: use typed constants for protocol
func classifyRequest(request *http.Request) protocolType {
	contentType := request.Header.Get("Content-Type")
	switch request.Method {
	case http.MethodPost:
		switch {
		case strings.HasPrefix(contentType, "application/grpc-web"):
			return grpcWebProtocol
		case strings.HasPrefix(contentType, "application/grpc"):
			return grpcStreamProtocol
		case strings.HasPrefix(contentType, "application/connect"):
			return connectStreamProtocol
		case strings.HasPrefix(contentType, "application/"):
			return connectUnaryProtocol

		}
	case http.MethodGet:
		// TODO: check for connect Get
		return connectGetProtocol
	}
	return unknownProtocol
}

type getBody struct {
	base func() (io.ReadCloser, error)
}

func (g *getBody) GetBody() (io.ReadCloser, error) {
	// TODO: record duration for attempt
	// TODO: increment retries
	return g.base()
}

// TODO: implement WriteTo
type bodyWrapper struct {
	base     io.ReadCloser
	protocol protocolType
	total    int64

	// Envelope framing
	prefix [5]byte
	index  uint64
	offset uint64
}

func (b *bodyWrapper) Read(p []byte) (n int, err error) {
	if b.protocol == connectUnaryProtocol {
		n, err = b.base.Read(p)
		b.total += int64(n)
		return n, err
	}

	if b.index == b.offset {
		b.index = 0
		b.offset = 0
	}
	if b.index == 0 {
		// read the prefix
		if nRead, err := io.ReadFull(b.base, b.prefix[:]); err != nil {
			n = copy(p, b.prefix[:nRead])
			return n, err
		}
		msgSize := binary.BigEndian.Uint32(b.prefix[1:5])
		b.offset = 5 + uint64(msgSize)
		b.total += int64(msgSize)
	}
	if b.index < 5 {
		// copy the prefix
		nCopy := copy(p, b.prefix[b.index:])
		b.index += uint64(nCopy)
		if b.index < 5 {
			return nCopy, nil
		}
		p = p[nCopy:]
		n += nCopy
	}
	// read the message up to the offset
	want := len(p)
	if diff := int(b.offset - b.index); want > diff {
		want = diff
	}
	nRead, err := b.base.Read(p[:want])
	if err != nil {
		return n, err
	}
	b.index += uint64(nRead)
	n += nRead
	return n, nil
}

func (b *bodyWrapper) Close() error {
	return b.base.Close()
}

type responseWrapper struct {
	base       http.ResponseWriter
	protocol   protocolType
	total      int64
	statusCode int
	code       connect.Code
	// Envelope framing
	prefix [5]byte
	index  uint64
	offset uint64
}

func (r *responseWrapper) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.base.WriteHeader(statusCode)
}
func (r *responseWrapper) Write(p []byte) (n int, err error) {
	if !r.protocol.isEnvelopeFraming() {
		n, err = r.base.Write(p)
		r.total += int64(n)
		return n, err
	}

	return r.base.Write(p)
}
func (r *responseWrapper) Header() http.Header {
	return r.base.Header()
}
func (r *responseWrapper) Flush() {
	r.base.(http.Flusher).Flush()
}
func (r *responseWrapper) Unwrap() http.ResponseWriter {
	return r.base
}

func getMessageSize(values url.Values) int {
	return len(values.Get("message"))
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
