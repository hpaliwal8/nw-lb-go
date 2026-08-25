// Package proxy is the transparent L7 gRPC proxy. It is registered with
// grpc.UnknownServiceHandler, so it forwards any service and method without knowing the schema:
// request and response bodies are moved as opaque byte slices and are never parsed.
package proxy

import (
	"fmt"

	"google.golang.org/grpc/mem"
)

// codecName is deliberately "proto". The name becomes the content-subtype on the wire, so
// reporting anything else would make the proxy speak application/grpc+<name> and no ordinary
// generated client would interoperate with it.
const codecName = "proto"

// Codec moves message bodies through the proxy without decoding them. Its message type is
// *mem.BufferSlice: the buffers gRPC handed us on the way in are the buffers we hand back on the
// way out.
//
// It is applied per-server with grpc.ForceServerCodecV2 and per-call with grpc.ForceCodecV2, and
// is never passed to encoding.RegisterCodecV2. Registering it would bind the name "proto"
// process-wide and every other gRPC client in this binary — the health checker above all — would
// suddenly be unable to marshal its generated messages.
//
// Reference counting, which is the whole subtlety of this type:
//
//   - Unmarshal takes a reference on the slice gRPC is about to free, so the value stored into v
//     carries exactly one reference owned by the proxy. Whoever called RecvMsg must Free it once.
//   - Marshal takes a reference on the slice it returns, because gRPC frees the marshalled result
//     once it has been written. That reference is the one gRPC consumes, which leaves the caller's
//     own reference intact — a slice may therefore be handed to SendMsg repeatedly (a retry
//     replaying the buffered first message does exactly that) and still needs exactly one Free.
type Codec struct{}

func (Codec) Marshal(v any) (mem.BufferSlice, error) {
	switch s := v.(type) {
	case mem.BufferSlice:
		s.Ref()
		return s, nil
	case *mem.BufferSlice:
		s.Ref()
		return *s, nil
	default:
		return nil, fmt.Errorf("proxy: codec cannot marshal %T, want mem.BufferSlice", v)
	}
}

func (Codec) Unmarshal(data mem.BufferSlice, v any) error {
	dst, ok := v.(*mem.BufferSlice)
	if !ok {
		return fmt.Errorf("proxy: codec cannot unmarshal into %T, want *mem.BufferSlice", v)
	}
	data.Ref()
	*dst = data
	return nil
}

func (Codec) Name() string { return codecName }
