package proxy

import (
	"bytes"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

// pooledSize is above mem.BufferPoolingThreshold, which is the only size range where buffers are
// reference counted at all — smaller payloads become plain slices whose Free is a no-op and would
// hide every accounting mistake this file exists to catch.
const pooledSize = 8 << 10

// countingPool records how many buffers were handed back, which is the observable moment a
// reference count reached zero.
type countingPool struct {
	gets atomic.Int64
	puts atomic.Int64
}

func (p *countingPool) Get(length int) *[]byte {
	p.gets.Add(1)
	b := make([]byte, length)
	return &b
}

func (p *countingPool) Put(b *[]byte) { p.puts.Add(1) }

func pooledSlice(pool mem.BufferPool, data []byte) mem.BufferSlice {
	return mem.BufferSlice{mem.Copy(data, pool)}
}

func TestCodecName(t *testing.T) {
	if got := (Codec{}).Name(); got != "proto" {
		t.Fatalf("Name() = %q, want %q: the name becomes the wire content-subtype", got, "proto")
	}
}

// TestCodecNotRegisteredGlobally guards the single most destructive mistake this package could
// make: binding the name "proto" process-wide would swap the real proto codec out from under every
// other gRPC client in the binary, the health checker included.
func TestCodecNotRegisteredGlobally(t *testing.T) {
	got := encoding.GetCodecV2("proto")
	if got == nil {
		t.Fatal(`encoding.GetCodecV2("proto") = nil, want the stdlib proto codec`)
	}
	if _, isOurs := got.(Codec); isOurs {
		t.Fatal(`encoding.GetCodecV2("proto") returned proxy.Codec: importing this package must not replace the global proto codec`)
	}
}

func TestCodecMarshal(t *testing.T) {
	pool := &countingPool{}
	slice := pooledSlice(pool, bytes.Repeat([]byte{'a'}, pooledSize))
	defer slice.Free()

	tests := []struct {
		name    string
		in      any
		wantErr string
	}{
		{name: "slice", in: slice},
		{name: "pointer to slice", in: &slice},
		{name: "empty slice", in: mem.BufferSlice{}},
		{name: "wrong type", in: "hello", wantErr: "string"},
		{name: "wrong pointer type", in: &struct{}{}, wantErr: "*struct {}"},
		{name: "nil", in: nil, wantErr: "<nil>"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := (Codec{}).Marshal(tc.in)
			if tc.wantErr != "" {
				if err == nil {
					out.Free()
					t.Fatalf("Marshal(%T) = nil error, want an error naming the type", tc.in)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Marshal error = %q, want it to name %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			// gRPC frees what Marshal returns; the caller's own slice must survive that.
			out.Free()
		})
	}

	if got := pool.puts.Load(); got != 0 {
		t.Fatalf("pool.Put called %d times while the caller still holds the slice, want 0", got)
	}
	if !bytes.Equal(slice.Materialize(), bytes.Repeat([]byte{'a'}, pooledSize)) {
		t.Fatal("slice contents changed after Marshal")
	}
}

func TestCodecUnmarshal(t *testing.T) {
	pool := &countingPool{}
	payload := bytes.Repeat([]byte{'z'}, pooledSize)

	tests := []struct {
		name    string
		into    any
		wantErr string
	}{
		{name: "pointer to slice", into: new(mem.BufferSlice)},
		{name: "value slice", into: mem.BufferSlice{}, wantErr: "mem.BufferSlice"},
		{name: "wrong type", into: new(string), wantErr: "*string"},
		{name: "nil", into: nil, wantErr: "<nil>"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := pooledSlice(pool, payload)
			err := (Codec{}).Unmarshal(data, tc.into)
			// gRPC frees data as soon as Unmarshal returns.
			data.Free()

			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("Unmarshal into %T = nil error, want an error naming the type", tc.into)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Unmarshal error = %q, want it to name %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			got := tc.into.(*mem.BufferSlice)
			if !bytes.Equal(got.Materialize(), payload) {
				t.Fatal("Unmarshal did not retain the payload past the caller's Free")
			}
			got.Free()
		})
	}
}

// TestCodecReferenceOwnership pins down the rule the whole proxy depends on: a slice obtained from
// RecvMsg carries exactly one reference owned by the proxy, and handing it to SendMsg is
// reference-neutral, so the same slice can be replayed onto a retry and still owes exactly one Free.
func TestCodecReferenceOwnership(t *testing.T) {
	pool := &countingPool{}
	c := Codec{}

	// What gRPC does on RecvMsg: hand the transport's slice to Unmarshal, then free its own copy.
	incoming := pooledSlice(pool, bytes.Repeat([]byte{'q'}, pooledSize))
	var owned mem.BufferSlice
	if err := c.Unmarshal(incoming, &owned); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	incoming.Free()
	if got := pool.puts.Load(); got != 0 {
		t.Fatalf("buffer returned to the pool after Unmarshal (puts=%d), want the proxy to still own it", got)
	}

	// What gRPC does on SendMsg, twice, standing in for an attempt and its retry.
	for i := range 2 {
		out, err := c.Marshal(owned)
		if err != nil {
			t.Fatalf("Marshal %d: %v", i, err)
		}
		out.Free()
		if got := pool.puts.Load(); got != 0 {
			t.Fatalf("send %d returned the buffer to the pool (puts=%d): a replayed first message would read freed memory", i, got)
		}
	}

	owned.Free()
	if got := pool.puts.Load(); got != 1 {
		t.Fatalf("pool.Put called %d times after the owner's single Free, want 1", got)
	}
	if got, want := pool.gets.Load(), int64(1); got != want {
		t.Fatalf("pool.Get called %d times, want %d", got, want)
	}
}
