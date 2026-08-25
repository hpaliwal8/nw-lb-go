package middleware

import "google.golang.org/grpc"

// passThrough is the identity interceptor. Returning it lets a disabled feature cost one extra
// call instead of a branch on every request.
func passThrough(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, ss)
}

// Chain composes interceptors into one, with the first argument outermost. The fold runs
// right-to-left so each interceptor's "handler" is the rest of the chain, resolved once at
// construction rather than per request. Nil entries are dropped.
func Chain(is ...grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	live := make([]grpc.StreamServerInterceptor, 0, len(is))
	for _, i := range is {
		if i != nil {
			live = append(live, i)
		}
	}

	switch len(live) {
	case 0:
		return passThrough
	case 1:
		return live[0]
	}

	chained := live[len(live)-1]
	for i := len(live) - 2; i >= 0; i-- {
		chained = link(live[i], chained)
	}
	return chained
}

func link(outer, inner grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return outer(srv, ss, info, func(srv any, ss grpc.ServerStream) error {
			return inner(srv, ss, info, handler)
		})
	}
}
