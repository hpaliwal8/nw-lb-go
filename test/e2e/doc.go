// Package e2e holds the whole-system integration test for the load balancer.
//
// It carries no production code. The tests live in the external test package e2e_test and build
// the entire stack in process — real backends, real pool, real health checker, real balancer,
// real proxy behind a real grpc.Server — so that nothing between the generated client and the
// upstream handlers is a stub.
package e2e
