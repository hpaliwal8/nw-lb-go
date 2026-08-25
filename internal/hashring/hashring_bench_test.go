package hashring

import "testing"

func benchRing() (*Ring, []string) {
	ids := []string{"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9"}
	return New(200, members(ids...)...), keys(1024)
}

func BenchmarkGet(b *testing.B) {
	r, ks := benchRing()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, ok := r.Get(ks[i&1023]); !ok {
			b.Fatal("Get failed on a populated ring")
		}
	}
}

func BenchmarkGetN3(b *testing.B) {
	r, ks := benchRing()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if got := r.GetN(ks[i&1023], 3); len(got) != 3 {
			b.Fatalf("GetN returned %d ids, want 3", len(got))
		}
	}
}

func BenchmarkGetParallel(b *testing.B) {
	r, ks := benchRing()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			r.Get(ks[i&1023])
			i++
		}
	})
}

func BenchmarkSet(b *testing.B) {
	ms := members("b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9")
	r := New(200)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		r.Set(ms)
	}
}
