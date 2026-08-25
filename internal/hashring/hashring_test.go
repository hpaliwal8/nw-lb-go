package hashring

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
)

func members(ids ...string) []Member {
	out := make([]Member, 0, len(ids))
	for _, id := range ids {
		out = append(out, Member{ID: id, Weight: 100})
	}
	return out
}

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("session-%d", i)
	}
	return out
}

func TestAccessors(t *testing.T) {
	tests := []struct {
		name         string
		virtualNodes int
		members      []Member
		wantLen      int
		wantVNodes   int
		wantIDs      []string
	}{
		{name: "empty", virtualNodes: 100, wantLen: 0, wantVNodes: 0, wantIDs: nil},
		{
			name: "single", virtualNodes: 10, members: members("a"),
			wantLen: 1, wantVNodes: 10, wantIDs: []string{"a"},
		},
		{
			name: "sorted by id", virtualNodes: 10, members: members("c", "a", "b"),
			wantLen: 3, wantVNodes: 30, wantIDs: []string{"a", "b", "c"},
		},
		{
			name:         "weighted",
			virtualNodes: 100,
			members:      []Member{{ID: "a", Weight: 100}, {ID: "b", Weight: 200}, {ID: "c", Weight: 50}},
			wantLen:      3, wantVNodes: 350, wantIDs: []string{"a", "b", "c"},
		},
		{
			name:         "weight floor of one",
			virtualNodes: 10,
			members:      []Member{{ID: "a", Weight: 1}},
			wantLen:      1, wantVNodes: 1, wantIDs: []string{"a"},
		},
		{
			name:         "non positive weight defaults to 100",
			virtualNodes: 50,
			members:      []Member{{ID: "a"}, {ID: "b", Weight: -7}},
			wantLen:      2, wantVNodes: 100, wantIDs: []string{"a", "b"},
		},
		{
			name: "duplicate ids collapse", virtualNodes: 10,
			members: []Member{{ID: "a", Weight: 100}, {ID: "a", Weight: 200}},
			wantLen: 1, wantVNodes: 20, wantIDs: []string{"a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(tt.virtualNodes, tt.members...)
			if got := r.Len(); got != tt.wantLen {
				t.Errorf("Len() = %d, want %d", got, tt.wantLen)
			}
			if got := r.VirtualNodes(); got != tt.wantVNodes {
				t.Errorf("VirtualNodes() = %d, want %d", got, tt.wantVNodes)
			}
			var gotIDs []string
			for _, m := range r.Members() {
				gotIDs = append(gotIDs, m.ID)
			}
			if !slices.Equal(gotIDs, tt.wantIDs) {
				t.Errorf("Members() ids = %v, want %v", gotIDs, tt.wantIDs)
			}
		})
	}
}

func TestEmptyRing(t *testing.T) {
	for _, r := range map[string]*Ring{"new": New(100), "emptied": New(100, members("a", "b")...)} {
		r.Set(nil)
		if id, ok := r.Get("k"); ok || id != "" {
			t.Errorf("Get on empty ring = (%q, %v), want (\"\", false)", id, ok)
		}
		if got := r.GetN("k", 3); got != nil {
			t.Errorf("GetN on empty ring = %v, want nil", got)
		}
		if got := r.Len(); got != 0 {
			t.Errorf("Len = %d, want 0", got)
		}
		if got := r.VirtualNodes(); got != 0 {
			t.Errorf("VirtualNodes = %d, want 0", got)
		}
	}
}

func TestHashesAreSorted(t *testing.T) {
	r := New(200, members("a", "b", "c", "d")...)
	s := r.snap.Load()
	if !slices.IsSorted(s.hashes) {
		t.Fatal("snapshot hashes are not sorted ascending")
	}
	if len(s.hashes) != len(s.owners) {
		t.Fatalf("parallel arrays diverge: %d hashes, %d owners", len(s.hashes), len(s.owners))
	}
}

func TestDeterministicAcrossInsertionOrder(t *testing.T) {
	base := members("alpha", "beta", "gamma", "delta", "epsilon")
	ref := New(200, base...)
	ks := keys(1000)

	rng := rand.New(rand.NewPCG(42, 1337))
	for round := range 8 {
		shuffled := slices.Clone(base)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

		other := New(200, shuffled...)
		if got, want := other.VirtualNodes(), ref.VirtualNodes(); got != want {
			t.Fatalf("round %d: VirtualNodes = %d, want %d", round, got, want)
		}
		if !slices.Equal(other.snap.Load().hashes, ref.snap.Load().hashes) {
			t.Fatalf("round %d: ring points differ for identical membership", round)
		}
		if !slices.Equal(other.snap.Load().owners, ref.snap.Load().owners) {
			t.Fatalf("round %d: ring owners differ for identical membership", round)
		}
		for _, k := range ks {
			want, _ := ref.Get(k)
			got, _ := other.Get(k)
			if got != want {
				t.Fatalf("round %d: Get(%q) = %q, want %q", round, k, got, want)
			}
		}
	}
}

func TestDeterministicAcrossMutationPath(t *testing.T) {
	built := New(200)
	for _, m := range members("d", "a", "c", "b") {
		built.Add(m)
	}
	built.Add(Member{ID: "zz", Weight: 100})
	built.Remove("zz")

	ref := New(200, members("a", "b", "c", "d")...)
	for _, k := range keys(1000) {
		want, _ := ref.Get(k)
		got, _ := built.Get(k)
		if got != want {
			t.Fatalf("Get(%q) after Add/Remove = %q, want %q", k, got, want)
		}
	}
}

func TestGetN(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e"}
	r := New(200, members(ids...)...)

	tests := []struct {
		name    string
		n       int
		wantLen int
	}{
		{name: "zero", n: 0, wantLen: 0},
		{name: "negative", n: -3, wantLen: 0},
		{name: "one", n: 1, wantLen: 1},
		{name: "three", n: 3, wantLen: 3},
		{name: "exactly all", n: 5, wantLen: 5},
		{name: "more than members", n: 12, wantLen: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range keys(500) {
				got := r.GetN(k, tt.n)
				if len(got) != tt.wantLen {
					t.Fatalf("GetN(%q, %d) returned %d ids, want %d", k, tt.n, len(got), tt.wantLen)
				}
				seen := make(map[string]bool, len(got))
				for _, id := range got {
					if seen[id] {
						t.Fatalf("GetN(%q, %d) = %v contains duplicate %q", k, tt.n, got, id)
					}
					if !slices.Contains(ids, id) {
						t.Fatalf("GetN(%q, %d) returned unknown id %q", k, tt.n, id)
					}
					seen[id] = true
				}
				if tt.wantLen > 0 {
					primary, ok := r.Get(k)
					if !ok || got[0] != primary {
						t.Fatalf("GetN(%q, %d)[0] = %q, want primary %q", k, tt.n, got[0], primary)
					}
				}
			}
		})
	}
}

func TestGetNIsPrefixStable(t *testing.T) {
	r := New(200, members("a", "b", "c", "d", "e", "f")...)
	for _, k := range keys(500) {
		full := r.GetN(k, 6)
		for n := 1; n <= 6; n++ {
			if got := r.GetN(k, n); !slices.Equal(got, full[:n]) {
				t.Fatalf("GetN(%q, %d) = %v, want prefix %v", k, n, got, full[:n])
			}
		}
	}
}

func TestGetNSingleMember(t *testing.T) {
	r := New(50, members("only")...)
	got := r.GetN("k", 4)
	if !slices.Equal(got, []string{"only"}) {
		t.Fatalf("GetN = %v, want [only]", got)
	}
}

func TestAddRemove(t *testing.T) {
	r := New(100, members("a", "b")...)

	r.Add(Member{ID: "c", Weight: 100})
	if got := r.Len(); got != 3 {
		t.Fatalf("Len after Add = %d, want 3", got)
	}

	r.Add(Member{ID: "c", Weight: 200})
	if got := r.Len(); got != 3 {
		t.Fatalf("Len after re-Add = %d, want 3 (replace, not append)", got)
	}
	if got := r.VirtualNodes(); got != 100+100+200 {
		t.Fatalf("VirtualNodes after re-Add = %d, want 400", got)
	}

	r.Remove("missing")
	if got := r.Len(); got != 3 {
		t.Fatalf("Len after removing unknown id = %d, want 3", got)
	}

	r.Remove("c")
	if got := r.Len(); got != 2 {
		t.Fatalf("Len after Remove = %d, want 2", got)
	}
	for _, m := range r.Members() {
		if m.ID == "c" {
			t.Fatal("removed member still present")
		}
	}

	r.Remove("a")
	r.Remove("b")
	if _, ok := r.Get("k"); ok {
		t.Fatal("Get succeeded after removing every member")
	}
}

func TestMembersReturnsCopy(t *testing.T) {
	r := New(100, members("a", "b")...)
	got := r.Members()
	got[0].ID = "mutated"
	got[0].Weight = 9999
	if again := r.Members(); again[0].ID != "a" || again[0].Weight != 100 {
		t.Fatalf("mutating the Members() result changed the ring: %+v", again[0])
	}
}

func TestMonotonicRemapping(t *testing.T) {
	const (
		vnodes  = 200
		numKeys = 100_000
	)
	ids := []string{"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9"}
	r := New(vnodes, members(ids...)...)
	ks := keys(numKeys)

	before := make([]string, numKeys)
	for i, k := range ks {
		owner, ok := r.Get(k)
		if !ok {
			t.Fatalf("Get(%q) failed on a populated ring", k)
		}
		before[i] = owner
	}

	const removed = "b4"
	r.Remove(removed)

	movedFromRemoved, movedOther := 0, 0
	for i, k := range ks {
		owner, ok := r.Get(k)
		if !ok {
			t.Fatalf("Get(%q) failed after removal", k)
		}
		if owner == removed {
			t.Fatalf("Get(%q) still returns the removed member", k)
		}
		switch {
		case before[i] == removed:
			movedFromRemoved++
		case owner != before[i]:
			movedOther++
		}
	}

	if movedOther != 0 {
		t.Errorf("%d keys not owned by %s were remapped, want 0", movedOther, removed)
	}
	if movedFromRemoved == 0 {
		t.Errorf("no keys were owned by %s before removal; test is not exercising remapping", removed)
	}
}

func TestMonotonicAddition(t *testing.T) {
	ids := []string{"b0", "b1", "b2", "b3", "b4"}
	r := New(200, members(ids...)...)
	ks := keys(50_000)

	before := make([]string, len(ks))
	for i, k := range ks {
		before[i], _ = r.Get(k)
	}

	r.Add(Member{ID: "b5", Weight: 100})

	badMoves := 0
	for i, k := range ks {
		owner, _ := r.Get(k)
		if owner != before[i] && owner != "b5" {
			badMoves++
		}
	}
	if badMoves != 0 {
		t.Errorf("%d keys moved between pre-existing members after an addition, want 0", badMoves)
	}
}

func TestBalance(t *testing.T) {
	const (
		vnodes  = 200
		numKeys = 100_000
	)
	ids := []string{"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9"}
	r := New(vnodes, members(ids...)...)

	load := make(map[string]int, len(ids))
	for _, k := range keys(numKeys) {
		owner, _ := r.Get(k)
		load[owner]++
	}

	if len(load) != len(ids) {
		t.Fatalf("only %d of %d members received keys", len(load), len(ids))
	}
	mean := float64(numKeys) / float64(len(ids))
	maxLoad := 0
	for _, id := range ids {
		if load[id] > maxLoad {
			maxLoad = load[id]
		}
	}
	if ratio := float64(maxLoad) / mean; ratio >= 1.5 {
		t.Errorf("max/mean load = %.3f, want < 1.5 (load: %v)", ratio, load)
	}
}

func TestWeightedDistribution(t *testing.T) {
	const numKeys = 100_000
	ms := []Member{
		{ID: "w100-a", Weight: 100},
		{ID: "w100-b", Weight: 100},
		{ID: "w100-c", Weight: 100},
		{ID: "w100-d", Weight: 100},
		{ID: "w200", Weight: 200},
	}
	r := New(400, ms...)

	load := make(map[string]int, len(ms))
	for _, k := range keys(numKeys) {
		owner, _ := r.Get(k)
		load[owner]++
	}

	baseTotal := 0
	for _, m := range ms[:4] {
		baseTotal += load[m.ID]
	}
	baseMean := float64(baseTotal) / 4
	if baseMean == 0 {
		t.Fatal("weight-100 members received no keys")
	}
	ratio := float64(load["w200"]) / baseMean
	if ratio < 1.5 || ratio > 2.5 {
		t.Errorf("weight-200 share / weight-100 share = %.3f, want within [1.5, 2.5] (load: %v)", ratio, load)
	}
}

func TestConcurrentReadsDuringWrites(t *testing.T) {
	r := New(200, members("a", "b", "c")...)
	ks := keys(256)

	var wg sync.WaitGroup
	done := make(chan struct{})

	for g := range 4 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			i := g
			for {
				select {
				case <-done:
					return
				default:
				}
				k := ks[i%len(ks)]
				i++
				if id, ok := r.Get(k); ok && id == "" {
					t.Errorf("Get returned an empty id with ok=true")
					return
				}
				got := r.GetN(k, 3)
				seen := make(map[string]bool, len(got))
				for _, id := range got {
					if seen[id] {
						t.Errorf("GetN returned duplicate id %q in %v", id, got)
						return
					}
					seen[id] = true
				}
				r.Members()
				r.Len()
				r.VirtualNodes()
			}
		}(g)
	}

	sets := [][]Member{
		members("a", "b", "c"),
		members("a", "b", "c", "d", "e"),
		members("x"),
		nil,
		members("a", "d", "f", "g"),
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for i := range 200 {
			r.Set(sets[i%len(sets)])
			r.Add(Member{ID: "transient", Weight: 100})
			r.Remove("transient")
		}
	}()

	wg.Wait()
}
