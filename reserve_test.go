// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nuid

import (
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingRand produces deterministic, distinct byte streams per reader.
type recordingRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func newRecordingRand(seed int64) *recordingRand {
	return &recordingRand{r: rand.New(rand.NewSource(seed))}
}

func (rr *recordingRand) Read(p []byte) (int, error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	for i := range p {
		p[i] = byte(rr.r.Intn(256))
	}
	return len(p), nil
}

// repeatingRand returns the same bytes forever, simulating a reset source.
type repeatingRand struct{}

func (repeatingRand) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(i)
	}
	return len(p), nil
}

func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func newTestReserver(t *testing.T, store Store, opts ...Option) *Reserver {
	t.Helper()
	return newTestReserverWithSeed(t, store, 42, opts...)
}

func newTestReserverWithSeed(t *testing.T, store Store, seed int64, opts ...Option) *Reserver {
	t.Helper()
	base := []Option{
		WithRand(newRecordingRand(seed)),
		WithClock(fixedClock(time.Unix(1700000000, 0))),
	}
	r, err := NewReserver(store, append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewReserver: %v", err)
	}
	return r
}

func TestReserveBasic(t *testing.T) {
	r := newTestReserver(t, NewMemoryStore())

	batch, err := r.Reserve(100)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if got, want := batch.Lo(), int64(0); got != want {
		t.Fatalf("Lo = %d, want %d", got, want)
	}
	if got, want := batch.Hi(), int64(100); got != want {
		t.Fatalf("Hi = %d, want %d", got, want)
	}
	if got, want := batch.Len(), int64(100); got != want {
		t.Fatalf("Len = %d, want %d", got, want)
	}
	if !ValidPrefix(batch.Prefix()) {
		t.Fatalf("invalid prefix %q", batch.Prefix())
	}
	if got, want := batch.First(), encodeSeq(batch.Prefix(), batch.Lo()); got != want {
		t.Fatalf("First = %q, want %q", got, want)
	}
	if got, want := batch.Last(), encodeSeq(batch.Prefix(), batch.Hi()-1); got != want {
		t.Fatalf("Last = %q, want %q", got, want)
	}
	if id, ok := batch.At(batch.Lo()); !ok || id != batch.First() {
		t.Fatalf("At(Lo) = %q,%v", id, ok)
	}
	if _, ok := batch.At(batch.Hi()); ok {
		t.Fatal("At(Hi) must be outside the reservation")
	}
	if _, ok := batch.At(batch.Lo() - 1); ok {
		t.Fatal("At(Lo-1) must be outside the reservation")
	}

	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		id, ok := batch.Next()
		if !ok {
			t.Fatalf("Next exhausted after %d IDs", i)
		}
		if !IsValid(id) {
			t.Fatalf("invalid NUID %q", id)
		}
		if id[:preLen] != batch.Prefix() {
			t.Fatalf("prefix mismatch: %q vs %q", id[:preLen], batch.Prefix())
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate ID %q", id)
		}
		seen[id] = struct{}{}
	}
	if _, ok := batch.Next(); ok {
		t.Fatal("expected Next to be exhausted")
	}
	if got := batch.Remaining(); got != 0 {
		t.Fatalf("Remaining = %d, want 0", got)
	}

	if got, want := r.HighWater(), int64(100); got != want {
		t.Fatalf("HighWater = %d, want %d", got, want)
	}
	if got, want := r.Remaining(), maxSeq-100; got != want {
		t.Fatalf("Remaining = %d, want %d", got, want)
	}
}

func TestReserveBatchesAreDisjointAndUseFreshPrefixes(t *testing.T) {
	r := newTestReserver(t, NewMemoryStore())

	ids := make(map[string]struct{})
	prefixes := make(map[string]struct{})
	var prevHi int64
	for i := 0; i < 10; i++ {
		batch, err := r.Reserve(50)
		if err != nil {
			t.Fatalf("Reserve %d: %v", i, err)
		}
		if batch.Lo() != prevHi {
			t.Fatalf("batch %d Lo = %d, want %d", i, batch.Lo(), prevHi)
		}
		prevHi = batch.Hi()
		if _, dup := prefixes[batch.Prefix()]; dup {
			t.Fatalf("batch %d reused prefix %q", i, batch.Prefix())
		}
		prefixes[batch.Prefix()] = struct{}{}
		for {
			id, ok := batch.Next()
			if !ok {
				break
			}
			if _, dup := ids[id]; dup {
				t.Fatalf("duplicate ID across batches: %q", id)
			}
			ids[id] = struct{}{}
		}
	}
}

func TestPersistedBeforeUseAndRecoverySkipsWholeBatch(t *testing.T) {
	store := NewMemoryStore()
	r := newTestReserver(t, store)

	batch, err := r.Reserve(1000)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	// Consume only part of the batch, then simulate a crash: the persisted
	// high-water mark must already cover the whole batch.
	for i := 0; i < 10; i++ {
		if _, ok := batch.Next(); !ok {
			t.Fatal("unexpected exhaustion")
		}
	}
	snap, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if snap.HighWater != batch.Hi() {
		t.Fatalf("persisted high water = %d, want %d", snap.HighWater, batch.Hi())
	}

	// Recover with a fresh reserver on the same store, as a new process would.
	r2 := newTestReserverWithSeed(t, store, 43)
	if got := r2.HighWater(); got != batch.Hi() {
		t.Fatalf("recovered high water = %d, want %d", got, batch.Hi())
	}
	next, err := r2.Reserve(100)
	if err != nil {
		t.Fatalf("Reserve after recovery: %v", err)
	}
	if next.Lo() != batch.Hi() {
		t.Fatalf("next Lo = %d, want %d (must skip the previous batch)", next.Lo(), batch.Hi())
	}
	if next.Lo() < batch.Hi() {
		t.Fatal("recovery reissued a range that could have been published")
	}
}

func TestFileStoreRoundTripAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nuid.snapshot")
	store := NewFileStore(path)

	r := newTestReserver(t, store)
	batch, err := r.Reserve(256)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	var issued []string
	for {
		id, ok := batch.Next()
		if !ok {
			break
		}
		issued = append(issued, id)
	}

	// A fresh store instance (new process) reads the same file.
	r2 := newTestReserverWithSeed(t, NewFileStore(path), 44)
	next, err := r2.Reserve(256)
	if err != nil {
		t.Fatalf("Reserve after reopen: %v", err)
	}
	if next.Lo() != batch.Hi() {
		t.Fatalf("recovered Lo = %d, want %d", next.Lo(), batch.Hi())
	}
	prev := make(map[string]struct{}, len(issued))
	for _, id := range issued {
		prev[id] = struct{}{}
	}
	for {
		id, ok := next.Next()
		if !ok {
			break
		}
		if _, dup := prev[id]; dup {
			t.Fatalf("recovered batch reused %q", id)
		}
	}
}

func TestCommitIsIdempotent(t *testing.T) {
	store := NewMemoryStore()
	snap := Snapshot{Version: snapshotVersion, Prefix: "0123456789AB", HighWater: 100, Epoch: 1, Updated: 1}
	got, applied, err := store.Commit(snap)
	if err != nil || !applied || got != snap {
		t.Fatalf("first Commit = %+v, %v, %v", got, applied, err)
	}
	got, applied, err = store.Commit(snap)
	if err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	if applied {
		t.Fatal("second Commit must be an idempotent no-op")
	}
	if got != snap {
		t.Fatalf("second Commit returned %+v, want %+v", got, snap)
	}
}

func TestFileStoreCommitIsIdempotentAndMonotonic(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "snap"))
	newer := Snapshot{Version: snapshotVersion, Prefix: "0123456789AB", HighWater: 500, Epoch: 2, Updated: 20}
	older := Snapshot{Version: snapshotVersion, Prefix: "ABCDEFGHIJKL", HighWater: 100, Epoch: 1, Updated: 10}

	if _, applied, err := store.Commit(newer); err != nil || !applied {
		t.Fatalf("Commit(newer) = %v, %v", applied, err)
	}
	if got, applied, err := store.Commit(newer); err != nil || applied || got != newer {
		t.Fatalf("duplicate Commit(newer) = %+v, %v, %v", got, applied, err)
	}
	// A late commit from a stale process must not lower the mark.
	got, applied, err := store.Commit(older)
	if err != nil {
		t.Fatalf("Commit(older): %v", err)
	}
	if applied {
		t.Fatal("stale commit must not be applied")
	}
	if got.HighWater != newer.HighWater {
		t.Fatalf("high water regressed to %d", got.HighWater)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.HighWater != newer.HighWater {
		t.Fatalf("persisted high water = %d, want %d", loaded.HighWater, newer.HighWater)
	}
}

func TestStaleReserverCannotLowerHighWater(t *testing.T) {
	store := NewMemoryStore()
	// Simulate a newer process that already reserved far ahead.
	_, _, err := store.Commit(Snapshot{Version: snapshotVersion, Prefix: "0123456789AB", HighWater: 1 << 20, Epoch: 10, Updated: 1})
	if err != nil {
		t.Fatalf("seed commit: %v", err)
	}

	// A reserver that loaded an empty store earlier tries to commit its stale
	// snapshot. Store.Commit must reject it.
	stale := Snapshot{Version: snapshotVersion, Prefix: "ABCDEFGHIJKL", HighWater: 10, Epoch: 1, Updated: 1}
	got, applied, err := store.Commit(stale)
	if err != nil {
		t.Fatalf("Commit(stale): %v", err)
	}
	if applied || got.HighWater != 1<<20 {
		t.Fatalf("stale commit applied=%v highwater=%d", applied, got.HighWater)
	}
}

// raceStore lets a competing reservation win exactly once, then behaves like a
// normal MemoryStore. It simulates another process committing between our Load
// and Commit.
type raceStore struct {
	*MemoryStore
	winner     Snapshot
	raced      bool
	committed  []Snapshot
	commitHook func(Snapshot)
}

func (r *raceStore) Commit(s Snapshot) (Snapshot, bool, error) {
	if r.commitHook != nil {
		r.commitHook(s)
	}
	if !r.raced {
		r.raced = true
		if _, _, err := r.MemoryStore.Commit(r.winner); err != nil {
			return Snapshot{}, false, err
		}
	}
	r.committed = append(r.committed, s)
	return r.MemoryStore.Commit(s)
}

func TestReserveRetriesWhenAnotherWriterAdvances(t *testing.T) {
	// The winner reserves [0, 500) but only 100 values were "used"; a crashed
	// process can never reclaim them, so the loser must start at 500.
	winner := Snapshot{Version: snapshotVersion, Prefix: "ABCDEFGHIJKL", HighWater: 500, Epoch: 3, Updated: 1}
	store := &raceStore{MemoryStore: NewMemoryStore(), winner: winner}

	r := newTestReserver(t, store)
	batch, err := r.Reserve(10)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if batch.Lo() != winner.HighWater {
		t.Fatalf("Lo = %d, want %d (must not overlap the winning reservation)", batch.Lo(), winner.HighWater)
	}
	if batch.Hi() != winner.HighWater+10 {
		t.Fatalf("Hi = %d, want %d", batch.Hi(), winner.HighWater+10)
	}
	if got, want := r.HighWater(), batch.Hi(); got != want {
		t.Fatalf("HighWater = %d, want %d", got, want)
	}
	if len(store.committed) != 2 {
		t.Fatalf("expected 2 commit attempts, got %d", len(store.committed))
	}
}

func TestReserveExhaustedWhenCompetingWriterTakesEverything(t *testing.T) {
	winner := Snapshot{Version: snapshotVersion, Prefix: "ABCDEFGHIJKL", HighWater: maxSeq, Epoch: 3, Updated: 1}
	store := &raceStore{MemoryStore: NewMemoryStore(), winner: winner}

	r := newTestReserver(t, store)
	if _, err := r.Reserve(10); !errors.Is(err, ErrReservationExhausted) {
		t.Fatalf("Reserve = %v, want ErrReservationExhausted", err)
	}
}

func TestReserveConflictAfterRepeatedLosses(t *testing.T) {
	// A store that always reports that another writer reserved first.
	store := &alwaysBehindStore{MemoryStore: NewMemoryStore()}
	r := newTestReserver(t, store)
	if _, err := r.Reserve(10); !errors.Is(err, ErrReservationConflict) {
		t.Fatalf("Reserve = %v, want ErrReservationConflict", err)
	}
	if store.calls != maxReserveAttempts {
		t.Fatalf("commit attempts = %d, want %d", store.calls, maxReserveAttempts)
	}
}

type alwaysBehindStore struct {
	*MemoryStore
	calls int
}

func (s *alwaysBehindStore) Commit(snap Snapshot) (Snapshot, bool, error) {
	s.calls++
	// Simulate a competing process that keeps winning the race: persist a
	// higher mark and report our snapshot as not applied.
	advanced := Snapshot{
		Version:   snapshotVersion,
		Prefix:    "0123456789AB",
		HighWater: snap.HighWater + 1000,
		Epoch:     snap.Epoch,
		Updated:   snap.Updated,
	}
	if _, _, err := s.MemoryStore.Commit(advanced); err != nil {
		return Snapshot{}, false, err
	}
	effective, _ := s.MemoryStore.Load()
	return effective, false, nil
}

func TestStoreIsCommittedBeforeReservationIsReturned(t *testing.T) {
	store := &raceStore{MemoryStore: NewMemoryStore(), raced: true}
	var persistedAtReturn int64 = -1
	store.commitHook = func(s Snapshot) {
		persistedAtReturn = s.HighWater
	}

	r := newTestReserver(t, store)
	batch, err := r.Reserve(77)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if persistedAtReturn != batch.Hi() {
		t.Fatalf("commit saw high water %d, batch is [%d,%d)", persistedAtReturn, batch.Lo(), batch.Hi())
	}
	if batch.Hi() > persistedAtReturn {
		t.Fatal("reservation exposes values above the persisted high-water mark")
	}
}

func TestReservationBoundsAndID(t *testing.T) {
	r := newTestReserver(t, NewMemoryStore())
	batch, err := r.Reserve(3)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	lo, hi := batch.Bounds()
	if lo != batch.Lo() || hi != batch.Hi() {
		t.Fatalf("Bounds = [%d,%d), want [%d,%d)", lo, hi, batch.Lo(), batch.Hi())
	}

	var ids []string
	for i := 0; i < 3; i++ {
		id, err := batch.ID()
		if err != nil {
			t.Fatalf("ID %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	if ids[0] != batch.First() || ids[2] != batch.Last() {
		t.Fatalf("IDs %v do not match [First,Last] = [%s,%s]", ids, batch.First(), batch.Last())
	}
	if _, err := batch.ID(); !errors.Is(err, ErrBatchExhausted) {
		t.Fatalf("ID after exhaustion = %v, want ErrBatchExhausted", err)
	}
}

func TestReserverSnapshotReflectsCommittedState(t *testing.T) {
	store := NewMemoryStore()
	r := newTestReserver(t, store)
	batch, err := r.Reserve(42)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	snap := r.Snapshot()
	if err := snap.Validate(); err != nil {
		t.Fatalf("Snapshot.Validate: %v", err)
	}
	if snap.HighWater != batch.Hi() || snap.Prefix != batch.Prefix() {
		t.Fatalf("Snapshot = %+v, want high water %d prefix %q", snap, batch.Hi(), batch.Prefix())
	}
	persisted, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if snap != persisted {
		t.Fatalf("Snapshot = %+v, persisted = %+v", snap, persisted)
	}
}

func TestSeparateFileStoreInstancesCoordinate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap")

	const instances = 4
	const each = 5
	const size = 32
	ids := make(chan string, instances*each*size)
	errs := make(chan error, instances)
	var wg sync.WaitGroup
	for i := 0; i < instances; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each goroutine has its own FileStore and Reserver, like a
			// separate process sharing the snapshot file.
			r, err := NewReserver(NewFileStore(path),
				WithRand(newRecordingRand(int64(5000+i))),
				WithClock(fixedClock(time.Unix(1700000000, 0))))
			if err != nil {
				errs <- err
				return
			}
			for j := 0; j < each; j++ {
				b, err := r.Reserve(size)
				if err != nil {
					errs <- err
					return
				}
				for {
					id, ok := b.Next()
					if !ok {
						break
					}
					ids <- id
				}
			}
		}(i)
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatalf("reserving: %v", err)
	}

	seen := make(map[string]struct{}, instances*each*size)
	for id := range ids {
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate ID across separate stores: %q", id)
		}
		seen[id] = struct{}{}
	}
	if got, want := len(seen), instances*each*size; got != want {
		t.Fatalf("got %d IDs, want %d", got, want)
	}
}

func TestFileStoreRejectsInvalidSnapshot(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "snap"))
	if _, _, err := store.Commit(Snapshot{Version: 99}); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("Commit(invalid) = %v, want ErrInvalidSnapshot", err)
	}
	// Nothing should have been written.
	if _, err := store.Load(); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("Load after rejected commit = %v, want ErrNoSnapshot", err)
	}
}

func TestBatchIDsAreValidAndSharePrefix(t *testing.T) {
	r := newTestReserver(t, NewMemoryStore())
	batch, err := r.Reserve(1000)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	prefix := batch.Prefix()
	for _, c := range prefix {
		if !strings.ContainsRune(digits, c) {
			t.Fatalf("prefix %q contains non-base62 character %q", prefix, c)
		}
	}
	for {
		id, ok := batch.Next()
		if !ok {
			break
		}
		if !IsValid(id) {
			t.Fatalf("invalid ID %q", id)
		}
		if id[:preLen] != prefix {
			t.Fatalf("ID %q does not use batch prefix %q", id, prefix)
		}
	}
}

func TestConcurrentReserversShareStore(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "snap"))

	const writers = 8
	const perWriter = 4
	const batchSize = 64

	results := make([][]string, writers)
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r, err := NewReserver(store,
				WithRand(newRecordingRand(int64(1000+w))),
				WithClock(fixedClock(time.Unix(1700000000, 0))))
			if err != nil {
				errs <- err
				return
			}
			for i := 0; i < perWriter; i++ {
				b, err := r.Reserve(batchSize)
				if err != nil {
					errs <- err
					return
				}
				for {
					id, ok := b.Next()
					if !ok {
						break
					}
					results[w] = append(results[w], id)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent reserving: %v", err)
	}

	all := make(map[string]struct{}, writers*perWriter*batchSize)
	for _, ids := range results {
		for _, id := range ids {
			if _, dup := all[id]; dup {
				t.Fatalf("duplicate ID across reservers: %q", id)
			}
			all[id] = struct{}{}
		}
	}
	if got, want := len(all), writers*perWriter*batchSize; got != want {
		t.Fatalf("got %d unique IDs, want %d", got, want)
	}
}

func TestReserveRejectsDuplicatePrefix(t *testing.T) {
	store := NewMemoryStore()
	r, err := NewReserver(store,
		WithRand(repeatingRand{}),
		WithClock(fixedClock(time.Unix(1700000000, 0))))
	if err != nil {
		t.Fatalf("NewReserver: %v", err)
	}
	if _, err := r.Reserve(10); err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	_, err = r.Reserve(10)
	if !errors.Is(err, ErrRandomSourceReset) {
		t.Fatalf("second Reserve err = %v, want ErrRandomSourceReset", err)
	}
}

func TestReserveRandomSourceFailure(t *testing.T) {
	r, err := NewReserver(NewMemoryStore(),
		WithRand(errReader{errors.New("boom")}),
		WithClock(fixedClock(time.Unix(1700000000, 0))))
	if err != nil {
		t.Fatalf("NewReserver: %v", err)
	}
	if _, err := r.Reserve(10); err == nil {
		t.Fatal("Reserve with failing random source succeeded")
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestReserveClockRollback(t *testing.T) {
	now := time.Unix(1700000000, 0)
	r, err := NewReserver(NewMemoryStore(),
		WithRand(newRecordingRand(7)),
		WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewReserver: %v", err)
	}
	if _, err := r.Reserve(10); err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	now = now.Add(-time.Second)
	if _, err := r.Reserve(10); !errors.Is(err, ErrClockRollback) {
		t.Fatalf("Reserve after rollback = %v, want ErrClockRollback", err)
	}
}

func TestNewReserverRejectsFutureSnapshot(t *testing.T) {
	store := NewMemoryStore()
	future := Snapshot{
		Version:   snapshotVersion,
		Prefix:    "0123456789AB",
		HighWater: 10,
		Epoch:     1,
		Updated:   time.Unix(1800000000, 0).UnixNano(),
	}
	if _, _, err := store.Commit(future); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	_, err := NewReserver(store, WithClock(fixedClock(time.Unix(1700000000, 0))))
	if !errors.Is(err, ErrClockRollback) {
		t.Fatalf("NewReserver = %v, want ErrClockRollback", err)
	}
}

func TestReserveExhaustion(t *testing.T) {
	store := NewMemoryStore()
	near := Snapshot{Version: snapshotVersion, Prefix: "0123456789AB", HighWater: maxSeq - 10, Epoch: 1, Updated: 1}
	if _, _, err := store.Commit(near); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	r, err := NewReserver(store,
		WithRand(newRecordingRand(3)),
		WithClock(fixedClock(time.Unix(1700000000, 0))))
	if err != nil {
		t.Fatalf("NewReserver: %v", err)
	}
	if got, want := r.Remaining(), int64(10); got != want {
		t.Fatalf("Remaining = %d, want %d", got, want)
	}
	if _, err := r.Reserve(11); !errors.Is(err, ErrReservationExhausted) {
		t.Fatalf("oversized Reserve = %v, want ErrReservationExhausted", err)
	}
	b, err := r.Reserve(10)
	if err != nil {
		t.Fatalf("exact Reserve: %v", err)
	}
	if b.Hi() != maxSeq {
		t.Fatalf("Hi = %d, want %d", b.Hi(), maxSeq)
	}
	if last := b.Last(); !IsValid(last) || last != encodeSeq(b.Prefix(), maxSeq-1) {
		t.Fatalf("Last = %q", last)
	}
	if _, err := r.Reserve(1); !errors.Is(err, ErrReservationExhausted) {
		t.Fatalf("Reserve after exhaustion = %v, want ErrReservationExhausted", err)
	}
}

func TestReserveInvalidSize(t *testing.T) {
	r := newTestReserver(t, NewMemoryStore())
	for _, n := range []int64{0, -1} {
		if _, err := r.Reserve(n); !errors.Is(err, ErrInvalidReservation) {
			t.Fatalf("Reserve(%d) = %v, want ErrInvalidReservation", n, err)
		}
	}
}

func TestNewReserverRequiresStore(t *testing.T) {
	if _, err := NewReserver(nil); !errors.Is(err, ErrNilStore) {
		t.Fatalf("NewReserver(nil) = %v, want ErrNilStore", err)
	}
}

func TestSnapshotValidate(t *testing.T) {
	good := Snapshot{Version: snapshotVersion, Prefix: "0123456789AB", HighWater: 1, Epoch: 1}
	if err := good.Validate(); err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}
	cases := map[string]Snapshot{
		"bad version":        {Version: 99, Prefix: "0123456789AB", HighWater: 1},
		"negative highwater": {Version: snapshotVersion, Prefix: "0123456789AB", HighWater: -1},
		"overflow highwater": {Version: snapshotVersion, Prefix: "0123456789AB", HighWater: maxSeq + 1},
		"bad prefix length":  {Version: snapshotVersion, Prefix: "abc", HighWater: 1},
		"bad prefix chars":   {Version: snapshotVersion, Prefix: "0123456789!!", HighWater: 1},
	}
	for name, snap := range cases {
		if err := snap.Validate(); !errors.Is(err, ErrInvalidSnapshot) {
			t.Fatalf("%s: Validate = %v, want ErrInvalidSnapshot", name, err)
		}
	}
	// A store that tracks only the watermark may omit the informational prefix.
	if err := (Snapshot{Version: snapshotVersion, HighWater: 5}).Validate(); err != nil {
		t.Fatalf("prefix-less snapshot rejected: %v", err)
	}
	empty := Snapshot{Version: snapshotVersion}
	if err := empty.Validate(); err != nil {
		t.Fatalf("empty snapshot rejected: %v", err)
	}
}

func TestFileStoreCorruptSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap")
	store := NewFileStore(path)
	if _, err := store.Load(); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("Load(empty) = %v, want ErrNoSnapshot", err)
	}
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("Load(corrupt) = %v, want ErrInvalidSnapshot", err)
	}
}

func TestConcurrentBatchConsumption(t *testing.T) {
	r := newTestReserver(t, NewMemoryStore())
	batch, err := r.Reserve(10000)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	const goroutines = 16
	out := make(chan string, 10000)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				id, ok := batch.Next()
				if !ok {
					return
				}
				out <- id
			}
		}()
	}
	wg.Wait()
	close(out)

	seen := make(map[string]struct{}, 10000)
	for id := range out {
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate ID from shared reservation: %q", id)
		}
		seen[id] = struct{}{}
	}
	if got, want := len(seen), 10000; got != want {
		t.Fatalf("got %d IDs, want %d", got, want)
	}
}

// Global Next must keep working exactly as before: unique under concurrency and
// prefix-rotating on rollover.
func TestGlobalNextConcurrentUniqueness(t *testing.T) {
	const goroutines = 8
	const perGoroutine = 20000
	out := make(chan string, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				out <- Next()
			}
		}()
	}
	wg.Wait()
	close(out)
	seen := make(map[string]struct{}, goroutines*perGoroutine)
	for id := range out {
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate global NUID: %q", id)
		}
		seen[id] = struct{}{}
	}
	if got, want := len(seen), goroutines*perGoroutine; got != want {
		t.Fatalf("got %d IDs, want %d", got, want)
	}
}

func TestNextRolloverPreservedAfterReservations(t *testing.T) {
	_ = Next()
	globalNUID.Lock()
	globalNUID.seq = maxSeq
	oldPre := append([]byte(nil), globalNUID.pre...)
	globalNUID.Unlock()

	id := Next()
	if !IsValid(id) {
		t.Fatalf("invalid NUID after rollover: %q", id)
	}
	globalNUID.Lock()
	newPre := append([]byte(nil), globalNUID.pre...)
	globalNUID.Unlock()
	if string(newPre) == string(oldPre) {
		t.Fatal("expected prefix rollover on global Next")
	}
}
