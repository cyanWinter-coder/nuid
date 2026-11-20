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
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Batch reservations
//
// A Reserver hands out Reservation values: contiguous, half-open ranges of NUID
// sequence values that all share one randomly generated prefix. The caller can
// format IDs from a reservation without touching the Reserver (or any other
// global lock), which keeps the fast path cheap while allowing the reserver to
// perform synchronized, durable bookkeeping once per batch.
//
// Persistence is write-ahead. Before a reservation is returned, the reserver
// records a high-water mark: the first sequence value that must never be used
// again. Only values strictly below the persisted high-water mark are handed
// out. If the process crashes, recovery resumes at the persisted high-water
// mark, skipping every range that could have been issued. A later reservation
// therefore never reuses a sequence value that an earlier one might have
// published.
//
// Sequence values carry a random prefix per batch. Within a batch the prefix
// and the sequence interval are fixed, so IDs are formatted exactly like those
// produced by Next and are accepted by IsValid.

var (
	// ErrReservationExhausted is returned when a reservation would exceed the
	// NUID sequence space (62^10 values).
	ErrReservationExhausted = errors.New("nuid: reservation space exhausted")

	// ErrBatchExhausted is returned when a Reservation has handed out every ID
	// it contains.
	ErrBatchExhausted = errors.New("nuid: reservation batch exhausted")

	// ErrClockRollback is returned when the wall clock moves backwards relative
	// to the last persisted or observed reservation time.
	ErrClockRollback = errors.New("nuid: clock moved backwards")

	// ErrRandomSourceReset is returned when the random source produces a prefix
	// that this reserver already issued, which indicates that the source was
	// reset or is not advancing.
	ErrRandomSourceReset = errors.New("nuid: random source reset detected")

	// ErrInvalidReservation is returned for a non-positive batch size.
	ErrInvalidReservation = errors.New("nuid: invalid reservation size")

	// ErrReservationConflict is returned when another writer keeps advancing the
	// persisted high-water mark so that no batch can be reserved.
	ErrReservationConflict = errors.New("nuid: reservation conflict")

	// ErrNoSnapshot is returned by Store.Load when nothing has been persisted.
	ErrNoSnapshot = errors.New("nuid: no persisted snapshot")

	// ErrInvalidSnapshot is returned for malformed or inconsistent snapshots.
	ErrInvalidSnapshot = errors.New("nuid: invalid snapshot")

	// ErrNonMonotonicStore is returned when a Store reports a high-water mark
	// lower than the one it was asked to persist.
	ErrNonMonotonicStore = errors.New("nuid: store high water mark regressed")

	// ErrNilStore is returned when a Reserver is created without a Store.
	ErrNilStore = errors.New("nuid: nil store")
)

const (
	// snapshotVersion is the on-disk format version understood by this package.
	snapshotVersion = 1

	// prefixHistory bounds how many recent batch prefixes are remembered for
	// random-source reset detection.
	prefixHistory = 256

	// maxReserveAttempts bounds retries when another writer advances the
	// persisted high-water mark first. It is generous because every rejected
	// attempt means another process reserved in between.
	maxReserveAttempts = 128

	// maxPrefixReads bounds reads from the random source while collecting
	// unbiased prefix bytes. A working source needs one or two reads.
	maxPrefixReads = 1024
)

// Snapshot is the durable state of a Reserver.
//
// HighWater is the first sequence value that has not been reserved. Every value
// below it may have been observed by an application, so it is never handed out
// again. Prefix describes the reservation that ends at HighWater; it is
// informational and is not reused by the next batch.
type Snapshot struct {
	// Version is the snapshot format version. It must be snapshotVersion.
	Version int `json:"version"`
	// Prefix is the 12 character NUID prefix used by the reservation that ends
	// at HighWater.
	Prefix string `json:"prefix"`
	// HighWater is the exclusive upper bound of all reserved sequence values.
	HighWater int64 `json:"high_water"`
	// Epoch is a monotonically increasing reservation generation.
	Epoch uint64 `json:"epoch"`
	// Updated is the wall-clock time (Unix nanoseconds) of the reservation.
	Updated int64 `json:"updated_unix_nano"`
}

// Validate reports whether the snapshot is well formed and safe to use.
func (s Snapshot) Validate() error {
	if s.Version != snapshotVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrInvalidSnapshot, s.Version)
	}
	if s.HighWater < 0 || s.HighWater > maxSeq {
		return fmt.Errorf("%w: high water mark %d out of range", ErrInvalidSnapshot, s.HighWater)
	}
	// The prefix is informational. Stores that only track the high-water mark
	// may leave it empty; a non-empty prefix must still be well formed.
	if s.Prefix != "" && !ValidPrefix(s.Prefix) {
		return fmt.Errorf("%w: bad prefix %q", ErrInvalidSnapshot, s.Prefix)
	}
	return nil
}

// ValidPrefix reports whether p is a valid 12 character NUID prefix.
func ValidPrefix(p string) bool {
	if len(p) != preLen {
		return false
	}
	for i := 0; i < len(p); i++ {
		c := p[i]
		if (c < '0' || c > '9') && (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
			return false
		}
	}
	return true
}

// Store persists reservation snapshots.
//
// Commit must apply snapshots atomically and monotonically:
//
//   - a snapshot whose HighWater is greater than the stored value advances the
//     stored state, returns that same snapshot, and reports applied == true;
//   - committing the same snapshot again is an idempotent no-op that reports
//     applied == false and returns the stored state unchanged;
//   - a snapshot whose HighWater is not greater than the stored value (for
//     example a late commit from a process that fell behind) must never lower
//     the stored high-water mark; applied is false and the current state is
//     returned.
//
// Returning the effective snapshot lets a caller detect that a concurrent
// writer advanced further and retry above the new high-water mark.
type Store interface {
	// Load returns the persisted snapshot, or ErrNoSnapshot when the store is
	// empty.
	Load() (Snapshot, error)
	// Commit durably applies s subject to the monotonicity rules above.
	Commit(s Snapshot) (effective Snapshot, applied bool, err error)
}

// MemoryStore is an in-process Store. It is useful for tests and for callers
// that persist snapshots through other means.
type MemoryStore struct {
	mu   sync.Mutex
	snap Snapshot
	set  bool
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

// Load implements Store.
func (m *MemoryStore) Load() (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.set {
		return Snapshot{}, ErrNoSnapshot
	}
	return m.snap, nil
}

// Commit implements Store.
func (m *MemoryStore) Commit(s Snapshot) (Snapshot, bool, error) {
	if err := s.Validate(); err != nil {
		return Snapshot{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.set && s.HighWater <= m.snap.HighWater {
		return m.snap, false, nil
	}
	m.snap = s
	m.set = true
	return m.snap, true, nil
}

// Reservation is a contiguous, half-open interval [Lo, Hi) of sequence values
// that all share the same NUID prefix.
//
// The whole interval is below the high-water mark that was persisted before the
// reservation was returned, so every ID it can produce is safe to publish,
// including after a crash.
//
// A Reservation is safe for concurrent use. Formatting an ID never touches the
// Reserver or a package-level lock, so callers are free to share the
// reservation with a pool of goroutines.
type Reservation struct {
	// next is accessed atomically and must be kept 64-bit aligned on 32-bit
	// architectures, so it is the first field.
	next   int64
	lo     int64
	hi     int64
	prefix string
}

// Prefix returns the 12 character prefix shared by every ID in the batch.
func (r *Reservation) Prefix() string { return r.prefix }

// Lo returns the inclusive lower sequence bound of the batch.
func (r *Reservation) Lo() int64 { return r.lo }

// Hi returns the exclusive upper sequence bound of the batch.
func (r *Reservation) Hi() int64 { return r.hi }

// Bounds returns the half-open interval [lo, hi) of sequence values in the
// batch. Every ID the batch can produce lies inside it, and every value inside
// it was durably reserved before the batch was returned.
func (r *Reservation) Bounds() (lo, hi int64) { return r.lo, r.hi }

// Len returns the number of IDs the batch can produce.
func (r *Reservation) Len() int64 { return r.hi - r.lo }

// First returns the ID for the lowest sequence value in the batch.
func (r *Reservation) First() string { return encodeSeq(r.prefix, r.lo) }

// Last returns the ID for the highest sequence value in the batch.
func (r *Reservation) Last() string { return encodeSeq(r.prefix, r.hi-1) }

// At formats the ID for an absolute sequence value in [Lo, Hi). It is a pure,
// concurrency-safe read that consumes no cursor.
func (r *Reservation) At(seq int64) (string, bool) {
	if seq < r.lo || seq >= r.hi {
		return "", false
	}
	return encodeSeq(r.prefix, seq), true
}

// Next returns the next unused ID in the batch. The second result is false once
// the batch is exhausted.
func (r *Reservation) Next() (string, bool) {
	for {
		cur := atomic.LoadInt64(&r.next)
		if cur >= r.hi {
			return "", false
		}
		if atomic.CompareAndSwapInt64(&r.next, cur, cur+1) {
			return encodeSeq(r.prefix, cur), true
		}
	}
}

// ID is Next with an explicit error. It returns ErrBatchExhausted once every
// value in the batch has been handed out.
func (r *Reservation) ID() (string, error) {
	if id, ok := r.Next(); ok {
		return id, nil
	}
	return "", fmt.Errorf("%w: [%d, %d)", ErrBatchExhausted, r.lo, r.hi)
}

// Remaining returns how many IDs have not been consumed through Next. IDs
// formatted directly with At do not advance the cursor.
func (r *Reservation) Remaining() int64 {
	n := r.hi - atomic.LoadInt64(&r.next)
	if n < 0 {
		return 0
	}
	return n
}

// encodeSeq renders pre (a 12 character prefix) followed by seq in base 62,
// matching the format produced by Next.
func encodeSeq(pre string, seq int64) string {
	var b [totalLen]byte
	copy(b[:preLen], pre)
	for i, l := len(b), seq; i > preLen; l /= base {
		i--
		b[i] = digits[l%base]
	}
	return string(b[:])
}

// Option configures a Reserver.
type Option func(*reserverConfig) error

type reserverConfig struct {
	now  func() time.Time
	rand io.Reader
}

// WithClock supplies the clock used for rollback detection and snapshot
// timestamps. It is intended for tests.
func WithClock(now func() time.Time) Option {
	return func(c *reserverConfig) error {
		if now == nil {
			return errors.New("nuid: nil clock")
		}
		c.now = now
		return nil
	}
}

// WithRand supplies the random source used to generate one prefix per batch. It
// is intended for tests; production code should use crypto/rand.
func WithRand(r io.Reader) Option {
	return func(c *reserverConfig) error {
		if r == nil {
			return errors.New("nuid: nil random source")
		}
		c.rand = r
		return nil
	}
}

// Reserver hands out durable, contiguous batches of NUIDs.
//
// Create one with NewReserver and load it with an existing Store to resume
// where the previous process stopped:
//
//	store := nuid.NewFileStore("/var/lib/index/nuid.snapshot")
//	r, err := nuid.NewReserver(store)
//	if err != nil {
//		return err
//	}
//	batch, err := r.Reserve(10000)
//	if err != nil {
//		return err
//	}
//	for {
//		id, ok := batch.Next()
//		if !ok {
//			break
//		}
//		use(id)
//	}
type Reserver struct {
	mu        sync.Mutex
	store     Store
	now       func() time.Time
	rand      io.Reader
	prefix    string
	highWater int64
	epoch     uint64
	lastTime  int64
	recent    []string
	recentPos int
	seen      map[string]struct{}
}

// NewReserver loads the persisted state from store and returns a Reserver that
// will continue above the stored high-water mark.
//
// If the persisted snapshot was written by a clock that is ahead of the current
// clock, ErrClockRollback is returned: IDs issued after a rollback would be
// ordered before IDs that may already be in use.
func NewReserver(store Store, opts ...Option) (*Reserver, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	cfg := reserverConfig{
		now:  time.Now,
		rand: rand.Reader,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	r := &Reserver{
		store:    store,
		now:      cfg.now,
		rand:     cfg.rand,
		lastTime: cfg.now().UnixNano(),
		recent:   make([]string, 0, prefixHistory),
		seen:     make(map[string]struct{}, prefixHistory),
	}

	snap, err := store.Load()
	if err != nil && !errors.Is(err, ErrNoSnapshot) {
		return nil, err
	}
	if err == nil {
		if err := snap.Validate(); err != nil {
			return nil, err
		}
		if snap.Updated > r.lastTime {
			return nil, fmt.Errorf("%w: snapshot time %d is after %d", ErrClockRollback, snap.Updated, r.lastTime)
		}
		r.highWater = snap.HighWater
		r.epoch = snap.Epoch
		r.prefix = snap.Prefix
		r.rememberPrefix(snap.Prefix)
	}
	return r, nil
}

// HighWater returns the first sequence value that has not been reserved.
func (r *Reserver) HighWater() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.highWater
}

// Remaining returns how many sequence values can still be reserved.
func (r *Reserver) Remaining() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maxSeq - r.highWater
}

// Snapshot returns the state the next reservation will build on.
func (r *Reserver) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Snapshot{
		Version:   snapshotVersion,
		Prefix:    r.prefix,
		HighWater: r.highWater,
		Epoch:     r.epoch,
		Updated:   r.lastTime,
	}
}

// Reserve durably reserves n contiguous sequence values and returns their
// bounds.
//
// The batch is [HighWater, HighWater+n). The new high-water mark (the batch's
// exclusive upper bound) is committed to the Store before the Reservation is
// returned; only values below that persisted mark are ever handed out. A crash
// can therefore only lose reserved-but-unused IDs. It can never reuse an ID that
// may already have been issued, because recovery resumes at the persisted
// high-water mark and skips the entire previous batch.
//
// Within a batch every ID shares one freshly generated random prefix; IDs are
// formatted exactly like those produced by Next and satisfy IsValid. Every new
// batch uses a new prefix.
//
// Reserve returns ErrReservationExhausted when the sequence space is too small
// for the request, ErrClockRollback when the wall clock moved backwards,
// ErrRandomSourceReset when the random source repeats a previously issued
// prefix, and ErrReservationConflict when a concurrent writer keeps advancing
// the stored high-water mark.
func (r *Reserver) Reserve(n int64) (*Reservation, error) {
	if n <= 0 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidReservation, n)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for attempt := 0; attempt < maxReserveAttempts; attempt++ {
		remaining := maxSeq - r.highWater
		if remaining <= 0 || n > remaining {
			return nil, fmt.Errorf("%w: requested %d, remaining %d", ErrReservationExhausted, n, remaining)
		}

		now := r.now().UnixNano()
		if now < r.lastTime {
			return nil, fmt.Errorf("%w: %d is before %d", ErrClockRollback, now, r.lastTime)
		}

		prefix, err := r.newPrefix()
		if err != nil {
			return nil, err
		}

		lo := r.highWater
		hi := lo + n
		epoch := r.epoch + 1
		snap := Snapshot{
			Version:   snapshotVersion,
			Prefix:    prefix,
			HighWater: hi,
			Epoch:     epoch,
			Updated:   now,
		}

		effective, applied, err := r.store.Commit(snap)
		if err != nil {
			return nil, err
		}
		if applied && effective == snap {
			// Our snapshot is durable, so the whole range [lo, hi) is safe to
			// use: it lies below the persisted high-water mark and no other
			// writer can claim it.
			r.highWater = hi
			r.prefix = prefix
			r.epoch = epoch
			r.lastTime = now
			r.rememberPrefix(prefix)

			return &Reservation{
				next:   lo,
				lo:     lo,
				hi:     hi,
				prefix: prefix,
			}, nil
		}

		// Either another writer advanced the high-water mark first, or the
		// store applied something other than our snapshot. Our range may
		// overlap what was issued, so adopt the effective state and retry
		// above it.
		if effective.HighWater < r.highWater || (applied && effective.HighWater < hi) {
			return nil, fmt.Errorf("%w: store reports %d, need at least %d", ErrNonMonotonicStore, effective.HighWater, hi)
		}
		r.highWater = effective.HighWater
		if effective.Epoch > r.epoch {
			r.epoch = effective.Epoch
		}
		r.rememberPrefix(effective.Prefix)
	}

	return nil, ErrReservationConflict
}

// newPrefix reads a fresh random prefix, refusing to reuse one that this
// Reserver recently issued. A healthy crypto source never repeats a 12
// character prefix; a repeat means the source was reset and uniqueness can no
// longer be assumed. Detection covers the most recent prefixHistory batches.
func (r *Reserver) newPrefix() (string, error) {
	var p [preLen]byte
	// Reject bytes at or above the largest multiple of base (248) so every
	// base-62 character is equally likely.
	const limit = 256 - (256 % base)
	var buf [preLen]byte
	for n, reads := 0, 0; n < preLen; {
		if reads++; reads > maxPrefixReads {
			return "", fmt.Errorf("nuid: random source produced no usable bytes")
		}
		if _, err := io.ReadFull(r.rand, buf[:]); err != nil {
			return "", fmt.Errorf("nuid: reading random source: %w", err)
		}
		for _, c := range buf {
			if int(c) >= limit {
				continue
			}
			p[n] = digits[int(c)%base]
			n++
			if n == preLen {
				break
			}
		}
	}
	prefix := string(p[:])
	if _, ok := r.seen[prefix]; ok {
		return "", fmt.Errorf("%w: prefix %q reused", ErrRandomSourceReset, prefix)
	}
	return prefix, nil
}

// rememberPrefix records a prefix in a bounded ring used for reset detection.
// The caller must hold r.mu.
func (r *Reserver) rememberPrefix(prefix string) {
	if prefix == "" {
		return
	}
	if _, ok := r.seen[prefix]; ok {
		return
	}
	if len(r.recent) < prefixHistory {
		r.recent = append(r.recent, prefix)
	} else {
		delete(r.seen, r.recent[r.recentPos])
		r.recent[r.recentPos] = prefix
		r.recentPos = (r.recentPos + 1) % prefixHistory
	}
	r.seen[prefix] = struct{}{}
}

var (
	_ Store = (*MemoryStore)(nil)
	_ Store = (*FileStore)(nil)
)
