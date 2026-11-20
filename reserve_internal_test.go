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
	"fmt"
	"testing"
)

func TestRingHistoryEvictsOldest(t *testing.T) {
	r := newTestReserver(t, NewMemoryStore())
	for i := 0; i < prefixHistory*2; i++ {
		p := fmt.Sprintf("%012d", i)
		r.rememberPrefix(p)
	}
	if got, want := len(r.recent), prefixHistory; got != want {
		t.Fatalf("history size = %d, want %d", got, want)
	}
	if got, want := len(r.seen), prefixHistory; got != want {
		t.Fatalf("seen size = %d, want %d", got, want)
	}
	// The oldest entries must have been evicted, the newest retained.
	if _, ok := r.seen[fmt.Sprintf("%012d", 0)]; ok {
		t.Fatal("oldest prefix was not evicted")
	}
	newest := fmt.Sprintf("%012d", prefixHistory*2-1)
	if _, ok := r.seen[newest]; !ok {
		t.Fatal("newest prefix was not retained")
	}
}

func TestHistoryRingWrapsRepeatedly(t *testing.T) {
	r := newTestReserver(t, NewMemoryStore())
	// Wrap the ring several times; the retained set must still be exactly the
	// most recent prefixHistory entries.
	const total = prefixHistory*3 + 7
	for i := 0; i < total; i++ {
		r.rememberPrefix(fmt.Sprintf("%012d", i))
	}
	if got, want := len(r.seen), prefixHistory; got != want {
		t.Fatalf("seen size = %d, want %d", got, want)
	}
	for i := total - prefixHistory; i < total; i++ {
		if _, ok := r.seen[fmt.Sprintf("%012d", i)]; !ok {
			t.Fatalf("recent prefix %d was evicted", i)
		}
	}
	if _, ok := r.seen[fmt.Sprintf("%012d", total-prefixHistory-1)]; ok {
		t.Fatal("prefix older than the window was retained")
	}
}

func TestOptionValidation(t *testing.T) {
	if _, err := NewReserver(NewMemoryStore(), WithClock(nil)); err == nil {
		t.Fatal("WithClock(nil) accepted")
	}
	if _, err := NewReserver(NewMemoryStore(), WithRand(nil)); err == nil {
		t.Fatal("WithRand(nil) accepted")
	}
}

func TestReserveErrorsAreWrapped(t *testing.T) {
	// Callers must be able to use errors.Is against the sentinel while still
	// getting context from the message.
	r := newTestReserver(t, NewMemoryStore())
	_, err := r.Reserve(0)
	if !errors.Is(err, ErrInvalidReservation) {
		t.Fatalf("errors.Is(%v, ErrInvalidReservation) = false", err)
	}
	if err.Error() == ErrInvalidReservation.Error() {
		t.Fatal("error should carry context")
	}
}
