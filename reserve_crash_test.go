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
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestReserverCrashRecovery runs a child process that reserves a batch, records
// the IDs it actually used, and then dies abruptly. The parent reopens the same
// snapshot and verifies that no ID from the crashed run can be issued again.
func TestReserverCrashRecovery(t *testing.T) {
	if os.Getenv("NUID_CRASH_HELPER") == "1" {
		runCrashHelper()
		return
	}

	dir := t.TempDir()
	snap := filepath.Join(dir, "nuid.snapshot")
	used := filepath.Join(dir, "used.txt")

	cmd := exec.Command(os.Args[0], "-test.run=^TestReserverCrashRecovery$")
	cmd.Env = append(os.Environ(),
		"NUID_CRASH_HELPER=1",
		"NUID_CRASH_SNAPSHOT="+snap,
		"NUID_CRASH_USED="+used,
	)
	// The helper calls os.Exit(0) after a partial consumption, simulating a
	// process that stops between two IDs without any cleanup.
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("helper failed: %v\n%s", err, out)
	}

	usedIDs := readLines(t, used)
	if len(usedIDs) == 0 {
		t.Fatal("helper did not record any IDs")
	}

	store := NewFileStore(snap)
	persisted, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if persisted.HighWater < int64(len(usedIDs)) {
		t.Fatalf("persisted high water %d is below the %d IDs that were used", persisted.HighWater, len(usedIDs))
	}

	prev := make(map[string]struct{}, len(usedIDs))
	for _, id := range usedIDs {
		prev[id] = struct{}{}
	}

	r, err := NewReserver(store)
	if err != nil {
		t.Fatalf("NewReserver: %v", err)
	}
	for i := 0; i < 100; i++ {
		batch, err := r.Reserve(16)
		if err != nil {
			t.Fatalf("Reserve after crash: %v", err)
		}
		for {
			id, ok := batch.Next()
			if !ok {
				break
			}
			if _, dup := prev[id]; dup {
				t.Fatalf("post-recovery ID reused a pre-crash ID: %q", id)
			}
		}
	}
}

func runCrashHelper() {
	snap := os.Getenv("NUID_CRASH_SNAPSHOT")
	used := os.Getenv("NUID_CRASH_USED")

	r, err := NewReserver(NewFileStore(snap))
	if err != nil {
		os.Exit(2)
	}
	batch, err := r.Reserve(1000)
	if err != nil {
		os.Exit(3)
	}

	f, err := os.Create(used)
	if err != nil {
		os.Exit(4)
	}
	w := bufio.NewWriter(f)
	// Consume part of the batch and make it durable from the application's
	// point of view, then die without cleaning up.
	for i := 0; i < 25; i++ {
		id, ok := batch.Next()
		if !ok {
			break
		}
		w.WriteString(id + "\n")
	}
	w.Flush()
	f.Sync()
	f.Close()

	os.Exit(0)
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func TestReserveAndNextAreIndependent(t *testing.T) {
	globalNUID.Lock()
	before := globalNUID.seq
	globalNUID.Unlock()

	r := newTestReserver(t, NewMemoryStore())
	if _, err := r.Reserve(10); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	globalNUID.Lock()
	after := globalNUID.seq
	globalNUID.Unlock()
	if before != after {
		t.Fatalf("global sequence changed from %d to %d", before, after)
	}

	batch, err := r.Reserve(1)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if id, ok := batch.Next(); !ok || !IsValid(id) {
		t.Fatalf("batched ID = %q,%v", id, ok)
	}
	if next := Next(); !IsValid(next) {
		t.Fatalf("global Next = %q", next)
	}
}

func TestReserverHighWaterIsInteger(t *testing.T) {
	// Reserving a very large batch must not overflow the bounds.
	r := newTestReserver(t, NewMemoryStore())
	batch, err := r.Reserve(maxSeq)
	if err != nil {
		t.Fatalf("Reserve(maxSeq): %v", err)
	}
	if batch.Hi() != maxSeq {
		t.Fatalf("Hi = %d, want %d", batch.Hi(), maxSeq)
	}
	if got, want := strconv.FormatInt(batch.Len(), 10), strconv.FormatInt(maxSeq, 10); got != want {
		t.Fatalf("Len = %s, want %s", got, want)
	}
	if _, err := r.Reserve(1); err == nil {
		t.Fatal("Reserve after full allocation succeeded")
	}
}
