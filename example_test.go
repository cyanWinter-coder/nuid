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

package nuid_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/nats-io/nuid"
)

// ExampleReserver shows the intended usage for a process that publishes IDs
// from several goroutines: reserve one batch, then format IDs from the batch
// without touching any shared lock.
func ExampleReserver() {
	dir, err := os.MkdirTemp("", "nuid-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	reserver, err := nuid.NewReserver(nuid.NewFileStore(filepath.Join(dir, "nuid.snapshot")))
	if err != nil {
		panic(err)
	}

	batch, err := reserver.Reserve(4)
	if err != nil {
		panic(err)
	}

	// The bounds are the durable reservation: every sequence value in
	// [lo, hi) is safe to use, even if the process crashes now.
	lo, hi := batch.Bounds()
	fmt.Println("reserved", hi-lo, "ids")

	var mu sync.Mutex
	var ids []string
	var wg sync.WaitGroup
	for i := 0; i < int(hi-lo); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, ok := batch.Next()
			if !ok {
				panic("batch exhausted")
			}
			mu.Lock()
			ids = append(ids, id)
			mu.Unlock()
		}()
	}
	wg.Wait()

	fmt.Println("all valid:", allValid(ids))
	// Output:
	// reserved 4 ids
	// all valid: true
}

func allValid(ids []string) bool {
	for _, id := range ids {
		if !nuid.IsValid(id) {
			return false
		}
	}
	return true
}

// ExampleReserver_recovery shows that a restart continues above the persisted
// high-water mark instead of replaying IDs from the previous run.
func ExampleReserver_recovery() {
	dir, err := os.MkdirTemp("", "nuid-recovery")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "nuid.snapshot")

	first, err := nuid.NewReserver(nuid.NewFileStore(path))
	if err != nil {
		panic(err)
	}
	batch, err := first.Reserve(100)
	if err != nil {
		panic(err)
	}
	last := batch.Last()

	// Simulate a crash and restart.
	second, err := nuid.NewReserver(nuid.NewFileStore(path))
	if err != nil {
		panic(err)
	}
	next, err := second.Reserve(1)
	if err != nil {
		panic(err)
	}

	fmt.Println("restart starts after previous batch:", next.Lo() == batch.Hi())
	fmt.Println("old last id is never reused:", next.First() != last)
	// Output:
	// restart starts after previous batch: true
	// old last id is never reused: true
}
