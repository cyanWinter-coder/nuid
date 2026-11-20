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
	"os"
	"path/filepath"
	"testing"
	"time"
)

func benchmarkReserver(b *testing.B) *Reserver {
	b.Helper()
	dir, err := os.MkdirTemp("", "nuid-bench")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	r, err := NewReserver(NewFileStore(filepath.Join(dir, "nuid.snapshot")),
		WithClock(func() time.Time { return time.Unix(1700000000, 0) }))
	if err != nil {
		b.Fatal(err)
	}
	return r
}

// BenchmarkReservationNext measures the hot path: formatting an ID from a
// reservation with no lock and no persistence.
func BenchmarkReservationNext(b *testing.B) {
	r := benchmarkReserver(b)
	batch, err := r.Reserve(1 << 30)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := batch.Next(); !ok {
			batch, err = r.Reserve(1 << 30)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkReserveAmortized measures one durable reservation amortized over a
// large batch.
func BenchmarkReserveAmortizedOverBatch(b *testing.B) {
	const batchSize = 100000
	r := benchmarkReserver(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Reserve(batchSize); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/id")
}

// BenchmarkGlobalNext is the unmodified global path, for comparison.
func BenchmarkGlobalNext(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Next()
	}
}
