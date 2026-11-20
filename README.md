# NUID

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![ReportCard](https://goreportcard.com/badge/github.com/nats-io/nuid)](https://goreportcard.com/report/github.com/nuid/nuid)
[![Build Status](https://github.com/nats-io/nuid/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/nats-io/nuid/actions)
[![Release](https://img.shields.io/badge/release-v1.0.1-1eb0fc.svg)](https://github.com/nats-io/nuid/releases/tag/v1.0.1)
[![Go Reference](https://pkg.go.dev/badge/github.com/nats-io/nuid.svg)](https://pkg.go.dev/github.com/nats-io/nuid)
[![Coverage Status](https://coveralls.io/repos/github/nats-io/nuid/badge.svg?branch=main)](https://coveralls.io/github/nats-io/nuid?branch=main)

A highly performant unique identifier generator.

## Installation

Use the `go` command:

	$ go get github.com/nats-io/nuid

## Basic Usage
```go

// Utilize the global locked instance
nuid := nuid.Next()

// Create an instance, these are not locked.
n := nuid.New()
nuid = n.Next()

// Generate a new crypto/rand seeded prefix.
// Generally not needed, happens automatically.
n.RandomizePrefix()
```

## Batch reservations

When IDs are assigned by a single process but published by many goroutines, or
when the process must not reuse IDs after a crash, reserve a contiguous batch
instead of calling `Next` per ID:

```go
store := nuid.NewFileStore("/var/lib/myapp/nuid.snapshot")
r, err := nuid.NewReserver(store)
if err != nil {
    return err
}
batch, err := r.Reserve(10000)
if err != nil {
    return err
}

// The batch is also described by its bounds. Every value in [lo, hi) was
// durably reserved before Reserve returned.
lo, hi := batch.Bounds()

// Any goroutine can format from the batch without a lock.
for {
    id, ok := batch.Next()
    if !ok {
        break
    }
    publish(id)
}

// Or index a specific slot by absolute sequence value.
id, ok := batch.At(lo + 7)
```

`Store` is an interface; `MemoryStore` and `FileStore` are provided. A custom
store can persist the same snapshot in a database, as long as commits are
monotonic and idempotent:

Design guarantees:

* **Write-ahead persistence.** `Reserve` stores the new high-water mark (the
  batch's exclusive upper bound) *before* returning the batch. Only sequence
  values below the persisted mark are ever handed out, so a crash can lose
  unused IDs but can never reuse an ID that may already have been published.
  On restart `NewReserver` resumes at the persisted high-water mark and skips
  the whole previous batch.
* **Lock-free allocation.** A `Reservation` has a fixed prefix and a
  half-open interval. `Next`, `At`, `First`, and `Last` format IDs with no
  shared lock, so the batch can be shared by concurrent workers.
* **Stable format.** IDs use the same base-62 encoding and 12 character prefix
  as `Next`; every ID passes `IsValid`. Each batch gets a fresh random prefix
  generated from `crypto/rand`; the prefix is fixed for the whole batch.
* **Idempotent, monotonic commits.** Storing the same snapshot twice is a
  no-op, and a late commit from a stale process can never lower the persisted
  high-water mark. Concurrent reservers that race are retried above the winner.
* **Explicit errors.** `ErrClockRollback`, `ErrRandomSourceReset`,
  `ErrReservationExhausted` (reserving past 62^10), `ErrBatchExhausted`,
  `ErrReservationConflict`, `ErrInvalidSnapshot`, and `ErrNonMonotonicStore`
  are reported with `errors.Is`. `WithClock` and `WithRand` exist for tests.

`nuid.Next()` itself is unchanged: it remains a single locked sequence with the
same random increment and prefix rollover on exhaustion, and it is independent
of any `Reserver`.

## Performance
NUID needs to be very fast to generate and be truly unique, all while being entropy pool friendly.
NUID uses 12 bytes of crypto generated data (entropy draining), and 10 bytes of pseudo-random
sequential data that increments with a pseudo-random increment.

Total length of a NUID string is 22 bytes of base 62 ascii text, so 62^22 or
2707803647802660400290261537185326956544 possibilities.

NUID can generate identifiers as fast as 60ns, or ~16 million per second. There is an associated
benchmark you can use to test performance on your own hardware.

## License

Unless otherwise noted, the NATS source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.
