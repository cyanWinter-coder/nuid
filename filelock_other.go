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

//go:build !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !windows && !js && !wasip1
// +build !darwin,!dragonfly,!freebsd,!illumos,!linux,!netbsd,!openbsd,!windows,!js,!wasip1

package nuid

import "os"

// Platforms without an advisory file lock in the standard library still get
// atomic snapshot replacement, so readers always observe a complete snapshot.
// Concurrent writers on these platforms must be coordinated by the application;
// otherwise the last writer wins instead of the highest high-water mark.
func lockExclusive(f *os.File) error { return nil }

func unlockFile(f *os.File) {}
