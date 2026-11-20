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

//go:build js || wasip1
// +build js wasip1

package nuid

import (
	"errors"
	"os"
)

// Single-threaded sandboxes have no advisory file locking. FileStore is rarely
// useful there; if it is used, commits are still atomic file replacements.
func lockExclusive(f *os.File) error {
	return errors.New("nuid: file locking is not supported on this platform")
}

func unlockFile(f *os.File) {}
