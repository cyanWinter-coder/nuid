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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileStore persists reservation snapshots in a single file.
//
// Writes are atomic (temporary file, fsync, rename, directory fsync) and are
// serialized against other processes through an advisory lock on
// "<path>.lock". Every commit re-reads the on-disk snapshot under that lock and
// only advances it, so snapshots written by a stale process can never lower the
// high-water mark or resurrect IDs the newer process already reserved.
type FileStore struct {
	mu   sync.Mutex
	path string
}

// NewFileStore returns a FileStore backed by path. The file and its lock file
// are created on the first successful commit; missing files are treated as
// empty.
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

// Path returns the snapshot file path.
func (f *FileStore) Path() string { return f.path }

// Load implements Store.
func (f *FileStore) Load() (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadLocked()
}

func (f *FileStore) loadLocked() (Snapshot, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Snapshot{}, ErrNoSnapshot
		}
		return Snapshot{}, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return Snapshot{}, fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
	}
	if err := snap.Validate(); err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

// Commit implements Store.
func (f *FileStore) Commit(s Snapshot) (Snapshot, bool, error) {
	if err := s.Validate(); err != nil {
		return Snapshot{}, false, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	unlock, err := lockFile(f.path + ".lock")
	if err != nil {
		return Snapshot{}, false, err
	}
	defer unlock()

	current, err := f.loadLocked()
	if err != nil && !errors.Is(err, ErrNoSnapshot) {
		return Snapshot{}, false, err
	}
	if err == nil && s.HighWater <= current.HighWater {
		// Already durable, or a late commit from a writer that fell behind.
		return current, false, nil
	}

	if err := f.writeLocked(s); err != nil {
		return Snapshot{}, false, err
	}
	return s, true, nil
}

// writeLocked atomically replaces the snapshot file. The caller must hold the
// advisory lock and f.mu.
func (f *FileStore) writeLocked(s Snapshot) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, filepath.Base(f.path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		tmp.Close()
		os.Remove(tmpName)
	}

	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return syncDir(dir)
}

// lockFile takes an exclusive advisory lock on path, creating the lock file if
// needed. The returned function releases the lock.
func lockFile(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := lockExclusive(f); err != nil {
		f.Close()
		return nil, err
	}
	return func() {
		unlockFile(f)
		f.Close()
	}, nil
}
