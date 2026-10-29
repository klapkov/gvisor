// Copyright 2026 The gVisor Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gofer

import (
	"sync"
	"time"

	"gvisor.dev/gvisor/pkg/abi/linux"
	"gvisor.dev/gvisor/pkg/context"
	"gvisor.dev/gvisor/pkg/log"
	"gvisor.dev/gvisor/pkg/sentry/vfs"
)

// hostNotifyInterval is the polling interval for checking host-side metadata
// changes on files with active inotify watches. This bridges the gap between
// the host filesystem (where external processes like Rep rotate certificates)
// and the sandbox inotify (which only tracks in-sandbox operations). 5 seconds
// is frequent enough to catch certificate rotations well before their validity
// window closes, while cheap enough to be negligible for idle sandboxes.
const hostNotifyInterval = 5 * time.Second

// hostNotifier manages the background poller that bridges host-side file
// changes to sandbox inotify watches.
type hostNotifier struct {
	stop chan struct{}
	done sync.WaitGroup

	// lastSeen tracks the poller's own last-observed metadata per inode,
	// independent of the shared metadata cache. This prevents missed events
	// when a concurrent access-time revalidation updates the cache between
	// host change and poll.
	mu       sync.Mutex
	lastSeen map[*inode]inodeAttrs
}

// inodeAttrs holds the metadata snapshot that the poller compares against.
type inodeAttrs struct {
	mtime int64
	ctime int64
	size  uint64
	mode  uint32
}

// startHostNotify starts a background goroutine that periodically polls
// host-side file metadata for inodes with active inotify watches.
//
// gVisor's inotify only tracks operations performed inside the sandbox. When
// an external process (e.g. Diego Rep rotating instance-identity certificates)
// modifies a bind-mounted file on the host, the sandbox can read the new
// content through the gofer but no inotify event is generated. This causes
// Envoy's SDS file watch to never fire, so Envoy serves expired certificates.
//
// The poller detects such changes by comparing poller-owned last-observed
// metadata against the remote filesystem and generating appropriate inotify
// events (IN_MODIFY, IN_ATTRIB) when discrepancies are found.
//
// Only active when InteropModeShared is in effect.
//
// Limitation: this poller stats existing inodes. If the host rotates a file
// via atomic rename (write temp, rename over target), the old inode/FD
// continues to see the old data. Rename-based rotation requires directory-
// level monitoring, which is not yet implemented. CF Rep writes certificate
// content in place, so the in-place modification path is the primary target.
//
// TODO(gvisor-cf): restart the poller after checkpoint/restore. The stop
// channel and goroutine are not preserved across save/restore, so bridged
// events would stop after a restore. This is not a production concern for CF
// containers.
func (fs *filesystem) startHostNotify() {
	if fs.opts.interop != InteropModeShared {
		return
	}
	hn := &hostNotifier{
		stop:     make(chan struct{}),
		lastSeen: make(map[*inode]inodeAttrs),
	}
	hn.done.Add(1)
	fs.hostNotifier = hn
	go fs.hostNotifyLoop()
}

// stopHostNotify signals the poller to exit and waits for it to finish.
// This ensures no in-flight poll is using inode handles or the lisafs client
// when Release proceeds with teardown.
func (fs *filesystem) stopHostNotify() {
	hn := fs.hostNotifier
	if hn == nil {
		return
	}
	close(hn.stop)
	hn.done.Wait()
}

func (fs *filesystem) hostNotifyLoop() {
	hn := fs.hostNotifier
	defer hn.done.Done()
	ticker := time.NewTicker(hostNotifyInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			fs.pollWatchedInodes()
		case <-hn.stop:
			return
		}
	}
}

// pollWatchedInodes finds all non-directory inodes with active inotify watches,
// refreshes their metadata from the host, and generates inotify events for any
// observed changes.
func (fs *filesystem) pollWatchedInodes() {
	hn := fs.hostNotifier

	// Collect inodes with active watches. TryIncRef prevents destruction
	// between collection and refresh. In practice, watches also prevent
	// dentry eviction, so the inode ref cannot drop to zero while we hold
	// ours.
	fs.inodeMu.Lock()
	var watched []*inode
	for _, i := range fs.inodeByKey {
		if i.watches.Size() > 0 && !i.isSynthetic() {
			if !i.refs.TryIncRef() {
				continue
			}
			watched = append(watched, i)
		}
	}
	fs.inodeMu.Unlock()

	if len(watched) == 0 {
		// Prune stale lastSeen entries when no inodes are watched.
		hn.mu.Lock()
		if len(hn.lastSeen) > 0 {
			hn.lastSeen = make(map[*inode]inodeAttrs)
		}
		hn.mu.Unlock()
		return
	}

	ctx := context.Background()
	for _, i := range watched {
		// Refresh metadata from the host.
		i.metadataMu.Lock()
		err := i.updateMetadataLocked(ctx, noHandle)
		currentMtime := i.mtime.Load()
		currentCtime := i.ctime.Load()
		currentSize := i.size.Load()
		currentMode := i.mode.Load()
		i.metadataMu.Unlock()

		if err != nil {
			log.Debugf("gofer.hostNotify: failed to refresh metadata for inode %d: %v", i.ino, err)
			i.refs.DecRef(func() {})
			continue
		}

		current := inodeAttrs{
			mtime: currentMtime,
			ctime: currentCtime,
			size:  currentSize,
			mode:  currentMode,
		}

		// Compare against poller-owned last-observed values.
		hn.mu.Lock()
		prev, seen := hn.lastSeen[i]
		hn.lastSeen[i] = current
		hn.mu.Unlock()

		if !seen {
			// First observation; establish baseline, no event.
			i.refs.DecRef(func() {})
			continue
		}

		var events uint32
		if current.mtime != prev.mtime || current.size != prev.size {
			events |= linux.IN_MODIFY
		}
		if current.mode != prev.mode {
			events |= linux.IN_ATTRIB
		}
		if current.ctime != prev.ctime && events == 0 {
			events |= linux.IN_ATTRIB
		}

		if events != 0 {
			i.watches.Notify(ctx, "", events, 0, vfs.InodeEvent, false)
		}

		i.refs.DecRef(func() {})
	}
}
