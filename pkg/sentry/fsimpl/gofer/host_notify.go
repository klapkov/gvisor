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

	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/abi/linux"
	"gvisor.dev/gvisor/pkg/context"
	"gvisor.dev/gvisor/pkg/log"
	"gvisor.dev/gvisor/pkg/sentry/vfs"
)

// hostNotifier manages the background poller that bridges host-side file
// changes to sandbox inotify watches. It tracks dentries with active watches
// and periodically polls the host for metadata and identity changes.
//
// For directories: stats each cached child by name from the parent on the
// host, compares identity (dev, ino) and metadata (mtime, ctime, size) with
// poller-owned last-seen values. Emits IN_MOVED_TO when a child's identity
// changes (host-side rename/replace), IN_DELETE when a child disappears,
// IN_MODIFY/IN_ATTRIB when metadata changes. This is the primary path for
// certificate rotation: Rep writes a temp file then renames it over the SDS
// YAML, and Envoy's watched_directory triggers on IN_MOVED_TO. New children
// that do not exist in the dentry cache are not detected (no readdir).
//
// For non-directory files: refreshes metadata via the file's own gofer
// FD/handle and compares with last-seen values. Emits IN_MODIFY/IN_ATTRIB.
// This covers in-place writes but not rename-based replacement (which is
// detected by the parent directory poll).
type hostNotifier struct {
	stop     chan struct{}
	done     sync.WaitGroup
	interval time.Duration

	mu        sync.Mutex
	watched   map[*dentry]struct{}
	dirState  map[*dentry]map[string]childSnapshot
	fileState map[*dentry]inodeAttrs
}

// childSnapshot holds the poller's last-observed identity and metadata for a
// child entry in a watched directory.
type childSnapshot struct {
	key   inoKey
	attrs inodeAttrs
}

// inodeAttrs holds the metadata snapshot that the poller compares against.
type inodeAttrs struct {
	mtime int64
	ctime int64
	size  uint64
	mode  uint32
}

// startHostNotify starts the host-notify poller if the filesystem has a
// non-zero hostInotifyPoll interval and is using InteropModeShared.
func (fs *filesystem) startHostNotify() {
	if fs.opts.hostInotifyPoll <= 0 || fs.opts.interop != InteropModeShared {
		return
	}
	hn := &hostNotifier{
		stop:      make(chan struct{}),
		interval:  fs.opts.hostInotifyPoll,
		watched:   make(map[*dentry]struct{}),
		dirState:  make(map[*dentry]map[string]childSnapshot),
		fileState: make(map[*dentry]inodeAttrs),
	}
	hn.done.Add(1)
	fs.hostNotifier = hn
	go fs.hostNotifyLoop()
}

// stopHostNotify signals the poller to exit and waits for it to finish.
// This ensures no in-flight poll is using gofer FDs or the lisafs client
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
	ticker := time.NewTicker(hn.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			fs.pollWatchedDentries()
		case <-hn.stop:
			return
		}
	}
}

// register adds a dentry to the set of watched dentries.
// Called from dentry.OnFirstWatch when the first inotify watch is added.
func (hn *hostNotifier) register(d *dentry) {
	hn.mu.Lock()
	defer hn.mu.Unlock()
	hn.watched[d] = struct{}{}
}

// reregisterWatchesRecursive walks the dentry tree rooted at d and registers
// any dentry that has active inotify watches. Used after checkpoint/restore
// to repopulate the hostNotifier's watched set.
func (d *dentry) reregisterWatchesRecursive(hn *hostNotifier) {
	if d.inode.watches.Size() > 0 {
		hn.register(d)
	}
	d.childrenMu.Lock()
	var children []*dentry
	for _, child := range d.children {
		if child != nil {
			children = append(children, child)
		}
	}
	d.childrenMu.Unlock()
	for _, child := range children {
		child.reregisterWatchesRecursive(hn)
	}
}

// unregister removes a dentry from the set of watched dentries and cleans up
// its associated state. Called from dentry.OnZeroWatches when the last watch
// is removed.
func (hn *hostNotifier) unregister(d *dentry) {
	hn.mu.Lock()
	defer hn.mu.Unlock()
	delete(hn.watched, d)
	delete(hn.dirState, d)
	delete(hn.fileState, d)
}

// pollWatchedDentries iterates over all registered dentries, polling the host
// for changes.
func (fs *filesystem) pollWatchedDentries() {
	dirs, files := fs.referenceWatchedDentries()
	if len(dirs) == 0 && len(files) == 0 {
		return
	}

	ctx := context.Background()

	for _, d := range dirs {
		fs.pollDirectory(ctx, d)
		d.DecRef(ctx)
	}
	for _, d := range files {
		fs.pollFile(ctx, d)
		d.DecRef(ctx)
	}
}

// referenceWatchedDentries returns the registered dentries, split into
// directories and non-directories, each with a reference that the caller must
// drop.
//
// A watched dentry can have zero references: inotify watches keep a dentry
// cached but do not hold a reference, so a watched file or directory that is
// not open has refs == 0 and TryIncRef would skip it. fs.renameMu serializes
// against destruction of zero-reference dentries (see dentry.IncRef), so
// holding it for reading makes IncRef safe for any dentry that is not already
// destroyed.
func (fs *filesystem) referenceWatchedDentries() (dirs, files []*dentry) {
	hn := fs.hostNotifier
	fs.renameMu.RLock()
	hn.mu.Lock()
	for d := range hn.watched {
		if d.refs.Load() < 0 {
			continue // destroyed
		}
		d.IncRef()
		if d.isDir() {
			dirs = append(dirs, d)
		} else {
			files = append(files, d)
		}
	}
	hn.mu.Unlock()
	fs.renameMu.RUnlock()
	return dirs, files
}

// pollChild is a cached child of a watched directory. dentry is set, with a
// reference that the caller must drop, only when the child has inotify watches
// of its own; otherwise only the name is needed.
type pollChild struct {
	name   string
	dentry *dentry
}

// pollChildren returns d's cached positive children.
//
// Cached children normally have zero references: Envoy, for example, reads
// its SDS files and closes them. Skipping such children (as TryIncRef would)
// misses exactly the host-side rename that the poller exists to report. The
// parent stat only needs the name; a reference is taken, under fs.renameMu as
// in referenceWatchedDentries, only to notify the child's own watches.
func (d *dentry) pollChildren() []pollChild {
	fs := d.inode.fs
	fs.renameMu.RLock()
	d.childrenMu.Lock()
	children := make([]pollChild, 0, len(d.children))
	for name, child := range d.children {
		if child == nil {
			continue // negative cache entry
		}
		pc := pollChild{name: name}
		if child.refs.Load() >= 0 && child.inode.watches.Size() > 0 {
			child.IncRef()
			pc.dentry = child
		}
		children = append(children, pc)
	}
	d.childrenMu.Unlock()
	fs.renameMu.RUnlock()
	return children
}

// pollDirectory checks a watched directory for host-side changes to its
// cached children. For each cached child, it stats the child by name from the
// parent on the host, comparing identity and metadata with poller-owned
// last-seen values.
func (fs *filesystem) pollDirectory(ctx context.Context, d *dentry) {
	hn := fs.hostNotifier

	children := d.pollChildren()
	if len(children) == 0 {
		return
	}
	defer func() {
		for _, ce := range children {
			if ce.dentry != nil {
				ce.dentry.DecRef(ctx)
			}
		}
	}()

	// Get or initialize state for this directory.
	hn.mu.Lock()
	prevChildren := hn.dirState[d]
	if prevChildren == nil {
		prevChildren = make(map[string]childSnapshot)
		hn.dirState[d] = prevChildren
	}
	hn.mu.Unlock()

	for _, ce := range children {
		cs, err := statChildFromParent(ctx, d, ce.name)
		if err != nil {
			log.Debugf("gofer.hostNotify: failed to stat child %q: %v", ce.name, err)
			continue
		}

		if !cs.found {
			// Child disappeared on the host.
			hn.mu.Lock()
			_, had := prevChildren[ce.name]
			delete(prevChildren, ce.name)
			hn.mu.Unlock()
			if had {
				d.inode.watches.Notify(ctx, ce.name, linux.IN_DELETE, 0, vfs.PathEvent, false)
			}
			continue
		}

		current := childSnapshot{key: cs.key, attrs: cs.attrs}

		hn.mu.Lock()
		prev, seen := prevChildren[ce.name]
		prevChildren[ce.name] = current
		hn.mu.Unlock()

		if !seen {
			// First observation; establish baseline.
			continue
		}

		// Check identity change (host-side rename/replace).
		if prev.key != current.key {
			// The file at this name is a different inode. Emit IN_MOVED_TO
			// on the parent directory so that Envoy's watched_directory
			// reloads. The existing gofer revalidation (InteropModeShared)
			// will invalidate the stale cached dentry on the next access.
			d.inode.watches.Notify(ctx, ce.name, linux.IN_MOVED_TO, 0, vfs.PathEvent, false)
			continue
		}

		// Same identity; check metadata change.
		var events uint32
		if current.attrs.mtime != prev.attrs.mtime || current.attrs.size != prev.attrs.size {
			events |= linux.IN_MODIFY
		}
		if current.attrs.mode != prev.attrs.mode {
			events |= linux.IN_ATTRIB
		}
		if current.attrs.ctime != prev.attrs.ctime && events == 0 {
			events |= linux.IN_ATTRIB
		}

		if events != 0 {
			// Notify on the child inode (for direct file watches) and on
			// the parent directory (for directory watches).
			if ce.dentry != nil {
				ce.dentry.inode.watches.Notify(ctx, "", events, 0, vfs.InodeEvent, false)
			}
			d.inode.watches.Notify(ctx, ce.name, events, 0, vfs.PathEvent, false)
		}
	}
}

// pollFile checks a watched non-directory file for host-side in-place metadata
// changes by refreshing metadata via the file's own gofer FD.
func (fs *filesystem) pollFile(ctx context.Context, d *dentry) {
	hn := fs.hostNotifier
	i := d.inode

	if i.isSynthetic() {
		return
	}

	i.metadataMu.Lock()
	err := i.updateMetadataLocked(ctx, noHandle)
	currentMtime := i.mtime.Load()
	currentCtime := i.ctime.Load()
	currentSize := i.size.Load()
	currentMode := i.mode.Load()
	i.metadataMu.Unlock()

	if err != nil {
		log.Debugf("gofer.hostNotify: failed to refresh file metadata for inode %d: %v", i.ino, err)
		return
	}

	current := inodeAttrs{
		mtime: currentMtime,
		ctime: currentCtime,
		size:  currentSize,
		mode:  currentMode,
	}

	hn.mu.Lock()
	prev, seen := hn.fileState[d]
	hn.fileState[d] = current
	hn.mu.Unlock()

	if !seen {
		return
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
}

// childStatResult holds the result of statting a child by name from its parent.
type childStatResult struct {
	found bool
	key   inoKey
	attrs inodeAttrs
}

// statChildFromParent stats a child by name from the parent directory on the
// host. For lisafs, this uses WalkStat on the parent's controlFD. For
// directfs, this uses unix.Statx on the parent's host FD.
func statChildFromParent(ctx context.Context, parent *dentry, name string) (childStatResult, error) {
	switch it := parent.inode.impl.(type) {
	case *lisafsInode:
		stats, err := it.controlFD.WalkStat(ctx, []string{name})
		if err != nil {
			return childStatResult{}, err
		}
		if len(stats) < 1 {
			return childStatResult{found: false}, nil
		}
		s := &stats[0]
		return childStatResult{
			found: true,
			key:   inoKeyFromStatx(s),
			attrs: inodeAttrs{
				mtime: dentryTimestamp(s.Mtime),
				ctime: dentryTimestamp(s.Ctime),
				size:  s.Size,
				mode:  uint32(s.Mode),
			},
		}, nil
	case *directfsInode:
		var stat unix.Statx_t
		err := unix.Statx(it.controlFD, name, unix.AT_SYMLINK_NOFOLLOW|unix.AT_NO_AUTOMOUNT, unix.STATX_BASIC_STATS|unix.STATX_BTIME, &stat)
		if err != nil {
			if err == unix.ENOENT || err == unix.ENOTDIR {
				return childStatResult{found: false}, nil
			}
			return childStatResult{}, err
		}
		return childStatResult{
			found: true,
			key:   inoKeyFromUnixStatx(&stat),
			attrs: inodeAttrs{
				mtime: dentryTimestampFromUnix(stat.Mtime),
				ctime: dentryTimestampFromUnix(stat.Ctime),
				size:  stat.Size,
				mode:  uint32(stat.Mode),
			},
		}, nil
	default:
		panic("unknown inode implementation")
	}
}
