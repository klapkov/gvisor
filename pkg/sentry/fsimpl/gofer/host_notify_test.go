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
	"testing"

	"gvisor.dev/gvisor/pkg/abi/linux"
	"gvisor.dev/gvisor/pkg/lisafs"
	"gvisor.dev/gvisor/pkg/sentry/contexttest"
	"gvisor.dev/gvisor/pkg/sentry/ktime"
	"gvisor.dev/gvisor/pkg/sentry/pgalloc"
)

// newHostNotifyTestTree returns a filesystem with a directory containing one
// cached, closed (zero-reference) regular file named "child".
func newHostNotifyTestTree(t *testing.T) (*filesystem, *dentry, *dentry) {
	t.Helper()
	ctx := contexttest.Context(t)
	fs := &filesystem{
		mf:          pgalloc.MemoryFileFromContext(ctx),
		inoByKey:    make(map[inoKey]uint64),
		inodeByKey:  make(map[inoKey]*inode),
		clock:       ktime.RealtimeClockFromContext(ctx),
		dentryCache: &dentryCache{maxCachedDentries: 100},
		client:      &lisafs.Client{},
		hostNotifier: &hostNotifier{
			watched:   make(map[*dentry]struct{}),
			dirState:  make(map[*dentry]map[string]childSnapshot),
			fileState: make(map[*dentry]inodeAttrs),
		},
	}

	parent, err := fs.newLisafsDentry(ctx, &lisafs.Inode{
		ControlFD: 1,
		Stat: lisafs.Statx{
			Mask: linux.STATX_TYPE | linux.STATX_MODE | linux.STATX_INO,
			Mode: linux.S_IFDIR | 0755,
			Ino:  1,
		},
	})
	if err != nil {
		t.Fatalf("fs.newLisafsDentry(parent): %v", err)
	}
	child, err := fs.newLisafsDentry(ctx, &lisafs.Inode{
		ControlFD: 2,
		Stat: lisafs.Statx{
			Mask: linux.STATX_TYPE | linux.STATX_MODE | linux.STATX_INO | linux.STATX_SIZE,
			Mode: linux.S_IFREG | 0644,
			Ino:  2,
		},
	})
	if err != nil {
		t.Fatalf("fs.newLisafsDentry(child): %v", err)
	}
	parent.opMu.Lock()
	parent.childrenMu.Lock()
	parent.cacheNewChildLocked(child, "child")
	parent.childrenMu.Unlock()
	parent.opMu.Unlock()

	// A file that was read and closed stays cached with no references.
	fs.renameMu.Lock()
	child.checkCachingLocked(ctx, true /* renameMuWriteLocked */)
	fs.renameMu.Unlock()
	if got := child.refs.Load(); got != 0 {
		t.Fatalf("cached closed child refs = %d, want 0", got)
	}
	return fs, parent, child
}

// TestPollChildrenIncludesUnreferencedChild guards CFAR-1544: Envoy reads its
// SDS files and closes them, so the cached children of its watched directory
// have zero references. The poller must still stat them by name, or a
// host-side rename of the file never produces IN_MOVED_TO.
func TestPollChildrenIncludesUnreferencedChild(t *testing.T) {
	_, parent, child := newHostNotifyTestTree(t)

	children := parent.pollChildren()
	if len(children) != 1 || children[0].name != "child" {
		t.Fatalf("pollChildren() = %+v, want one entry named %q", children, "child")
	}
	// The child has no watches of its own, so no reference is needed.
	if children[0].dentry != nil {
		t.Errorf("pollChildren() took a reference on an unwatched child")
	}
	if got := child.refs.Load(); got != 0 {
		t.Errorf("child refs after pollChildren() = %d, want 0", got)
	}
}

func TestPollChildrenSkipsNegativeEntries(t *testing.T) {
	_, parent, _ := newHostNotifyTestTree(t)
	parent.childrenMu.Lock()
	parent.cacheNegativeLookupLocked("missing")
	parent.childrenMu.Unlock()

	children := parent.pollChildren()
	if len(children) != 1 || children[0].name != "child" {
		t.Fatalf("pollChildren() = %+v, want only %q", children, "child")
	}
}

// TestReferenceWatchedDentriesIncludesUnreferenced guards CFAR-1544: inotify
// watches do not hold dentry references, so a watched file that is not open
// has zero references and must still be polled.
func TestReferenceWatchedDentriesIncludesUnreferenced(t *testing.T) {
	fs, parent, child := newHostNotifyTestTree(t)
	fs.hostNotifier.watched[child] = struct{}{}
	fs.hostNotifier.watched[parent] = struct{}{}

	dirs, files := fs.referenceWatchedDentries()
	if len(dirs) != 1 || dirs[0] != parent {
		t.Errorf("dirs = %v, want [parent]", dirs)
	}
	if len(files) != 1 || files[0] != child {
		t.Fatalf("files = %v, want [child]", files)
	}
	if got := child.refs.Load(); got != 1 {
		t.Errorf("child refs = %d, want 1 (poller reference)", got)
	}
	parent.decRefNoCaching()
	child.decRefNoCaching()
}

func TestReferenceWatchedDentriesSkipsDestroyed(t *testing.T) {
	fs, _, child := newHostNotifyTestTree(t)
	fs.hostNotifier.watched[child] = struct{}{}
	child.refs.Store(-1)

	dirs, files := fs.referenceWatchedDentries()
	if len(dirs) != 0 || len(files) != 0 {
		t.Fatalf("referenceWatchedDentries() = %v, %v, want none", dirs, files)
	}
	if got := child.refs.Load(); got != -1 {
		t.Errorf("destroyed child refs = %d, want -1", got)
	}
}
