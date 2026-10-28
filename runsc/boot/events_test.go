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

package boot

import (
	"reflect"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/abi/linux"
	"gvisor.dev/gvisor/pkg/hostarch"
	"gvisor.dev/gvisor/pkg/safemem"
	"gvisor.dev/gvisor/pkg/sentry/pgalloc"
	"gvisor.dev/gvisor/pkg/sentry/usage"
	"gvisor.dev/gvisor/runsc/specutils"
)

// TestSandboxStatsEvent exercises real Event collection with two real kernel
// tasks and committed MemoryFile pages. It does not supply fabricated counters
// or mock the memory/CPU collector. Full Garden/resource acceptance is separate.
func TestSandboxStatsEvent(t *testing.T) {
	spec := testSpec()
	spec.Process.Args = []string{"/bin/sleep", "60"}
	spec.Annotations = map[string]string{specutils.AnnotationStatsScope: "sandbox"}
	l, cleanup, err := createLoader(testConfig(), spec)
	if err != nil {
		t.Fatal(err)
	}
	started := false
	defer func() {
		if !started {
			if err := l.k.Start(); err != nil {
				t.Error(err)
			}
		}
		l.k.Kill(linux.WaitStatusTerminationSignal(linux.SIGKILL))
		l.k.WaitExited()
		if got := l.k.TaskSet().Root.NumTasks(); got != 0 {
			t.Errorf("cleanup retained %d kernel tasks", got)
		}
		cleanup()
		l.Destroy()
	}()
	parent, _, err := l.createContainerProcess(&l.root)
	if err != nil {
		t.Fatal(err)
	}
	l.processes[execID{cid: l.sandboxID}].tg = parent
	const childID = "stats-child"
	l.k.RegisterContainerName(childID, childID)
	args := l.root.procArgs
	args.ContainerID = childID
	args.MountNamespace = nil // Take a fresh reference on the parent's namespace.
	args.InitialCgroups = nil
	args.Filename = "/bin/sh" // Do not inherit the parent's resolved /bin/sleep.
	args.Argv = []string{"/bin/sh", "-c", "while :; do :; done"}
	child, _, err := l.k.CreateProcess(args)
	if err != nil {
		t.Fatal(err)
	}
	l.processes[execID{cid: childID}] = &execProcess{tg: child}
	l.containerIDs[childID] = childID
	event := func(cid string) EventOut {
		t.Helper()
		var out EventOut
		if err := l.ctrl.manager.Event(&cid, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	before := event(l.sandboxID)
	t.Run("aggregate-pids", func(t *testing.T) {
		if before.Event.Data.Pids.Current != 2 {
			t.Fatalf("aggregate PIDs = %d, want 2", before.Event.Data.Pids.Current)
		}
	})
	const allocation = 8 << 20
	mf := l.k.MemoryFile()
	pages, err := mf.Allocate(allocation, pgalloc.AllocOpts{Kind: usage.Anonymous, Mode: pgalloc.AllocateAndCommit})
	if err != nil {
		t.Fatal(err)
	}
	defer mf.DecRef(pages)
	// AllocateAndCommit may use fallocate without faulting the pages into our
	// mapping. Touch the complete allocation so the mincore-based collector
	// observes resident memory, not merely allocated backing-file blocks.
	mapping, err := mf.MapInternal(pages, hostarch.Write)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := safemem.ZeroSeq(mapping); err != nil || n != allocation {
		t.Fatalf("touch allocation: wrote %d/%d bytes: %v", n, allocation, err)
	}
	backingUsage, err := mf.TotalUsage()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("resident fixture: touched=%d, backing usage=%d, event baseline=%d", allocation, backingUsage, before.Event.Data.Memory.Usage.Usage)
	// UpdateUsage throttles scans for at least one clock tick. Require the
	// complete committed allocation, but allow at most five seconds to observe
	// it through the real collector rather than assuming an immediate scan.
	deadline := time.Now().Add(5 * time.Second)
	var after EventOut
	t.Run("resident-memory", func(t *testing.T) {
		for {
			after = event(l.sandboxID)
			if after.Event.Data.Memory.Usage.Usage >= before.Event.Data.Memory.Usage.Usage+allocation {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("sandbox memory before=%d, after=%d, want >=%d increase within 5s", before.Event.Data.Memory.Usage.Usage, after.Event.Data.Memory.Usage.Usage, allocation)
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	t.Logf("sandbox memory before=%d after=%d", before.Event.Data.Memory.Usage.Usage, after.Event.Data.Memory.Usage.Usage)
	childBefore := event(childID)
	if childBefore.Event.Data.Pids.Current != 1 {
		t.Fatalf("child PIDs = %d, want 1", childBefore.Event.Data.Pids.Current)
	}
	// Removing aggregate scope retains the legacy pause-root and child path.
	delete(spec.Annotations, specutils.AnnotationStatsScope)
	pause := event(l.sandboxID)
	if pause.Event.Data.Memory.Usage.Usage != 0 || pause.Event.Data.Pids.Current != 1 {
		t.Fatalf("unannotated pause stats changed: %+v", pause.Event.Data)
	}
	childDefault := event(childID)
	if !reflect.DeepEqual(childDefault.Event.Data, childBefore.Event.Data) {
		t.Fatalf("child stats changed after root scope removal: %+v -> %+v", childBefore.Event.Data, childDefault.Event.Data)
	}
	spec.Annotations[specutils.AnnotationStatsScope] = "sandbox"
	if err := l.k.Start(); err != nil {
		t.Fatal(err)
	}
	started = true
	// The busy child must accumulate measured CPU within five seconds. Kill
	// and remove it, then verify that the parent's cumulative total survives.
	deadline = time.Now().Add(5 * time.Second)
	var running EventOut
	for {
		running = event(l.sandboxID)
		childCPU := child.CPUStats()
		if childCPU.UserTime+childCPU.SysTime >= 100*time.Millisecond {
			break
		}
		if !child.HasNonExitingTasks() {
			t.Fatalf("busy child exited before CPU assertion: %v", child.ExitStatus())
		}
		if time.Now().After(deadline) {
			t.Fatalf("busy child did not accumulate 100ms CPU within 5s: child=%+v, aggregate=%+v", childCPU, running.Event.Data.CPU.Usage)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Run("aggregate-cpu", func(t *testing.T) {
		if running.Event.Data.CPU.Usage.Total < uint64(100*time.Millisecond) {
			t.Fatalf("aggregate CPU = %d, want >=100ms measured child usage", running.Event.Data.CPU.Usage.Total)
		}
	})
	if err := l.destroySubcontainer(childID); err != nil {
		t.Fatal(err)
	}
	removed := event(l.sandboxID)
	t.Logf("child deletion: cumulative CPU=%d -> %d, PIDs=%d", running.Event.Data.CPU.Usage.Total, removed.Event.Data.CPU.Usage.Total, removed.Event.Data.Pids.Current)
	t.Run("cpu-after-deletion", func(t *testing.T) {
		if removed.Event.Data.CPU.Usage.Total < running.Event.Data.CPU.Usage.Total || removed.Event.Data.CPU.Usage.Total < uint64(100*time.Millisecond) {
			t.Fatalf("cumulative CPU lost on child deletion: %d -> %d, want >=100ms", running.Event.Data.CPU.Usage.Total, removed.Event.Data.CPU.Usage.Total)
		}
	})
	if removed.Event.Data.Pids.Current != 1 {
		t.Fatalf("post-delete PIDs = %d, want 1", removed.Event.Data.Pids.Current)
	}
	if err := l.ctrl.manager.Event(new(string), new(EventOut)); err == nil {
		t.Fatal("stats accepted a missing container")
	}
}
