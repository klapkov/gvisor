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

package container

import (
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"gvisor.dev/gvisor/runsc/boot"
	"gvisor.dev/gvisor/runsc/sandbox"
	"gvisor.dev/gvisor/runsc/specutils"
)

func TestSandboxStatsRetainsMeasuredCPUWithoutHostCgroup(t *testing.T) {
	for _, tc := range []struct {
		name        string
		annotations map[string]string
		want        uint64
	}{
		{"application-parent", map[string]string{specutils.AnnotationStatsScope: "sandbox"}, 321},
		{"pause-default", nil, 0},
		{"explicit-container", map[string]string{specutils.AnnotationStatsScope: "container"}, 0},
		{"child-cannot-aggregate", map[string]string{specutils.AnnotationStatsScope: "sandbox", specutils.ContainerdContainerTypeAnnotation: "container"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Container{ID: "parent", Spec: &specs.Spec{Annotations: tc.annotations}, Sandbox: &sandbox.Sandbox{ID: "parent"}}
			// PID0 has no host cgroup. Aggregate CPU must retain the kernel's
			// measured result, even when no live-container usage map is present.
			event := boot.EventOut{Event: boot.Event{Data: boot.Stats{CPU: boot.CPU{Usage: boot.CPUUsage{Total: 321}}}}}
			c.populateStats(&event)
			if got := event.Event.Data.CPU.Usage.Total; got != tc.want {
				t.Fatalf("CPU total = %d, want %d", got, tc.want)
			}
		})
	}
}
