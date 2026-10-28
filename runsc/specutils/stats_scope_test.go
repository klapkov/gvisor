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

package specutils

import (
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"gvisor.dev/gvisor/runsc/config"
)

func TestSandboxStatsScope(t *testing.T) {
	if SandboxStats(nil) {
		t.Fatal("nil spec requests aggregate stats")
	}
	for _, tc := range []struct {
		name        string
		annotations map[string]string
		want        bool
		invalid     bool
	}{
		{"default", nil, false, false},
		{"container", map[string]string{AnnotationStatsScope: "container"}, false, false},
		{"application", map[string]string{AnnotationStatsScope: "sandbox"}, true, false},
		{"cri-pause", map[string]string{ContainerdContainerTypeAnnotation: "sandbox"}, false, false},
		{"cri-application", map[string]string{ContainerdContainerTypeAnnotation: "sandbox", AnnotationStatsScope: "sandbox"}, true, false},
		{"child", map[string]string{ContainerdContainerTypeAnnotation: "container", AnnotationStatsScope: "sandbox"}, false, true},
		{"crio-child", map[string]string{CRIOContainerTypeAnnotation: "container", AnnotationStatsScope: "sandbox"}, false, true},
		{"typo", map[string]string{AnnotationStatsScope: "sandbbox"}, false, true},
		{"empty", map[string]string{AnnotationStatsScope: ""}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := &specs.Spec{Process: &specs.Process{Args: []string{"/bin/true"}}, Root: &specs.Root{Path: "/"}, Annotations: tc.annotations}
			if got := SandboxStats(spec); got != tc.want {
				t.Fatalf("SandboxStats = %t, want %t", got, tc.want)
			}
			err := ValidateSpec(spec, &config.Config{})
			if (err != nil) != tc.invalid || (err != nil && !strings.Contains(err.Error(), AnnotationStatsScope)) {
				t.Fatalf("ValidateSpec = %v, want invalid=%t", err, tc.invalid)
			}
		})
	}
}
