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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeStateFile(t *testing.T, rootDir, sandboxID, containerID string) {
	t.Helper()
	path := buildPath(rootDir, FullID{SandboxID: sandboxID, ContainerID: containerID}, stateFileExtension)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("creating state directory: %v", err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("creating state file: %v", err)
	}
}

// TestFindContainerIDPrefersExactMatch checks that an exact container ID is
// resolved even when other containers use it as a prefix.
func TestFindContainerIDPrefersExactMatch(t *testing.T) {
	rootDir := t.TempDir()
	const parentID = "parent"
	writeStateFile(t, rootDir, parentID, parentID)
	writeStateFile(t, rootDir, parentID, parentID+"-child-0")
	writeStateFile(t, rootDir, parentID, parentID+"-child-1")

	got, err := findContainerID(rootDir, parentID)
	if err != nil {
		t.Fatalf("findContainerID(%q) failed: %v", parentID, err)
	}
	if got.ContainerID != parentID {
		t.Fatalf("findContainerID(%q) = %q, want %q", parentID, got.ContainerID, parentID)
	}
}

// TestFindContainerIDMissingParentIsNotFound checks that a deleted container
// is reported as not found, rather than ambiguous, when only containers whose
// IDs extend it with a "-" separator remain, including a single one.
func TestFindContainerIDMissingParentIsNotFound(t *testing.T) {
	for _, children := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d-children", children), func(t *testing.T) {
			rootDir := t.TempDir()
			const parentID = "parent"
			for i := 0; i < children; i++ {
				writeStateFile(t, rootDir, parentID, fmt.Sprintf("%s-child-%d", parentID, i))
			}

			if _, err := findContainerID(rootDir, parentID); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("findContainerID(%q) = %v, want os.ErrNotExist", parentID, err)
			}
		})
	}
}

// TestFindContainerIDAbbreviation checks that a unique prefix that is not
// followed by a "-" separator still resolves to the container.
func TestFindContainerIDAbbreviation(t *testing.T) {
	rootDir := t.TempDir()
	writeStateFile(t, rootDir, "sandbox", "abcdef")

	got, err := findContainerID(rootDir, "abc")
	if err != nil {
		t.Fatalf("findContainerID(%q) failed: %v", "abc", err)
	}
	if got.ContainerID != "abcdef" {
		t.Fatalf("findContainerID(%q) = %q, want %q", "abc", got.ContainerID, "abcdef")
	}
}
