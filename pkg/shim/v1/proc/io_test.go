// Copyright 2026 The gVisor Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux

package proc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runc "github.com/containerd/go-runc"
	"golang.org/x/sys/unix"
)

type copyPipesFixture struct {
	readers [2]*os.File
	fds     [2]int
	paths   [2]string
	writers [2]io.WriteCloser
	done    chan struct{}
	workers sync.WaitGroup
}

func newCopyPipesFixture(t *testing.T, sharedFile bool) *copyPipesFixture {
	return newCopyPipesFixtureWithStdoutEOF(t, sharedFile, false)
}

func newCopyPipesFixtureWithStdoutEOF(t *testing.T, sharedFile, stdoutEOF bool) *copyPipesFixture {
	t.Helper()
	directory := t.TempDir()
	rio, err := runc.NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatal(err)
	}
	command := &exec.Cmd{}
	rio.Set(command)
	fixture := &copyPipesFixture{
		writers: [2]io.WriteCloser{command.Stdout.(io.WriteCloser), command.Stderr.(io.WriteCloser)},
		done:    make(chan struct{}),
	}
	var copies sync.WaitGroup
	defer func() {
		go func() {
			copies.Wait()
			close(fixture.done)
		}()
	}()
	t.Cleanup(func() {
		rio.Close()
		deadline := time.Now().Add(2 * time.Second)
		buffer := make([]byte, 64<<10)
		for {
			select {
			case <-fixture.done:
				for _, reader := range fixture.readers {
					if reader != nil {
						reader.Close()
					}
				}
				workersDone := make(chan struct{})
				go func() {
					fixture.workers.Wait()
					close(workersDone)
				}()
				select {
				case <-workersDone:
					t.Log("copyPipes cleanup released copy and fixture workers")
				case <-time.After(2 * time.Second):
					t.Error("fixture cleanup did not release workers within 2s")
				}
				return
			default:
			}
			if time.Now().After(deadline) {
				t.Error("copyPipes cleanup did not release workers within 2s")
				return
			}
			for _, path := range fixture.paths {
				if sharedFile || path == "" {
					continue
				}
				fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
				if err == nil {
					unix.Read(fd, buffer)
					unix.Close(fd)
				}
			}
			time.Sleep(time.Millisecond)
		}
	})
	for index := range fixture.paths {
		fixture.paths[index] = filepath.Join(directory, fmt.Sprintf("output-%d", index))
		if sharedFile {
			if index == 1 {
				fixture.paths[index] = fixture.paths[0]
				continue
			}
			if err := os.WriteFile(fixture.paths[index], nil, 0600); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := unix.Mkfifo(fixture.paths[index], 0600); err != nil {
			t.Fatal(err)
		}
		fd, err := unix.Open(fixture.paths[index], unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		fixture.fds[index] = fd
		fixture.readers[index] = os.NewFile(uintptr(fd), fixture.paths[index])
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETPIPE_SZ, 4096); err != nil {
			t.Fatal(err)
		}
	}
	if stdoutEOF {
		fixture.writers[0].Close()
	}
	err = copyPipes(context.Background(), rio, "", fixture.paths[0], fixture.paths[1], &copies)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestCopyPipesConsumerCloseReleasesOutput(t *testing.T) {
	for output, name := range []string{"stdout", "stderr"} {
		t.Run(name, func(t *testing.T) {
			fixture := newCopyPipesFixture(t, false)
			var progress atomic.Int64
			producerDone := make(chan struct{})
			fixture.workers.Add(1)
			go func() {
				defer fixture.workers.Done()
				defer close(producerDone)
				defer fixture.writers[output].Close()
				chunk := bytes.Repeat([]byte("x"), 4096)
				for {
					if _, err := fixture.writers[output].Write(chunk); err != nil {
						return
					}
					progress.Add(1)
				}
			}()
			reader := fixture.readers[output]
			capacity, err := unix.FcntlInt(uintptr(fixture.fds[output]), unix.F_GETPIPE_SZ, 0)
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				queued, err := unix.IoctlGetInt(fixture.fds[output], unix.TIOCINQ)
				if err != nil {
					t.Fatal(err)
				}
				before := progress.Load()
				if queued == capacity && before > 0 {
					time.Sleep(50 * time.Millisecond)
					if progress.Load() == before {
						break
					}
				}
				if time.Now().After(deadline) {
					t.Fatalf("did not establish backpressure: queued=%d capacity=%d writes=%d", queued, capacity, progress.Load())
				}
				time.Sleep(time.Millisecond)
			}
			select {
			case <-producerDone:
				t.Fatal("producer exited before consumer close")
			default:
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-producerDone:
			case <-time.After(2 * time.Second):
				t.Fatal("producer blocked after consumer closed, before fixture cleanup")
			}
			control := []byte("other stream remains usable\n")
			if _, err := fixture.writers[1-output].Write(control); err != nil {
				t.Fatal(err)
			}
			fixture.writers[1-output].Close()
			actual := make([]byte, len(control))
			if _, err := io.ReadFull(fixture.readers[1-output], actual); err != nil || !bytes.Equal(actual, control) {
				t.Fatalf("unrelated output = %q, error %v", actual, err)
			}
			select {
			case <-fixture.done:
			case <-time.After(2 * time.Second):
				t.Fatal("copyPipes WaitGroup blocked after backpressured consumer closed")
			}
		})
	}
}

func TestCopyPipesSharedFileEarlyStdoutEOF(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		t.Run(fmt.Sprintf("attempt-%d", attempt), func(t *testing.T) {
			fixture := newCopyPipesFixtureWithStdoutEOF(t, true, true)
			payload := []byte("delayed stderr\n")
			if _, err := fixture.writers[1].Write(payload); err != nil {
				t.Fatal(err)
			}
			fixture.writers[1].Close()
			select {
			case <-fixture.done:
			case <-time.After(2 * time.Second):
				t.Fatal("shared-file workers blocked after early stdout EOF")
			}
			content, err := os.ReadFile(fixture.paths[0])
			if err != nil || !bytes.Equal(content, payload) {
				t.Fatalf("stderr after stdout EOF = %q, error %v", content, err)
			}
		})
	}
}

func TestCopyPipesNormalStreaming(t *testing.T) {
	fixture := newCopyPipesFixture(t, false)
	for output := range fixture.writers {
		payload := bytes.Repeat([]byte(fmt.Sprintf("output-%d\n", output)), 32<<10)
		expected := sha256.Sum256(payload)
		result := make(chan error, 1)
		fixture.workers.Add(2)
		go func() {
			defer fixture.workers.Done()
			defer fixture.writers[output].Close()
			_, err := fixture.writers[output].Write(payload)
			result <- err
		}()
		drained := make(chan error, 1)
		go func() {
			defer fixture.workers.Done()
			hash := sha256.New()
			count, err := io.Copy(hash, fixture.readers[output])
			if err == nil && (count != int64(len(payload)) || !bytes.Equal(hash.Sum(nil), expected[:])) {
				err = fmt.Errorf("output %d checksum/length mismatch: bytes=%d", output, count)
			}
			drained <- err
		}()
		for _, done := range []<-chan error{result, drained} {
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("output %d streaming timed out", output)
			}
		}
	}
	select {
	case <-fixture.done:
	case <-time.After(2 * time.Second):
		t.Fatal("normal streaming WaitGroup timed out")
	}
}

func TestCopyPipesSharedFile(t *testing.T) {
	fixture := newCopyPipesFixture(t, true)
	for output, payload := range []string{"stdout\n", "stderr\n"} {
		if _, err := io.WriteString(fixture.writers[output], payload); err != nil {
			t.Fatal(err)
		}
		fixture.writers[output].Close()
		if output == 0 {
			deadline := time.Now().Add(2 * time.Second)
			for {
				content, err := os.ReadFile(fixture.paths[0])
				if err != nil {
					t.Fatal(err)
				}
				if string(content) == payload {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("stdout did not reach shared file within 2s")
				}
				time.Sleep(time.Millisecond)
			}
		}
	}
	select {
	case <-fixture.done:
	case <-time.After(2 * time.Second):
		t.Fatal("shared-file WaitGroup timed out")
	}
	content, err := os.ReadFile(fixture.paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(content, []byte("stdout\n")) != 1 || bytes.Count(content, []byte("stderr\n")) != 1 || len(content) != len("stdout\nstderr\n") {
		t.Fatalf("shared output = %q", content)
	}
}
