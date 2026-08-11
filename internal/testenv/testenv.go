//go:build integration

// Package testenv starts the infrastructure the integration tier runs against:
// a Pulsar broker to carry canal-json, and a TiDB instance for derived tables
// to be rebuilt into. Each is a single throwaway container, started by the
// test that asks for it and torn down with it — there is no stack to bring up
// first, and nothing to remember to shut down.
//
// Note what is deliberately absent: TiCDC, and the PD/TiKV cluster it needs.
// cdcfresh consumes bytes from a topic and cannot distinguish a live
// changefeed from a replay of one, so these tests publish the canal-json
// captured from a real TiCDC in internal/canaljson/testdata. Standing up four
// more containers to regenerate payloads that are already committed would add
// minutes to every run and prove nothing the fixtures do not. test/cdcstack
// holds that full stack, for the rare occasion the fixtures need recapturing.
package testenv

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Golden returns a canal-json fixture captured from a real TiCDC. The path is
// resolved from this file's own location rather than the working directory, so
// tests in any package can ask for one.
func Golden(t *testing.T, name string) []byte {
	t.Helper()
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve testenv source path")
	}
	path := filepath.Join(filepath.Dir(self), "..", "canaljson", "testdata", name+".json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return b
}

// safeName turns a test name into something usable as a Pulsar topic segment
// and a SQL identifier. Subtests contain "/", which is legal in neither.
func safeName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
