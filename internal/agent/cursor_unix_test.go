//go:build unix

package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCursorAgentRunCancellationTerminatesProcessGroup(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "grandchild.pid")
	stub := filepath.Join(dir, "cursor-agent")
	script := `#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","model":"GPT-5.6 Luna 1M Extra High","session_id":"cancel-session"}'
( sleep 120 >/dev/null 2>&1 ) &
echo $! > "` + pidFile + `"
sleep 120
`
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	type runResult struct {
		result *Result
		err    error
	}
	done := make(chan runResult, 1)
	go func() {
		result, err := (&cursorAgent{bin: stub, model: "gpt-5.6-luna-xhigh"}).Run(ctx, RunOpts{CWD: dir, Prompt: "test"})
		done <- runResult{result: result, err: err}
	}()
	grandchild := waitForPidFile(t, pidFile, 5*time.Second)
	cancel()
	select {
	case outcome := <-done:
		if outcome.err == nil || (!strings.Contains(outcome.err.Error(), "context canceled") && !strings.Contains(outcome.err.Error(), "signal: terminated")) {
			t.Fatalf("cancellation error = %v", outcome.err)
		}
		if outcome.result == nil || outcome.result.SessionID != "cancel-session" || outcome.result.Model != "GPT-5.6 Luna 1M Extra High" {
			t.Fatalf("partial cancellation result = %+v", outcome.result)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("cursor did not return after cancellation")
	}
	if !pidGoneWithin(grandchild, 5*time.Second) {
		t.Fatalf("cursor grandchild pid %d survived cancellation", grandchild)
	}
}
