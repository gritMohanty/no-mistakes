package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/agentcfg"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestCursorBuildArgsPreservesParameterizedModel(t *testing.T) {
	a := &cursorAgent{model: "gpt-5.6-luna-xhigh"}
	want := []string{
		"--print", "--output-format", "stream-json", "--force", "--trust",
		"--model", "gpt-5.6-luna-xhigh",
	}
	got := a.buildArgs()
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("buildArgs = %q, want %q", got, want)
	}
}

func TestCursorModelMatchesEffectiveParameterizedIdentity(t *testing.T) {
	tests := []struct {
		requested string
		served    string
		want      bool
	}{
		{"gpt-5.6-luna-xhigh", "GPT-5.6 Luna 1M Extra High", true},
		{"gpt-5.6-luna-xhigh", "GPT-5.6 Luna 1M Medium", false},
		{"gpt-5.6-luna-xhigh", "GPT-5.6 Terra 1M Extra High", false},
		{"gpt-5.6-luna-high-fast", "GPT-5.6 Luna 1M High", false},
		{"gpt-5.6-luna-high-fast", "GPT-5.6 Luna 1M High Fast", true},
		{"gpt-5.6-luna-xhigh[reasoning=medium]", "GPT-5.6 Luna 1M Extra High", false},
		{"gpt-5.6-luna-xhigh[fast=false]", "GPT-5.6 Luna 1M Extra High Fast", false},
		{"gpt-5.6-luna-medium", "GPT-5.6 Luna 1M", true},
	}
	for _, tt := range tests {
		if got := cursorModelMatches(tt.requested, tt.served); got != tt.want {
			t.Errorf("cursorModelMatches(%q, %q) = %v, want %v", tt.requested, tt.served, got, tt.want)
		}
	}
}

func TestCursorManagedFactorySelectsNativeOnlyForSubscriptionRuns(t *testing.T) {
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "/managed/fm-subscription-check.mjs")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	ag, err := NewWithOptions(types.AgentCursor, "acpx", nil, Options{Profile: agentcfg.Profile{Model: "gpt-5.6-luna-xhigh"}})
	if err != nil {
		t.Fatalf("managed Cursor construction: %v", err)
	}
	wrapped, ok := ag.(*subscriptionAgent)
	if !ok {
		t.Fatalf("managed Cursor type = %T, want subscriptionAgent", ag)
	}
	native, ok := wrapped.Agent.(*cursorAgent)
	if !ok {
		t.Fatalf("managed Cursor wrapped type = %T, want cursorAgent", wrapped.Agent)
	}
	if native.bin != "cursor-agent" || native.model != "gpt-5.6-luna-xhigh" {
		t.Fatalf("managed Cursor native config = %+v", native)
	}

	managedACP, err := NewWithOptions(types.AgentName("acp:cursor"), "acpx", nil, Options{Profile: agentcfg.Profile{Model: "gpt-5.6-luna-xhigh"}})
	if err != nil {
		t.Fatalf("managed explicit ACP construction: %v", err)
	}
	managedACPWrapper, ok := managedACP.(*subscriptionAgent)
	if !ok {
		t.Fatalf("managed explicit ACP type = %T, want subscriptionAgent", managedACP)
	}
	if _, ok := managedACPWrapper.Agent.(*acpxAgent); !ok {
		t.Fatalf("managed explicit ACP wrapped type = %T, want acpxAgent", managedACPWrapper.Agent)
	}

	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "")
	ag, err = NewWithOptions(types.AgentCursor, "acpx", nil, Options{Profile: agentcfg.Profile{Model: "gpt-5.6-luna-xhigh"}})
	if err != nil {
		t.Fatalf("unmanaged construction: %v", err)
	}
	if _, ok := ag.(*acpxAgent); !ok {
		t.Fatalf("unmanaged type = %T, want acpxAgent", ag)
	}
}

func TestCursorManagedFactoryRejectsCustomACPBinary(t *testing.T) {
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "/managed/fm-subscription-check.mjs")
	_, err := NewWithOptions(types.AgentCursor, "/custom/acpx", nil, Options{Profile: agentcfg.Profile{Model: "gpt-5.6-luna-xhigh"}})
	if err == nil || !strings.Contains(err.Error(), "custom Cursor ACP binary") {
		t.Fatalf("custom managed binary error = %v", err)
	}
}

func TestCursorParserConsumesObservedStreamJSON(t *testing.T) {
	stream := strings.NewReader(strings.Join([]string{
		`{"type":"system","subtype":"init","model":"GPT-5.6 Luna 1M Extra High","session_id":"cursor-session"}`,
		`{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"first "}]},"session_id":"cursor-session"}`,
		`{"type":"tool_call","subtype":"completed","call_id":"call-1","session_id":"cursor-session"}`,
		`{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"answer"}]},"session_id":"cursor-session"}`,
		`{"type":"result","subtype":"success","is_error":false,"result":"first answer","session_id":"cursor-session","usage":{"inputTokens":11,"outputTokens":7,"cacheReadTokens":3,"cacheWriteTokens":2}}`,
	}, "\n"))
	var chunks []string
	p := &cursorStreamParser{onChunk: func(text string) { chunks = append(chunks, text) }}
	if err := p.parse(context.Background(), stream); err != nil {
		t.Fatal(err)
	}
	if !p.terminal || p.malformedResult || p.finalText() != "first answer" {
		t.Fatalf("parser terminal=%v malformed=%v text=%q", p.terminal, p.malformedResult, p.finalText())
	}
	if p.model != "GPT-5.6 Luna 1M Extra High" || p.sessionID != "cursor-session" {
		t.Fatalf("identity model=%q session=%q", p.model, p.sessionID)
	}
	if strings.Join(chunks, "") != "first answer" {
		t.Fatalf("chunks = %q", chunks)
	}
	if p.usage.InputTokens != 11 || p.usage.OutputTokens != 7 || p.usage.CacheReadTokens != 3 || p.usage.CacheCreationTokens != 2 || !p.usage.Reported {
		t.Fatalf("usage = %+v", p.usage)
	}
}

func TestCursorParserKeepsObjectErrorAndRejectsMalformedResult(t *testing.T) {
	var errorParser cursorStreamParser
	if err := errorParser.parse(context.Background(), strings.NewReader(`{"type":"error","error":{"code":"quota_exhausted","message":"included usage exhausted"}}`+"\n")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(errorParser.errorMessage, "quota_exhausted") {
		t.Fatalf("object error lost: %q", errorParser.errorMessage)
	}

	var malformed cursorStreamParser
	if err := malformed.parse(context.Background(), strings.NewReader(`{"type":"system","subtype":"init","model":"GPT-5.6 Luna 1M Extra High"}`+"\n"+`{"type":"result","subtype":"success","result":{"unexpected":true}}`+"\n")); err != nil {
		t.Fatal(err)
	}
	if !malformed.terminal || !malformed.malformedResult {
		t.Fatalf("malformed result terminal=%v malformed=%v", malformed.terminal, malformed.malformedResult)
	}

	var emptyError cursorStreamParser
	if err := emptyError.parse(context.Background(), strings.NewReader(`{"type":"error"}`+"\n")); err != nil {
		t.Fatal(err)
	}
	if emptyError.errorMessage == "" {
		t.Fatal("error event without payload was ignored")
	}
}

func TestCursorRunParsesSuccessfulStreamAndInvocation(t *testing.T) {
	dir := t.TempDir()
	stub := filepath.Join(dir, "cursor-agent")
	argsFile := filepath.Join(dir, "args")
	envFile := filepath.Join(dir, "env")
	script := `#!/bin/sh
printf '%s\n' "$@" > "$CURSOR_ARGS_FILE"
printf '%s\n' "$NO_MISTAKES_GATE" > "$CURSOR_GATE_ENV_FILE"
printf '%s\n' '{"type":"system","subtype":"init","model":"GPT-5.6 Luna 1M Extra High","session_id":"ok-session"}'
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"answer"}]},"session_id":"ok-session"}'
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"answer","session_id":"ok-session","usage":{"inputTokens":2,"outputTokens":1}}'
`
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	a := &cursorAgent{bin: stub, model: "gpt-5.6-luna-xhigh"}
	result, err := a.runOnce(context.Background(), RunOpts{
		CWD:    dir,
		Prompt: "test",
		Env:    []string{"CURSOR_ARGS_FILE=" + argsFile, "CURSOR_GATE_ENV_FILE=" + envFile},
	})
	if err != nil {
		t.Fatalf("successful run error = %v", err)
	}
	if result == nil || result.Text != "answer" || result.SessionID != "ok-session" || result.Model != "GPT-5.6 Luna 1M Extra High" || result.Usage.InputTokens != 2 {
		t.Fatalf("successful result = %+v", result)
	}
	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	argText := string(args)
	if !strings.Contains(argText, "--output-format\nstream-json") || !strings.Contains(argText, "--model\ngpt-5.6-luna-xhigh") {
		t.Fatalf("native args = %q", argText)
	}
	gate, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(gate)) != "1" {
		t.Fatalf("managed gate marker = %q", gate)
	}
}

func TestCursorRunRejectsTruncatedStream(t *testing.T) {
	dir := t.TempDir()
	stub := filepath.Join(dir, "cursor-agent")
	if err := os.WriteFile(stub, []byte(`#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","model":"GPT-5.6 Luna 1M Extra High"}'
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"partial"}]}}'
`), 0o755); err != nil {
		t.Fatal(err)
	}
	a := &cursorAgent{bin: stub, model: "gpt-5.6-luna-xhigh"}
	_, err := a.runOnce(context.Background(), RunOpts{CWD: dir, Prompt: "test"})
	if err == nil || !strings.Contains(err.Error(), "no terminal result") {
		t.Fatalf("truncated stream error = %v", err)
	}
}

func TestCursorRunRejectsNonzeroExitAfterObservedOutput(t *testing.T) {
	dir := t.TempDir()
	stub := filepath.Join(dir, "cursor-agent")
	script := `#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","model":"GPT-5.6 Luna 1M Extra High","session_id":"failed-session"}'
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"partial"}]},"session_id":"failed-session"}'
exit 1
`
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	a := &cursorAgent{bin: stub, model: "gpt-5.6-luna-xhigh"}
	result, err := a.runOnce(context.Background(), RunOpts{CWD: dir, Prompt: "test"})
	if err == nil || !strings.Contains(err.Error(), "cursor exited") {
		t.Fatalf("run error = %v, want nonzero exit", err)
	}
	if result == nil || result.SessionID != "failed-session" || result.Model != "GPT-5.6 Luna 1M Extra High" {
		t.Fatalf("failed result = %+v", result)
	}
}

func TestCursorUsageRefusalIsTerminalAndActionable(t *testing.T) {
	dir := t.TempDir()
	stub := filepath.Join(dir, "cursor-agent")
	if err := os.WriteFile(stub, []byte(`#!/bin/sh
printf '%s\n' "You've hit your usage limit" >&2
exit 1
`), 0o755); err != nil {
		t.Fatal(err)
	}
	a := &cursorAgent{bin: stub, model: "gpt-5.6-luna-xhigh"}
	oldBackoff := transientBackoff
	backoffCalls := 0
	transientBackoff = func(context.Context, int) error {
		backoffCalls++
		return nil
	}
	t.Cleanup(func() { transientBackoff = oldBackoff })
	_, err := a.Run(context.Background(), RunOpts{CWD: dir, Prompt: "test"})
	if err == nil || !strings.Contains(err.Error(), "preflight available but execution quota refused") {
		t.Fatalf("usage refusal error = %v", err)
	}
	if backoffCalls != 0 {
		t.Fatalf("usage refusal retried %d times", backoffCalls)
	}
}

func TestCursorRetryRechecksSubscriptionBeforeLaunchingSecondChild(t *testing.T) {
	dir := t.TempDir()
	checkPath := filepath.Join(dir, "subscription-check.mjs")
	checkCount := filepath.Join(dir, "check-count")
	childCount := filepath.Join(dir, "child-count")
	if err := os.WriteFile(checkPath, []byte(`import fs from "node:fs";
const path = process.env.CURSOR_CHECK_COUNT;
const count = fs.existsSync(path) ? Number(fs.readFileSync(path, "utf8")) : 0;
fs.writeFileSync(path, String(count + 1));
if (count > 0) { console.error("quota_exhausted"); process.exit(1); }
`), 0o600); err != nil {
		t.Fatal(err)
	}
	stub := filepath.Join(dir, "cursor-agent")
	if err := os.WriteFile(stub, []byte(`#!/bin/sh
count=0
if [ -f "$CURSOR_CHILD_COUNT" ]; then count=$(cat "$CURSOR_CHILD_COUNT"); fi
echo $((count + 1)) > "$CURSOR_CHILD_COUNT"
printf '%s\n' 'connection reset' >&2
exit 1
`), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", checkPath)
	t.Setenv("CURSOR_CHECK_COUNT", checkCount)
	oldBackoff := transientBackoff
	transientBackoff = func(context.Context, int) error { return nil }
	t.Cleanup(func() { transientBackoff = oldBackoff })

	a := &cursorAgent{bin: stub, model: "gpt-5.6-luna-xhigh"}
	_, err := a.Run(context.Background(), RunOpts{CWD: dir, Prompt: "test", Env: []string{"CURSOR_CHILD_COUNT=" + childCount}})
	if err == nil || !strings.Contains(err.Error(), "subscription preflight failed") {
		t.Fatalf("retry preflight error = %v", err)
	}
	children, err := os.ReadFile(childCount)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(children)) != "1" {
		t.Fatalf("native child launches = %q, want one before second preflight", children)
	}
	checks, err := os.ReadFile(checkCount)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(checks)) != "2" {
		t.Fatalf("subscription checks = %q, want one per attempted run", checks)
	}
}
