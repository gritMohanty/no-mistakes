package agent

import (
	"context"
	"os"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/agentcfg"
	"github.com/kunchenguid/no-mistakes/internal/runenv"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestSubscriptionPolicyFactory(t *testing.T) {
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "/managed/fm-subscription-check.mjs")
	opts := Options{Profile: agentcfg.Profile{Model: "model"}}
	unsafe := opts
	unsafe.Environment = runenv.Overlay{Set: map[string]string{"OPENAI_API_KEY": "test-only"}}
	if _, err := NewWithOptions(types.AgentCodex, "fake", nil, unsafe); err == nil {
		t.Fatal("credential overlay accepted")
	}
	for _, overlay := range []runenv.Overlay{
		{Set: map[string]string{"GRIT_SUBSCRIPTION_POLICY": "0"}},
		{Unset: []string{"GRIT_SUBSCRIPTION_POLICY_FILE"}},
	} {
		unsafe = opts
		unsafe.Environment = overlay
		if _, err := NewWithOptions(types.AgentCodex, "fake", nil, unsafe); err == nil {
			t.Fatalf("subscription environment overlay accepted: %#v", overlay)
		}
	}
	for _, name := range []types.AgentName{types.AgentClaude, types.AgentGrok, types.AgentAuto, "acp:evil"} {
		if _, err := NewWithOptions(name, "fake", nil, opts); err == nil {
			t.Fatalf("forbidden agent %q accepted", name)
		}
	}
	if _, err := NewWithOptions(types.AgentCodex, "fake", nil, opts); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWithOptions(types.AgentCodex, "fake", []string{"--config", "model_provider=other"}, opts); err == nil {
		t.Fatal("raw override accepted")
	}
	if _, err := NewWithOptions(types.AgentCodex, "fake", nil, Options{}); err == nil {
		t.Fatal("implicit model accepted")
	}
	opts.ACPRegistryOverrides = map[string]string{"cursor": "evil"}
	if _, err := NewWithOptions("cursor", "fake", nil, opts); err == nil {
		t.Fatal("ACP override accepted")
	}
}

func TestSubscriptionPolicyRejectsInvocationEnvironmentOverride(t *testing.T) {
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "/managed/fm-subscription-check.mjs")
	wrapped := &subscriptionAgent{Agent: subscriptionTestAgent{}, harness: "codex", model: "model", effort: "high"}
	if _, err := wrapped.Run(context.Background(), RunOpts{Env: []string{"GRIT_SUBSCRIPTION_POLICY=0"}}); err == nil {
		t.Fatal("invocation environment override accepted")
	}
}

func TestSubscriptionPolicyAllowsManagedInvocationAfterPreflight(t *testing.T) {
	checkPath := t.TempDir() + "/check.mjs"
	if err := os.WriteFile(checkPath, []byte(`
if (process.env.GRIT_SUBSCRIPTION_POLICY !== "1") process.exit(2);
if (!process.env.GRIT_SUBSCRIPTION_POLICY_FILE.startsWith("/")) process.exit(3);
`), 0o600); err != nil {
		t.Fatal(err)
	}
	policyFile := t.TempDir() + "/subscription-policy.json"
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", policyFile)
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", checkPath)
	ran := false
	wrapped := &subscriptionAgent{Agent: subscriptionTestAgent{ran: &ran}, harness: "codex", model: "model", effort: "high"}
	if _, err := wrapped.Run(context.Background(), RunOpts{}); err != nil {
		t.Fatalf("managed invocation rejected: %v", err)
	}
	if !ran {
		t.Fatal("managed invocation did not reach the underlying agent")
	}
}

type subscriptionTestAgent struct{ ran *bool }

func (subscriptionTestAgent) Name() string { return "test" }

func (a subscriptionTestAgent) Run(context.Context, RunOpts) (*Result, error) {
	if a.ran != nil {
		*a.ran = true
	}
	return &Result{}, nil
}

func (subscriptionTestAgent) Close() error { return nil }
