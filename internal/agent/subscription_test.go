package agent

import (
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
