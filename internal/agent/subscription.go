package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

// This fork's managed home opts in once at launch. Every constructed gate
// agent, including a configured fallback, is checked before it can execute.
func subscriptionOnly() bool { return os.Getenv("GRIT_SUBSCRIPTION_POLICY") == "1" }

func isSubscriptionPolicyEnv(key string) bool {
	return strings.HasPrefix(key, "GRIT_SUBSCRIPTION_")
}

func validateSubscriptionAgent(name types.AgentName, args []string, opts Options) error {
	for key, value := range opts.Environment.Set {
		if isSubscriptionPolicyEnv(key) && value != os.Getenv(key) {
			return fmt.Errorf("subscription policy refuses environment override %s", key)
		}
	}
	for _, key := range opts.Environment.Unset {
		if isSubscriptionPolicyEnv(key) {
			return fmt.Errorf("subscription policy refuses environment removal %s", key)
		}
	}
	for key, value := range opts.Environment.Set {
		if value != "" && (strings.Contains(strings.ToUpper(key), "API_KEY") || key == "OPENAI_BASE_URL" || key == "OPENAI_API_BASE") {
			return fmt.Errorf("subscription policy refuses environment override %s", key)
		}
	}
	if name != types.AgentCodex && string(name) != "cursor" && string(name) != "acp:cursor" {
		return fmt.Errorf("subscription policy refuses agent %q: select codex or cursor explicitly", name)
	}
	if opts.Profile.Model == "" {
		return fmt.Errorf("subscription policy requires an explicit agent_config model")
	}
	if len(opts.ACPRegistryOverrides) != 0 {
		return fmt.Errorf("subscription policy refuses custom ACP commands")
	}
	if len(args) != 0 {
		return fmt.Errorf("subscription policy uses agent_config; raw agent_args_override is disabled")
	}
	if !filepath.IsAbs(os.Getenv("GRIT_SUBSCRIPTION_CHECK")) {
		return fmt.Errorf("subscription policy requires an absolute GRIT_SUBSCRIPTION_CHECK")
	}
	return nil
}

type subscriptionAgent struct {
	Agent
	harness, model, effort string
}

func (a *subscriptionAgent) Run(ctx context.Context, opts RunOpts) (*Result, error) {
	for _, entry := range append(os.Environ(), opts.Env...) {
		key, value, _ := strings.Cut(entry, "=")
		if isSubscriptionPolicyEnv(key) && value != os.Getenv(key) {
			return nil, fmt.Errorf("subscription policy refuses environment override %s", key)
		}
		if value != "" && (strings.Contains(strings.ToUpper(key), "API_KEY") || key == "OPENAI_BASE_URL" || key == "OPENAI_API_BASE") {
			return nil, fmt.Errorf("subscription policy refuses credential or endpoint override %s", key)
		}
	}
	harness := strings.TrimPrefix(a.harness, "acp:")
	check := exec.CommandContext(ctx, "node", os.Getenv("GRIT_SUBSCRIPTION_CHECK"), harness, a.model, a.effort, "0")
	if out, err := check.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("subscription preflight failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return a.Agent.Run(ctx, opts)
}
