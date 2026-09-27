package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/daemon"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/spf13/cobra"
)

const (
	launchFenceHealthTimeout = 5 * time.Second
	launchFenceIPCTimeout    = 8 * time.Second
)

func newAxiFenceLaunchNonceCmd() *cobra.Command {
	var params ipc.FenceLaunchNonceParams
	var checkCapability bool
	cmd := &cobra.Command{
		Use:    "fence-launch-nonce",
		Short:  "Durably fence an exact managed launch nonce that has no run",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !checkCapability {
				if err := validateFenceLaunchNonceCLIParams(&params); err != nil {
					return err
				}
			}
			if os.Getenv("NM_HOME") == "" || !filepath.IsAbs(os.Getenv("NM_HOME")) {
				return fmt.Errorf("absolute NM_HOME is required for a managed launch nonce fence")
			}
			p, err := paths.New()
			if err != nil {
				return fmt.Errorf("resolve no-mistakes paths: %w", err)
			}
			if err := daemon.EnsureDaemon(p); err != nil {
				return fmt.Errorf("ensure managed daemon for launch fence capability: %w", err)
			}
			client, err := ipc.Dial(p.Socket())
			if err != nil {
				return fmt.Errorf("connect to no-mistakes daemon: %w", err)
			}
			defer client.Close()

			var health ipc.HealthResult
			if err := client.CallWithTimeout(ipc.MethodHealth, &ipc.HealthParams{}, &health, launchFenceHealthTimeout); err != nil {
				return fmt.Errorf("check managed launch fence capability: %w", err)
			}
			if err := verifyLaunchFenceDaemonHealth(health); err != nil {
				return err
			}
			if checkCapability {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]any{
					"capability": "launch_nonce_fence", "supported": true,
				})
			}

			var result ipc.FenceLaunchNonceResult
			if err := client.CallWithTimeout(ipc.MethodFenceLaunchNonce, &params, &result, launchFenceIPCTimeout); err != nil {
				return fmt.Errorf("fence launch nonce: %w", err)
			}
			if result.Branch != params.Branch || result.LaunchNonce != params.LaunchNonce {
				return fmt.Errorf("daemon returned a fence result for a different launch identity")
			}
			if result.Fenced {
				if result.RepoID == "" || result.CreatedAt <= 0 || result.ExistingRunID != "" {
					return fmt.Errorf("daemon returned an incomplete durable launch fence acknowledgement")
				}
			} else if result.ExistingRunID != "" {
				if err := json.NewEncoder(cmd.OutOrStdout()).Encode(result); err != nil {
					return err
				}
				// A run that won the insertion race is a complete, typed outcome
				// for the caller to reconcile by exact run ID. It is not a fence
				// success, as the explicit fenced:false field makes clear.
				return nil
			} else {
				return fmt.Errorf("daemon did not confirm a durable launch nonce fence")
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
		},
	}
	cmd.Flags().BoolVar(&checkCapability, "check-capability", false, "verify managed daemon fence support without changing state")
	cmd.Flags().StringVar(&params.TaskID, "task-id", "", "Firstmate task ID")
	cmd.Flags().StringVar(&params.Branch, "branch", "", "reserved run branch")
	cmd.Flags().StringVar(&params.LaunchNonce, "launch-nonce", "", "reserved launch nonce")
	cmd.Flags().StringVar(&params.SubmittedHeadSHA, "submitted-head-sha", "", "reserved submitted head SHA")
	cmd.Flags().StringVar(&params.ValidationGeneration, "validation-generation", "", "reserved validation generation")
	cmd.Flags().StringVar(&params.IntentDigest, "intent-digest", "", "reserved intent digest")
	return cmd
}

func validateFenceLaunchNonceCLIParams(params *ipc.FenceLaunchNonceParams) error {
	for name, value := range map[string]string{
		"--task-id": params.TaskID, "--branch": params.Branch, "--launch-nonce": params.LaunchNonce,
		"--submitted-head-sha": params.SubmittedHeadSHA, "--validation-generation": params.ValidationGeneration,
		"--intent-digest": params.IntentDigest,
	} {
		if value == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	return nil
}

func verifyLaunchFenceDaemonHealth(health ipc.HealthResult) error {
	if health.Status != "ok" || !health.SubscriptionPolicy || !health.SupportsLaunchNonceFence {
		return fmt.Errorf("daemon does not advertise managed launch nonce fence support")
	}
	for name, pair := range map[string][2]string{
		"FM_HOME":                       {os.Getenv("FM_HOME"), health.SubscriptionFirstmateHome},
		"GRIT_SUBSCRIPTION_POLICY_FILE": {os.Getenv("GRIT_SUBSCRIPTION_POLICY_FILE"), health.SubscriptionPolicyFile},
		"GRIT_SUBSCRIPTION_CHECK":       {os.Getenv("GRIT_SUBSCRIPTION_CHECK"), health.SubscriptionCheck},
	} {
		if !sameCLIPhysicalPath(pair[0], pair[1]) {
			return fmt.Errorf("%s does not match the managed daemon pin", name)
		}
	}
	return nil
}

func sameCLIPhysicalPath(left, right string) bool {
	if !filepath.IsAbs(left) || !filepath.IsAbs(right) {
		return false
	}
	l, leftErr := filepath.EvalSymlinks(left)
	r, rightErr := filepath.EvalSymlinks(right)
	return leftErr == nil && rightErr == nil && filepath.Clean(l) == filepath.Clean(r)
}
