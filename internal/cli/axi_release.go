package cli

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/spf13/cobra"
)

const heldRunReleaseIPCTimeout = 8 * time.Second

func newAxiReleaseHeldRunCmd() *cobra.Command {
	var params ipc.ReleaseHeldRunParams
	cmd := &cobra.Command{
		Use:    "release-held-run",
		Short:  "Release a held run after Captain binds its exact launch receipt",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			for name, value := range map[string]string{
				"--run-id": params.RunID, "--task-id": params.TaskID, "--branch": params.Branch,
				"--launch-nonce": params.LaunchNonce, "--submitted-head-sha": params.SubmittedHeadSHA,
				"--validation-generation": params.ValidationGeneration, "--intent-digest": params.IntentDigest,
			} {
				if value == "" {
					return fmt.Errorf("%s is required", name)
				}
			}
			p, err := paths.New()
			if err != nil {
				return fmt.Errorf("resolve no-mistakes paths: %w", err)
			}
			client, err := ipc.Dial(p.Socket())
			if err != nil {
				return fmt.Errorf("connect to no-mistakes daemon: %w", err)
			}
			defer client.Close()
			var result ipc.ReleaseHeldRunResult
			if err := client.CallWithTimeout(ipc.MethodReleaseHeldRun, &params, &result, heldRunReleaseIPCTimeout); err != nil {
				return fmt.Errorf("release held run: %w", err)
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
		},
	}
	cmd.Flags().StringVar(&params.RunID, "run-id", "", "no-mistakes run ID")
	cmd.Flags().StringVar(&params.TaskID, "task-id", "", "Firstmate task ID")
	cmd.Flags().StringVar(&params.Branch, "branch", "", "reserved run branch")
	cmd.Flags().StringVar(&params.LaunchNonce, "launch-nonce", "", "reserved launch nonce")
	cmd.Flags().StringVar(&params.SubmittedHeadSHA, "submitted-head-sha", "", "reserved submitted head SHA")
	cmd.Flags().StringVar(&params.ValidationGeneration, "validation-generation", "", "reserved validation generation")
	cmd.Flags().StringVar(&params.IntentDigest, "intent-digest", "", "reserved intent digest")
	return cmd
}
