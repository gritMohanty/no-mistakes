package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/ipc"
)

func TestRequireHeldRunBindReleaseCapabilityRejectsOldDaemon(t *testing.T) {
	root := t.TempDir()
	policyFile := filepath.Join(root, "policy.json")
	check := filepath.Join(root, "check.sh")
	home := filepath.Join(root, "firstmate")
	if err := os.WriteFile(policyFile, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(check, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", policyFile)
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", check)
	t.Setenv("FM_HOME", home)

	for _, supported := range []bool{false, true} {
		t.Run(map[bool]string{false: "old daemon", true: "current daemon"}[supported], func(t *testing.T) {
			socketPath := filepath.Join(makeSocketSafeTempDir(t), "bind-release-health.sock")
			srv := ipc.NewServer()
			srv.Handle(ipc.MethodHealth, func(_ context.Context, _ json.RawMessage) (interface{}, error) {
				return &ipc.HealthResult{
					Status:                     "ok",
					SubscriptionPolicy:         true,
					SubscriptionPolicyFile:     policyFile,
					SubscriptionCheck:          check,
					SubscriptionFirstmateHome:  home,
					SupportsHeldRunBindRelease: supported,
				}, nil
			})
			startIPCServer(t, srv, socketPath)
			client := dialReady(t, socketPath)
			defer client.Close()

			err := requireHeldRunBindReleaseCapability(client)
			if supported && err != nil {
				t.Fatalf("current daemon rejected bind-release capability: %v", err)
			}
			if !supported && (err == nil || !strings.Contains(err.Error(), "does not advertise")) {
				t.Fatalf("old daemon error = %v, want explicit capability refusal", err)
			}
		})
	}
}

func TestRequireHeldRunBindReleaseCapabilityRequiresManagedPolicy(t *testing.T) {
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "")
	if err := requireHeldRunBindReleaseCapability(nil); err == nil || !strings.Contains(err.Error(), "requires a managed subscription policy") {
		t.Fatalf("error = %v, want managed-policy refusal", err)
	}
}
