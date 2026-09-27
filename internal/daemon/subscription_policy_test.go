package daemon

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
)

func keepSubscriptionEnvironmentStable(t *testing.T) {
	t.Helper()
	oldApplyShellEnv := applyShellEnvToProcess
	applyShellEnvToProcess = func(...string) error { return nil }
	t.Cleanup(func() { applyShellEnvToProcess = oldApplyShellEnv })
}

func configureManagedSubscriptionTestEnv(t *testing.T) (home, policyFile, checkFile string) {
	t.Helper()
	root := t.TempDir()
	home = filepath.Join(root, "firstmate-home")
	if err := os.MkdirAll(filepath.Join(home, "config"), 0o700); err != nil {
		t.Fatal(err)
	}
	registryFile := filepath.Join(root, "captain-registry.json")
	registry := map[string]any{"firstmate": map[string]any{"home": home}}
	data, err := json.Marshal(registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(registryFile, data, 0o600); err != nil {
		t.Fatal(err)
	}
	policyFile = filepath.Join(home, "config", "subscription-policy.json")
	policy, err := json.Marshal(map[string]any{"registry": registryFile})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(policyFile, policy, 0o600); err != nil {
		t.Fatal(err)
	}
	checkFile = filepath.Join(root, "fm-subscription-check.mjs")
	if err := os.WriteFile(checkFile, []byte("// test fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FM_HOME", home)
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", policyFile)
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", checkFile)
	return home, policyFile, checkFile
}

func TestEnsureDaemonRefusesUnmanagedDaemonForManagedCaller(t *testing.T) {
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", "")
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "")
	keepSubscriptionEnvironmentStable(t)
	p, _ := startTestDaemon(t)
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", t.TempDir()+"/subscription-policy.json")

	err := EnsureDaemon(p)
	if err == nil || !strings.Contains(err.Error(), "existing unmanaged daemon") {
		t.Fatalf("EnsureDaemon error = %v, want unmanaged-daemon refusal", err)
	}
}

func TestEnsureDaemonAcceptsMatchingManagedDaemon(t *testing.T) {
	configureManagedSubscriptionTestEnv(t)
	keepSubscriptionEnvironmentStable(t)
	p, _ := startTestDaemon(t)

	if err := EnsureDaemon(p); err != nil {
		t.Fatalf("EnsureDaemon rejected matching managed daemon: %v", err)
	}
}

func TestEnsureDaemonRefusesMismatchedManagedPolicyFile(t *testing.T) {
	keepSubscriptionEnvironmentStable(t)
	_, _, _ = configureManagedSubscriptionTestEnv(t)
	p, _ := startTestDaemon(t)
	other := filepath.Join(t.TempDir(), "other-policy.json")
	if err := os.WriteFile(other, []byte(`{"registry":"/tmp/registry.json"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", other)

	err := EnsureDaemon(p)
	if err == nil || !strings.Contains(err.Error(), "policy file") {
		t.Fatalf("EnsureDaemon error = %v, want policy-file refusal", err)
	}
}

func TestEnsureDaemonRefusesMismatchedManagedSubscriptionCheck(t *testing.T) {
	keepSubscriptionEnvironmentStable(t)
	configureManagedSubscriptionTestEnv(t)
	p, _ := startTestDaemon(t)
	other := filepath.Join(t.TempDir(), "other-check.mjs")
	if err := os.WriteFile(other, []byte("// test fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", other)

	err := EnsureDaemon(p)
	if err == nil || !strings.Contains(err.Error(), "subscription check") {
		t.Fatalf("EnsureDaemon error = %v, want subscription-check refusal", err)
	}
}

func TestEnsureDaemonRefusesMissingManagedPolicyPath(t *testing.T) {
	keepSubscriptionEnvironmentStable(t)
	t.Setenv("FM_HOME", "")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", "")
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "")
	p, _ := startTestDaemon(t)
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", "")
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "/managed/fm-subscription-check.mjs")

	err := EnsureDaemon(p)
	if err == nil || !strings.Contains(err.Error(), "GRIT_SUBSCRIPTION_POLICY_FILE") {
		t.Fatalf("EnsureDaemon error = %v, want missing-policy-path refusal", err)
	}
}

func TestEnsureDaemonAcceptsLegacyManagedModeWithoutFirstmateHome(t *testing.T) {
	keepSubscriptionEnvironmentStable(t)
	_, policyFile, checkFile := configureManagedSubscriptionTestEnv(t)
	t.Setenv("FM_HOME", "")
	p, _ := startTestDaemon(t)
	waitForManagedPolicyTestDaemon(t, p)

	if err := EnsureDaemon(p); err != nil {
		t.Fatalf("EnsureDaemon rejected legacy managed mode without FM_HOME: %v", err)
	}
	if got := policyFile; got == "" || checkFile == "" {
		t.Fatal("test fixture did not provide managed policy paths")
	}
}

func waitForManagedPolicyTestDaemon(t *testing.T, p *paths.Paths) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		client, err := ipc.Dial(p.Socket())
		if err == nil {
			var health ipc.HealthResult
			err = client.Call(ipc.MethodHealth, &ipc.HealthParams{}, &health)
			client.Close()
			if err == nil && health.Status == "ok" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("managed policy test daemon did not become healthy")
}
