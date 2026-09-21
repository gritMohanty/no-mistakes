package daemon

import (
	"strings"
	"testing"
)

func keepSubscriptionEnvironmentStable(t *testing.T) {
	t.Helper()
	oldApplyShellEnv := applyShellEnvToProcess
	applyShellEnvToProcess = func(...string) error { return nil }
	t.Cleanup(func() { applyShellEnvToProcess = oldApplyShellEnv })
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
	policyFile := t.TempDir() + "/subscription-policy.json"
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", policyFile)
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "/managed/fm-subscription-check.mjs")
	keepSubscriptionEnvironmentStable(t)
	p, _ := startTestDaemon(t)

	if err := EnsureDaemon(p); err != nil {
		t.Fatalf("EnsureDaemon rejected matching managed daemon: %v", err)
	}
}

func TestEnsureDaemonRefusesMismatchedManagedPolicyFile(t *testing.T) {
	keepSubscriptionEnvironmentStable(t)
	policyFile := t.TempDir() + "/subscription-policy.json"
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", policyFile)
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "/managed/fm-subscription-check.mjs")
	p, _ := startTestDaemon(t)
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", t.TempDir()+"/other-policy.json")

	err := EnsureDaemon(p)
	if err == nil || !strings.Contains(err.Error(), "policy file") {
		t.Fatalf("EnsureDaemon error = %v, want policy-file refusal", err)
	}
}

func TestEnsureDaemonRefusesMismatchedManagedSubscriptionCheck(t *testing.T) {
	keepSubscriptionEnvironmentStable(t)
	policyFile := t.TempDir() + "/subscription-policy.json"
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", policyFile)
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "/managed/fm-subscription-check.mjs")
	p, _ := startTestDaemon(t)
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "/managed/other-check.mjs")

	err := EnsureDaemon(p)
	if err == nil || !strings.Contains(err.Error(), "subscription check") {
		t.Fatalf("EnsureDaemon error = %v, want subscription-check refusal", err)
	}
}

func TestEnsureDaemonRefusesMissingManagedPolicyPath(t *testing.T) {
	keepSubscriptionEnvironmentStable(t)
	t.Setenv("GRIT_SUBSCRIPTION_POLICY", "1")
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", "")
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", "/managed/fm-subscription-check.mjs")
	p, _ := startTestDaemon(t)

	err := EnsureDaemon(p)
	if err == nil || !strings.Contains(err.Error(), "GRIT_SUBSCRIPTION_POLICY_FILE") {
		t.Fatalf("EnsureDaemon error = %v, want missing-policy-path refusal", err)
	}
}
