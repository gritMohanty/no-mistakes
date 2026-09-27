package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/ipc"
)

func TestLaunchFenceHealthRequiresNewManagedDaemonCapability(t *testing.T) {
	root := t.TempDir()
	fmHome := filepath.Join(root, "fm-home")
	policyFile := filepath.Join(fmHome, "policy.json")
	checkFile := filepath.Join(root, "check.mjs")
	for _, path := range []string{fmHome, filepath.Dir(policyFile), filepath.Dir(checkFile)} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{policyFile, checkFile} {
		if err := os.WriteFile(path, []byte("test"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("FM_HOME", fmHome)
	t.Setenv("GRIT_SUBSCRIPTION_POLICY_FILE", policyFile)
	t.Setenv("GRIT_SUBSCRIPTION_CHECK", checkFile)

	oldDaemon := ipc.HealthResult{
		Status: "ok", SubscriptionPolicy: true, SubscriptionFirstmateHome: fmHome,
		SubscriptionPolicyFile: policyFile, SubscriptionCheck: checkFile,
	}
	if err := verifyLaunchFenceDaemonHealth(oldDaemon); err == nil || !strings.Contains(err.Error(), "does not advertise") {
		t.Fatalf("old daemon health result = %v", err)
	}
	newDaemon := oldDaemon
	newDaemon.SupportsLaunchNonceFence = true
	if err := verifyLaunchFenceDaemonHealth(newDaemon); err != nil {
		t.Fatalf("matching managed fence capability refused: %v", err)
	}
	t.Setenv("FM_HOME", t.TempDir())
	if err := verifyLaunchFenceDaemonHealth(newDaemon); err == nil || !strings.Contains(err.Error(), "FM_HOME") {
		t.Fatalf("wrong caller FM_HOME result = %v", err)
	}
}
