package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestHeldRunBindReleaseWaitsForCaptainProofAcrossIngressPaths(t *testing.T) {
	for _, ingress := range []string{"push", "fallback"} {
		t.Run(ingress, func(t *testing.T) {
			_, policyFile, checkFile := configureManagedSubscriptionTestEnv(t)
			step := &mockPassStep{name: types.StepReview}
			p, d := startTestDaemonWithSteps(t, func() []pipeline.Step { return []pipeline.Step{step} })
			repo, headSHA := setupTestGitRepo(t, p, d, "held-"+ingress)

			const taskID = "task-meta-l21"
			const nonce = "nonce-meta-l21"
			const generation = "generation-meta-l21"
			const intent = "prove the held launch remains paused"
			client, err := ipc.Dial(p.Socket())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			var receipt ipc.LaunchReceipt
			if ingress == "push" {
				var pushed ipc.PushReceivedResult
				if err := client.Call(ipc.MethodPushReceived, &ipc.PushReceivedParams{
					Gate: p.RepoDir(repo.ID), Ref: "refs/heads/main",
					Old: strings.Repeat("0", 40), New: headSHA,
					Intent: intent, LaunchNonce: nonce, ValidationGeneration: generation,
					RequireBindRelease: true,
				}, &pushed); err != nil {
					t.Fatal(err)
				}
				if pushed.RunID == "" {
					t.Fatalf("push result = %#v", pushed)
				}
				var premature ipc.ReleaseHeldRunResult
				prematureErr := client.Call(ipc.MethodReleaseHeldRun, &ipc.ReleaseHeldRunParams{
					RunID: pushed.RunID, TaskID: taskID, Branch: "main", LaunchNonce: nonce,
					SubmittedHeadSHA: headSHA, ValidationGeneration: generation, IntentDigest: digestIntent(intent),
				}, &premature)
				if prematureErr == nil {
					t.Fatal("unclaimed push receipt was released")
				}
				var claimed ipc.ClaimLaunchReceiptResult
				if err := client.Call(ipc.MethodClaimLaunchReceipt, &ipc.ClaimLaunchReceiptParams{
					RepoID: repo.ID, Branch: "main", LaunchNonce: nonce, SubmittedHeadSHA: headSHA,
					ValidationGeneration: generation, IntentDigest: digestIntent(intent), RequireBindRelease: true,
				}, &claimed); err != nil {
					t.Fatal(err)
				}
				if claimed.Receipt == nil {
					t.Fatal("push launch receipt was not claimable")
				}
				receipt = *claimed.Receipt
			} else {
				var fresh ipc.StartFreshRunResult
				if err := client.Call(ipc.MethodStartFreshRun, &ipc.StartFreshRunParams{
					RepoID: repo.ID, Branch: "main", HeadSHA: headSHA, Intent: intent,
					LaunchNonce: nonce, ValidationGeneration: generation, RequireBindRelease: true,
				}, &fresh); err != nil {
					t.Fatal(err)
				}
				receipt = fresh.Receipt
			}
			if receipt.RunID == "" || receipt.LaunchNonce != nonce || receipt.ValidationGeneration != generation {
				t.Fatalf("launch receipt = %#v", receipt)
			}
			run, err := d.GetRun(receipt.RunID)
			if err != nil || run == nil || !run.LaunchBindReleaseRequired || run.LaunchReceiptClaimedAt == nil || run.Status != types.RunPending {
				t.Fatalf("held run before release = %#v, err=%v", run, err)
			}
			if got := step.execCnt.Load(); got != 0 {
				t.Fatalf("pipeline step executions before bind = %d, want 0", got)
			}

			params := ipc.ReleaseHeldRunParams{
				RunID: receipt.RunID, TaskID: taskID, Branch: "main", LaunchNonce: nonce,
				SubmittedHeadSHA: headSHA, ValidationGeneration: generation, IntentDigest: digestIntent(intent),
			}
			writeCaptainHeldRunProof(t, os.Getenv("FM_HOME"), p.Root(), p.DB(), policyFile, checkFile, repo, run, headSHA, generation, taskID, nonce, params.IntentDigest)

			var released ipc.ReleaseHeldRunResult
			if err := client.Call(ipc.MethodReleaseHeldRun, &params, &released); err != nil {
				t.Fatal(err)
			}
			if released.RunID != run.ID || released.ReleasedAt <= 0 || released.AlreadyReleased {
				t.Fatalf("release result = %#v", released)
			}
			terminal := waitForRunTerminalState(t, d, run.ID)
			if terminal.Status != types.RunCompleted {
				t.Fatalf("run status after bind release = %q, want completed", terminal.Status)
			}
			if got := step.execCnt.Load(); got != 1 {
				t.Fatalf("pipeline step executions after bind = %d, want 1", got)
			}
			waitForPathAbsent(t, run.WorktreePath())

			var replay ipc.ReleaseHeldRunResult
			if err := client.Call(ipc.MethodReleaseHeldRun, &params, &replay); err != nil {
				t.Fatalf("exact release replay after worktree cleanup: %v", err)
			}
			if !replay.AlreadyReleased || replay.ReleasedAt != released.ReleasedAt || replay.Status != types.RunCompleted {
				t.Fatalf("historical release replay = %#v, first release = %#v", replay, released)
			}
			if got := step.execCnt.Load(); got != 1 {
				t.Fatalf("historical release replay restarted pipeline: executions=%d", got)
			}
		})
	}
}

func TestHeldRunBindReleaseRetriesUnknownAckAndDoesNotResurrectCancelledRun(t *testing.T) {
	var releaseCalls atomic.Int32
	casReached := make(chan struct{})
	executeReached := make(chan struct{})
	allowExecuteCheck := make(chan struct{})
	fixture := newDirectHeldRunFixture(t, "retry-race", &heldRunTestHooks{
		afterReleaseCAS: func(string) error {
			if releaseCalls.Add(1) == 1 {
				close(casReached)
				return errors.New("simulated lost release acknowledgement")
			}
			return nil
		},
		beforeExecute: func(string) {
			close(executeReached)
			<-allowExecuteCheck
		},
	})

	if _, err := fixture.manager.HandleReleaseHeldRun(context.Background(), &fixture.params); err == nil || !strings.Contains(err.Error(), "lost release acknowledgement") {
		t.Fatalf("first release error = %v, want simulated lost acknowledgement", err)
	}
	<-casReached
	stored, err := fixture.db.GetRun(fixture.run.ID)
	if err != nil || stored.LaunchReleasedAt == nil {
		t.Fatalf("release CAS was not durable: run=%#v err=%v", stored, err)
	}
	if got := fixture.step.execCnt.Load(); got != 0 {
		t.Fatalf("step executed before retry acknowledged release: %d", got)
	}

	var replay ipc.ReleaseHeldRunResult
	var releaseErr error
	releaseDone := make(chan struct{})
	go func() {
		replay, releaseErr = fixture.manager.HandleReleaseHeldRun(context.Background(), &fixture.params)
		close(releaseDone)
	}()
	select {
	case <-executeReached:
	case <-time.After(5 * time.Second):
		t.Fatal("held waiter did not reach the deterministic pre-execute barrier")
	}
	select {
	case <-releaseDone:
	case <-time.After(5 * time.Second):
		t.Fatal("idempotent release retry did not return")
	}
	if releaseErr != nil || !replay.AlreadyReleased || replay.ReleasedAt != *stored.LaunchReleasedAt {
		t.Fatalf("release retry = %#v, err=%v", replay, releaseErr)
	}

	if err := fixture.manager.HandleCancel(fixture.run.ID); err != nil {
		t.Fatal(err)
	}
	close(allowExecuteCheck)
	terminal := waitForHeldRunTerminalState(t, fixture.db, fixture.run.ID)
	if terminal.Status != types.RunCancelled {
		t.Fatalf("run status after cancellation between release and Execute = %q, want cancelled", terminal.Status)
	}
	if got := fixture.step.execCnt.Load(); got != 0 {
		t.Fatalf("pipeline executed after cancellation before Execute: %d", got)
	}
}

func TestHeldRunBindReleaseCrashRecoveryDoesNotReplayOrphans(t *testing.T) {
	fixture := newDirectHeldRunFixture(t, "restart", &heldRunTestHooks{
		afterReleaseCAS: func(string) error { return errors.New("simulate daemon loss after durable release") },
	})
	if _, err := fixture.manager.HandleReleaseHeldRun(context.Background(), &fixture.params); err == nil {
		t.Fatal("release unexpectedly acknowledged before simulated daemon loss")
	}
	stored, err := fixture.db.GetRun(fixture.run.ID)
	if err != nil || stored.LaunchReleasedAt == nil || stored.Status != types.RunPending {
		t.Fatalf("post-CAS held row = %#v, err=%v", stored, err)
	}
	if got := fixture.step.execCnt.Load(); got != 0 {
		t.Fatalf("step executed before crash recovery: %d", got)
	}

	if _, err := fixture.db.RecoverStaleRunsExcept("daemon crashed during execution", nil); err != nil {
		t.Fatal(err)
	}
	fixture.manager.Shutdown()
	terminal := waitForHeldRunTerminalState(t, fixture.db, fixture.run.ID)
	if terminal.Status != types.RunFailed {
		t.Fatalf("recovered status = %q, want failed", terminal.Status)
	}
	waitForPathAbsent(t, fixture.run.WorktreePath())

	// A fresh manager has no in-memory waiter. Exact historical replay reports
	// the durable permission and terminal status but cannot restart the pipeline.
	restarted := NewRunManager(fixture.db, fixture.paths, func() []pipeline.Step { return []pipeline.Step{fixture.step} })
	restarted.managedFirstmateHome = fixture.firstmateHome
	restarted.managedSubscriptionPolicyFile = fixture.policyFile
	restarted.managedSubscriptionCheck = fixture.checkFile
	var replay ipc.ReleaseHeldRunResult
	if replay, err = restarted.HandleReleaseHeldRun(context.Background(), &fixture.params); err != nil {
		t.Fatalf("terminal exact replay after restart: %v", err)
	}
	if !replay.AlreadyReleased || replay.ReleasedAt != *stored.LaunchReleasedAt || replay.Status != types.RunFailed {
		t.Fatalf("restart historical replay = %#v", replay)
	}
	if got := fixture.step.execCnt.Load(); got != 0 {
		t.Fatalf("restart replay started pipeline: %d", got)
	}
}

func TestHeldRunBindReleaseRefusesPendingRowWithoutLiveWaiter(t *testing.T) {
	fixture := newDirectHeldRunFixture(t, "orphan", nil)
	reattached := NewRunManager(fixture.db, fixture.paths, func() []pipeline.Step { return []pipeline.Step{fixture.step} })
	reattached.managedFirstmateHome = fixture.firstmateHome
	reattached.managedSubscriptionPolicyFile = fixture.policyFile
	reattached.managedSubscriptionCheck = fixture.checkFile

	_, err := reattached.HandleReleaseHeldRun(context.Background(), &fixture.params)
	if err == nil || !strings.Contains(err.Error(), "no live daemon waiter") {
		t.Fatalf("orphaned pending release error = %v, want no-live-waiter refusal", err)
	}
	stored, err := fixture.db.GetRun(fixture.run.ID)
	if err != nil || stored.LaunchReleasedAt != nil || stored.Status != types.RunPending {
		t.Fatalf("orphaned row after rejected release = %#v, err=%v", stored, err)
	}
	if got := fixture.step.execCnt.Load(); got != 0 {
		t.Fatalf("orphaned release executed a step: %d", got)
	}
}

func TestHeldRunBindReleaseRejectsInvalidCaptainProof(t *testing.T) {
	fixture := newDirectHeldRunFixture(t, "proof-negative", nil)
	base := filepath.Join(fixture.firstmateHome, "state", "safehouse-bridge", "no-mistakes")
	launchDir := filepath.Join(base, "launches")
	reservationPath := filepath.Join(launchDir, fixture.params.TaskID+"."+fixture.params.LaunchNonce+".json")
	statePath := reservationPath + ".state"
	bindingPath := filepath.Join(base, "bindings", fixture.params.RunID+".json")

	tests := []struct {
		name   string
		mutate func(*testing.T) func()
	}{
		{name: "missing reservation", mutate: func(t *testing.T) func() { return temporarilyRenameProof(t, reservationPath) }},
		{name: "missing state", mutate: func(t *testing.T) func() { return temporarilyRenameProof(t, statePath) }},
		{name: "missing binding", mutate: func(t *testing.T) func() { return temporarilyRenameProof(t, bindingPath) }},
		{name: "malformed reservation", mutate: func(t *testing.T) func() { return temporarilyReplaceProof(t, reservationPath, []byte("{")) }},
		{name: "mismatched run", mutate: func(t *testing.T) func() { return mutateProofField(t, bindingPath, []string{"runId"}, "another-run") }},
		{name: "mismatched nonce", mutate: func(t *testing.T) func() {
			return mutateProofField(t, reservationPath, []string{"launchNonce"}, "another-nonce")
		}},
		{name: "mismatched generation", mutate: func(t *testing.T) func() {
			return mutateProofField(t, reservationPath, []string{"validationGeneration"}, "another-generation")
		}},
		{name: "mismatched head", mutate: func(t *testing.T) func() {
			return mutateProofField(t, reservationPath, []string{"headSha"}, strings.Repeat("a", 40))
		}},
		{name: "mismatched intent digest", mutate: func(t *testing.T) func() {
			return mutateProofField(t, reservationPath, []string{"intentDigest"}, strings.Repeat("b", 64))
		}},
		{name: "mismatched task", mutate: func(t *testing.T) func() {
			return mutateProofField(t, reservationPath, []string{"taskId"}, "another-task")
		}},
		{name: "mismatched scope", mutate: func(t *testing.T) func() {
			return mutateProofField(t, bindingPath, []string{"identity", "executionScopeVersion"}, "another-scope")
		}},
		{name: "wrong daemon database", mutate: func(t *testing.T) func() {
			return mutateProofField(t, bindingPath, []string{"source", "databasePath"}, filepath.Join(t.TempDir(), "other.sqlite"))
		}},
		{name: "wrong daemon home", mutate: func(t *testing.T) func() {
			return mutateProofField(t, bindingPath, []string{"source", "managedEnv", "nmHome"}, t.TempDir())
		}},
		{name: "first release from bound state", mutate: func(t *testing.T) func() { return mutateProofField(t, statePath, []string{"status"}, "bound") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			restore := tc.mutate(t)
			_, err := fixture.manager.HandleReleaseHeldRun(context.Background(), &fixture.params)
			restore()
			if err == nil {
				t.Fatal("invalid Captain proof was accepted")
			}
			stored, getErr := fixture.db.GetRun(fixture.run.ID)
			if getErr != nil || stored.LaunchReleasedAt != nil || stored.Status != types.RunPending {
				t.Fatalf("held row after proof rejection = %#v, err=%v", stored, getErr)
			}
			if got := fixture.step.execCnt.Load(); got != 0 {
				t.Fatalf("proof rejection executed pipeline step: %d", got)
			}
		})
	}

	if os.PathSeparator != '\\' {
		t.Run("symlinked proof file", func(t *testing.T) {
			restore := temporarilySymlinkProof(t, reservationPath)
			_, err := fixture.manager.HandleReleaseHeldRun(context.Background(), &fixture.params)
			restore()
			if err == nil {
				t.Fatal("symlinked Captain proof was accepted")
			}
			assertHeldRunNotReleased(t, fixture)
		})
		t.Run("symlinked proof parent", func(t *testing.T) {
			restore := temporarilySymlinkDirectory(t, launchDir)
			_, err := fixture.manager.HandleReleaseHeldRun(context.Background(), &fixture.params)
			restore()
			if err == nil {
				t.Fatal("symlinked Captain proof parent was accepted")
			}
			assertHeldRunNotReleased(t, fixture)
		})
	}
	stored, err := fixture.db.GetRun(fixture.run.ID)
	if err != nil || stored.LaunchReleasedAt != nil || stored.Status != types.RunPending {
		t.Fatalf("held row after symlink proof rejection = %#v, err=%v", stored, err)
	}
	if got := fixture.step.execCnt.Load(); got != 0 {
		t.Fatalf("invalid proofs executed pipeline step: %d", got)
	}
}

func assertHeldRunNotReleased(t *testing.T, fixture *directHeldRunFixture) {
	t.Helper()
	stored, err := fixture.db.GetRun(fixture.run.ID)
	if err != nil || stored.LaunchReleasedAt != nil || stored.Status != types.RunPending {
		t.Fatalf("held row after rejected proof = %#v, err=%v", stored, err)
	}
	if got := fixture.step.execCnt.Load(); got != 0 {
		t.Fatalf("rejected proof executed pipeline step: %d", got)
	}
}

type directHeldRunFixture struct {
	manager       *RunManager
	paths         *paths.Paths
	db            *db.DB
	repo          *db.Repo
	run           *db.Run
	step          *mockPassStep
	params        ipc.ReleaseHeldRunParams
	firstmateHome string
	policyFile    string
	checkFile     string
}

func newDirectHeldRunFixture(t *testing.T, suffix string, hooks *heldRunTestHooks) *directHeldRunFixture {
	t.Helper()
	firstmateHome, policyFile, checkFile := configureManagedSubscriptionTestEnv(t)
	root := t.TempDir()
	p := paths.WithRoot(filepath.Join(root, "nm-home"))
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	d, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })

	mockAgent := writeMockClaude(t, t.TempDir())
	configYAML := "agent: codex\nagent_path_override:\n  codex: " + mockAgent + "\nagent_config:\n  codex:\n    model: gpt-5.4\n    effort: high\n"
	if err := os.WriteFile(p.ConfigFile(), []byte(configYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	repo, headSHA := setupTestGitRepo(t, p, d, "held-"+suffix)
	step := &mockPassStep{name: types.StepReview}
	manager := NewRunManager(d, p, func() []pipeline.Step { return []pipeline.Step{step} })
	manager.managedSubscriptionPolicy = true
	manager.managedFirstmateHome = firstmateHome
	manager.managedSubscriptionPolicyFile = policyFile
	manager.managedSubscriptionCheck = checkFile
	manager.heldRunHooks = hooks
	t.Cleanup(manager.Shutdown)

	intent := "hold the pipeline until Captain binding has durable proof"
	nonce := "nonce-" + suffix
	generation := "generation-" + suffix
	fresh, err := manager.HandleStartFreshRun(context.Background(), &ipc.StartFreshRunParams{
		RepoID: repo.ID, Branch: "main", HeadSHA: headSHA, Intent: intent,
		LaunchNonce: nonce, ValidationGeneration: generation, RequireBindRelease: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.RunID == "" {
		t.Fatalf("fresh held run returned empty receipt: %#v", fresh)
	}
	run, err := d.GetRun(fresh.RunID)
	if err != nil || run == nil {
		t.Fatalf("get fresh held run: run=%#v err=%v", run, err)
	}
	params := ipc.ReleaseHeldRunParams{
		RunID: run.ID, TaskID: "task-" + suffix, Branch: run.Branch, LaunchNonce: nonce,
		SubmittedHeadSHA: headSHA, ValidationGeneration: generation, IntentDigest: digestIntent(intent),
	}
	writeCaptainHeldRunProof(t, firstmateHome, p.Root(), p.DB(), policyFile, checkFile, repo, run, headSHA, generation, params.TaskID, nonce, params.IntentDigest)
	return &directHeldRunFixture{
		manager: manager, paths: p, db: d, repo: repo, run: run, step: step, params: params,
		firstmateHome: firstmateHome, policyFile: policyFile, checkFile: checkFile,
	}
}

func mutateProofField(t *testing.T, path string, keys []string, value any) func() {
	t.Helper()
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(original, &document); err != nil {
		t.Fatal(err)
	}
	current := document
	for _, key := range keys[:len(keys)-1] {
		next, ok := current[key].(map[string]any)
		if !ok {
			t.Fatalf("proof field path %v is missing %q", keys, key)
		}
		current = next
	}
	current[keys[len(keys)-1]] = value
	updated, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(updated, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	var restored atomic.Bool
	restore := func() {
		if restored.CompareAndSwap(false, true) {
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Errorf("restore Captain proof: %v", err)
			}
		}
	}
	t.Cleanup(restore)
	return restore
}

func temporarilyRenameProof(t *testing.T, path string) func() {
	t.Helper()
	backup := path + ".held-run-test-backup"
	if err := os.Rename(path, backup); err != nil {
		t.Fatal(err)
	}
	var restored atomic.Bool
	restore := func() {
		if restored.CompareAndSwap(false, true) {
			if err := os.Rename(backup, path); err != nil {
				t.Errorf("restore Captain proof %q: %v", path, err)
			}
		}
	}
	t.Cleanup(restore)
	return restore
}

func temporarilyReplaceProof(t *testing.T, path string, replacement []byte) func() {
	t.Helper()
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, replacement, 0o600); err != nil {
		t.Fatal(err)
	}
	var restored atomic.Bool
	restore := func() {
		if restored.CompareAndSwap(false, true) {
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Errorf("restore Captain proof %q: %v", path, err)
			}
		}
	}
	t.Cleanup(restore)
	return restore
}

func temporarilySymlinkProof(t *testing.T, path string) func() {
	t.Helper()
	backup := path + ".held-run-test-backup"
	if err := os.Rename(path, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(backup, path); err != nil {
		_ = os.Rename(backup, path)
		t.Fatal(err)
	}
	var restored atomic.Bool
	restore := func() {
		if restored.CompareAndSwap(false, true) {
			_ = os.Remove(path)
			if err := os.Rename(backup, path); err != nil {
				t.Errorf("restore Captain proof %q: %v", path, err)
			}
		}
	}
	t.Cleanup(restore)
	return restore
}

func temporarilySymlinkDirectory(t *testing.T, path string) func() {
	t.Helper()
	backup := path + ".held-run-test-backup"
	if err := os.Rename(path, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(backup, path); err != nil {
		_ = os.Rename(backup, path)
		t.Fatal(err)
	}
	var restored atomic.Bool
	restore := func() {
		if restored.CompareAndSwap(false, true) {
			_ = os.Remove(path)
			if err := os.Rename(backup, path); err != nil {
				t.Errorf("restore Captain proof directory %q: %v", path, err)
			}
		}
	}
	t.Cleanup(restore)
	return restore
}

func TestHeldRunBindReleaseCancellationBeforeCASDoesNotStartPipeline(t *testing.T) {
	_, policyFile, checkFile := configureManagedSubscriptionTestEnv(t)
	step := &mockPassStep{name: types.StepReview}
	p, d := startTestDaemonWithSteps(t, func() []pipeline.Step { return []pipeline.Step{step} })
	repo, headSHA := setupTestGitRepo(t, p, d, "held-cancel")

	const taskID = "task-meta-l21-cancel"
	const nonce = "nonce-meta-l21-cancel"
	const generation = "generation-meta-l21-cancel"
	const intent = "cancel before releasing a held run"
	client, err := ipc.Dial(p.Socket())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var fresh ipc.StartFreshRunResult
	if err := client.Call(ipc.MethodStartFreshRun, &ipc.StartFreshRunParams{
		RepoID: repo.ID, Branch: "main", HeadSHA: headSHA, Intent: intent,
		LaunchNonce: nonce, ValidationGeneration: generation, RequireBindRelease: true,
	}, &fresh); err != nil {
		t.Fatal(err)
	}
	run, err := d.GetRun(fresh.Receipt.RunID)
	if err != nil || run == nil {
		t.Fatalf("get held run: run=%#v err=%v", run, err)
	}
	params := ipc.ReleaseHeldRunParams{
		RunID: run.ID, TaskID: taskID, Branch: "main", LaunchNonce: nonce,
		SubmittedHeadSHA: headSHA, ValidationGeneration: generation, IntentDigest: digestIntent(intent),
	}
	writeCaptainHeldRunProof(t, os.Getenv("FM_HOME"), p.Root(), p.DB(), policyFile, checkFile, repo, run, headSHA, generation, taskID, nonce, params.IntentDigest)

	var cancelled ipc.CancelRunResult
	if err := client.Call(ipc.MethodCancelRun, &ipc.CancelRunParams{RunID: run.ID}, &cancelled); err != nil {
		t.Fatal(err)
	}
	terminal := waitForHeldRunTerminalState(t, d, run.ID)
	if terminal.Status != types.RunCancelled {
		t.Fatalf("run status after cancellation = %q, want cancelled", terminal.Status)
	}
	var released ipc.ReleaseHeldRunResult
	if err := client.Call(ipc.MethodReleaseHeldRun, &params, &released); err == nil {
		t.Fatalf("release after cancellation unexpectedly succeeded: %#v", released)
	}
	if got := step.execCnt.Load(); got != 0 {
		t.Fatalf("pipeline step executions after cancellation = %d, want 0", got)
	}
}

func writeCaptainHeldRunProof(t *testing.T, firstmateHome, nmHome, databasePath, policyFile, checkFile string, repo *db.Repo, run *db.Run, headSHA, generation, taskID, nonce, intentDigest string) {
	t.Helper()
	nativeWorktree := filepath.Join(t.TempDir(), "treehouse-task")
	if err := os.MkdirAll(nativeWorktree, 0o700); err != nil {
		t.Fatal(err)
	}
	identity := captainExecutionIdentity{
		Schema: "captain-execution-identity.v1", RequestID: "request-l21", TaskID: taskID,
		AttemptID: "attempt-1", TicketKey: "META-31", ProjectID: "captain-project",
		CanonicalProjectPath: repo.WorkingPath, ExecutionScopeVersion: "scope-v1",
	}
	reservation := captainLaunchReservation{
		Schema: captainLaunchReservationSchema, TaskID: taskID, Identity: identity,
		NativeWorktreePath: nativeWorktree, Branch: run.Branch, HeadSHA: headSHA,
		LaunchNonce: nonce, ValidationGeneration: generation, IntentDigest: intentDigest,
	}
	state := captainLaunchState{
		Schema: captainLaunchStateSchema, TaskID: taskID, LaunchNonce: nonce,
		Status: "bound_pending_release", RunID: run.ID,
	}
	physicalRunWorktree, err := filepath.EvalSymlinks(run.WorktreePath())
	if err != nil {
		t.Fatal(err)
	}
	binding := captainLaunchBinding{
		Schema: captainLaunchBindingSchema, RunID: run.ID, Identity: identity,
		ProjectPath: repo.WorkingPath, WorktreePath: physicalRunWorktree, NativeWorktreePath: nativeWorktree,
		Branch: run.Branch, LaunchHeadSHA: headSHA,
		Source: captainLaunchSource{
			DatabasePath: databasePath, RunID: run.ID,
			ManagedEnv: captainManagedEnv{Marker: "1", PolicyFile: policyFile, Check: checkFile, NMHome: nmHome},
		},
	}
	base := filepath.Join(firstmateHome, "state", "safehouse-bridge", "no-mistakes")
	launchDir := filepath.Join(base, "launches")
	bindingDir := filepath.Join(base, "bindings")
	if err := os.MkdirAll(launchDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(bindingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeJSONFile(t, filepath.Join(launchDir, taskID+"."+nonce+".json"), reservation)
	writeJSONFile(t, filepath.Join(launchDir, taskID+"."+nonce+".json.state"), state)
	writeJSONFile(t, filepath.Join(bindingDir, run.ID+".json"), binding)
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitForPathAbsent(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected worktree cleanup to remove %q", path)
}

func waitForHeldRunTerminalState(t *testing.T, d *db.DB, runID string) *db.Run {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := d.GetRun(runID)
		if err != nil {
			t.Fatal(err)
		}
		if run != nil && run.Status.Terminal() {
			return run
		}
		time.Sleep(25 * time.Millisecond)
	}
	run, _ := d.GetRun(runID)
	t.Fatalf("run %s did not reach terminal state; current status=%v", runID, run)
	return nil
}
