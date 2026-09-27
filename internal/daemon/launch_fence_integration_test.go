package daemon

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type directLaunchFenceFixture struct {
	manager       *RunManager
	database      *db.DB
	repo          *db.Repo
	params        ipc.FenceLaunchNonceParams
	firstmateHome string
	projectPath   string
	nativePath    string
}

func newDirectLaunchFenceFixture(t *testing.T, suffix string) *directLaunchFenceFixture {
	t.Helper()
	firstmateHome, policyFile, checkFile := configureManagedSubscriptionTestEnv(t)
	root, err := os.MkdirTemp("", "fence")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	p := paths.WithRoot(filepath.Join(root, "nm-home"))
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	projectPath := filepath.Join(root, "registered-project")
	if err := os.MkdirAll(projectPath, 0o700); err != nil {
		t.Fatal(err)
	}
	projectPath, err = filepath.EvalSymlinks(projectPath)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := database.InsertRepoWithID("repo-"+suffix, projectPath, "file:///unused", "main")
	if err != nil {
		t.Fatal(err)
	}
	nativePath := filepath.Join(root, "native-task-worktree")
	if err := os.MkdirAll(nativePath, 0o700); err != nil {
		t.Fatal(err)
	}
	nativePath, err = filepath.EvalSymlinks(nativePath)
	if err != nil {
		t.Fatal(err)
	}
	manager := NewRunManager(database, p, nil)
	manager.managedSubscriptionPolicy = true
	manager.managedFirstmateHome = firstmateHome
	manager.managedSubscriptionPolicyFile = policyFile
	manager.managedSubscriptionCheck = checkFile
	t.Cleanup(manager.Shutdown)

	params := ipc.FenceLaunchNonceParams{
		TaskID: "task-" + suffix, Branch: "feature/fence", LaunchNonce: "nonce-" + suffix,
		SubmittedHeadSHA: strings.Repeat("a", 40), ValidationGeneration: "generation-" + suffix,
		IntentDigest: strings.Repeat("b", 64),
	}
	identity := captainExecutionIdentity{
		Schema: "captain-execution-identity.v1", RequestID: "request-" + suffix,
		TaskID: params.TaskID, AttemptID: "attempt-" + suffix, TicketKey: "META-31",
		ProjectID: "captain-project", CanonicalProjectPath: projectPath, ExecutionScopeVersion: "scope-" + suffix,
	}
	reservation := captainLaunchReservation{
		Schema: captainLaunchReservationSchema, TaskID: params.TaskID, Identity: identity,
		NativeWorktreePath: nativePath, Branch: params.Branch, HeadSHA: params.SubmittedHeadSHA,
		LaunchNonce: params.LaunchNonce, ValidationGeneration: params.ValidationGeneration,
		IntentDigest: params.IntentDigest,
	}
	claim := captainLaunchClaim{
		Schema: captainClaimSchema, ID: identity.RequestID, RequestID: identity.RequestID,
		TicketKey: identity.TicketKey, ProjectID: identity.ProjectID, ProjectPath: projectPath,
		ExecutionScopeVersion: identity.ExecutionScopeVersion, Status: "claimed",
	}
	base := filepath.Join(firstmateHome, "state", "safehouse-bridge")
	launches := filepath.Join(base, "no-mistakes", "launches")
	claims := filepath.Join(base, "claims")
	if err := os.MkdirAll(launches, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(claims, 0o700); err != nil {
		t.Fatal(err)
	}
	writeJSONFile(t, filepath.Join(launches, params.TaskID+"."+params.LaunchNonce+".json"), reservation)
	writeJSONFile(t, filepath.Join(claims, identity.RequestID+".json"), claim)
	return &directLaunchFenceFixture{
		manager: manager, database: database, repo: repo, params: params,
		firstmateHome: firstmateHome, projectPath: projectPath, nativePath: nativePath,
	}
}

func TestFenceLaunchNoncePersistsExactNoRunProofAndReplays(t *testing.T) {
	fixture := newDirectLaunchFenceFixture(t, "valid")
	client := launchFenceTestClient(t, fixture.manager, fixture.database)
	var health ipc.HealthResult
	if err := client.Call(ipc.MethodHealth, &ipc.HealthParams{}, &health); err != nil {
		t.Fatal(err)
	}
	if !health.SupportsLaunchNonceFence || !health.SubscriptionPolicy || health.SubscriptionFirstmateHome != fixture.firstmateHome {
		t.Fatalf("managed fence capability health = %#v", health)
	}
	var first ipc.FenceLaunchNonceResult
	err := client.Call(ipc.MethodFenceLaunchNonce, &fixture.params, &first)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Fenced || first.AlreadyFenced || first.CreatedAt <= 0 || first.RepoID != fixture.repo.ID || first.ExistingRunID != "" {
		t.Fatalf("first no-run fence = %#v", first)
	}
	if run, err := fixture.database.GetRunByLaunchNonce(fixture.repo.ID, fixture.params.Branch, fixture.params.LaunchNonce); err != nil || run != nil {
		t.Fatalf("fence created a synthetic run: run=%#v err=%v", run, err)
	}
	fence, err := fixture.database.GetLaunchNonceFence(fixture.repo.ID, fixture.params.Branch, fixture.params.LaunchNonce)
	if err != nil || fence == nil || fence.TaskID != fixture.params.TaskID || fence.NativeWorktreePath != fixture.nativePath || fence.CanonicalProjectPath != fixture.projectPath {
		t.Fatalf("persisted exact fence = %#v, err=%v", fence, err)
	}
	var second ipc.FenceLaunchNonceResult
	err = client.Call(ipc.MethodFenceLaunchNonce, &fixture.params, &second)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Fenced || !second.AlreadyFenced || second.CreatedAt != first.CreatedAt {
		t.Fatalf("exact fence replay = %#v, first = %#v", second, first)
	}
	statePath := filepath.Join(fixture.firstmateHome, "state", "safehouse-bridge", "no-mistakes", "launches", fixture.params.TaskID+"."+fixture.params.LaunchNonce+".json.state")
	writeJSONFile(t, statePath, captainLaunchState{
		Schema: captainLaunchStateSchema, TaskID: fixture.params.TaskID, LaunchNonce: fixture.params.LaunchNonce,
		Status: "reconciled", Disposition: "no_run_fenced",
	})
	var stateReplay ipc.FenceLaunchNonceResult
	if err := client.Call(ipc.MethodFenceLaunchNonce, &fixture.params, &stateReplay); err != nil {
		t.Fatalf("replay after Captain persisted no-run disposition: %v", err)
	}
	if !stateReplay.Fenced || !stateReplay.AlreadyFenced || stateReplay.CreatedAt != first.CreatedAt {
		t.Fatalf("state-backed exact fence replay = %#v", stateReplay)
	}
}

func TestDelayedFencedLaunchCannotSupersedeNewerRunAcrossIngressPaths(t *testing.T) {
	firstmateHome, _, _ := configureManagedSubscriptionTestEnv(t)
	step := &fenceMarkerBlockStep{started: make(chan struct{}, 1), cancelled: make(chan struct{}), release: make(chan struct{})}
	p, database := startTestDaemonWithSteps(t, func() []pipeline.Step { return []pipeline.Step{step} })
	repo, headSHA := setupTestGitRepo(t, p, database, "fenced-delayed-ingress")
	client, err := ipc.Dial(p.Socket())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer step.unblock()

	const activeNonce = "nonce-newer-active-run"
	var active ipc.StartFreshRunResult
	if err := client.Call(ipc.MethodStartFreshRun, &ipc.StartFreshRunParams{
		RepoID: repo.ID, Branch: "main", HeadSHA: headSHA, Intent: "keep this newer run active",
		LaunchNonce: activeNonce, ValidationGeneration: "generation-newer-active",
	}, &active); err != nil {
		t.Fatalf("start newer same-branch run: %v", err)
	}
	select {
	case <-step.started:
	case <-time.After(10 * time.Second):
		t.Fatal("newer same-branch run did not reach the blocking pipeline step")
	}
	activeRun, err := database.GetRun(active.Receipt.RunID)
	if err != nil || activeRun == nil || activeRun.Status != types.RunRunning {
		t.Fatalf("newer run before delayed launch: run=%#v err=%v", activeRun, err)
	}
	if got := step.calls.Load(); got != 1 {
		t.Fatalf("pipeline marker calls before fencing delayed nonce = %d, want 1", got)
	}

	const taskID = "task-delayed-fence"
	const oldNonce = "nonce-delayed-fenced-launch"
	const generation = "generation-delayed-fenced-launch"
	const intent = "this exact no-run request is fenced before its delayed producer arrives"
	intentDigest := digestIntent(intent)
	nativePath := filepath.Join(t.TempDir(), "native-task-worktree")
	if err := os.MkdirAll(nativePath, 0o700); err != nil {
		t.Fatal(err)
	}
	nativePath, err = filepath.EvalSymlinks(nativePath)
	if err != nil {
		t.Fatal(err)
	}
	identity := captainExecutionIdentity{
		Schema: "captain-execution-identity.v1", RequestID: "request-delayed-fence",
		TaskID: taskID, AttemptID: "attempt-delayed-fence", TicketKey: "META-31",
		ProjectID: "fence-test-project", CanonicalProjectPath: repo.WorkingPath,
		ExecutionScopeVersion: "scope-delayed-fence",
	}
	reservation := captainLaunchReservation{
		Schema: captainLaunchReservationSchema, TaskID: taskID, Identity: identity,
		NativeWorktreePath: nativePath, Branch: "main", HeadSHA: headSHA,
		LaunchNonce: oldNonce, ValidationGeneration: generation, IntentDigest: intentDigest,
	}
	claim := captainLaunchClaim{
		Schema: captainClaimSchema, ID: identity.RequestID, RequestID: identity.RequestID,
		TicketKey: identity.TicketKey, ProjectID: identity.ProjectID, ProjectPath: repo.WorkingPath,
		ExecutionScopeVersion: identity.ExecutionScopeVersion, Status: "claimed",
	}
	proofBase := filepath.Join(firstmateHome, "state", "safehouse-bridge")
	launchDir := filepath.Join(proofBase, "no-mistakes", "launches")
	claimDir := filepath.Join(proofBase, "claims")
	if err := os.MkdirAll(launchDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(claimDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeJSONFile(t, filepath.Join(launchDir, taskID+"."+oldNonce+".json"), reservation)
	writeJSONFile(t, filepath.Join(claimDir, identity.RequestID+".json"), claim)

	var fence ipc.FenceLaunchNonceResult
	if err := client.Call(ipc.MethodFenceLaunchNonce, &ipc.FenceLaunchNonceParams{
		TaskID: taskID, Branch: "main", LaunchNonce: oldNonce, SubmittedHeadSHA: headSHA,
		ValidationGeneration: generation, IntentDigest: intentDigest,
	}, &fence); err != nil {
		t.Fatalf("persist exact no-run fence: %v", err)
	}
	if !fence.Fenced || fence.ExistingRunID != "" {
		t.Fatalf("no-run fence result = %#v", fence)
	}

	for _, ingress := range []string{"push", "fallback"} {
		t.Run(ingress, func(t *testing.T) {
			var err error
			switch ingress {
			case "push":
				var pushed ipc.PushReceivedResult
				err = client.Call(ipc.MethodPushReceived, &ipc.PushReceivedParams{
					Gate: p.RepoDir(repo.ID), Ref: "refs/heads/main",
					Old: strings.Repeat("0", 40), New: headSHA,
					Intent: intent, LaunchNonce: oldNonce, ValidationGeneration: generation,
					RequireBindRelease: true,
				}, &pushed)
			case "fallback":
				var fresh ipc.StartFreshRunResult
				err = client.Call(ipc.MethodStartFreshRun, &ipc.StartFreshRunParams{
					RepoID: repo.ID, Branch: "main", HeadSHA: headSHA, Intent: intent,
					LaunchNonce: oldNonce, ValidationGeneration: generation, RequireBindRelease: true,
				}, &fresh)
			}
			if err == nil || !strings.Contains(err.Error(), "durable no-run fence") {
				t.Fatalf("delayed %s ingress error = %v, want durable fence refusal", ingress, err)
			}
			if run, err := database.GetRunByLaunchNonce(repo.ID, "main", oldNonce); err != nil || run != nil {
				t.Fatalf("delayed %s ingress created a run: run=%#v err=%v", ingress, run, err)
			}
			stillActive, err := database.GetRun(active.Receipt.RunID)
			if err != nil || stillActive == nil || stillActive.Status != types.RunRunning {
				t.Fatalf("delayed %s ingress superseded newer run: run=%#v err=%v", ingress, stillActive, err)
			}
			if got := step.calls.Load(); got != 1 {
				t.Fatalf("delayed %s ingress reached pipeline marker %d times, want 1", ingress, got)
			}
			select {
			case <-step.cancelled:
				t.Fatalf("delayed %s ingress cancelled the newer run", ingress)
			default:
			}
		})
	}

	step.unblock()
	if terminal := waitForRunTerminalState(t, database, active.Receipt.RunID); terminal.Status != types.RunCompleted {
		t.Fatalf("newer run status after releasing test step = %q, want completed", terminal.Status)
	}
	if got := step.calls.Load(); got != 1 {
		t.Fatalf("pipeline marker final calls = %d, want exactly 1", got)
	}
}

type fenceMarkerBlockStep struct {
	started   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	releaseMu sync.Once
	calls     atomic.Int32
}

func (s *fenceMarkerBlockStep) Name() types.StepName { return types.StepReview }

func (s *fenceMarkerBlockStep) Execute(sctx *pipeline.StepContext) (*pipeline.StepOutcome, error) {
	s.calls.Add(1)
	select {
	case s.started <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return &pipeline.StepOutcome{}, nil
	case <-sctx.Ctx.Done():
		close(s.cancelled)
		return nil, sctx.Ctx.Err()
	}
}

func (s *fenceMarkerBlockStep) unblock() {
	s.releaseMu.Do(func() { close(s.release) })
}

func launchFenceTestClient(t *testing.T, manager *RunManager, database *db.DB) *ipc.Client {
	t.Helper()
	server := ipc.NewServer()
	registerHandlers(server, manager, database, func() {})
	if err := server.Listen(manager.paths.Socket()); err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.ServeReady() }()
	t.Cleanup(server.Close)
	client, err := ipc.Dial(manager.paths.Socket())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestFenceLaunchNonceReturnsExistingRunInsteadOfNoRunAck(t *testing.T) {
	fixture := newDirectLaunchFenceFixture(t, "run-wins")
	run, err := fixture.database.InsertRunWithIntentAndLaunchNonce(
		fixture.repo.ID, fixture.params.Branch, fixture.params.SubmittedHeadSHA, fixture.params.SubmittedHeadSHA,
		nil, fixture.params.LaunchNonce, fixture.params.ValidationGeneration, fixture.params.IntentDigest, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	client := launchFenceTestClient(t, fixture.manager, fixture.database)
	var result ipc.FenceLaunchNonceResult
	err = client.Call(ipc.MethodFenceLaunchNonce, &fixture.params, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fenced || result.CreatedAt != 0 || result.ExistingRunID != run.ID {
		t.Fatalf("fence result hid existing run or claimed success: %#v, run=%s", result, run.ID)
	}
	if fence, err := fixture.database.GetLaunchNonceFence(fixture.repo.ID, fixture.params.Branch, fixture.params.LaunchNonce); err != nil || fence != nil {
		t.Fatalf("run winner unexpectedly received a no-run fence: fence=%#v err=%v", fence, err)
	}
}

func TestFenceLaunchNonceRejectsChangedClaimOrActiveCaptainState(t *testing.T) {
	t.Run("missing claim", func(t *testing.T) {
		fixture := newDirectLaunchFenceFixture(t, "missing-claim")
		claimPath := filepath.Join(fixture.firstmateHome, "state", "safehouse-bridge", "claims", "request-missing-claim.json")
		if err := os.Remove(claimPath); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.manager.HandleFenceLaunchNonce(context.Background(), &fixture.params); err == nil {
			t.Fatal("missing current claim was accepted")
		}
		assertNoLaunchFence(t, fixture)
	})
	t.Run("claim scope mismatch", func(t *testing.T) {
		fixture := newDirectLaunchFenceFixture(t, "claim-mismatch")
		claimPath := filepath.Join(fixture.firstmateHome, "state", "safehouse-bridge", "claims", "request-claim-mismatch.json")
		mutateJSONFile(t, claimPath, []string{"executionScopeVersion"}, "different-scope")
		if _, err := fixture.manager.HandleFenceLaunchNonce(context.Background(), &fixture.params); err == nil {
			t.Fatal("mismatched current claim was accepted")
		}
		assertNoLaunchFence(t, fixture)
	})
	t.Run("claim project path mismatch", func(t *testing.T) {
		fixture := newDirectLaunchFenceFixture(t, "claim-project-mismatch")
		otherProject := filepath.Join(filepath.Dir(fixture.projectPath), "other-project")
		if err := os.MkdirAll(otherProject, 0o700); err != nil {
			t.Fatal(err)
		}
		claimPath := filepath.Join(fixture.firstmateHome, "state", "safehouse-bridge", "claims", "request-claim-project-mismatch.json")
		mutateJSONFile(t, claimPath, []string{"projectPath"}, otherProject)
		if _, err := fixture.manager.HandleFenceLaunchNonce(context.Background(), &fixture.params); err == nil {
			t.Fatal("claim for another project path was accepted")
		}
		assertNoLaunchFence(t, fixture)
	})
	t.Run("wrong pinned home", func(t *testing.T) {
		fixture := newDirectLaunchFenceFixture(t, "wrong-home")
		fixture.manager.managedFirstmateHome = t.TempDir()
		if _, err := fixture.manager.HandleFenceLaunchNonce(context.Background(), &fixture.params); err == nil {
			t.Fatal("reservation from another Firstmate home was accepted")
		}
		assertNoLaunchFence(t, fixture)
	})
	if os.PathSeparator != '\\' {
		t.Run("symlinked claim", func(t *testing.T) {
			fixture := newDirectLaunchFenceFixture(t, "claim-symlink")
			claimPath := filepath.Join(fixture.firstmateHome, "state", "safehouse-bridge", "claims", "request-claim-symlink.json")
			restore := temporarilySymlinkProof(t, claimPath)
			if _, err := fixture.manager.HandleFenceLaunchNonce(context.Background(), &fixture.params); err == nil {
				t.Fatal("symlinked current claim was accepted")
			}
			restore()
			assertNoLaunchFence(t, fixture)
		})
	}
	for _, status := range []string{"prepared", "bound_pending_release", "bound"} {
		t.Run("state-"+status, func(t *testing.T) {
			fixture := newDirectLaunchFenceFixture(t, "state-"+status)
			statePath := filepath.Join(fixture.firstmateHome, "state", "safehouse-bridge", "no-mistakes", "launches", fixture.params.TaskID+"."+fixture.params.LaunchNonce+".json.state")
			writeJSONFile(t, statePath, captainLaunchState{
				Schema: captainLaunchStateSchema, TaskID: fixture.params.TaskID, LaunchNonce: fixture.params.LaunchNonce,
				Status: status, RunID: "run-in-progress",
			})
			if _, err := fixture.manager.HandleFenceLaunchNonce(context.Background(), &fixture.params); err == nil {
				t.Fatalf("active Captain state %q was accepted", status)
			}
			assertNoLaunchFence(t, fixture)
		})
	}
}

func TestFenceLaunchNonceRequiresOneRegisteredProjectMatch(t *testing.T) {
	fixture := newDirectLaunchFenceFixture(t, "ambiguous")
	if os.PathSeparator == '\\' {
		t.Skip("physical symlink alias fixture is unsupported on Windows")
	}
	alias := filepath.Join(filepath.Dir(fixture.projectPath), "registered-project-alias")
	if err := os.Symlink(fixture.projectPath, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.database.InsertRepoWithID("repo-duplicate", alias, "file:///unused", "main"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.HandleFenceLaunchNonce(context.Background(), &fixture.params); err == nil || !strings.Contains(err.Error(), "ambiguously") {
		t.Fatalf("ambiguous registered path result = %v", err)
	}
	assertNoLaunchFence(t, fixture)
}

func TestFenceLaunchNonceRequiresManagedSubscriptionDaemon(t *testing.T) {
	fixture := newDirectLaunchFenceFixture(t, "unmanaged")
	fixture.manager.managedSubscriptionPolicy = false
	if _, err := fixture.manager.HandleFenceLaunchNonce(context.Background(), &fixture.params); err == nil {
		t.Fatal("unmanaged daemon accepted a managed no-run fence")
	}
	assertNoLaunchFence(t, fixture)
}

func TestFenceLaunchNonceAllowsRemovedNativeWorktreeButRejectsBadReservation(t *testing.T) {
	t.Run("native worktree removed", func(t *testing.T) {
		fixture := newDirectLaunchFenceFixture(t, "removed-native")
		if err := os.RemoveAll(fixture.nativePath); err != nil {
			t.Fatal(err)
		}
		result, err := fixture.manager.HandleFenceLaunchNonce(context.Background(), &fixture.params)
		if err != nil {
			t.Fatalf("fence after native task cleanup: %v", err)
		}
		if !result.Fenced || result.CreatedAt <= 0 {
			t.Fatalf("fence after native task cleanup = %#v", result)
		}
	})
	t.Run("relative native path", func(t *testing.T) {
		fixture := newDirectLaunchFenceFixture(t, "relative-native")
		reservationPath := filepath.Join(fixture.firstmateHome, "state", "safehouse-bridge", "no-mistakes", "launches", fixture.params.TaskID+"."+fixture.params.LaunchNonce+".json")
		mutateJSONFile(t, reservationPath, []string{"nativeWorktreePath"}, "relative/worktree")
		if _, err := fixture.manager.HandleFenceLaunchNonce(context.Background(), &fixture.params); err == nil {
			t.Fatal("relative native worktree path was accepted")
		}
		assertNoLaunchFence(t, fixture)
	})
	if os.PathSeparator != '\\' {
		t.Run("symlinked reservation", func(t *testing.T) {
			fixture := newDirectLaunchFenceFixture(t, "reservation-symlink")
			reservationPath := filepath.Join(fixture.firstmateHome, "state", "safehouse-bridge", "no-mistakes", "launches", fixture.params.TaskID+"."+fixture.params.LaunchNonce+".json")
			restore := temporarilySymlinkProof(t, reservationPath)
			if _, err := fixture.manager.HandleFenceLaunchNonce(context.Background(), &fixture.params); err == nil {
				t.Fatal("symlinked reservation was accepted")
			}
			restore()
			assertNoLaunchFence(t, fixture)
		})
	}
}

func assertNoLaunchFence(t *testing.T, fixture *directLaunchFenceFixture) {
	t.Helper()
	fence, err := fixture.database.GetLaunchNonceFence(fixture.repo.ID, fixture.params.Branch, fixture.params.LaunchNonce)
	if err != nil || fence != nil {
		t.Fatalf("rejected proof left a fence: %#v, err=%v", fence, err)
	}
}

func mutateJSONFile(t *testing.T, path string, keys []string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	current := document
	for _, key := range keys[:len(keys)-1] {
		next, ok := current[key].(map[string]any)
		if !ok {
			t.Fatalf("JSON field path %v missing %q", keys, key)
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
}
