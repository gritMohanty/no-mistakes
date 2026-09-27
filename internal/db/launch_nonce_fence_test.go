package db

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func openFenceTestPair(t *testing.T) (*DB, *DB, string, *Repo, LaunchNonceFence) {
	t.Helper()
	root := t.TempDir()
	dbPath := filepath.Join(root, "state.sqlite")
	first, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open first DB: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	repo, err := first.InsertRepo(filepath.Join(root, "project"), "https://example.test/project.git", "main")
	if err != nil {
		t.Fatalf("insert repo: %v", err)
	}
	second, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open independent DB handle: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	fence := LaunchNonceFence{
		RepoID:                repo.ID,
		Branch:                "feature/fenced-launch",
		LaunchNonce:           "fm-fence-0123456789abcdef",
		SubmittedHeadSHA:      strings.Repeat("a", 40),
		ValidationGeneration:  "generation-001",
		IntentDigest:          strings.Repeat("b", 64),
		TaskID:                "task-fence-001",
		RequestID:             strings.Repeat("c", 32),
		AttemptID:             "attempt-fence-001",
		TicketKey:             "L21-FENCE",
		ProjectID:             "demo",
		CanonicalProjectPath:  repo.WorkingPath,
		ExecutionScopeVersion: "scope-fence-001",
		NativeWorktreePath:    filepath.Join(root, "native-worktree"),
	}
	return first, second, dbPath, repo, fence
}

func runCount(t *testing.T, database *DB) int {
	t.Helper()
	var count int
	if err := database.sql.QueryRow(`SELECT count(*) FROM runs`).Scan(&count); err != nil {
		t.Fatalf("count runs: %v", err)
	}
	return count
}

func TestLaunchNonceFenceBlocksHeldAndOrdinaryRunsAndSurvivesRestart(t *testing.T) {
	first, second, dbPath, repo, proof := openFenceTestPair(t)
	created, err := first.CreateLaunchNonceFence(proof)
	if err != nil || !created.Created || created.AlreadyFenced || created.Fence == nil || created.ExistingRunID != "" {
		t.Fatalf("create fence = %+v, err = %v", created, err)
	}
	if runCount(t, first) != 0 {
		t.Fatal("no-run fence created a synthetic run")
	}

	replayed, err := second.CreateLaunchNonceFence(proof)
	if err != nil || !replayed.AlreadyFenced || replayed.Created || replayed.Fence == nil || replayed.Fence.CreatedAt != created.Fence.CreatedAt {
		t.Fatalf("exact fence replay = %+v, err = %v", replayed, err)
	}
	wrong := proof
	wrong.SubmittedHeadSHA = strings.Repeat("d", 40)
	if _, err := second.CreateLaunchNonceFence(wrong); err == nil || !strings.Contains(err.Error(), "different durable fence") {
		t.Fatalf("mismatched fence replay error = %v", err)
	}

	if _, err := second.InsertHeldRunWithIntentAndLaunchNonce(repo.ID, proof.Branch, proof.SubmittedHeadSHA, proof.SubmittedHeadSHA,
		nil, proof.LaunchNonce, proof.ValidationGeneration, proof.IntentDigest, ""); err == nil || !strings.Contains(err.Error(), "durable no-run fence") {
		t.Fatalf("held insert after fence error = %v", err)
	}
	if _, err := second.InsertRunWithIntentAndLaunchNonce(repo.ID, proof.Branch, proof.SubmittedHeadSHA, proof.SubmittedHeadSHA,
		nil, proof.LaunchNonce, proof.ValidationGeneration, proof.IntentDigest, ""); err == nil || !strings.Contains(err.Error(), "durable no-run fence") {
		t.Fatalf("ordinary insert after fence error = %v", err)
	}
	// The key is deliberately scoped by both repository and branch.
	if _, err := second.InsertRunWithIntentAndLaunchNonce(repo.ID, "feature/other", proof.SubmittedHeadSHA, proof.SubmittedHeadSHA,
		nil, proof.LaunchNonce, proof.ValidationGeneration, proof.IntentDigest, ""); err != nil {
		t.Fatalf("same nonce on a different branch should be independent: %v", err)
	}
	otherRepo, err := second.InsertRepo(filepath.Join(filepath.Dir(repo.WorkingPath), "other-project"), "https://example.test/other.git", "main")
	if err != nil {
		t.Fatalf("insert second repo: %v", err)
	}
	if _, err := second.InsertRunWithIntentAndLaunchNonce(otherRepo.ID, proof.Branch, proof.SubmittedHeadSHA, proof.SubmittedHeadSHA,
		nil, proof.LaunchNonce, proof.ValidationGeneration, proof.IntentDigest, ""); err != nil {
		t.Fatalf("same nonce in a different repo should be independent: %v", err)
	}
	if runCount(t, first) != 2 {
		t.Fatalf("runs after independent tuple inserts = %d, want 2", runCount(t, first))
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	defer restarted.Close()
	persisted, err := restarted.GetLaunchNonceFence(proof.RepoID, proof.Branch, proof.LaunchNonce)
	if err != nil || persisted == nil || persisted.CreatedAt != created.Fence.CreatedAt {
		t.Fatalf("persisted fence = %+v, err = %v", persisted, err)
	}
	if _, err := restarted.InsertHeldRunWithIntentAndLaunchNonce(repo.ID, proof.Branch, proof.SubmittedHeadSHA, proof.SubmittedHeadSHA,
		nil, proof.LaunchNonce, proof.ValidationGeneration, proof.IntentDigest, ""); err == nil || !strings.Contains(err.Error(), "durable no-run fence") {
		t.Fatalf("held insert after restart error = %v", err)
	}
	if runCount(t, restarted) != 2 {
		t.Fatalf("restart changed run count: %d", runCount(t, restarted))
	}
}

func TestLaunchNonceFenceRunWinsReturnsExactRun(t *testing.T) {
	first, second, _, repo, proof := openFenceTestPair(t)
	run, err := first.InsertRunWithIntentAndLaunchNonce(repo.ID, proof.Branch, proof.SubmittedHeadSHA, proof.SubmittedHeadSHA,
		nil, proof.LaunchNonce, proof.ValidationGeneration, proof.IntentDigest, "")
	if err != nil {
		t.Fatalf("insert run: %v", err)
	}
	result, err := second.CreateLaunchNonceFence(proof)
	if err != nil || result.ExistingRunID != run.ID || result.Fence != nil || result.Created || result.AlreadyFenced {
		t.Fatalf("run-wins fence result = %+v, err = %v", result, err)
	}
	if persisted, err := second.GetLaunchNonceFence(repo.ID, proof.Branch, proof.LaunchNonce); err != nil || persisted != nil {
		t.Fatalf("run-wins fence persisted = %+v, err = %v", persisted, err)
	}
}

func TestLaunchNonceFenceBlocksUpdatingNonceLessRunIntoFencedTuple(t *testing.T) {
	database, _, _, repo, proof := openFenceTestPair(t)
	if result, err := database.CreateLaunchNonceFence(proof); err != nil || !result.Created {
		t.Fatalf("create fence = %+v, err = %v", result, err)
	}
	legacy, err := database.InsertRun(repo.ID, proof.Branch, proof.SubmittedHeadSHA, proof.SubmittedHeadSHA)
	if err != nil {
		t.Fatalf("insert legacy nonce-less run: %v", err)
	}
	_, err = database.sql.Exec(`UPDATE runs SET launch_nonce = ?, launch_validation_generation = ?, launch_intent_digest = ? WHERE id = ?`,
		proof.LaunchNonce, proof.ValidationGeneration, proof.IntentDigest, legacy.ID)
	if err == nil || !strings.Contains(err.Error(), "durable no-run fence") {
		t.Fatalf("updating nonce-less run into fenced tuple error = %v", err)
	}
	unchanged, err := database.GetRun(legacy.ID)
	if err != nil || unchanged == nil || unchanged.LaunchNonce != nil {
		t.Fatalf("legacy run after refused update = %+v, err = %v", unchanged, err)
	}
}

func TestLaunchNonceFenceRaceAcrossIndependentConnectionsHasOneWinner(t *testing.T) {
	first, second, _, repo, proof := openFenceTestPair(t)
	start := make(chan struct{})
	type insertResult struct {
		run *Run
		err error
	}
	runs := make(chan insertResult, 1)
	fences := make(chan struct {
		result LaunchNonceFenceInsertResult
		err    error
	}, 1)
	go func() {
		<-start
		run, err := first.InsertHeldRunWithIntentAndLaunchNonce(repo.ID, proof.Branch, proof.SubmittedHeadSHA, proof.SubmittedHeadSHA,
			nil, proof.LaunchNonce, proof.ValidationGeneration, proof.IntentDigest, "")
		runs <- insertResult{run: run, err: err}
	}()
	go func() {
		<-start
		result, err := second.CreateLaunchNonceFence(proof)
		fences <- struct {
			result LaunchNonceFenceInsertResult
			err    error
		}{result: result, err: err}
	}()
	close(start)
	runResult := <-runs
	fenceResult := <-fences
	if fenceResult.err != nil {
		t.Fatalf("fence insert: %v", fenceResult.err)
	}
	if runResult.err == nil {
		if runResult.run == nil || fenceResult.result.ExistingRunID != runResult.run.ID || fenceResult.result.Fence != nil {
			t.Fatalf("run won without exact conflict report: run=%+v fence=%+v", runResult.run, fenceResult.result)
		}
		if persisted, err := second.GetLaunchNonceFence(repo.ID, proof.Branch, proof.LaunchNonce); err != nil || persisted != nil {
			t.Fatalf("run-wins race persisted fence=%+v err=%v", persisted, err)
		}
		return
	}
	if !fenceResult.result.Created || fenceResult.result.Fence == nil || fenceResult.result.ExistingRunID != "" {
		t.Fatalf("fence did not win after run insert refused: %+v", fenceResult.result)
	}
	if !strings.Contains(runResult.err.Error(), "durable no-run fence") {
		t.Fatalf("fence-wins run error = %v", runResult.err)
	}
	if runCount(t, first) != 0 {
		t.Fatalf("fence-wins race left run row: count=%d", runCount(t, first))
	}
	if _, err := second.InsertRunWithIntentAndLaunchNonce(repo.ID, proof.Branch, proof.SubmittedHeadSHA, proof.SubmittedHeadSHA,
		nil, proof.LaunchNonce, proof.ValidationGeneration, proof.IntentDigest, ""); err == nil {
		t.Fatal("ordinary run insert escaped a fence that won the concurrent race")
	}
}

func TestLaunchNonceFenceRejectsIncompleteOrMalformedProof(t *testing.T) {
	database := openTestDB(t)
	root := t.TempDir()
	repo, err := database.InsertRepo(filepath.Join(root, "project"), "https://example.test/project.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	valid := LaunchNonceFence{
		RepoID: repo.ID, Branch: "feature/fenced", LaunchNonce: "nonce-valid-01",
		SubmittedHeadSHA: strings.Repeat("a", 40), ValidationGeneration: "generation-1", IntentDigest: strings.Repeat("b", 64),
		TaskID: "task-1", RequestID: strings.Repeat("c", 32), AttemptID: "attempt-1", TicketKey: "T-1", ProjectID: "demo",
		CanonicalProjectPath: repo.WorkingPath, ExecutionScopeVersion: "scope-1", NativeWorktreePath: filepath.Join(root, "native"),
	}
	cases := []struct {
		name   string
		mutate func(*LaunchNonceFence)
	}{
		{name: "missing generation", mutate: func(proof *LaunchNonceFence) { proof.ValidationGeneration = " " }},
		{name: "short head", mutate: func(proof *LaunchNonceFence) { proof.SubmittedHeadSHA = "abc" }},
		{name: "non-hex digest", mutate: func(proof *LaunchNonceFence) { proof.IntentDigest = strings.Repeat("z", 64) }},
		{name: "relative project path", mutate: func(proof *LaunchNonceFence) { proof.CanonicalProjectPath = "project" }},
		{name: "relative native path", mutate: func(proof *LaunchNonceFence) { proof.NativeWorktreePath = "worktree" }},
	}
	for index, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			proof := valid
			proof.LaunchNonce = fmt.Sprintf("nonce-invalid-%02d", index)
			test.mutate(&proof)
			if _, err := database.CreateLaunchNonceFence(proof); err == nil {
				t.Fatal("malformed proof unexpectedly fenced a nonce")
			}
		})
	}
	if runCount(t, database) != 0 {
		t.Fatal("invalid proof changed the runs table")
	}
}
