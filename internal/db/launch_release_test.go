package db

import (
	"path/filepath"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestReleaseHeldRunIsExactIdempotentAndDoesNotRestartTerminalRuns(t *testing.T) {
	d := openTestDB(t)
	repo, err := d.InsertRepo(t.TempDir(), "https://example.com/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	const branch = "feature"
	const head = "submitted-head"
	const nonce = "launch-nonce"
	const generation = "generation-1"
	const digest = "intent-digest"

	run, err := d.InsertHeldRunWithIntentAndLaunchNonce(repo.ID, branch, head, "base", nil, nonce, generation, digest, "")
	if err != nil {
		t.Fatal(err)
	}
	if !run.LaunchBindReleaseRequired || run.LaunchReleasedAt != nil {
		t.Fatalf("new held run release state = required %v, released_at %v", run.LaunchBindReleaseRequired, run.LaunchReleasedAt)
	}
	if _, _, err := d.ReleaseHeldRun(run.ID, branch, nonce, generation, head, digest); err == nil {
		t.Fatal("unclaimed launch receipt was released")
	}
	beforeClaim, err := d.GetRun(run.ID)
	if err != nil || beforeClaim.LaunchReleasedAt != nil {
		t.Fatalf("pre-claim release changed durable state: %#v, err=%v", beforeClaim, err)
	}
	claimed, firstClaim, err := d.ClaimHeldLaunchReceipt(repo.ID, branch, nonce, head, generation, digest, "")
	if err != nil || !firstClaim || claimed.ID != run.ID {
		t.Fatalf("claim held launch receipt = %#v, first=%v, err=%v", claimed, firstClaim, err)
	}

	released, alreadyReleased, err := d.ReleaseHeldRun(run.ID, branch, nonce, generation, head, digest)
	if err != nil || alreadyReleased {
		t.Fatalf("first release = %#v, already=%v, err=%v", released, alreadyReleased, err)
	}
	if released.LaunchReleasedAt == nil || released.Status != types.RunPending {
		t.Fatalf("first release state = %#v, want durable timestamp while pending", released)
	}
	releasedAt := *released.LaunchReleasedAt

	replayed, alreadyReleased, err := d.ReleaseHeldRun(run.ID, branch, nonce, generation, head, digest)
	if err != nil || !alreadyReleased || replayed.LaunchReleasedAt == nil || *replayed.LaunchReleasedAt != releasedAt {
		t.Fatalf("idempotent release = %#v, already=%v, err=%v", replayed, alreadyReleased, err)
	}

	for _, proof := range []struct {
		branch, nonce, generation, head, digest string
	}{
		{branch: "other", nonce: nonce, generation: generation, head: head, digest: digest},
		{branch: branch, nonce: "other", generation: generation, head: head, digest: digest},
		{branch: branch, nonce: nonce, generation: "other", head: head, digest: digest},
		{branch: branch, nonce: nonce, generation: generation, head: "other", digest: digest},
		{branch: branch, nonce: nonce, generation: generation, head: head, digest: "other"},
	} {
		if _, _, err := d.ReleaseHeldRun(run.ID, proof.branch, proof.nonce, proof.generation, proof.head, proof.digest); err == nil {
			t.Fatalf("mismatched proof was accepted: %#v", proof)
		}
	}

	if err := d.UpdateRunErrorStatus(run.ID, "daemon restarted after release", types.RunFailed); err != nil {
		t.Fatal(err)
	}
	terminalReplay, alreadyReleased, err := d.ReleaseHeldRun(run.ID, branch, nonce, generation, head, digest)
	if err != nil || !alreadyReleased || terminalReplay.Status != types.RunFailed || terminalReplay.LaunchReleasedAt == nil || *terminalReplay.LaunchReleasedAt != releasedAt {
		t.Fatalf("terminal release replay = %#v, already=%v, err=%v", terminalReplay, alreadyReleased, err)
	}

	terminalHeld, err := d.InsertHeldRunWithIntentAndLaunchNonce(repo.ID, "other-feature", "other-head", "base", nil, "other-nonce", generation, digest, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunErrorStatus(terminalHeld.ID, "cancelled before release", types.RunCancelled); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.ReleaseHeldRun(terminalHeld.ID, terminalHeld.Branch, "other-nonce", generation, "other-head", digest); err == nil {
		t.Fatal("terminal held run without a release timestamp was released")
	}
	current, err := d.GetRun(terminalHeld.ID)
	if err != nil || current.LaunchReleasedAt != nil {
		t.Fatalf("terminal unreleased row = %#v, err=%v", current, err)
	}
}

func TestStartReleasedHeldRunIsOnePendingOnlyTransition(t *testing.T) {
	d := openTestDB(t)
	repo, err := d.InsertRepo(t.TempDir(), "https://example.com/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	const branch = "feature"
	const head = "submitted-head"
	const nonce = "launch-nonce"
	const generation = "generation-1"
	const digest = "intent-digest"
	run, err := d.InsertHeldRunWithIntentAndLaunchNonce(repo.ID, branch, head, "base", nil, nonce, generation, digest, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, started, err := d.StartReleasedHeldRun(run.ID); err != nil || started {
		t.Fatalf("unreleased held run start = started %v, err=%v", started, err)
	}
	if _, claimed, err := d.ClaimHeldLaunchReceipt(repo.ID, branch, nonce, head, generation, digest, ""); err != nil || !claimed {
		t.Fatalf("claim held launch = claimed %v, err=%v", claimed, err)
	}
	if _, already, err := d.ReleaseHeldRun(run.ID, branch, nonce, generation, head, digest); err != nil || already {
		t.Fatalf("release held launch = already %v, err=%v", already, err)
	}
	startedRun, started, err := d.StartReleasedHeldRun(run.ID)
	if err != nil || !started || startedRun.Status != types.RunRunning {
		t.Fatalf("released held run start = %#v, started %v, err=%v", startedRun, started, err)
	}
	current, started, err := d.StartReleasedHeldRun(run.ID)
	if err != nil || started || current.Status != types.RunRunning {
		t.Fatalf("duplicate held run start = %#v, started %v, err=%v", current, started, err)
	}
	if err := d.UpdateRunErrorStatus(run.ID, "terminal before duplicate start", types.RunFailed); err != nil {
		t.Fatal(err)
	}
	terminal, started, err := d.StartReleasedHeldRun(run.ID)
	if err != nil || started || terminal.Status != types.RunFailed {
		t.Fatalf("terminal held run start = %#v, started %v, err=%v", terminal, started, err)
	}
}

func TestOpenMigratesLaunchBindReleaseWithoutBackfillingHistoricalRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := d.InsertRepo(t.TempDir(), "https://example.com/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := d.InsertRun(repo.ID, "legacy", "legacy-head", "base")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP TRIGGER runs_launch_bind_release_required_immutable`,
		`DROP TRIGGER runs_launch_released_at_immutable`,
		`ALTER TABLE runs DROP COLUMN launch_released_at`,
		`ALTER TABLE runs DROP COLUMN launch_bind_release_required`,
	} {
		if _, err := d.sql.Exec(statement); err != nil {
			t.Fatalf("prepare old schema (%s): %v", statement, err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	got, err := d.GetRun(legacy.ID)
	if err != nil || got.LaunchBindReleaseRequired || got.LaunchReleasedAt != nil {
		t.Fatalf("migrated historical run = %#v, err=%v", got, err)
	}
	managed, err := d.InsertHeldRunWithIntentAndLaunchNonce(repo.ID, "managed", "head", "base", nil, "nonce", "generation", "digest", "")
	if err != nil || !managed.LaunchBindReleaseRequired || managed.LaunchReleasedAt != nil {
		t.Fatalf("new managed run after migration = %#v, err=%v", managed, err)
	}
}

func TestHeldRunReleaseFieldsAreImmutable(t *testing.T) {
	d := openTestDB(t)
	repo, err := d.InsertRepo(t.TempDir(), "https://example.com/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.InsertHeldRunWithIntentAndLaunchNonce(repo.ID, "bad", "head", "base", nil, "nonce", " ", "digest", ""); err == nil {
		t.Fatal("held run without validation generation was inserted")
	}
	if _, err := d.InsertHeldRunWithIntentAndLaunchNonce(repo.ID, "bad", "head", "base", nil, "nonce", "generation", " ", ""); err == nil {
		t.Fatal("held run without intent digest was inserted")
	}
	run, err := d.InsertHeldRunWithIntentAndLaunchNonce(repo.ID, "feature", "head", "base", nil, "nonce", "generation", "digest", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := d.ClaimHeldLaunchReceipt(repo.ID, run.Branch, "nonce", "head", "generation", "digest", ""); err != nil || !claimed {
		t.Fatalf("claim held launch receipt: claimed=%v, err=%v", claimed, err)
	}
	if _, err := d.sql.Exec(`UPDATE runs SET launch_bind_release_required = 0 WHERE id = ?`, run.ID); err == nil {
		t.Fatal("held-run requirement could be removed")
	}
	released, _, err := d.ReleaseHeldRun(run.ID, run.Branch, "nonce", "generation", "head", "digest")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.sql.Exec(`UPDATE runs SET launch_released_at = NULL WHERE id = ?`, run.ID); err == nil {
		t.Fatal("release timestamp could be cleared")
	}
	if _, err := d.sql.Exec(`UPDATE runs SET launch_released_at = launch_released_at + 1 WHERE id = ?`, run.ID); err == nil {
		t.Fatal("release timestamp could be changed")
	}
	if released.LaunchReleasedAt == nil {
		t.Fatal("release did not persist timestamp")
	}
}
