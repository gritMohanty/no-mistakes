package daemon

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
)

const captainClaimSchema = "captain-firstmate-claim.v1"

type captainLaunchClaim struct {
	Schema                string `json:"schema"`
	ID                    string `json:"id"`
	RequestID             string `json:"requestId"`
	TicketKey             string `json:"ticketKey"`
	ProjectID             string `json:"project"`
	ProjectPath           string `json:"projectPath"`
	ExecutionScopeVersion string `json:"executionScopeVersion"`
	Status                string `json:"status"`
}

// HandleFenceLaunchNonce persists a no-run tombstone for an exact Captain
// reservation. It never treats a missing run lookup as proof: the SQLite
// fence/run triggers serialize this write against every launch insertion.
func (m *RunManager) HandleFenceLaunchNonce(ctx context.Context, params *ipc.FenceLaunchNonceParams) (ipc.FenceLaunchNonceResult, error) {
	if err := ctx.Err(); err != nil {
		return ipc.FenceLaunchNonceResult{}, err
	}
	if !m.managedSubscriptionPolicy || m.managedFirstmateHome == "" {
		return ipc.FenceLaunchNonceResult{}, fmt.Errorf("daemon is not pinned to a managed Firstmate home")
	}
	if err := validateFenceLaunchNonceParams(params); err != nil {
		return ipc.FenceLaunchNonceResult{}, err
	}
	reservation, err := readCaptainFenceReservation(m.managedFirstmateHome, params)
	if err != nil {
		return ipc.FenceLaunchNonceResult{}, err
	}
	if err := verifyCaptainFenceClaim(m.managedFirstmateHome, reservation); err != nil {
		return ipc.FenceLaunchNonceResult{}, err
	}
	if err := verifyCaptainFenceState(m.managedFirstmateHome, params); err != nil {
		return ipc.FenceLaunchNonceResult{}, err
	}
	repo, err := m.registeredProjectRepo(reservation.Identity.CanonicalProjectPath)
	if err != nil {
		return ipc.FenceLaunchNonceResult{}, err
	}
	if ctx.Err() != nil {
		return ipc.FenceLaunchNonceResult{}, ctx.Err()
	}

	proof := db.LaunchNonceFence{
		RepoID:                repo.ID,
		Branch:                reservation.Branch,
		LaunchNonce:           reservation.LaunchNonce,
		SubmittedHeadSHA:      reservation.HeadSHA,
		ValidationGeneration:  reservation.ValidationGeneration,
		IntentDigest:          reservation.IntentDigest,
		TaskID:                reservation.Identity.TaskID,
		RequestID:             reservation.Identity.RequestID,
		AttemptID:             reservation.Identity.AttemptID,
		TicketKey:             reservation.Identity.TicketKey,
		ProjectID:             reservation.Identity.ProjectID,
		CanonicalProjectPath:  reservation.Identity.CanonicalProjectPath,
		ExecutionScopeVersion: reservation.Identity.ExecutionScopeVersion,
		NativeWorktreePath:    reservation.NativeWorktreePath,
	}
	var inserted db.LaunchNonceFenceInsertResult
	_, err = m.withBranchLock(repo.ID, reservation.Branch, func() (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		var insertErr error
		inserted, insertErr = m.db.CreateLaunchNonceFence(proof)
		return "", insertErr
	})
	if err != nil {
		return ipc.FenceLaunchNonceResult{}, fmt.Errorf("persist launch no-run fence: %w", err)
	}
	result := ipc.FenceLaunchNonceResult{
		RepoID:      repo.ID,
		Branch:      reservation.Branch,
		LaunchNonce: reservation.LaunchNonce,
	}
	if inserted.ExistingRunID != "" {
		result.ExistingRunID = inserted.ExistingRunID
		return result, nil
	}
	if inserted.Fence == nil || (!inserted.Created && !inserted.AlreadyFenced) {
		return ipc.FenceLaunchNonceResult{}, fmt.Errorf("database did not confirm a durable launch nonce fence")
	}
	result.Fenced = true
	result.AlreadyFenced = inserted.AlreadyFenced
	result.CreatedAt = inserted.Fence.CreatedAt
	return result, nil
}

func validateFenceLaunchNonceParams(params *ipc.FenceLaunchNonceParams) error {
	if params == nil || !safeCaptainID(params.TaskID) || !safeCaptainID(params.LaunchNonce) {
		return fmt.Errorf("valid Captain task ID and launch nonce are required")
	}
	if params.Branch == "" || len(params.Branch) > 255 || strings.TrimSpace(params.Branch) != params.Branch || strings.ContainsAny(params.Branch, "\x00\r\n") {
		return fmt.Errorf("valid launch branch is required")
	}
	if err := validateLaunchNonce(params.LaunchNonce); err != nil {
		return err
	}
	if err := validateValidationGeneration(params.ValidationGeneration); err != nil {
		return err
	}
	if !validFenceHex(params.SubmittedHeadSHA, 40) || !validFenceHex(params.IntentDigest, 64) {
		return fmt.Errorf("full submitted head SHA and intent SHA-256 digest are required")
	}
	return nil
}

func readCaptainFenceReservation(home string, params *ipc.FenceLaunchNonceParams) (*captainLaunchReservation, error) {
	if !filepath.IsAbs(home) {
		return nil, fmt.Errorf("daemon has no absolute pinned Firstmate home")
	}
	reservation := &captainLaunchReservation{}
	relative := filepath.Join("state", "safehouse-bridge", "no-mistakes", "launches", params.TaskID+"."+params.LaunchNonce+".json")
	if err := readPinnedRegularJSON(home, relative, reservation); err != nil {
		return nil, fmt.Errorf("read Captain launch reservation: %w", err)
	}
	if reservation.Schema != captainLaunchReservationSchema || reservation.TaskID != params.TaskID ||
		reservation.Identity.TaskID != params.TaskID || reservation.LaunchNonce != params.LaunchNonce ||
		reservation.Branch != params.Branch || !strings.EqualFold(reservation.HeadSHA, params.SubmittedHeadSHA) ||
		reservation.ValidationGeneration != params.ValidationGeneration || !strings.EqualFold(reservation.IntentDigest, params.IntentDigest) ||
		!validCaptainIdentity(reservation.Identity) || !safeCaptainID(reservation.Identity.RequestID) {
		return nil, fmt.Errorf("Captain launch reservation does not match the exact no-run fence request")
	}
	// The native checkout may already be gone by the time an operator fences
	// a no-run reservation. Validate its recorded canonical location without
	// requiring that task worktree to remain on disk.
	nativePath, err := canonicalPathAllowMissing(reservation.NativeWorktreePath)
	if err != nil || nativePath != filepath.Clean(reservation.NativeWorktreePath) {
		return nil, fmt.Errorf("Captain reservation native worktree path is invalid or noncanonical")
	}
	return reservation, nil
}

func verifyCaptainFenceClaim(home string, reservation *captainLaunchReservation) error {
	claim := &captainLaunchClaim{}
	relative := filepath.Join("state", "safehouse-bridge", "claims", reservation.Identity.RequestID+".json")
	if err := readPinnedRegularJSON(home, relative, claim); err != nil {
		return fmt.Errorf("read current Captain claim: %w", err)
	}
	if claim.Schema != captainClaimSchema || claim.ID != reservation.Identity.RequestID ||
		claim.RequestID != reservation.Identity.RequestID || claim.Status != "claimed" ||
		claim.TicketKey != reservation.Identity.TicketKey || claim.ProjectID != reservation.Identity.ProjectID ||
		claim.ExecutionScopeVersion != reservation.Identity.ExecutionScopeVersion ||
		!samePhysicalPath(claim.ProjectPath, reservation.Identity.CanonicalProjectPath) {
		return fmt.Errorf("current Captain claim does not match the reserved task, project, and scope")
	}
	return nil
}

func verifyCaptainFenceState(home string, params *ipc.FenceLaunchNonceParams) error {
	relative := filepath.Join("state", "safehouse-bridge", "no-mistakes", "launches", params.TaskID+"."+params.LaunchNonce+".json.state")
	state := &captainLaunchState{}
	err := readPinnedRegularJSON(home, relative, state)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read existing Captain launch state: %w", err)
	}
	if state.Schema != captainLaunchStateSchema || state.TaskID != params.TaskID || state.LaunchNonce != params.LaunchNonce ||
		state.Status != "reconciled" || state.Disposition != "no_run_fenced" || state.RunID != "" {
		return fmt.Errorf("existing Captain launch state is not an exact no-run fence replay")
	}
	return nil
}

func (m *RunManager) registeredProjectRepo(projectPath string) (*db.Repo, error) {
	repos, err := m.db.GetRepos()
	if err != nil {
		return nil, fmt.Errorf("read registered repositories: %w", err)
	}
	var matched *db.Repo
	for _, repo := range repos {
		if !samePhysicalPath(repo.WorkingPath, projectPath) {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("Captain project path ambiguously matches multiple registered repositories")
		}
		matched = repo
	}
	if matched == nil {
		return nil, fmt.Errorf("Captain project path does not match a registered repository")
	}
	return matched, nil
}

func validFenceHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
