package db

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// LaunchNonceFence is an immutable tombstone proving that one exact managed
// launch nonce was fenced before any run row was created. It intentionally
// lives outside runs so a no-run disposition does not fabricate a pipeline.
type LaunchNonceFence struct {
	RepoID                string
	Branch                string
	LaunchNonce           string
	SubmittedHeadSHA      string
	ValidationGeneration  string
	IntentDigest          string
	TaskID                string
	RequestID             string
	AttemptID             string
	TicketKey             string
	ProjectID             string
	CanonicalProjectPath  string
	ExecutionScopeVersion string
	NativeWorktreePath    string
	CreatedAt             int64
}

// LaunchNonceFenceInsertResult distinguishes a durable fence from a matching
// run that won the race. ExistingRunID is never a successful no-run fence.
type LaunchNonceFenceInsertResult struct {
	Fence         *LaunchNonceFence
	Created       bool
	AlreadyFenced bool
	ExistingRunID string
}

// CreateLaunchNonceFence inserts the exact no-run tombstone. SQLite triggers
// serialize this write against all run inserts with the same repo/branch/nonce
// tuple, even across daemon processes and restarts.
func (d *DB) CreateLaunchNonceFence(fence LaunchNonceFence) (LaunchNonceFenceInsertResult, error) {
	normalized, err := normalizeLaunchNonceFence(fence)
	if err != nil {
		return LaunchNonceFenceInsertResult{}, err
	}
	normalized.CreatedAt = now()
	result, err := d.sql.Exec(
		`INSERT OR IGNORE INTO launch_nonce_fences (
			repo_id, branch, launch_nonce, submitted_head_sha, validation_generation,
			intent_digest, task_id, request_id, attempt_id, ticket_key, project_id,
			canonical_project_path, execution_scope_version, native_worktree_path, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.RepoID, normalized.Branch, normalized.LaunchNonce, normalized.SubmittedHeadSHA,
		normalized.ValidationGeneration, normalized.IntentDigest, normalized.TaskID,
		normalized.RequestID, normalized.AttemptID, normalized.TicketKey, normalized.ProjectID,
		normalized.CanonicalProjectPath, normalized.ExecutionScopeVersion, normalized.NativeWorktreePath,
		normalized.CreatedAt,
	)
	if err != nil {
		// If a run's INSERT acquired SQLite's writer slot first, the trigger
		// rejects this fence. Return that exact run identity to the caller; it
		// must continue through run reconciliation instead of claiming no-run.
		run, readErr := d.GetRunByLaunchNonce(normalized.RepoID, normalized.Branch, normalized.LaunchNonce)
		if readErr != nil {
			return LaunchNonceFenceInsertResult{}, fmt.Errorf("insert launch nonce fence: %w (check existing run: %v)", err, readErr)
		}
		if run != nil {
			return LaunchNonceFenceInsertResult{ExistingRunID: run.ID}, nil
		}
		return LaunchNonceFenceInsertResult{}, fmt.Errorf("insert launch nonce fence: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return LaunchNonceFenceInsertResult{}, fmt.Errorf("read launch nonce fence insert result: %w", err)
	}
	if rows == 1 {
		return LaunchNonceFenceInsertResult{Fence: &normalized, Created: true}, nil
	}

	existing, err := d.GetLaunchNonceFence(normalized.RepoID, normalized.Branch, normalized.LaunchNonce)
	if err != nil {
		return LaunchNonceFenceInsertResult{}, err
	}
	if existing != nil {
		if !sameLaunchNonceFence(*existing, normalized) {
			return LaunchNonceFenceInsertResult{}, fmt.Errorf("launch nonce already has a different durable fence")
		}
		return LaunchNonceFenceInsertResult{Fence: existing, AlreadyFenced: true}, nil
	}
	run, err := d.GetRunByLaunchNonce(normalized.RepoID, normalized.Branch, normalized.LaunchNonce)
	if err != nil {
		return LaunchNonceFenceInsertResult{}, err
	}
	if run != nil {
		return LaunchNonceFenceInsertResult{ExistingRunID: run.ID}, nil
	}
	return LaunchNonceFenceInsertResult{}, fmt.Errorf("launch nonce fence insert affected no row without an existing fence or run")
}

// GetLaunchNonceFence returns the immutable fence for one exact tuple.
func (d *DB) GetLaunchNonceFence(repoID, branch, launchNonce string) (*LaunchNonceFence, error) {
	fence := &LaunchNonceFence{}
	err := d.sql.QueryRow(`SELECT repo_id, branch, launch_nonce, submitted_head_sha,
		validation_generation, intent_digest, task_id, request_id, attempt_id,
		ticket_key, project_id, canonical_project_path, execution_scope_version,
		native_worktree_path, created_at
		FROM launch_nonce_fences WHERE repo_id = ? AND branch = ? AND launch_nonce = ?`,
		repoID, branch, launchNonce,
	).Scan(&fence.RepoID, &fence.Branch, &fence.LaunchNonce, &fence.SubmittedHeadSHA,
		&fence.ValidationGeneration, &fence.IntentDigest, &fence.TaskID, &fence.RequestID,
		&fence.AttemptID, &fence.TicketKey, &fence.ProjectID, &fence.CanonicalProjectPath,
		&fence.ExecutionScopeVersion, &fence.NativeWorktreePath, &fence.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get launch nonce fence: %w", err)
	}
	return fence, nil
}

func normalizeLaunchNonceFence(fence LaunchNonceFence) (LaunchNonceFence, error) {
	for name, value := range map[string]string{
		"repo ID":                 fence.RepoID,
		"branch":                  fence.Branch,
		"launch nonce":            fence.LaunchNonce,
		"validation generation":   fence.ValidationGeneration,
		"task ID":                 fence.TaskID,
		"request ID":              fence.RequestID,
		"attempt ID":              fence.AttemptID,
		"ticket key":              fence.TicketKey,
		"project ID":              fence.ProjectID,
		"execution scope version": fence.ExecutionScopeVersion,
	} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 512 || strings.ContainsAny(value, "\x00\r\n") {
			return LaunchNonceFence{}, fmt.Errorf("launch nonce fence %s is invalid", name)
		}
	}
	if !validHexLength(fence.SubmittedHeadSHA, 40) {
		return LaunchNonceFence{}, fmt.Errorf("launch nonce fence submitted head must be a full 40-character hexadecimal SHA")
	}
	if !validHexLength(fence.IntentDigest, 64) {
		return LaunchNonceFence{}, fmt.Errorf("launch nonce fence intent digest must be a full 64-character hexadecimal SHA-256")
	}
	if !filepath.IsAbs(fence.CanonicalProjectPath) || !filepath.IsAbs(fence.NativeWorktreePath) ||
		strings.TrimSpace(fence.CanonicalProjectPath) != fence.CanonicalProjectPath || strings.TrimSpace(fence.NativeWorktreePath) != fence.NativeWorktreePath {
		return LaunchNonceFence{}, fmt.Errorf("launch nonce fence project and native worktree paths must be absolute")
	}
	fence.SubmittedHeadSHA = strings.ToLower(fence.SubmittedHeadSHA)
	fence.IntentDigest = strings.ToLower(fence.IntentDigest)
	return fence, nil
}

func validHexLength(value string, length int) bool {
	if len(value) != length {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func sameLaunchNonceFence(left, right LaunchNonceFence) bool {
	return left.RepoID == right.RepoID && left.Branch == right.Branch && left.LaunchNonce == right.LaunchNonce &&
		left.SubmittedHeadSHA == right.SubmittedHeadSHA && left.ValidationGeneration == right.ValidationGeneration &&
		left.IntentDigest == right.IntentDigest && left.TaskID == right.TaskID && left.RequestID == right.RequestID &&
		left.AttemptID == right.AttemptID && left.TicketKey == right.TicketKey && left.ProjectID == right.ProjectID &&
		left.CanonicalProjectPath == right.CanonicalProjectPath && left.ExecutionScopeVersion == right.ExecutionScopeVersion &&
		left.NativeWorktreePath == right.NativeWorktreePath
}
