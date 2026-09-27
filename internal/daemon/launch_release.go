package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	captainLaunchReservationSchema = "captain-no-mistakes-launch-reservation.v1"
	captainLaunchStateSchema       = "captain-no-mistakes-launch-state.v1"
	captainLaunchBindingSchema     = "captain-no-mistakes-binding.v1"
)

type heldRunControl struct {
	mu         sync.Mutex
	released   chan struct{}
	releasedAt int64
	cancelled  bool
	started    bool
}

// heldRunTestHooks expose the two durable-ordering boundaries that are
// otherwise difficult to exercise deterministically. Production managers
// leave this nil.
type heldRunTestHooks struct {
	afterReleaseCAS func(runID string) error
	beforeExecute   func(runID string)
}

func newHeldRunControl() *heldRunControl { return &heldRunControl{released: make(chan struct{})} }

func (h *heldRunControl) wasReleased() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.releasedAt > 0
}

type captainExecutionIdentity struct {
	Schema                string `json:"schema"`
	RequestID             string `json:"requestId"`
	TaskID                string `json:"taskId"`
	AttemptID             string `json:"attemptId"`
	TicketKey             string `json:"ticketKey"`
	ProjectID             string `json:"projectId"`
	CanonicalProjectPath  string `json:"canonicalProjectPath"`
	ExecutionScopeVersion string `json:"executionScopeVersion"`
}

type captainLaunchReservation struct {
	Schema               string                   `json:"schema"`
	TaskID               string                   `json:"taskId"`
	Identity             captainExecutionIdentity `json:"identity"`
	NativeWorktreePath   string                   `json:"nativeWorktreePath"`
	Branch               string                   `json:"branch"`
	HeadSHA              string                   `json:"headSha"`
	LaunchNonce          string                   `json:"launchNonce"`
	ValidationGeneration string                   `json:"validationGeneration"`
	IntentDigest         string                   `json:"intentDigest"`
}

type captainLaunchState struct {
	Schema      string `json:"schema"`
	TaskID      string `json:"taskId"`
	LaunchNonce string `json:"launchNonce"`
	Status      string `json:"status"`
	RunID       string `json:"runId"`
}

type captainManagedEnv struct {
	Marker     string `json:"marker"`
	PolicyFile string `json:"policyFile"`
	Check      string `json:"check"`
	NMHome     string `json:"nmHome"`
}

type captainLaunchSource struct {
	DatabasePath string            `json:"databasePath"`
	RunID        string            `json:"runId"`
	ManagedEnv   captainManagedEnv `json:"managedEnv"`
}

type captainLaunchBinding struct {
	Schema             string                   `json:"schema"`
	RunID              string                   `json:"runId"`
	Identity           captainExecutionIdentity `json:"identity"`
	ProjectPath        string                   `json:"projectPath"`
	WorktreePath       string                   `json:"worktreePath"`
	NativeWorktreePath string                   `json:"nativeWorktreePath"`
	Branch             string                   `json:"branch"`
	LaunchHeadSHA      string                   `json:"launchHeadSha"`
	Source             captainLaunchSource      `json:"source"`
}

func (m *RunManager) HandleReleaseHeldRun(ctx context.Context, params *ipc.ReleaseHeldRunParams) (ipc.ReleaseHeldRunResult, error) {
	if err := ctx.Err(); err != nil {
		return ipc.ReleaseHeldRunResult{}, err
	}
	if m.managedFirstmateHome == "" {
		return ipc.ReleaseHeldRunResult{}, fmt.Errorf("daemon is not pinned to a managed Firstmate home")
	}
	run, err := m.verifyCaptainHeldRun(params)
	if err != nil {
		return ipc.ReleaseHeldRunResult{}, err
	}
	m.mu.Lock()
	control := m.heldRuns[run.ID]
	m.mu.Unlock()
	if run.LaunchReleasedAt != nil {
		if control == nil && !run.Status.Terminal() {
			return ipc.ReleaseHeldRunResult{}, fmt.Errorf("held run %q has a durable release but no live waiter; run state requires crash recovery", run.ID)
		}
		if control != nil && !run.Status.Terminal() {
			control.mu.Lock()
			if !control.cancelled && ctx.Err() == nil && !control.started && control.releasedAt == 0 {
				control.releasedAt = *run.LaunchReleasedAt
				close(control.released)
			}
			control.mu.Unlock()
		}
		return releaseHeldRunResult(run, true), nil
	}
	if control == nil {
		return ipc.ReleaseHeldRunResult{}, fmt.Errorf("held run %q has no live daemon waiter; refusing to persist a release without an executor", run.ID)
	}

	control.mu.Lock()
	defer control.mu.Unlock()
	if control.cancelled || ctx.Err() != nil {
		return ipc.ReleaseHeldRunResult{}, fmt.Errorf("held run %q was cancelled before bind release", run.ID)
	}
	if control.started {
		current, getErr := m.db.GetRun(run.ID)
		if getErr != nil {
			return ipc.ReleaseHeldRunResult{}, getErr
		}
		if current.LaunchReleasedAt != nil {
			return releaseHeldRunResult(current, true), nil
		}
		return ipc.ReleaseHeldRunResult{}, fmt.Errorf("held run %q has already entered the executor without a durable release", run.ID)
	}

	released, already, err := m.db.ReleaseHeldRun(run.ID, params.Branch, params.LaunchNonce, params.ValidationGeneration, params.SubmittedHeadSHA, params.IntentDigest)
	if err != nil {
		return ipc.ReleaseHeldRunResult{}, err
	}
	if released.LaunchReleasedAt == nil {
		return ipc.ReleaseHeldRunResult{}, fmt.Errorf("held run %q has no durable release timestamp", run.ID)
	}
	if !already && m.heldRunHooks != nil && m.heldRunHooks.afterReleaseCAS != nil {
		if err := m.heldRunHooks.afterReleaseCAS(run.ID); err != nil {
			return ipc.ReleaseHeldRunResult{}, err
		}
	}
	control.releasedAt = *released.LaunchReleasedAt
	if !already {
		close(control.released)
	}
	return releaseHeldRunResult(released, already), nil
}

func (m *RunManager) finishHeldRunBeforeExecute(run *db.Run, runCtx context.Context) {
	// Startup recovery may already have made this row terminal after a daemon
	// crash. Preserve that durable result instead of replacing it with the
	// shutdown cancellation used to wake the old in-memory waiter.
	current, err := m.db.GetRun(run.ID)
	if err != nil {
		slog.Error("failed to inspect held run before terminalizing", "run_id", run.ID, "error", err)
	} else if current != nil && current.Status.Terminal() {
		run.Status = current.Status
		run.Error = current.Error
		return
	}

	cause := context.Cause(runCtx)
	if cause == nil {
		cause = fmt.Errorf("held launch cancelled before executor start")
	}
	message := cause.Error()
	status := types.RunFailed
	if message == types.RunCancelReasonAbortedByUser || message == types.RunCancelReasonSuperseded || message == "daemon shutting down" {
		status = types.RunCancelled
	}
	run.Status = status
	run.Error = &message
	if err := m.db.UpdateRunErrorStatus(run.ID, message, status); err != nil {
		slog.Error("failed to persist held-run cancellation", "run_id", run.ID, "error", err)
	}
}

func (m *RunManager) beginHeldRunExecutor(run *db.Run, runCtx context.Context, held *heldRunControl) bool {
	held.mu.Lock()
	if held.cancelled || runCtx.Err() != nil {
		held.mu.Unlock()
		m.finishHeldRunBeforeExecute(run, runCtx)
		return false
	}
	current, transitioned, err := m.db.StartReleasedHeldRun(run.ID)
	if err != nil {
		held.mu.Unlock()
		m.failHeldRunBeforeExecute(run, fmt.Sprintf("start held executor: %v", err))
		return false
	}
	if current == nil {
		held.mu.Unlock()
		m.failHeldRunBeforeExecute(run, "held launch disappeared before executor start")
		return false
	}
	if !transitioned && current.Status.Terminal() {
		run.Status = current.Status
		run.Error = current.Error
		held.mu.Unlock()
		return false
	}
	if !transitioned {
		held.mu.Unlock()
		m.failHeldRunBeforeExecute(run, "held launch is no longer pending with a durable bind release")
		return false
	}
	// The DB CAS prevents another terminal writer from being overwritten. The
	// held lock serializes this durable start transition with HandleCancel.
	run.Status = current.Status
	run.Error = current.Error
	held.started = true
	held.mu.Unlock()
	return true
}

func (m *RunManager) failHeldRunBeforeExecute(run *db.Run, message string) {
	run.Status = types.RunFailed
	run.Error = &message
	if err := m.db.UpdateRunErrorStatus(run.ID, message, types.RunFailed); err != nil {
		slog.Error("failed to persist held-run start failure", "run_id", run.ID, "error", err)
	}
}

func releaseHeldRunResult(run *db.Run, already bool) ipc.ReleaseHeldRunResult {
	return ipc.ReleaseHeldRunResult{
		RunID:           run.ID,
		ReleasedAt:      *run.LaunchReleasedAt,
		Status:          run.Status,
		AlreadyReleased: already,
	}
}

func (m *RunManager) verifyCaptainHeldRun(params *ipc.ReleaseHeldRunParams) (*db.Run, error) {
	if params == nil || !safeCaptainID(params.RunID) || !safeCaptainID(params.TaskID) ||
		strings.TrimSpace(params.Branch) == "" || strings.TrimSpace(params.LaunchNonce) == "" ||
		strings.TrimSpace(params.ValidationGeneration) == "" || strings.TrimSpace(params.SubmittedHeadSHA) == "" ||
		strings.TrimSpace(params.IntentDigest) == "" {
		return nil, fmt.Errorf("complete held-run release identity is required")
	}
	run, err := m.db.GetRun(params.RunID)
	if err != nil {
		return nil, fmt.Errorf("get held run: %w", err)
	}
	if run == nil || !run.LaunchBindReleaseRequired || run.LaunchReceiptClaimedAt == nil {
		return nil, fmt.Errorf("run %q is not a claimed held launch", params.RunID)
	}
	if run.Branch != params.Branch || run.LaunchNonce == nil || *run.LaunchNonce != params.LaunchNonce ||
		run.LaunchValidationGeneration == nil || *run.LaunchValidationGeneration != params.ValidationGeneration ||
		run.SubmittedHeadSHA == nil || *run.SubmittedHeadSHA != params.SubmittedHeadSHA ||
		run.LaunchIntentDigest == nil || *run.LaunchIntentDigest != params.IntentDigest {
		return nil, fmt.Errorf("run %q does not match the held launch release identity", params.RunID)
	}
	historicalReplay := run.LaunchReleasedAt != nil
	if !historicalReplay && run.Status != types.RunPending {
		return nil, fmt.Errorf("run %q is not a pending held launch", params.RunID)
	}
	repo, err := m.db.GetRepo(run.RepoID)
	if err != nil {
		return nil, fmt.Errorf("get held-run repository: %w", err)
	}
	if repo == nil {
		return nil, fmt.Errorf("held-run repository is unavailable")
	}

	reservation, state, binding, err := readCaptainLaunchProof(m.managedFirstmateHome, params.TaskID, params.LaunchNonce, params.RunID)
	if err != nil {
		return nil, err
	}
	if reservation.TaskID != params.TaskID || reservation.Identity.TaskID != params.TaskID || reservation.LaunchNonce != params.LaunchNonce ||
		reservation.Branch != params.Branch || !strings.EqualFold(reservation.HeadSHA, params.SubmittedHeadSHA) ||
		reservation.ValidationGeneration != params.ValidationGeneration || !strings.EqualFold(reservation.IntentDigest, params.IntentDigest) {
		return nil, fmt.Errorf("Captain launch reservation does not match the exact held run")
	}
	if !historicalReplay && state.Status != "bound_pending_release" {
		return nil, fmt.Errorf("Captain launch state is not bound pending release")
	}
	if historicalReplay && state.Status != "bound_pending_release" && state.Status != "bound" {
		return nil, fmt.Errorf("Captain launch state is not bound pending release")
	}
	if state.RunID != params.RunID || binding.RunID != params.RunID || binding.Branch != params.Branch ||
		!strings.EqualFold(binding.LaunchHeadSHA, params.SubmittedHeadSHA) {
		return nil, fmt.Errorf("Captain binding does not match the exact held run")
	}
	if !sameCaptainIdentity(binding.Identity, reservation.Identity) || !validCaptainIdentity(reservation.Identity) {
		return nil, fmt.Errorf("Captain binding identity conflicts with its launch reservation")
	}
	pathEqual := samePhysicalPath
	if historicalReplay {
		pathEqual = sameRecordedPhysicalPath
	}
	if !pathEqual(binding.NativeWorktreePath, reservation.NativeWorktreePath) ||
		!pathEqual(binding.ProjectPath, repo.WorkingPath) ||
		!pathEqual(binding.ProjectPath, reservation.Identity.CanonicalProjectPath) ||
		!pathEqual(binding.WorktreePath, run.WorktreePath()) {
		return nil, fmt.Errorf("Captain binding paths do not match the registered project root, reserved native worktree, and no-mistakes run worktree")
	}
	if binding.Source.RunID != params.RunID || binding.Source.ManagedEnv.Marker != "1" ||
		!samePhysicalPath(binding.Source.ManagedEnv.PolicyFile, m.managedSubscriptionPolicyFile) ||
		!samePhysicalPath(binding.Source.ManagedEnv.Check, m.managedSubscriptionCheck) ||
		!samePhysicalPath(binding.Source.ManagedEnv.NMHome, m.paths.Root()) ||
		!samePhysicalPath(binding.Source.DatabasePath, m.paths.DB()) {
		return nil, fmt.Errorf("Captain binding source does not identify this managed daemon and database")
	}
	return run, nil
}

func readCaptainLaunchProof(home, taskID, nonce, runID string) (*captainLaunchReservation, *captainLaunchState, *captainLaunchBinding, error) {
	if !filepath.IsAbs(home) || !safeCaptainID(taskID) || !safeCaptainID(nonce) || !safeCaptainID(runID) {
		return nil, nil, nil, fmt.Errorf("invalid pinned Captain launch proof path")
	}
	base := filepath.Join("state", "safehouse-bridge", "no-mistakes")
	reservation := &captainLaunchReservation{}
	state := &captainLaunchState{}
	binding := &captainLaunchBinding{}
	reservationPath := filepath.Join(base, "launches", taskID+"."+nonce+".json")
	statePath := reservationPath + ".state"
	bindingPath := filepath.Join(base, "bindings", runID+".json")
	if err := readPinnedRegularJSON(home, reservationPath, reservation); err != nil {
		return nil, nil, nil, fmt.Errorf("read Captain launch reservation: %w", err)
	}
	if err := readPinnedRegularJSON(home, statePath, state); err != nil {
		return nil, nil, nil, fmt.Errorf("read Captain launch state: %w", err)
	}
	if err := readPinnedRegularJSON(home, bindingPath, binding); err != nil {
		return nil, nil, nil, fmt.Errorf("read Captain launch binding: %w", err)
	}
	if reservation.Schema != captainLaunchReservationSchema || state.Schema != captainLaunchStateSchema || binding.Schema != captainLaunchBindingSchema {
		return nil, nil, nil, fmt.Errorf("Captain launch proof schema is unsupported")
	}
	if reservation.TaskID != taskID || reservation.LaunchNonce != nonce || state.TaskID != taskID || state.LaunchNonce != nonce {
		return nil, nil, nil, fmt.Errorf("Captain launch proof identifiers conflict with their file paths")
	}
	return reservation, state, binding, nil
}

func readPinnedRegularJSON(home, relative string, target any) error {
	root, err := canonicalPhysicalPath(home)
	if err != nil {
		return err
	}
	if filepath.IsAbs(relative) || filepath.Clean(relative) != relative || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("Captain proof path escapes pinned home")
	}
	parts := strings.Split(relative, string(filepath.Separator))
	current := root
	for index, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("Captain proof path contains a symlink")
		}
		if index < len(parts)-1 && !info.IsDir() {
			return fmt.Errorf("Captain proof parent is not a directory")
		}
		if index == len(parts)-1 && !info.Mode().IsRegular() {
			return fmt.Errorf("Captain proof is not a regular file")
		}
	}
	before, err := os.Lstat(current)
	if err != nil {
		return err
	}
	file, err := os.Open(current)
	if err != nil {
		return err
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil {
		return err
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return fmt.Errorf("Captain proof changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return fmt.Errorf("Captain proof exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("Captain proof contains trailing data")
	}
	return nil
}

func canonicalPhysicalPath(value string) (string, error) {
	if !filepath.IsAbs(value) || strings.TrimSpace(value) != value {
		return "", fmt.Errorf("path must be absolute")
	}
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func samePhysicalPath(left, right string) bool {
	l, leftErr := canonicalPhysicalPath(left)
	r, rightErr := canonicalPhysicalPath(right)
	return leftErr == nil && rightErr == nil && l == r
}

func sameRecordedPhysicalPath(left, right string) bool {
	return sameRecordedPath(left, right)
}

func sameRecordedPath(left, right string) bool {
	l, leftErr := canonicalPathAllowMissing(left)
	r, rightErr := canonicalPathAllowMissing(right)
	return leftErr == nil && rightErr == nil && l == r
}

func canonicalPathAllowMissing(value string) (string, error) {
	if !filepath.IsAbs(value) || strings.TrimSpace(value) != value {
		return "", fmt.Errorf("path must be absolute")
	}
	absolute, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	absolute = filepath.Clean(absolute)
	candidate := absolute
	var suffix []string
	for {
		resolved, resolveErr := filepath.EvalSymlinks(candidate)
		if resolveErr == nil {
			for index := len(suffix) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, suffix[index])
			}
			return filepath.Clean(resolved), nil
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return "", resolveErr
		}
		suffix = append(suffix, filepath.Base(candidate))
		candidate = parent
	}
}

func validCaptainIdentity(identity captainExecutionIdentity) bool {
	return identity.Schema == "captain-execution-identity.v1" && safeCaptainID(identity.TaskID) &&
		strings.TrimSpace(identity.RequestID) != "" && strings.TrimSpace(identity.AttemptID) != "" &&
		strings.TrimSpace(identity.TicketKey) != "" && strings.TrimSpace(identity.ProjectID) != "" &&
		strings.TrimSpace(identity.ExecutionScopeVersion) != "" && filepath.IsAbs(identity.CanonicalProjectPath)
}

func sameCaptainIdentity(left, right captainExecutionIdentity) bool {
	return left.Schema == right.Schema && left.RequestID == right.RequestID && left.TaskID == right.TaskID &&
		left.AttemptID == right.AttemptID && left.TicketKey == right.TicketKey && left.ProjectID == right.ProjectID &&
		left.ExecutionScopeVersion == right.ExecutionScopeVersion &&
		samePhysicalPath(left.CanonicalProjectPath, right.CanonicalProjectPath)
}

func safeCaptainID(value string) bool {
	if value == "" || len(value) > 200 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._:-", r)) {
			return false
		}
	}
	return true
}
