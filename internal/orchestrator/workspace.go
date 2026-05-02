package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

type WorkspaceManager interface {
	Prepare(ctx context.Context, spec WorkspaceSpec) (PreparedWorkspace, error)
	DescribeChanges(ctx context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error)
	ApplyChanges(ctx context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error)
	Cleanup(ctx context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error)
}

type WorkspaceSpec struct {
	TaskID       string
	WorkerID     string
	WorkDir      string
	BaseWorkDir  string
	BaseRevision string
}

type PreparedWorkspace struct {
	Root          string `json:"root"`
	CWD           string `json:"cwd"`
	SourceRoot    string `json:"sourceRoot"`
	WorkspaceName string `json:"workspaceName"`
	Change        string `json:"change"`
	BaseChange    string `json:"baseChange"`
	Status        string `json:"status"`
	SourceStatus  string `json:"sourceStatus"`
	Mode          string `json:"mode"`
	Dirty         bool   `json:"dirty"`
	SourceDirty   bool   `json:"sourceDirty"`
	VCSType       string `json:"vcsType"`
	CleanupPolicy string `json:"cleanupPolicy"`
	WorkerID      string `json:"workerId"`
	TaskID        string `json:"taskId"`
}

type WorkspaceChangedFile struct {
	Path   string `json:"path"`
	Status string `json:"status"`
}

type WorkspaceChanges struct {
	Root          string                 `json:"root"`
	CWD           string                 `json:"cwd"`
	WorkspaceName string                 `json:"workspaceName"`
	Mode          string                 `json:"mode"`
	VCSType       string                 `json:"vcsType"`
	Status        string                 `json:"status"`
	DiffStat      string                 `json:"diffStat"`
	Diff          string                 `json:"diff,omitempty"`
	ChangedFiles  []WorkspaceChangedFile `json:"changedFiles"`
	Dirty         bool                   `json:"dirty"`
	Error         string                 `json:"error,omitempty"`
	Artifacts     []WorkspaceArtifact    `json:"artifacts,omitempty"`
}

type WorkspaceArtifact struct {
	ID       string         `json:"id"`
	Kind     string         `json:"kind"`
	Name     string         `json:"name,omitempty"`
	Path     string         `json:"path,omitempty"`
	Content  string         `json:"content,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type WorkspaceResult string

const (
	WorkspaceResultSucceeded WorkspaceResult = "succeeded"
	WorkspaceResultFailed    WorkspaceResult = "failed"
	WorkspaceResultCanceled  WorkspaceResult = "canceled"
)

type WorkspaceCleanupPolicy string

const (
	WorkspaceCleanupRetain           WorkspaceCleanupPolicy = "retain"
	WorkspaceCleanupDeleteOnSuccess  WorkspaceCleanupPolicy = "delete_on_success"
	WorkspaceCleanupDeleteOnTerminal WorkspaceCleanupPolicy = "delete_on_terminal"
)

type WorkspaceCleanup struct {
	Root          string          `json:"root"`
	CWD           string          `json:"cwd"`
	WorkspaceName string          `json:"workspaceName"`
	Mode          string          `json:"mode"`
	VCSType       string          `json:"vcsType"`
	Policy        string          `json:"policy"`
	Result        WorkspaceResult `json:"result"`
	Cleaned       bool            `json:"cleaned"`
	Reason        string          `json:"reason,omitempty"`
	Error         string          `json:"error,omitempty"`
}

type WorkspaceMode string
type WorkspaceVCS string

const (
	WorkspaceModeIsolated WorkspaceMode = "isolated"
	WorkspaceModeShared   WorkspaceMode = "shared"

	WorkspaceVCSAuto WorkspaceVCS = "auto"
	WorkspaceVCSJJ   WorkspaceVCS = "jj"
	WorkspaceVCSGit  WorkspaceVCS = "git"
)

func NewWorkspaceManager(vcs WorkspaceVCS, mode WorkspaceMode, workspaceRoot string, cleanupPolicy WorkspaceCleanupPolicy) WorkspaceManager {
	if vcs == "" {
		vcs = WorkspaceVCSAuto
	}
	switch vcs {
	case WorkspaceVCSJJ:
		return NewJJWorkspaceManager(mode, workspaceRoot, cleanupPolicy)
	case WorkspaceVCSGit:
		return NewGitWorkspaceManager(mode, workspaceRoot, cleanupPolicy)
	default:
		return AutoWorkspaceManager{Mode: mode, WorkspaceRoot: workspaceRoot, CleanupPolicy: cleanupPolicy}
	}
}

type AutoWorkspaceManager struct {
	Mode          WorkspaceMode
	WorkspaceRoot string
	CleanupPolicy WorkspaceCleanupPolicy
}

func (m AutoWorkspaceManager) Prepare(ctx context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	absWorkDir, err := filepath.Abs(spec.WorkDir)
	if err != nil {
		return PreparedWorkspace{}, err
	}
	if _, err := runJJ(ctx, absWorkDir, "root"); err == nil {
		manager := NewJJWorkspaceManager(m.Mode, m.WorkspaceRoot, m.cleanupPolicy())
		manager.CleanupPolicy = m.cleanupPolicy()
		return manager.Prepare(ctx, spec)
	}
	if _, err := runGit(ctx, absWorkDir, "rev-parse", "--show-toplevel"); err == nil {
		manager := NewGitWorkspaceManager(m.Mode, m.WorkspaceRoot, m.cleanupPolicy())
		manager.CleanupPolicy = m.cleanupPolicy()
		return manager.Prepare(ctx, spec)
	}
	return PreparedWorkspace{}, fmt.Errorf("workdir is not inside a supported VCS workspace: %s", absWorkDir)
}

func (m AutoWorkspaceManager) Cleanup(ctx context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error) {
	switch workspace.VCSType {
	case "jj":
		manager := NewJJWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		manager.CleanupPolicy = WorkspaceCleanupPolicy(workspace.CleanupPolicy)
		return manager.Cleanup(ctx, workspace, result)
	case "git":
		manager := NewGitWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		manager.CleanupPolicy = WorkspaceCleanupPolicy(workspace.CleanupPolicy)
		return manager.Cleanup(ctx, workspace, result)
	default:
		cleanup := cleanupSkipped(workspace, result, "unsupported VCS type")
		return cleanup, nil
	}
}

func (m AutoWorkspaceManager) DescribeChanges(ctx context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	switch workspace.VCSType {
	case "jj":
		manager := NewJJWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		return manager.DescribeChanges(ctx, workspace)
	case "git":
		manager := NewGitWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		return manager.DescribeChanges(ctx, workspace)
	default:
		changes := baseWorkspaceChanges(workspace)
		changes.Error = "unsupported VCS type"
		return changes, nil
	}
}

func (m AutoWorkspaceManager) DescribeDiff(ctx context.Context, workspace PreparedWorkspace) (string, error) {
	switch workspace.VCSType {
	case "jj":
		manager := NewJJWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		return manager.DescribeDiff(ctx, workspace)
	case "git":
		manager := NewGitWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		return manager.DescribeDiff(ctx, workspace)
	default:
		return "", fmt.Errorf("unsupported VCS type %q", workspace.VCSType)
	}
}

func (m AutoWorkspaceManager) ApplyChanges(ctx context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	switch workspace.VCSType {
	case "jj":
		manager := NewJJWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		return manager.ApplyChanges(ctx, workspace, changes)
	case "git":
		manager := NewGitWorkspaceManager(WorkspaceMode(workspace.Mode), "", WorkspaceCleanupPolicy(workspace.CleanupPolicy))
		return manager.ApplyChanges(ctx, workspace, changes)
	default:
		return baseWorkerApplyResult(workspace, "unsupported"), fmt.Errorf("unsupported VCS type %q", workspace.VCSType)
	}
}

func (m AutoWorkspaceManager) cleanupPolicy() WorkspaceCleanupPolicy {
	if m.CleanupPolicy != "" {
		return m.CleanupPolicy
	}
	return WorkspaceCleanupRetain
}

type JJWorkspaceManager struct {
	Mode          WorkspaceMode
	WorkspaceRoot string
	CleanupPolicy WorkspaceCleanupPolicy
}

func NewJJWorkspaceManager(mode WorkspaceMode, workspaceRoot string, cleanupPolicy WorkspaceCleanupPolicy) JJWorkspaceManager {
	if mode == "" {
		mode = WorkspaceModeIsolated
	}
	if workspaceRoot == "" {
		workspaceRoot = defaultWorkspaceRoot()
	} else {
		workspaceRoot = expandWorkspaceRoot(workspaceRoot)
	}
	if cleanupPolicy == "" {
		cleanupPolicy = WorkspaceCleanupRetain
	}
	return JJWorkspaceManager{
		Mode:          mode,
		WorkspaceRoot: workspaceRoot,
		CleanupPolicy: cleanupPolicy,
	}
}

func (m JJWorkspaceManager) Prepare(ctx context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	if strings.TrimSpace(spec.WorkDir) == "" {
		return PreparedWorkspace{}, errors.New("workdir is required")
	}
	absWorkDir, err := filepath.Abs(spec.WorkDir)
	if err != nil {
		return PreparedWorkspace{}, err
	}

	root, err := runJJ(ctx, absWorkDir, "root")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("resolve jj root: %w", err)
	}
	root = strings.TrimSpace(root)
	if root == "" {
		return PreparedWorkspace{}, errors.New("jj root returned empty path")
	}

	baseChange, err := runJJ(ctx, root, "log", "-r", "@", "--no-graph")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read jj working copy change: %w", err)
	}
	baseChange = strings.TrimSpace(baseChange)
	baseRevision := strings.TrimSpace(spec.BaseRevision)
	if baseRevision == "" {
		baseRevision = "@"
	}
	if spec.BaseWorkDir != "" && spec.BaseRevision == "" {
		baseRevision = "@"
		baseChange, err = runJJ(ctx, spec.BaseWorkDir, "log", "-r", baseRevision, "--no-graph")
		if err != nil {
			return PreparedWorkspace{}, fmt.Errorf("read jj base workspace change: %w", err)
		}
		baseChange = strings.TrimSpace(baseChange)
	} else if baseRevision != "@" {
		baseChange, err = runJJ(ctx, root, "log", "-r", baseRevision, "--no-graph")
		if err != nil {
			return PreparedWorkspace{}, fmt.Errorf("read jj base revision: %w", err)
		}
		baseChange = strings.TrimSpace(baseChange)
	}

	sourceStatus, err := runJJ(ctx, root, "status")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read jj status: %w", err)
	}
	sourceStatus = strings.TrimSpace(sourceStatus)

	if m.Mode == WorkspaceModeShared {
		return PreparedWorkspace{
			Root:          root,
			CWD:           root,
			SourceRoot:    root,
			WorkspaceName: "shared",
			Change:        baseChange,
			BaseChange:    baseChange,
			Status:        sourceStatus,
			SourceStatus:  sourceStatus,
			Dirty:         isDirty(sourceStatus),
			SourceDirty:   isDirty(sourceStatus),
			Mode:          string(WorkspaceModeShared),
			VCSType:       "jj",
			CleanupPolicy: string(WorkspaceCleanupRetain),
			WorkerID:      spec.WorkerID,
			TaskID:        spec.TaskID,
		}, nil
	}

	destination, workspaceName, err := m.workspaceDestination(root, spec)
	if err != nil {
		return PreparedWorkspace{}, err
	}
	if _, err := os.Stat(destination); err == nil {
		return PreparedWorkspace{}, fmt.Errorf("workspace destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return PreparedWorkspace{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return PreparedWorkspace{}, err
	}

	workerMessage := "Worker " + shortID(spec.WorkerID)
	addDir := root
	if spec.BaseWorkDir != "" && spec.BaseRevision == "" {
		addDir = spec.BaseWorkDir
	}
	if _, err := runJJ(ctx, addDir, "workspace", "add", "--name", workspaceName, "--revision", baseRevision, "--message", workerMessage, "--sparse-patterns", "copy", destination); err != nil {
		return PreparedWorkspace{}, fmt.Errorf("create isolated jj workspace: %w", err)
	}

	change, err := runJJ(ctx, destination, "log", "-r", "@", "--no-graph")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read isolated jj working copy change: %w", err)
	}
	change = strings.TrimSpace(change)

	status, err := runJJ(ctx, destination, "status")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read isolated jj status: %w", err)
	}
	status = strings.TrimSpace(status)

	return PreparedWorkspace{
		Root:          destination,
		CWD:           destination,
		SourceRoot:    root,
		WorkspaceName: workspaceName,
		Change:        change,
		BaseChange:    baseChange,
		Status:        status,
		SourceStatus:  sourceStatus,
		Dirty:         isDirty(status),
		SourceDirty:   isDirty(sourceStatus),
		Mode:          string(WorkspaceModeIsolated),
		VCSType:       "jj",
		CleanupPolicy: m.cleanupPolicy(),
		WorkerID:      spec.WorkerID,
		TaskID:        spec.TaskID,
	}, nil
}

func (m JJWorkspaceManager) workspaceDestination(root string, spec WorkspaceSpec) (string, string, error) {
	name := "aged-" + shortID(spec.WorkerID)
	workspaceRoot := m.WorkspaceRoot
	if workspaceRoot == "" {
		workspaceRoot = defaultWorkspaceRoot()
	} else {
		workspaceRoot = expandWorkspaceRoot(workspaceRoot)
	}
	if !filepath.IsAbs(workspaceRoot) {
		workspaceRoot = filepath.Join(root, workspaceRoot)
	}
	return filepath.Join(workspaceRoot, name), name, nil
}

func (m JJWorkspaceManager) cleanupPolicy() string {
	if m.CleanupPolicy != "" {
		return string(m.CleanupPolicy)
	}
	return string(WorkspaceCleanupRetain)
}

func (m JJWorkspaceManager) Cleanup(ctx context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error) {
	cleanup, shouldClean := cleanupDecision(workspace, result)
	if !shouldClean {
		return cleanup, nil
	}
	if workspace.Mode != string(WorkspaceModeIsolated) {
		cleanup.Reason = "shared workspace is not removed"
		return cleanup, nil
	}
	if workspace.WorkspaceName == "" || workspace.Root == "" || workspace.SourceRoot == "" {
		cleanup.Error = "workspace cleanup requires workspace name, root, and source root"
		return cleanup, errors.New(cleanup.Error)
	}
	if _, err := runJJ(ctx, workspace.SourceRoot, "workspace", "forget", workspace.WorkspaceName); err != nil {
		cleanup.Error = err.Error()
		return cleanup, fmt.Errorf("forget jj workspace: %w", err)
	}
	if err := os.RemoveAll(workspace.Root); err != nil {
		cleanup.Error = err.Error()
		return cleanup, fmt.Errorf("remove jj workspace directory: %w", err)
	}
	cleanup.Cleaned = true
	return cleanup, nil
}

func (m JJWorkspaceManager) DescribeChanges(ctx context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	changes := baseWorkspaceChanges(workspace)
	if workspace.CWD == "" {
		changes.Error = "workspace cwd is required"
		return changes, errors.New(changes.Error)
	}

	status, err := runJJRefreshingStale(ctx, workspace.CWD, "status")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read jj workspace status: %w", err)
	}
	changes.Status = strings.TrimSpace(status)
	changes.Dirty = isDirty(changes.Status)

	diffStat, err := runJJRefreshingStale(ctx, workspace.CWD, "diff", "--stat")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read jj workspace diff stat: %w", err)
	}
	changes.DiffStat = strings.TrimSpace(diffStat)

	summary, err := runJJRefreshingStale(ctx, workspace.CWD, "diff", "--summary")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read jj workspace diff summary: %w", err)
	}
	changes.ChangedFiles = parseJJDiffSummary(summary)
	return changes, nil
}

func (m JJWorkspaceManager) DescribeDiff(ctx context.Context, workspace PreparedWorkspace) (string, error) {
	if workspace.CWD == "" {
		return "", errors.New("workspace cwd is required")
	}
	return runJJRefreshingStale(ctx, workspace.CWD, "diff", "--git")
}

func (m JJWorkspaceManager) ApplyChanges(ctx context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	result := baseWorkerApplyResult(workspace, "jj_new_merge")
	result.AppliedFiles = changes.ChangedFiles
	if workspace.Mode != string(WorkspaceModeIsolated) {
		return result, errors.New("applying jj worker changes requires an isolated workspace")
	}
	if workspace.SourceRoot == "" || workspace.WorkspaceName == "" {
		return result, errors.New("applying jj worker changes requires source root and workspace name")
	}
	if len(changes.ChangedFiles) == 0 && !changes.Dirty {
		return result, nil
	}
	workerRevision := workspace.WorkspaceName + "@"
	message := "Apply worker " + shortID(workspace.WorkerID)
	if _, err := runJJRefreshingStale(ctx, workspace.CWD, "status"); err != nil {
		return result, fmt.Errorf("refresh jj worker workspace: %w", err)
	}
	if err := ensureJJDescription(ctx, workspace.CWD, "@", "Worker "+shortID(workspace.WorkerID)); err != nil {
		return result, fmt.Errorf("describe jj worker revision: %w", err)
	}
	if _, err := runJJ(ctx, workspace.SourceRoot, "new", "--message", message, "@", workerRevision); err != nil {
		return result, fmt.Errorf("create jj merge revision: %w", err)
	}
	return result, nil
}

func ensureJJDescription(ctx context.Context, dir string, revision string, message string) error {
	description, err := runJJ(ctx, dir, "log", "-r", revision, "--no-graph", "-T", "description")
	if err != nil {
		return err
	}
	if strings.TrimSpace(description) != "" {
		return nil
	}
	_, err = runJJ(ctx, dir, "describe", "--message", message, revision)
	return err
}

func isDirty(status string) bool {
	return !strings.Contains(status, "The working copy has no changes.")
}

var unsafeWorkspaceNameChars = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

func shortID(id string) string {
	id = unsafeWorkspaceNameChars.ReplaceAllString(id, "-")
	id = strings.Trim(id, "-")
	if len(id) > 12 {
		return id[:12]
	}
	if id == "" {
		return "worker"
	}
	return id
}

func runJJ(ctx context.Context, dir string, args ...string) (string, error) {
	return runCommand(ctx, dir, "jj", args...)
}

func runJJRefreshingStale(ctx context.Context, dir string, args ...string) (string, error) {
	out, err := runJJ(ctx, dir, args...)
	if err == nil || !strings.Contains(err.Error(), "working copy is stale") {
		return out, err
	}
	if _, updateErr := runJJ(ctx, dir, "workspace", "update-stale"); updateErr != nil {
		return out, err
	}
	return runJJ(ctx, dir, args...)
}

type GitWorkspaceManager struct {
	Mode          WorkspaceMode
	WorkspaceRoot string
	CleanupPolicy WorkspaceCleanupPolicy
}

func NewGitWorkspaceManager(mode WorkspaceMode, workspaceRoot string, cleanupPolicy WorkspaceCleanupPolicy) GitWorkspaceManager {
	if mode == "" {
		mode = WorkspaceModeIsolated
	}
	if workspaceRoot == "" {
		workspaceRoot = defaultWorkspaceRoot()
	} else {
		workspaceRoot = expandWorkspaceRoot(workspaceRoot)
	}
	if cleanupPolicy == "" {
		cleanupPolicy = WorkspaceCleanupRetain
	}
	return GitWorkspaceManager{
		Mode:          mode,
		WorkspaceRoot: workspaceRoot,
		CleanupPolicy: cleanupPolicy,
	}
}

func (m GitWorkspaceManager) Prepare(ctx context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	if strings.TrimSpace(spec.WorkDir) == "" {
		return PreparedWorkspace{}, errors.New("workdir is required")
	}
	absWorkDir, err := filepath.Abs(spec.WorkDir)
	if err != nil {
		return PreparedWorkspace{}, err
	}

	root, err := runGit(ctx, absWorkDir, "rev-parse", "--show-toplevel")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("resolve git root: %w", err)
	}
	root = strings.TrimSpace(root)
	if root == "" {
		return PreparedWorkspace{}, errors.New("git root returned empty path")
	}

	baseChange, err := runGit(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read git HEAD: %w", err)
	}
	baseChange = strings.TrimSpace(baseChange)
	baseRef := strings.TrimSpace(spec.BaseRevision)
	if baseRef == "" {
		baseRef = "HEAD"
	}
	if baseRef != "HEAD" {
		baseChange, err = runGit(ctx, root, "rev-parse", baseRef)
		if err != nil {
			return PreparedWorkspace{}, fmt.Errorf("read git base revision: %w", err)
		}
		baseChange = strings.TrimSpace(baseChange)
	}

	sourceStatus, err := runGit(ctx, root, "status", "--short", "--branch")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read git status: %w", err)
	}
	sourceStatus = strings.TrimSpace(sourceStatus)
	sourceDirty := gitSourceStatusDirty(sourceStatus)

	if m.Mode == WorkspaceModeShared {
		return PreparedWorkspace{
			Root:          root,
			CWD:           root,
			SourceRoot:    root,
			WorkspaceName: "shared",
			Change:        baseChange,
			BaseChange:    baseChange,
			Status:        sourceStatus,
			SourceStatus:  sourceStatus,
			Dirty:         gitStatusDirty(sourceStatus),
			SourceDirty:   sourceDirty,
			Mode:          string(WorkspaceModeShared),
			VCSType:       "git",
			CleanupPolicy: string(WorkspaceCleanupRetain),
			WorkerID:      spec.WorkerID,
			TaskID:        spec.TaskID,
		}, nil
	}
	destination, workspaceName := gitWorkspaceDestination(root, m.WorkspaceRoot, spec)
	if _, err := os.Stat(destination); err == nil {
		return PreparedWorkspace{}, fmt.Errorf("workspace destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return PreparedWorkspace{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return PreparedWorkspace{}, err
	}
	if _, err := runGit(ctx, root, "worktree", "add", "--detach", destination, baseRef); err != nil {
		return PreparedWorkspace{}, fmt.Errorf("create isolated git worktree: %w", err)
	}
	if strings.TrimSpace(spec.BaseWorkDir) != "" {
		if err := copyGitWorkspaceChanges(ctx, spec.BaseWorkDir, destination); err != nil {
			return PreparedWorkspace{}, err
		}
	}

	change, err := runGit(ctx, destination, "rev-parse", "HEAD")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read isolated git HEAD: %w", err)
	}
	change = strings.TrimSpace(change)

	status, err := runGit(ctx, destination, "status", "--short", "--branch")
	if err != nil {
		return PreparedWorkspace{}, fmt.Errorf("read isolated git status: %w", err)
	}
	status = strings.TrimSpace(status)

	return PreparedWorkspace{
		Root:          destination,
		CWD:           destination,
		SourceRoot:    root,
		WorkspaceName: workspaceName,
		Change:        change,
		BaseChange:    baseChange,
		Status:        status,
		SourceStatus:  sourceStatus,
		Dirty:         gitStatusDirty(status),
		SourceDirty:   sourceDirty,
		Mode:          string(WorkspaceModeIsolated),
		VCSType:       "git",
		CleanupPolicy: m.cleanupPolicy(),
		WorkerID:      spec.WorkerID,
		TaskID:        spec.TaskID,
	}, nil
}

func (m GitWorkspaceManager) cleanupPolicy() string {
	if m.CleanupPolicy != "" {
		return string(m.CleanupPolicy)
	}
	return string(WorkspaceCleanupRetain)
}

func (m GitWorkspaceManager) Cleanup(ctx context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error) {
	cleanup, shouldClean := cleanupDecision(workspace, result)
	if !shouldClean {
		return cleanup, nil
	}
	if workspace.Mode != string(WorkspaceModeIsolated) {
		cleanup.Reason = "shared workspace is not removed"
		return cleanup, nil
	}
	if workspace.Root == "" || workspace.SourceRoot == "" {
		cleanup.Error = "workspace cleanup requires root and source root"
		return cleanup, errors.New(cleanup.Error)
	}
	if _, err := runGit(ctx, workspace.SourceRoot, "worktree", "remove", "--force", workspace.Root); err != nil {
		cleanup.Error = err.Error()
		return cleanup, fmt.Errorf("remove git worktree: %w", err)
	}
	cleanup.Cleaned = true
	return cleanup, nil
}

func (m GitWorkspaceManager) DescribeChanges(ctx context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	changes := baseWorkspaceChanges(workspace)
	if workspace.CWD == "" {
		changes.Error = "workspace cwd is required"
		return changes, errors.New(changes.Error)
	}

	status, err := runGit(ctx, workspace.CWD, "status", "--short", "--branch")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read git workspace status: %w", err)
	}
	changes.Status = strings.TrimSpace(status)

	baseRef, err := gitWorkspaceDiffBase(ctx, workspace)
	if err != nil {
		changes.Error = err.Error()
		return changes, err
	}
	diffStat, err := runGit(ctx, workspace.CWD, "diff", "--stat", baseRef, "--")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read git workspace diff stat: %w", err)
	}
	changes.DiffStat = strings.TrimSpace(diffStat)

	nameStatus, err := runGit(ctx, workspace.CWD, "diff", "--name-status", "-z", baseRef, "--")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read git workspace changed files: %w", err)
	}
	changedFiles := parseGitNameStatus(nameStatus)
	untracked, err := runGit(ctx, workspace.CWD, "ls-files", "--others", "--exclude-standard")
	if err != nil {
		changes.Error = err.Error()
		return changes, fmt.Errorf("read git workspace untracked files: %w", err)
	}
	for _, path := range strings.Split(untracked, "\n") {
		path = strings.TrimSpace(path)
		if path != "" {
			changedFiles = append(changedFiles, WorkspaceChangedFile{Path: path, Status: "untracked"})
		}
	}
	changes.ChangedFiles = changedFiles
	changes.Dirty = gitStatusDirty(changes.Status) || changes.DiffStat != "" || len(changes.ChangedFiles) > 0
	return changes, nil
}

func (m GitWorkspaceManager) DescribeDiff(ctx context.Context, workspace PreparedWorkspace) (string, error) {
	if workspace.CWD == "" {
		return "", errors.New("workspace cwd is required")
	}
	baseRef, err := gitWorkspaceDiffBase(ctx, workspace)
	if err != nil {
		return "", err
	}
	diff, err := runGit(ctx, workspace.CWD, "diff", "--binary", baseRef, "--")
	if err != nil {
		return "", err
	}
	untracked, err := runGit(ctx, workspace.CWD, "ls-files", "--others", "--exclude-standard")
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	builder.WriteString(diff)
	for _, path := range strings.Split(untracked, "\n") {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		fileDiff, err := runGitNoIndexDiff(ctx, workspace.CWD, os.DevNull, path)
		if err != nil {
			return "", err
		}
		builder.WriteString(fileDiff)
	}
	return builder.String(), nil
}

func gitWorkspaceDiffBase(ctx context.Context, workspace PreparedWorkspace) (string, error) {
	baseRef := strings.TrimSpace(workspace.BaseChange)
	if baseRef == "" {
		return "HEAD", nil
	}
	if _, err := runGit(ctx, workspace.CWD, "rev-parse", "--verify", "--quiet", baseRef+"^{commit}"); err != nil {
		return "", fmt.Errorf("verify git workspace base %q: %w", baseRef, err)
	}
	return baseRef, nil
}

func (m GitWorkspaceManager) ApplyChanges(ctx context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	result := baseWorkerApplyResult(workspace, "git_commit_merge")
	result.AppliedFiles = changes.ChangedFiles
	if workspace.Mode != string(WorkspaceModeIsolated) {
		return result, errors.New("applying git worker changes requires an isolated workspace")
	}
	if workspace.SourceRoot == "" || workspace.Root == "" {
		return result, errors.New("applying git worker changes requires source and workspace roots")
	}
	if len(changes.ChangedFiles) == 0 && !changes.Dirty {
		return result, nil
	}
	if _, err := runGit(ctx, workspace.Root, "add", "-A"); err != nil {
		return result, fmt.Errorf("stage git worker changes: %w", err)
	}
	if _, err := runGit(ctx, workspace.Root, "commit", "-m", "Apply worker "+shortID(workspace.WorkerID)); err != nil {
		return result, fmt.Errorf("commit git worker changes: %w", err)
	}
	commit, err := runGit(ctx, workspace.Root, "rev-parse", "HEAD")
	if err != nil {
		return result, fmt.Errorf("read git worker commit: %w", err)
	}
	if err := ensureGitTrackedClean(ctx, workspace.SourceRoot, "merge git worker commit"); err != nil {
		return result, err
	}
	if _, err := runGit(ctx, workspace.SourceRoot, "merge", "--no-ff", strings.TrimSpace(commit)); err != nil {
		if cleanupErr := abortFailedGitMerge(ctx, workspace.SourceRoot); cleanupErr != nil {
			return result, fmt.Errorf("merge git worker commit: %w; restore source checkout: %v", err, cleanupErr)
		}
		return result, fmt.Errorf("merge git worker commit: %w", err)
	}
	return result, nil
}

func ensureGitTrackedClean(ctx context.Context, dir string, operation string) error {
	status, err := gitTrackedStatus(ctx, dir)
	if err != nil {
		return fmt.Errorf("read git status before %s: %w", operation, err)
	}
	if status != "" {
		return fmt.Errorf("%s requires a clean tracked checkout; commit, stash, or resolve tracked changes first:\n%s", operation, status)
	}
	return nil
}

func gitTrackedStatus(ctx context.Context, dir string) (string, error) {
	status, err := runGit(ctx, dir, "status", "--porcelain=v1", "--untracked-files=no")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(status), nil
}

func abortFailedGitMerge(ctx context.Context, dir string) error {
	if _, err := runGit(ctx, dir, "merge", "--abort"); err == nil {
		return nil
	}
	if _, err := runGit(ctx, dir, "reset", "--hard", "HEAD"); err != nil {
		return fmt.Errorf("git reset --hard HEAD: %w", err)
	}
	return nil
}

func copyGitWorkspaceChanges(ctx context.Context, source string, destination string) error {
	copiedTracked, err := copyGitTrackedChanges(ctx, source, destination)
	if err != nil {
		return err
	}
	copiedUntracked, err := copyGitUntrackedFiles(ctx, source, destination)
	if err != nil {
		return err
	}
	if !copiedTracked && !copiedUntracked {
		return nil
	}
	if _, err := runGit(ctx, destination, "add", "-A"); err != nil {
		return fmt.Errorf("stage git base workspace diff: %w", err)
	}
	if _, err := runCommand(ctx, destination, "git", "-c", "user.name=aged", "-c", "user.email=aged@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "Base worker candidate"); err != nil {
		return fmt.Errorf("commit git base workspace diff: %w", err)
	}
	return nil
}

func copyGitTrackedChanges(ctx context.Context, source string, destination string) (bool, error) {
	baseRef, err := runGit(ctx, destination, "rev-parse", "HEAD")
	if err != nil {
		return false, fmt.Errorf("read git base workspace destination HEAD: %w", err)
	}
	baseRef = strings.TrimSpace(baseRef)
	if baseRef == "" {
		baseRef = "HEAD"
	}
	nameStatus, err := runGit(ctx, source, "diff", "--name-status", "-z", baseRef, "--")
	if err != nil {
		return false, fmt.Errorf("list git base workspace tracked changes: %w", err)
	}
	if nameStatus == "" {
		return false, nil
	}
	fields := strings.Split(nameStatus, "\x00")
	copied := false
	for i := 0; i < len(fields); {
		status := fields[i]
		i++
		if status == "" {
			continue
		}
		if i >= len(fields) {
			return false, fmt.Errorf("parse git base workspace tracked changes: status %q missing path", status)
		}
		path := fields[i]
		i++
		if path == "" {
			continue
		}
		if strings.HasPrefix(status, "R") || strings.HasPrefix(status, "C") {
			if i >= len(fields) {
				return false, fmt.Errorf("parse git base workspace tracked changes: status %q missing destination path", status)
			}
			nextPath := fields[i]
			i++
			if strings.HasPrefix(status, "R") {
				if err := removeWorkspacePath(destination, path); err != nil {
					return false, fmt.Errorf("copy git base workspace tracked rename %q: %w", path, err)
				}
			}
			path = nextPath
		}
		if strings.HasPrefix(status, "D") {
			if err := removeWorkspacePath(destination, path); err != nil {
				return false, fmt.Errorf("copy git base workspace tracked delete %q: %w", path, err)
			}
			copied = true
			continue
		}
		sourcePath, err := safeWorkspacePath(source, path)
		if err != nil {
			return false, fmt.Errorf("copy git base workspace tracked file %q: %w", path, err)
		}
		destinationPath, err := safeWorkspacePath(destination, path)
		if err != nil {
			return false, fmt.Errorf("copy git base workspace tracked file %q: %w", path, err)
		}
		if err := copyWorkspaceFile(sourcePath, destinationPath); err != nil {
			return false, fmt.Errorf("copy git base workspace tracked file %q: %w", path, err)
		}
		copied = true
	}
	return copied, nil
}

func applyGitPatchToWorkspace(ctx context.Context, dir string, patch string) error {
	patch = normalizePatchText(patch)
	if patch == "" {
		return nil
	}
	apply := func(args ...string) error {
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		cmd.Stdin = strings.NewReader(patch)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			detail := strings.TrimSpace(stderr.String())
			if detail != "" {
				return fmt.Errorf("%w: %s", err, detail)
			}
			return err
		}
		return nil
	}
	if err := apply("apply", "--whitespace=nowarn"); err == nil {
		return nil
	}
	return apply("apply", "--3way", "--whitespace=nowarn")
}

func normalizePatchText(patch string) string {
	if strings.TrimSpace(patch) == "" {
		return ""
	}
	if !strings.HasSuffix(patch, "\n") {
		return patch + "\n"
	}
	return patch
}

func copyGitUntrackedFiles(ctx context.Context, source string, destination string) (bool, error) {
	untracked, err := runGit(ctx, source, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return false, fmt.Errorf("list git base workspace untracked files: %w", err)
	}
	copied := false
	for _, path := range strings.Split(untracked, "\x00") {
		if path == "" {
			continue
		}
		sourcePath, err := safeWorkspacePath(source, path)
		if err != nil {
			return false, fmt.Errorf("copy git base workspace untracked file %q: %w", path, err)
		}
		destinationPath, err := safeWorkspacePath(destination, path)
		if err != nil {
			return false, fmt.Errorf("copy git base workspace untracked file %q: %w", path, err)
		}
		if err := copyWorkspaceFile(sourcePath, destinationPath); err != nil {
			return false, fmt.Errorf("copy git base workspace untracked file %q: %w", path, err)
		}
		copied = true
	}
	return copied, nil
}

func safeWorkspacePath(root string, relativePath string) (string, error) {
	if filepath.IsAbs(relativePath) {
		return "", errors.New("absolute paths are not allowed")
	}
	clean := filepath.Clean(relativePath)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", errors.New("paths outside the workspace are not allowed")
	}
	return filepath.Join(root, clean), nil
}

func removeWorkspacePath(root string, relativePath string) error {
	path, err := safeWorkspacePath(root, relativePath)
	if err != nil {
		return err
	}
	err = os.RemoveAll(path)
	if err != nil {
		return err
	}
	return pruneEmptyParents(root, filepath.Dir(path))
}

func pruneEmptyParents(root string, dir string) error {
	root = filepath.Clean(root)
	for {
		dir = filepath.Clean(dir)
		if dir == root || dir == "." || dir == string(os.PathSeparator) || !strings.HasPrefix(dir, root+string(os.PathSeparator)) {
			return nil
		}
		if err := os.Remove(dir); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if errors.Is(err, os.ErrPermission) {
				return nil
			}
			return nil
		}
		dir = filepath.Dir(dir)
	}
}

func copyWorkspaceFile(source string, destination string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		target, err := os.Readlink(source)
		if err != nil {
			return err
		}
		_ = os.Remove(destination)
		return os.Symlink(target, destination)
	}
	contents, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return os.WriteFile(destination, contents, info.Mode().Perm())
}

func cleanupDecision(workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, bool) {
	policy := WorkspaceCleanupPolicy(workspace.CleanupPolicy)
	if policy == "" {
		policy = WorkspaceCleanupRetain
	}
	cleanup := WorkspaceCleanup{
		Root:          workspace.Root,
		CWD:           workspace.CWD,
		WorkspaceName: workspace.WorkspaceName,
		Mode:          workspace.Mode,
		VCSType:       workspace.VCSType,
		Policy:        string(policy),
		Result:        result,
	}
	switch policy {
	case WorkspaceCleanupDeleteOnSuccess:
		if result == WorkspaceResultSucceeded {
			return cleanup, true
		}
		cleanup.Reason = "policy only deletes successful workspaces"
		return cleanup, false
	case WorkspaceCleanupDeleteOnTerminal:
		return cleanup, true
	default:
		cleanup.Reason = "policy retains workspaces"
		return cleanup, false
	}
}

func cleanupSkipped(workspace PreparedWorkspace, result WorkspaceResult, reason string) WorkspaceCleanup {
	return WorkspaceCleanup{
		Root:          workspace.Root,
		CWD:           workspace.CWD,
		WorkspaceName: workspace.WorkspaceName,
		Mode:          workspace.Mode,
		VCSType:       workspace.VCSType,
		Policy:        workspace.CleanupPolicy,
		Result:        result,
		Reason:        reason,
	}
}

func baseWorkspaceChanges(workspace PreparedWorkspace) WorkspaceChanges {
	return WorkspaceChanges{
		Root:          workspace.Root,
		CWD:           workspace.CWD,
		WorkspaceName: workspace.WorkspaceName,
		Mode:          workspace.Mode,
		VCSType:       workspace.VCSType,
		Status:        workspace.Status,
		Dirty:         workspace.Dirty,
	}
}

func baseWorkerApplyResult(workspace PreparedWorkspace, method string) WorkerApplyResult {
	return WorkerApplyResult{
		SourceRoot:    workspace.SourceRoot,
		WorkspaceRoot: workspace.Root,
		Method:        method,
	}
}

func parseJJDiffSummary(summary string) []WorkspaceChangedFile {
	var files []WorkspaceChangedFile
	for _, line := range strings.Split(summary, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		status, path, ok := strings.Cut(line, " ")
		if !ok {
			files = append(files, WorkspaceChangedFile{Path: line, Status: "changed"})
			continue
		}
		files = append(files, WorkspaceChangedFile{Path: strings.TrimSpace(path), Status: normalizeChangeStatus(status)})
	}
	return files
}

func parseGitPorcelain(status string) []WorkspaceChangedFile {
	var files []WorkspaceChangedFile
	for _, line := range strings.Split(status, "\n") {
		if strings.TrimSpace(line) == "" || len(line) < 4 {
			continue
		}
		code := strings.TrimSpace(line[:2])
		path := strings.TrimSpace(line[3:])
		if before, after, ok := strings.Cut(path, " -> "); ok {
			files = append(files, WorkspaceChangedFile{Path: before, Status: "renamed_from"})
			path = after
		}
		files = append(files, WorkspaceChangedFile{Path: path, Status: normalizeChangeStatus(code)})
	}
	return files
}

func parseGitNameStatus(nameStatus string) []WorkspaceChangedFile {
	var files []WorkspaceChangedFile
	fields := strings.Split(nameStatus, "\x00")
	for i := 0; i < len(fields); {
		status := fields[i]
		i++
		if status == "" {
			continue
		}
		if i >= len(fields) {
			break
		}
		path := fields[i]
		i++
		if path == "" {
			continue
		}
		if strings.HasPrefix(status, "R") || strings.HasPrefix(status, "C") {
			if i >= len(fields) {
				break
			}
			nextPath := fields[i]
			i++
			if strings.HasPrefix(status, "R") {
				files = append(files, WorkspaceChangedFile{Path: path, Status: "renamed_from"})
				files = append(files, WorkspaceChangedFile{Path: nextPath, Status: "renamed"})
			} else {
				files = append(files, WorkspaceChangedFile{Path: nextPath, Status: "added"})
			}
			continue
		}
		files = append(files, WorkspaceChangedFile{Path: path, Status: normalizeChangeStatus(status)})
	}
	return files
}

func normalizeChangeStatus(status string) string {
	switch {
	case strings.Contains(status, "?"):
		return "untracked"
	case strings.Contains(status, "A"):
		return "added"
	case strings.Contains(status, "D"):
		return "deleted"
	case strings.Contains(status, "R"):
		return "renamed"
	case strings.Contains(status, "C"):
		return "copied"
	case strings.Contains(status, "M"):
		return "modified"
	default:
		return strings.ToLower(status)
	}
}

func gitWorkspaceDestination(root string, workspaceRoot string, spec WorkspaceSpec) (string, string) {
	name := "aged-" + shortID(spec.WorkerID)
	if workspaceRoot == "" {
		workspaceRoot = defaultWorkspaceRoot()
	} else {
		workspaceRoot = expandWorkspaceRoot(workspaceRoot)
	}
	if !filepath.IsAbs(workspaceRoot) {
		workspaceRoot = filepath.Join(root, workspaceRoot)
	}
	return filepath.Join(workspaceRoot, name), name
}

func defaultWorkspaceRoot() string {
	home, err := os.UserHomeDir()
	if err == nil && strings.TrimSpace(home) != "" {
		return filepath.Join(home, ".aged", "workspaces")
	}
	return ".aged/workspaces"
}

func expandWorkspaceRoot(root string) string {
	root = strings.TrimSpace(root)
	if root == "~" {
		home, err := os.UserHomeDir()
		if err == nil && strings.TrimSpace(home) != "" {
			return home
		}
		return root
	}
	if strings.HasPrefix(root, "~/") {
		home, err := os.UserHomeDir()
		if err == nil && strings.TrimSpace(home) != "" {
			return filepath.Join(home, strings.TrimPrefix(root, "~/"))
		}
	}
	return root
}

func gitStatusDirty(status string) bool {
	for _, line := range strings.Split(status, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "##") {
			return true
		}
	}
	return false
}

func gitSourceStatusDirty(status string) bool {
	for _, line := range strings.Split(status, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "##") || gitStatusLineIsAgedState(line) {
			continue
		}
		return true
	}
	return false
}

func gitStatusLineIsAgedState(line string) bool {
	if !strings.HasPrefix(line, "?? ") {
		return false
	}
	path := strings.TrimSpace(strings.TrimPrefix(line, "?? "))
	return path == ".aged" || path == ".aged/" || strings.HasPrefix(path, ".aged/")
}

func runGit(ctx context.Context, dir string, args ...string) (string, error) {
	return runCommand(ctx, dir, "git", args...)
}

func runGitNoIndexDiff(ctx context.Context, dir string, before string, after string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "diff", "--no-index", "--", before, after)
	cmd.Dir = dir
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return stdout.String(), nil
		}
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = strings.TrimSpace(stdout.String())
		}
		if detail != "" {
			return "", fmt.Errorf("%w: %s", err, detail)
		}
		return "", err
	}
	return stdout.String(), nil
}

func runCommand(ctx context.Context, dir string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	if env := commandEnv(name); env != nil {
		cmd.Env = env
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = strings.TrimSpace(stdout.String())
		}
		if detail != "" {
			return "", fmt.Errorf("%w: %s", err, detail)
		}
		return "", err
	}
	return stdout.String(), nil
}

func commandEnv(name string) []string {
	if filepath.Base(name) != "gh" {
		return nil
	}
	return sanitizeGitHubCLIEnv(os.Environ())
}

func sanitizeGitHubCLIEnv(env []string) []string {
	drop := map[string]bool{
		"SSL_CERT_FILE":       true,
		"SSL_CERT_DIR":        true,
		"GIT_SSL_CAINFO":      true,
		"REQUESTS_CA_BUNDLE":  true,
		"CURL_CA_BUNDLE":      true,
		"NODE_EXTRA_CA_CERTS": true,
		"NIX_SSL_CERT_FILE":   true,
	}
	out := make([]string, 0, len(env))
	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if ok && drop[key] {
			continue
		}
		out = append(out, entry)
	}
	return out
}
