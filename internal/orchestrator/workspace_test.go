package orchestrator

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestJJWorkspaceManagerPreparesCurrentRepo(t *testing.T) {
	if _, err := exec.LookPath("jj"); err != nil {
		t.Skip("jj is not installed")
	}
	workspace, err := NewJJWorkspaceManager(WorkspaceModeShared, "", WorkspaceCleanupRetain).Prepare(context.Background(), WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "worker",
		WorkDir:  ".",
	})
	if err != nil {
		t.Fatal(err)
	}
	if workspace.Root == "" {
		t.Fatalf("Root is empty")
	}
	if workspace.CWD != workspace.Root {
		t.Fatalf("CWD = %q, want root %q", workspace.CWD, workspace.Root)
	}
	if workspace.VCSType != "jj" {
		t.Fatalf("VCSType = %q", workspace.VCSType)
	}
	if workspace.Mode != "shared" {
		t.Fatalf("Mode = %q", workspace.Mode)
	}
	if !strings.Contains(workspace.Change, "@") {
		t.Fatalf("Change does not describe @: %q", workspace.Change)
	}
	if !strings.Contains(workspace.Status, "Working copy") && !strings.Contains(workspace.Status, "The working copy") {
		t.Fatalf("Status does not look like jj status: %q", workspace.Status)
	}
}

func TestAutoWorkspaceManagerUsesCurrentRepoVCS(t *testing.T) {
	workspace, err := NewWorkspaceManager(WorkspaceVCSAuto, WorkspaceModeShared, "", WorkspaceCleanupRetain).Prepare(context.Background(), WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "worker",
		WorkDir:  ".",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "git"
	if _, err := exec.LookPath("jj"); err == nil {
		if out, err := exec.Command("jj", "root").CombinedOutput(); err == nil && strings.TrimSpace(string(out)) != "" {
			want = "jj"
		}
	}
	if workspace.VCSType != want {
		t.Fatalf("VCSType = %q, want %q", workspace.VCSType, want)
	}
}

func TestGitStatusDirty(t *testing.T) {
	if gitStatusDirty("## main") {
		t.Fatalf("branch-only status should be clean")
	}
	if !gitStatusDirty("## main\n M file.go") {
		t.Fatalf("modified file should be dirty")
	}
	if gitSourceStatusDirty("## main\n?? .aged/") {
		t.Fatalf("untracked aged state should not dirty git source checkouts")
	}
	if !gitSourceStatusDirty("## main\n?? .aged/\n M file.go") {
		t.Fatalf("real source changes should dirty git source checkouts")
	}
}

func TestDefaultWorkspaceRootUsesUserAgedDirectory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	gitManager := NewGitWorkspaceManager(WorkspaceModeIsolated, "", WorkspaceCleanupRetain)
	if gitManager.WorkspaceRoot != filepath.Join(home, ".aged", "workspaces") {
		t.Fatalf("git workspace root = %q", gitManager.WorkspaceRoot)
	}

	jjManager := NewJJWorkspaceManager(WorkspaceModeIsolated, "", WorkspaceCleanupRetain)
	if jjManager.WorkspaceRoot != filepath.Join(home, ".aged", "workspaces") {
		t.Fatalf("jj workspace root = %q", jjManager.WorkspaceRoot)
	}
}

func TestParseJJDiffSummary(t *testing.T) {
	files := parseJJDiffSummary("M internal/orchestrator/service.go\nA web/src/types.ts\nD old.txt\n")
	if len(files) != 3 {
		t.Fatalf("files = %+v", files)
	}
	if files[0] != (WorkspaceChangedFile{Path: "internal/orchestrator/service.go", Status: "modified"}) {
		t.Fatalf("first file = %+v", files[0])
	}
	if files[1] != (WorkspaceChangedFile{Path: "web/src/types.ts", Status: "added"}) {
		t.Fatalf("second file = %+v", files[1])
	}
	if files[2] != (WorkspaceChangedFile{Path: "old.txt", Status: "deleted"}) {
		t.Fatalf("third file = %+v", files[2])
	}
}

func TestParseGitPorcelain(t *testing.T) {
	files := parseGitPorcelain(" M internal/orchestrator/service.go\n?? web/src/types.ts\nR  old.txt -> new.txt\n")
	if len(files) != 4 {
		t.Fatalf("files = %+v", files)
	}
	if files[0] != (WorkspaceChangedFile{Path: "internal/orchestrator/service.go", Status: "modified"}) {
		t.Fatalf("first file = %+v", files[0])
	}
	if files[1] != (WorkspaceChangedFile{Path: "web/src/types.ts", Status: "untracked"}) {
		t.Fatalf("second file = %+v", files[1])
	}
	if files[2] != (WorkspaceChangedFile{Path: "old.txt", Status: "renamed_from"}) {
		t.Fatalf("third file = %+v", files[2])
	}
	if files[3] != (WorkspaceChangedFile{Path: "new.txt", Status: "renamed"}) {
		t.Fatalf("fourth file = %+v", files[3])
	}
}

func TestParseGitNameStatus(t *testing.T) {
	files := parseGitNameStatus("M\x00internal/orchestrator/service.go\x00A\x00web/src/types.ts\x00R100\x00old.txt\x00new.txt\x00")
	if len(files) != 4 {
		t.Fatalf("files = %+v", files)
	}
	if files[0] != (WorkspaceChangedFile{Path: "internal/orchestrator/service.go", Status: "modified"}) {
		t.Fatalf("first file = %+v", files[0])
	}
	if files[1] != (WorkspaceChangedFile{Path: "web/src/types.ts", Status: "added"}) {
		t.Fatalf("second file = %+v", files[1])
	}
	if files[2] != (WorkspaceChangedFile{Path: "old.txt", Status: "renamed_from"}) {
		t.Fatalf("third file = %+v", files[2])
	}
	if files[3] != (WorkspaceChangedFile{Path: "new.txt", Status: "renamed"}) {
		t.Fatalf("fourth file = %+v", files[3])
	}
}

func TestJJWorkspaceManagerDescribesIsolatedWorkerCommit(t *testing.T) {
	repo := initJJTestRepo(t)
	manager := NewJJWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain)

	workspace, err := manager.Prepare(context.Background(), WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "worker-123",
		WorkDir:  repo,
	})
	if err != nil {
		t.Fatal(err)
	}

	description := strings.TrimSpace(runTestJJ(t, workspace.CWD, "log", "-r", "@", "--no-graph", "-T", "description"))
	if description != "Worker worker-123" {
		t.Fatalf("worker description = %q", description)
	}
}

func TestJJWorkspaceManagerApplyBackfillsWorkerDescription(t *testing.T) {
	ctx := context.Background()
	repo := initJJTestRepo(t)
	manager := NewJJWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain)
	workspace, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "worker1",
		WorkDir:  repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.CWD, "file.txt"), []byte("worker\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestJJ(t, workspace.CWD, "describe", "--message", "")

	changes, err := manager.DescribeChanges(ctx, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !changes.Dirty {
		t.Fatal("worker changes should be dirty")
	}
	if _, err := manager.ApplyChanges(ctx, workspace, changes); err != nil {
		t.Fatal(err)
	}

	workerDescription := strings.TrimSpace(runTestJJ(t, workspace.CWD, "log", "-r", "@", "--no-graph", "-T", "description"))
	if workerDescription != "Worker worker1" {
		t.Fatalf("worker description = %q", workerDescription)
	}
	sourceDescription := strings.TrimSpace(runTestJJ(t, repo, "log", "-r", "@", "--no-graph", "-T", "description"))
	if sourceDescription != "Apply worker worker1" {
		t.Fatalf("source description = %q", sourceDescription)
	}
}

func TestGitWorkspaceManagerCopiesUntrackedBaseCandidate(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	manager := NewGitWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain)

	base, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "base-worker",
		WorkDir:  repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base.CWD, "file.txt"), []byte("base candidate\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base.CWD, "tests", "bench"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base.CWD, "tests", "bench", "serve_ab.ts"), []byte("console.log('bench')\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	followUp, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:      "task",
		WorkerID:    "followup-worker",
		WorkDir:     repo,
		BaseWorkDir: base.CWD,
	})
	if err != nil {
		t.Fatal(err)
	}

	contents, err := os.ReadFile(filepath.Join(followUp.CWD, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "base candidate\n" {
		t.Fatalf("tracked file contents = %q", contents)
	}
	untrackedContents, err := os.ReadFile(filepath.Join(followUp.CWD, "tests", "bench", "serve_ab.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if string(untrackedContents) != "console.log('bench')\n" {
		t.Fatalf("untracked file contents = %q", untrackedContents)
	}
	status := strings.TrimSpace(runTestGit(t, followUp.CWD, "status", "--porcelain=v1"))
	if status != "" {
		t.Fatalf("follow-up workspace status = %q, want clean committed base candidate", status)
	}
	subject := strings.TrimSpace(runTestGit(t, followUp.CWD, "log", "-1", "--pretty=%s"))
	if subject != "Base worker candidate" {
		t.Fatalf("follow-up base commit subject = %q", subject)
	}
}

func TestGitWorkspaceManagerCopiesStagedNewBaseCandidate(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	manager := NewGitWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain)

	base, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "base-worker",
		WorkDir:  repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base.CWD, "file.txt"), []byte("base candidate\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base.CWD, "tools"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base.CWD, "tools", "serve_bench.ts"), []byte("console.log('bench')\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, base.CWD, "add", "tools/serve_bench.ts")

	followUp, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:      "task",
		WorkerID:    "followup-worker",
		WorkDir:     repo,
		BaseWorkDir: base.CWD,
	})
	if err != nil {
		t.Fatal(err)
	}

	contents, err := os.ReadFile(filepath.Join(followUp.CWD, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "base candidate\n" {
		t.Fatalf("tracked file contents = %q", contents)
	}
	stagedContents, err := os.ReadFile(filepath.Join(followUp.CWD, "tools", "serve_bench.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if string(stagedContents) != "console.log('bench')\n" {
		t.Fatalf("staged file contents = %q", stagedContents)
	}
	status := strings.TrimSpace(runTestGit(t, followUp.CWD, "status", "--porcelain=v1"))
	if status != "" {
		t.Fatalf("follow-up workspace status = %q, want clean committed base candidate", status)
	}
}

func TestGitWorkspaceManagerCopiesCommittedBaseCandidateWithDirtyFollowUp(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	manager := NewGitWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain)

	base, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "base-worker",
		WorkDir:  repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base.CWD, ".github", "workflows"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base.CWD, ".github", "workflows", "ci.yml"), []byte("name: CI\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	followUp, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:      "task",
		WorkerID:    "followup-worker",
		WorkDir:     repo,
		BaseWorkDir: base.CWD,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(followUp.CWD, "file.txt"), []byte("follow-up repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	reviewer, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:      "task",
		WorkerID:    "reviewer-worker",
		WorkDir:     repo,
		BaseWorkDir: followUp.CWD,
	})
	if err != nil {
		t.Fatal(err)
	}

	workflow, err := os.ReadFile(filepath.Join(reviewer.CWD, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(workflow) != "name: CI\n" {
		t.Fatalf("workflow contents = %q", workflow)
	}
	contents, err := os.ReadFile(filepath.Join(reviewer.CWD, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "follow-up repair\n" {
		t.Fatalf("file contents = %q", contents)
	}
	status := strings.TrimSpace(runTestGit(t, reviewer.CWD, "status", "--porcelain=v1"))
	if status != "" {
		t.Fatalf("reviewer workspace status = %q, want clean committed base candidate", status)
	}
	nameStatus := strings.TrimSpace(runTestGit(t, reviewer.CWD, "diff", "--name-status", "HEAD~1", "HEAD", "--"))
	if !strings.Contains(nameStatus, "A\t.github/workflows/ci.yml") || !strings.Contains(nameStatus, "M\tfile.txt") {
		t.Fatalf("reviewer base commit changes = %q", nameStatus)
	}
}

func TestGitWorkspaceManagerDescribesCommittedBaseCandidateChanges(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	manager := NewGitWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain)

	base, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "base-worker",
		WorkDir:  repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base.CWD, ".github", "workflows"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base.CWD, ".github", "workflows", "ci.yml"), []byte("name: CI\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	reviewer, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:      "task",
		WorkerID:    "reviewer-worker",
		WorkDir:     repo,
		BaseWorkDir: base.CWD,
	})
	if err != nil {
		t.Fatal(err)
	}
	status := strings.TrimSpace(runTestGit(t, reviewer.CWD, "status", "--porcelain=v1"))
	if status != "" {
		t.Fatalf("reviewer git status = %q, want clean", status)
	}

	changes, err := manager.DescribeChanges(ctx, reviewer)
	if err != nil {
		t.Fatal(err)
	}
	if !changes.Dirty {
		t.Fatal("committed base candidate should be reported as dirty relative to project base")
	}
	if len(changes.ChangedFiles) != 1 || changes.ChangedFiles[0] != (WorkspaceChangedFile{Path: ".github/workflows/ci.yml", Status: "added"}) {
		t.Fatalf("changed files = %+v", changes.ChangedFiles)
	}
	if !strings.Contains(changes.DiffStat, ".github/workflows/ci.yml") {
		t.Fatalf("diff stat = %q", changes.DiffStat)
	}
	diff, err := manager.DescribeDiff(ctx, reviewer)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(diff, "diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml") {
		t.Fatalf("diff does not include committed candidate file:\n%s", diff)
	}
}

func TestGitWorkspaceManagerApplyConflictAbortsSourceMerge(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	runTestGit(t, repo, "config", "user.name", "aged-test")
	runTestGit(t, repo, "config", "user.email", "aged-test@example.invalid")
	runTestGit(t, repo, "config", "commit.gpgsign", "false")
	manager := NewGitWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain)

	workspace, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "worker-conflict",
		WorkDir:  repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.CWD, "file.txt"), []byte("worker\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	changes, err := manager.DescribeChanges(ctx, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", "file.txt")
	runTestGit(t, repo, "commit", "-m", "source")

	_, err = manager.ApplyChanges(ctx, workspace, changes)
	if err == nil {
		t.Fatal("ApplyChanges succeeded; want merge conflict")
	}
	status := strings.TrimSpace(runTestGit(t, repo, "status", "--porcelain=v1"))
	if status != "" {
		t.Fatalf("source status = %q, want clean after failed merge", status)
	}
	contents, err := os.ReadFile(filepath.Join(repo, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "source\n" {
		t.Fatalf("source file contents = %q, want committed source contents", contents)
	}
}

func TestApplyGitPatchAcceptsMissingTrailingNewline(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("worker\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	patch := strings.TrimRight(runTestGit(t, repo, "diff", "--binary", "HEAD", "--", "file.txt"), "\n")
	runTestGit(t, repo, "checkout", "--", "file.txt")

	if err := applyGitPatchToWorkspace(ctx, repo, patch); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(repo, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "worker\n" {
		t.Fatalf("file contents = %q", contents)
	}
}

func TestGitWorkspaceManagerAllowsLegacyAgedStateInSource(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	if err := os.MkdirAll(filepath.Join(repo, ".aged", "workspaces", "legacy"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, ".aged", "workspaces", "legacy", "state"), []byte("old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	manager := NewGitWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain)
	workspace, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "worker-with-legacy-state",
		WorkDir:  repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(workspace.SourceStatus, "?? .aged/") {
		t.Fatalf("source status = %q, want legacy state recorded", workspace.SourceStatus)
	}
	if workspace.SourceDirty {
		t.Fatalf("legacy aged state should not mark source dirty")
	}
}

func TestGitWorkspaceManagerAllowsDirtySourceForIsolatedWorkspace(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("dirty source\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	manager := NewGitWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain)
	workspace, err := manager.Prepare(ctx, WorkspaceSpec{
		TaskID:   "task",
		WorkerID: "worker-dirty-source",
		WorkDir:  repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !workspace.SourceDirty {
		t.Fatalf("dirty source should be recorded")
	}
	if workspace.Dirty {
		t.Fatalf("isolated workspace should start clean")
	}
	contents, err := os.ReadFile(filepath.Join(workspace.CWD, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "base\n" {
		t.Fatalf("isolated workspace contents = %q, want committed source", contents)
	}
}

func TestNativeWorkspaceApplyResultsPreserveMethodAndFiles(t *testing.T) {
	changedFiles := []WorkspaceChangedFile{
		{Path: "internal/orchestrator/workspace.go", Status: "modified"},
		{Path: "web/src/main.tsx", Status: "added"},
	}
	changes := WorkspaceChanges{
		Dirty:        true,
		ChangedFiles: changedFiles,
	}

	tests := []struct {
		name      string
		workspace PreparedWorkspace
		apply     func(context.Context, PreparedWorkspace, WorkspaceChanges) (WorkerApplyResult, error)
		method    string
	}{
		{
			name: "jj",
			workspace: PreparedWorkspace{
				Root:          "/tmp/worker",
				SourceRoot:    "/tmp/source",
				WorkspaceName: "aged-worker",
				Mode:          string(WorkspaceModeShared),
			},
			apply:  NewJJWorkspaceManager(WorkspaceModeIsolated, "", WorkspaceCleanupRetain).ApplyChanges,
			method: "jj_new_merge",
		},
		{
			name: "git",
			workspace: PreparedWorkspace{
				Root:       "/tmp/worker",
				SourceRoot: "/tmp/source",
				Mode:       string(WorkspaceModeShared),
			},
			apply:  NewGitWorkspaceManager(WorkspaceModeIsolated, "", WorkspaceCleanupRetain).ApplyChanges,
			method: "git_commit_merge",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.apply(context.Background(), tt.workspace, changes)
			if err == nil {
				t.Fatalf("expected validation error")
			}
			if result.Method != tt.method {
				t.Fatalf("method = %q, want %q", result.Method, tt.method)
			}
			if len(result.AppliedFiles) != len(changedFiles) {
				t.Fatalf("applied files = %+v", result.AppliedFiles)
			}
			for i := range changedFiles {
				if result.AppliedFiles[i] != changedFiles[i] {
					t.Fatalf("applied files = %+v", result.AppliedFiles)
				}
			}
		})
	}
}

func initGitTestRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repo := t.TempDir()
	runTestGit(t, repo, "init")
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", "file.txt")
	runTestGit(t, repo, "-c", "user.name=aged-test", "-c", "user.email=aged-test@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "base")
	return repo
}

func initJJTestRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("jj"); err != nil {
		t.Skip("jj is not installed")
	}
	repo := t.TempDir()
	runTestJJ(t, repo, "git", "init", "--colocate")
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestJJ(t, repo, "describe", "--message", "base")
	return repo
}

func runTestGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func runTestJJ(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("jj", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("jj %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func TestCleanupDecisionPolicies(t *testing.T) {
	workspace := PreparedWorkspace{
		Root:          "/tmp/workspace",
		CWD:           "/tmp/workspace",
		WorkspaceName: "aged-worker",
		Mode:          string(WorkspaceModeIsolated),
		VCSType:       "git",
	}

	workspace.CleanupPolicy = string(WorkspaceCleanupRetain)
	cleanup, shouldClean := cleanupDecision(workspace, WorkspaceResultSucceeded)
	if shouldClean || cleanup.Cleaned {
		t.Fatalf("retain policy should not clean")
	}

	workspace.CleanupPolicy = string(WorkspaceCleanupDeleteOnSuccess)
	_, shouldClean = cleanupDecision(workspace, WorkspaceResultFailed)
	if shouldClean {
		t.Fatalf("delete_on_success should not clean failed workspaces")
	}
	_, shouldClean = cleanupDecision(workspace, WorkspaceResultSucceeded)
	if !shouldClean {
		t.Fatalf("delete_on_success should clean successful workspaces")
	}

	workspace.CleanupPolicy = string(WorkspaceCleanupDeleteOnTerminal)
	_, shouldClean = cleanupDecision(workspace, WorkspaceResultCanceled)
	if !shouldClean {
		t.Fatalf("delete_on_terminal should clean canceled workspaces")
	}
}
