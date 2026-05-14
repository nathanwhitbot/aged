package orchestrator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"
)

func TestServiceUsesBrainSelectedWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &recordingRunner{kind: "chosen"}
	workspaces := fakeWorkspaceManager{cwd: t.TempDir()}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "chosen",
		Prompt:     "worker prompt from brain",
		Rationale:  "test brain chose this worker",
		Steps:      []PlanStep{{Title: "Run", Description: "Execute"}},
	}}, map[string]worker.Runner{"chosen": runner}, t.TempDir(), workspaces)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !strings.Contains(runner.prompt, "worker prompt from brain") {
		t.Fatalf("runner prompt = %q", runner.prompt)
	}
	if !strings.Contains(runner.prompt, "Run every command from this execution workspace:\n"+workspaces.cwd) {
		t.Fatalf("runner prompt did not include execution workspace: %q", runner.prompt)
	}
	if runner.workDir != workspaces.cwd {
		t.Fatalf("runner workDir = %q, want %q", runner.workDir, workspaces.cwd)
	}
	if !hasEvent(snapshot.Events, core.EventTaskPlanned, task.ID, "") {
		t.Fatalf("missing task.planned event")
	}
	if !hasEvent(snapshot.Events, core.EventWorkerWorkspace, task.ID, "") {
		t.Fatalf("missing worker.workspace_prepared event")
	}
	if !hasEvent(snapshot.Events, core.EventWorkerCleanup, task.ID, "") {
		t.Fatalf("missing worker.workspace_cleaned event")
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "chosen") {
		t.Fatalf("missing worker.created with chosen kind")
	}
	if len(snapshot.Workers) != 1 {
		t.Fatalf("workers = %d, want 1", len(snapshot.Workers))
	}
	if snapshot.Workers[0].Prompt != runner.prompt {
		t.Fatalf("snapshot worker prompt = %q, want runner prompt %q", snapshot.Workers[0].Prompt, runner.prompt)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "prompt", runner.prompt) {
		t.Fatalf("missing worker.created prompt")
	}
}

func TestProjectHealthCatchesGitHubGraphQLBadCredentials(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	fakeBin := t.TempDir()
	ghPath := filepath.Join(fakeBin, "gh")
	script := `#!/bin/sh
if [ "$1" = "auth" ] && [ "$2" = "status" ]; then
  echo "github.com"
  exit 0
fi
cat >&2 <<'JSON'
{"message":"Bad credentials","documentation_url":"https://docs.github.com/rest","status":"401"}
JSON
exit 1
`
	if err := os.WriteFile(ghPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	projectDir := t.TempDir()
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "do it"}}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if _, err := service.CreateProject(ctx, core.Project{
		ID:          "repo",
		LocalPath:   projectDir,
		Repo:        "fork-owner/repo",
		DefaultBase: "main",
	}); err != nil {
		t.Fatal(err)
	}

	health, err := service.ProjectHealth(ctx, "repo")
	if err != nil {
		t.Fatal(err)
	}
	if health.OK {
		t.Fatalf("health OK = true, want false: %+v", health)
	}
	if health.GitHubStatus != "auth_bad_credentials" {
		t.Fatalf("github status = %q, want auth_bad_credentials; errors=%v", health.GitHubStatus, health.Errors)
	}
	if !strings.Contains(strings.Join(health.Errors, "\n"), "GitHub credentials rejected (401 Bad credentials)") {
		t.Fatalf("health errors = %v", health.Errors)
	}
}

func TestServicePassesReasoningEffortToWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &recordingRunner{kind: "codex"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind:      "codex",
		Prompt:          "worker prompt",
		ReasoningEffort: "low",
	}}, map[string]worker.Runner{"codex": runner}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Cheap worker",
		Prompt: "Use a cheap effort level.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if runner.reasoningEffort != "low" {
		t.Fatalf("reasoning effort = %q, want low", runner.reasoningEffort)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "reasoningEffort", "low") {
		t.Fatalf("missing reasoning effort metadata in worker.created")
	}
}

func TestApplyRemotePatchConflictDoesNotDirtySource(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("worker\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	patch := runTestGit(t, repo, "diff", "--binary", "HEAD", "--", "file.txt")
	runTestGit(t, repo, "checkout", "--", "file.txt")
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", "file.txt")
	runTestGit(t, repo, "-c", "user.name=aged-test", "-c", "user.email=aged-test@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "source")

	_, err := applyRemotePatch(ctx, core.Project{LocalPath: repo}, PreparedWorkspace{WorkerID: "remote-worker"}, WorkspaceChanges{
		Diff:  patch,
		Dirty: true,
		ChangedFiles: []WorkspaceChangedFile{{
			Path:   "file.txt",
			Status: "modified",
		}},
	})
	if err == nil {
		t.Fatal("applyRemotePatch succeeded; want conflict")
	}
	status := strings.TrimSpace(runTestGit(t, repo, "status", "--porcelain=v1"))
	if status != "" {
		t.Fatalf("source status = %q, want clean after failed remote apply", status)
	}
	contents, err := os.ReadFile(filepath.Join(repo, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "source\n" {
		t.Fatalf("source file contents = %q, want committed source contents", contents)
	}
}

func TestBaseWorkspaceSpecUsesGitBaseWorkerRecordedBaseChange(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()
	service := NewServiceWithWorkspaceManager(store, StaticBrain{}, nil, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   "task",
		WorkerID: "base-worker",
		Payload: core.MustJSON(PreparedWorkspace{
			CWD:        "/tmp/base-worker",
			VCSType:    "git",
			BaseChange: "base-worker-start",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	spec, err := service.baseWorkspaceSpec(ctx, WorkspaceSpec{
		TaskID:       "task",
		WorkerID:     "followup-worker",
		BaseRevision: "current-project-head",
	}, "base-worker")
	if err != nil {
		t.Fatal(err)
	}
	if spec.BaseWorkDir != "/tmp/base-worker" {
		t.Fatalf("base workdir = %q", spec.BaseWorkDir)
	}
	if spec.BaseRevision != "base-worker-start" {
		t.Fatalf("base revision = %q, want recorded base change", spec.BaseRevision)
	}
}

func TestServiceDedupesExternalSourceTasks(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	req := core.CreateTaskRequest{
		Title:      "GitHub issue owner/repo#123",
		Prompt:     "Fix the issue.",
		Source:     "github",
		ExternalID: "owner/repo#123",
		Metadata:   core.MustJSON(map[string]any{"repo": "owner/repo", "issue": 123}),
	}
	first, err := service.CreateTask(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.CreateTask(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Fatalf("duplicate task id = %s, want %s", second.ID, first.ID)
	}
	snapshot := waitForTaskStatus(t, store, first.ID, core.TaskSucceeded)
	if countEvents(snapshot.Events, core.EventTaskCreated, first.ID) != 1 {
		t.Fatalf("task.created count = %d, want 1", countEvents(snapshot.Events, core.EventTaskCreated, first.ID))
	}
	found, ok, err := service.FindTaskByExternalID(ctx, "github", "owner/repo#123")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || found.ID != first.ID {
		t.Fatalf("lookup = %+v ok=%v", found, ok)
	}
}

func TestServiceAssistantRecordsQuestionAndAnswer(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := fixedAssistantBrain{
		fixedBrain: fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "unused"}},
		answer:     "Use a worker task for code changes.",
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	response, err := service.Ask(ctx, core.AssistantRequest{Message: "Can you open PRs?"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Message != brain.answer {
		t.Fatalf("answer = %q", response.Message)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventAssistantAsked, "") != 1 {
		t.Fatalf("assistant.asked count = %d, want 1", countEvents(snapshot.Events, core.EventAssistantAsked, ""))
	}
	if countEvents(snapshot.Events, core.EventAssistantAnswered, "") != 1 {
		t.Fatalf("assistant.answered count = %d, want 1", countEvents(snapshot.Events, core.EventAssistantAnswered, ""))
	}
}

func TestServiceResumesAssistantProviderSession(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	assistant := &recordingAssistant{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "unused"}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetAssistant(assistant)

	first, err := service.Ask(ctx, core.AssistantRequest{ConversationID: "c1", Message: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if first.ProviderSessionID != "session-1" {
		t.Fatalf("first session = %q", first.ProviderSessionID)
	}
	second, err := service.Ask(ctx, core.AssistantRequest{ConversationID: "c1", Message: "again"})
	if err != nil {
		t.Fatal(err)
	}
	if second.ProviderSessionID != "session-1" {
		t.Fatalf("second session = %q", second.ProviderSessionID)
	}
	if len(assistant.requests) != 2 || assistant.requests[1].ProviderSessionID != "session-1" {
		t.Fatalf("assistant requests = %+v", assistant.requests)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventAssistantAnswered, "") != 2 {
		t.Fatalf("assistant.answered count = %d, want 2", countEvents(snapshot.Events, core.EventAssistantAnswered, ""))
	}
}

func TestServiceGeneratesMissingTaskTitle(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetTitleGenerator(fakeTitleGenerator{title: "Generated Parser Title"})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Prompt: "implement parser retries when the upstream endpoint times out",
	})
	if err != nil {
		t.Fatal(err)
	}
	if task.Title != "Generated Parser Title" {
		t.Fatalf("title = %q", task.Title)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if snapshot.Tasks[0].Title != "Generated Parser Title" {
		t.Fatalf("snapshot title = %q", snapshot.Tasks[0].Title)
	}
	var metadata map[string]any
	if err := json.Unmarshal(snapshot.Tasks[0].Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["titleGenerated"] != true {
		t.Fatalf("metadata = %+v", metadata)
	}
}

func TestNormalizeCreateTaskRequestDefaultsCompletionModeToGitHubAndPreservesExplicitLocal(t *testing.T) {
	tests := []struct {
		name   string
		prompt string
		want   string
	}{
		{
			name:   "default work prompt",
			prompt: "Take a look at TODOs in the code and fix them.",
			want:   "github",
		},
		{
			name:   "no problem is not no pr",
			prompt: "No problem, fix this.",
			want:   "github",
		},
		{
			name:   "explicit no pr",
			prompt: "Fix this, no PR.",
			want:   "local",
		},
		{
			name:   "explicit local only",
			prompt: "Fix this local-only.",
			want:   "local",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req, err := NormalizeCreateTaskRequest(core.CreateTaskRequest{
				Title:  "Fix TODOs",
				Prompt: test.prompt,
			})
			if err != nil {
				t.Fatal(err)
			}
			var metadata map[string]any
			if err := json.Unmarshal(req.Metadata, &metadata); err != nil {
				t.Fatal(err)
			}
			if metadata["completionMode"] != test.want {
				t.Fatalf("metadata = %+v, want completionMode %q", metadata, test.want)
			}
		})
	}

	explicitLocalReq, err := NormalizeCreateTaskRequest(core.CreateTaskRequest{
		Title:  "Fix TODOs",
		Prompt: "Take a look at TODOs in the code and fix them.",
		Metadata: core.MustJSON(map[string]any{
			"completionMode": "local",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(explicitLocalReq.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["completionMode"] != "local" {
		t.Fatalf("explicit local metadata = %+v", metadata)
	}

	loopReq, err := NormalizeCreateTaskRequest(core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Keep making bounded progress.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode": "loop",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	var loopMetadata map[string]any
	if err := json.Unmarshal(loopReq.Metadata, &loopMetadata); err != nil {
		t.Fatal(err)
	}
	if _, ok := loopMetadata["completionMode"]; ok {
		t.Fatalf("loop metadata should not default completionMode: %+v", loopMetadata)
	}
}

func TestServiceFallsBackWhenTitleGeneratorFails(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetTitleGenerator(fakeTitleGenerator{err: errors.New("model unavailable")})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Prompt: "implement parser retries when upstream endpoint times out",
	})
	if err != nil {
		t.Fatal(err)
	}
	if task.Title != "implement parser retries when upstream endpoint" {
		t.Fatalf("title = %q", task.Title)
	}
}

func TestServicePublishesPullRequestAfterApplyingSingleWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	applyCalls := 0
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: fixedBrain{plan: Plan{WorkerKind: "change", Prompt: "make change"}},
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
		applyCalls: &applyCalls,
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Implement feature", Prompt: "Do it."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)

	pr, err := service.PublishTaskPullRequest(ctx, task.ID, core.PublishPullRequestRequest{
		Repo: "owner/repo",
		Base: "main",
	})
	if err != nil {
		t.Fatal(err)
	}
	if applyCalls != 0 {
		t.Fatalf("apply calls = %d, want 0", applyCalls)
	}
	if pr.URL == "" || pr.Repo != "owner/repo" {
		t.Fatalf("pr = %+v", pr)
	}
	if publisher.published.WorkerID == "" {
		t.Fatalf("publisher worker id was empty")
	}

	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
	if hasEvent(snapshot.Events, core.EventWorkerApplied, task.ID, publisher.published.WorkerID) {
		t.Fatalf("worker was applied during PR publish")
	}
	if !hasEvent(snapshot.Events, core.EventPRPublished, task.ID, "") {
		t.Fatalf("missing pr published event")
	}
}

func TestServiceGitHubCompletionModePublishesFinalCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	applyCalls := 0
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, ".github"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, ".github", "pull_request_template.md"), []byte("## Repo checklist\n- [ ] Tests pass\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain:      fixedBrain{plan: Plan{WorkerKind: "change", Prompt: "make change"}},
		workDir:    workspace,
		cwd:        workspace,
		sourceRoot: workspace,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
		applyCalls: &applyCalls,
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Implement feature",
		Prompt:   "Do it.",
		Metadata: core.MustJSON(map[string]any{"completionMode": "github"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForPullRequests(t, store, task.ID, 1)
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
	if snapshot.PullRequests[0].BabysitterTaskID != task.ID {
		t.Fatalf("babysitter task id = %q, want %q", snapshot.PullRequests[0].BabysitterTaskID, task.ID)
	}
	task = snapshot.Tasks[0]
	if task.Status != core.TaskWaiting {
		t.Fatalf("task status = %q, want waiting while PR is open", task.Status)
	}
	if task.ObjectiveStatus != core.ObjectiveWaitingExternal || task.ObjectivePhase != "pr_opened" {
		t.Fatalf("objective = %q phase %q", task.ObjectiveStatus, task.ObjectivePhase)
	}
	if task.FinalCandidateWorkerID == "" || publisher.published.WorkerID != task.FinalCandidateWorkerID {
		t.Fatalf("published worker = %q, final candidate = %q", publisher.published.WorkerID, task.FinalCandidateWorkerID)
	}
	if len(task.Artifacts) != 1 || task.Artifacts[0].Kind != "github_pull_request" {
		t.Fatalf("artifacts = %+v", task.Artifacts)
	}
	if !hasMilestone(task.Milestones, "candidate_ready") || !hasMilestone(task.Milestones, "pr_opened") {
		t.Fatalf("milestones = %+v", task.Milestones)
	}
	if publisher.published.Body != "" {
		t.Fatalf("completion-mode publish invented a pull request body:\n%s", publisher.published.Body)
	}
	if applyCalls != 0 {
		t.Fatalf("apply calls = %d, want 0", applyCalls)
	}
}

func TestServiceGitHubCompletionModeUsesReplanPullRequestBody(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "make change",
		},
		decisions: []ReplanDecision{{
			Action:          "complete",
			Rationale:       "worker completed the remote follow-up",
			PullRequestBody: "## Summary\n- Implemented the remote follow-up.\n\n## Validation\n- go test ./...",
		}},
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: brain,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Remote follow-up",
		Prompt: "Implement the remote follow-up.",
		Metadata: core.MustJSON(map[string]any{
			"completionMode": "github",
			"source":         "remote-worker",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if !strings.Contains(publisher.published.Body, "Implemented the remote follow-up.") {
		t.Fatalf("published body = %q, want replan-authored PR body", publisher.published.Body)
	}
}

func TestServiceGitHubIssueCompletionModeAppendsClosingReferenceToPullRequestBody(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "make change",
		},
		decisions: []ReplanDecision{{
			Action:          "complete",
			Rationale:       "worker completed the GitHub issue task",
			PullRequestBody: "## Summary\n- Implemented the issue fix.",
		}},
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: brain,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:      "GitHub issue owner/repo#12: Fix parser",
		Prompt:     "Work on GitHub issue owner/repo#12.",
		Source:     "github-issue",
		ExternalID: "owner/repo#12",
		Metadata:   core.MustJSON(map[string]any{"completionMode": "github"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if !strings.Contains(publisher.published.Body, "Implemented the issue fix.") {
		t.Fatalf("published body = %q, want replan-authored PR body", publisher.published.Body)
	}
	if !strings.Contains(publisher.published.Body, "Closes owner/repo#12") {
		t.Fatalf("published body = %q, want GitHub issue closing reference", publisher.published.Body)
	}
}

func TestPullRequestBodyWithIssueClosingReferenceDoesNotDuplicateExistingClosingKeyword(t *testing.T) {
	task := core.Task{
		Metadata: core.MustJSON(map[string]any{
			"source":     "github-issue",
			"externalId": "owner/repo#12",
		}),
	}
	tests := []struct {
		name        string
		body        string
		publishRepo string
		want        string
	}{
		{
			name:        "same repo shorthand fix",
			body:        "## Summary\n- Fixed it.\n\nFixes #12",
			publishRepo: "owner/repo",
			want:        "## Summary\n- Fixed it.\n\nFixes #12",
		},
		{
			name:        "qualified closes",
			body:        "## Summary\n- Fixed it.\n\nCloses owner/repo#12",
			publishRepo: "owner/repo",
			want:        "## Summary\n- Fixed it.\n\nCloses owner/repo#12",
		},
		{
			name:        "issue url resolves",
			body:        "## Summary\n- Fixed it.\n\nResolves https://github.com/owner/repo/issues/12",
			publishRepo: "owner/repo",
			want:        "## Summary\n- Fixed it.\n\nResolves https://github.com/owner/repo/issues/12",
		},
		{
			name:        "plain issue mention is not enough",
			body:        "## Summary\n- See #12 for context.",
			publishRepo: "owner/repo",
			want:        "## Summary\n- See #12 for context.\n\nCloses owner/repo#12",
		},
		{
			name:        "different issue number is not enough",
			body:        "## Summary\n- Fixed another issue.\n\nFixes owner/repo#120",
			publishRepo: "owner/repo",
			want:        "## Summary\n- Fixed another issue.\n\nFixes owner/repo#120\n\nCloses owner/repo#12",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pullRequestBodyWithIssueClosingReference(tc.body, task, tc.publishRepo)
			if got != tc.want {
				t.Fatalf("body = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestServiceCanceledFollowUpDoesNotPublishPullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan string, 1)
	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "make change",
		Spawns: []SpawnRequest{{
			ID:         "review",
			Role:       "reviewer",
			Reason:     "Review before publishing.",
			WorkerKind: "review",
		}},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"review": &blockingEventRunner{kind: "review", started: started},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Implement feature",
		Prompt:   "Do it.",
		Metadata: core.MustJSON(map[string]any{"completionMode": "github"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reviewWorkerID := ""
	for _, candidate := range snapshot.Workers {
		if candidate.TaskID == task.ID && candidate.Kind == "review" && candidate.Status == core.WorkerRunning {
			reviewWorkerID = candidate.ID
			break
		}
	}
	if reviewWorkerID == "" {
		t.Fatalf("missing running review worker: %+v", snapshot.Workers)
	}
	if err := service.CancelWorker(ctx, reviewWorkerID); err != nil {
		t.Fatal(err)
	}
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskCanceled)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want 0", publisher.publishCalls)
	}
	if len(snapshot.PullRequests) != 0 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
}

func TestServiceGitHubCompletionModeRepairsPublishConflict(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	applyCalls := 0
	publisher := &fakePullRequestPublisher{
		errOnce: errors.New("remote patch has conflicts or no longer applies cleanly; patch does not apply"),
	}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "make change",
		},
		decisions: []ReplanDecision{{
			Action:    "complete",
			Rationale: "initial candidate is ready",
		}, {
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "change",
				Prompt:     "repair publish conflict against current checkout",
			},
		}, {
			Action:    "complete",
			Rationale: "repair worker is final",
		}},
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain:     brain,
		publisher: publisher,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "web/src/main.tsx", Status: "modified"}},
		},
		applyCalls: &applyCalls,
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Collapsible Project Add Dialog",
		Prompt:   "Make the project add dialog collapsible.",
		Metadata: core.MustJSON(map[string]any{"completionMode": "github"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForPullRequests(t, store, task.ID, 1)
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
	if !replanStatesContainResultError(brain.states, "completion publish failed") {
		t.Fatalf("replan states did not include publish failure context: %+v", brain.states)
	}
	if publisher.publishCalls != 2 {
		t.Fatalf("publish calls = %d, want 2", publisher.publishCalls)
	}
	if len(publisher.publishedWorkers) != 2 || publisher.publishedWorkers[0] == publisher.publishedWorkers[1] {
		t.Fatalf("published workers = %+v, want original then repair", publisher.publishedWorkers)
	}
	task = snapshot.Tasks[0]
	if task.FinalCandidateWorkerID != publisher.publishedWorkers[1] {
		t.Fatalf("final candidate = %q, published repair = %q", task.FinalCandidateWorkerID, publisher.publishedWorkers[1])
	}
	if task.Status != core.TaskWaiting || task.Error != "" {
		t.Fatalf("task status/error = %q/%q", task.Status, task.Error)
	}
	if applyCalls != 0 {
		t.Fatalf("apply calls = %d, want 0 for local PR publish", applyCalls)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "completion_publish_recovery", "completed") {
		t.Fatalf("missing completed publish recovery action")
	}
}

func TestServiceGitHubCompletionModeRejectsSameCandidateAfterPublishConflict(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{
		errOnce: errors.New("remote patch has conflicts or no longer applies cleanly; patch does not apply"),
	}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "make change",
		},
		decisions: []ReplanDecision{{
			Action:    "complete",
			Rationale: "initial candidate is ready",
		}, {
			Action:    "complete",
			Rationale: "same candidate is still ready",
		}, {
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "change",
				Prompt:     "repair publish conflict against current checkout",
			},
		}, {
			Action:    "complete",
			Rationale: "repair worker is final",
		}},
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain:     brain,
		publisher: publisher,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "web/src/main.tsx", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Collapsible Project Add Dialog",
		Prompt:   "Make the project add dialog collapsible.",
		Metadata: core.MustJSON(map[string]any{"completionMode": "github"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 2 {
		t.Fatalf("publish calls = %d, want original failure then repaired publish", publisher.publishCalls)
	}
	if len(publisher.publishedWorkers) != 2 || publisher.publishedWorkers[0] == publisher.publishedWorkers[1] {
		t.Fatalf("published workers = %+v, want blocked original then new repair", publisher.publishedWorkers)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "replan_completion_rejected", "rejected") {
		t.Fatalf("missing rejected same-candidate completion action")
	}
	if !hasTaskAction(snapshot.Events, task.ID, "completion_publish_recovery", "completed") {
		t.Fatalf("missing completed publish recovery action")
	}
	if len(brain.states) < 3 {
		t.Fatalf("replan states = %d, want initial plus recovery turns", len(brain.states))
	}
	recoveryState := brain.states[1]
	if len(recoveryState.BlockedFinalCandidateIDs) != 1 || recoveryState.BlockedFinalCandidateIDs[0] != publisher.publishedWorkers[0] {
		t.Fatalf("blocked final candidates = %+v, want %q", recoveryState.BlockedFinalCandidateIDs, publisher.publishedWorkers[0])
	}
	if recoveryState.RecoveryHint == "" {
		t.Fatalf("missing recovery hint")
	}
}

func TestServiceGitHubCompletionModeRepeatsPublishRecoveryForNewConflictingCandidates(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{errCount: 2}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "make change",
		},
		decisions: []ReplanDecision{{
			Action:    "complete",
			Rationale: "initial candidate is ready",
		}, {
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "change",
				Prompt:     "repair first publish conflict against current checkout",
			},
		}, {
			Action:    "complete",
			Rationale: "first repair is final",
		}, {
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "change",
				Prompt:     "review the repaired candidate one more time",
				Spawns: []SpawnRequest{{
					ID:         "post-repair-review",
					Role:       "Final regression reviewer",
					Reason:     "review the repaired candidate before completion",
					WorkerKind: "change",
				}},
			},
		}, {
			Action:    "complete",
			Rationale: "second repair is final",
		}},
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain:     brain,
		publisher: publisher,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Improve Long-Term Planning Intelligence",
		Prompt:   "Improve long-term planning intelligence.",
		Metadata: core.MustJSON(map[string]any{"completionMode": "github"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 3 {
		t.Fatalf("publish calls = %d, want two failed publishes then success", publisher.publishCalls)
	}
	if len(publisher.publishedWorkers) != 3 ||
		publisher.publishedWorkers[0] == publisher.publishedWorkers[1] ||
		publisher.publishedWorkers[1] == publisher.publishedWorkers[2] {
		t.Fatalf("published workers = %+v, want distinct candidates", publisher.publishedWorkers)
	}
	task = snapshot.Tasks[0]
	if task.Error != "" || task.FinalCandidateWorkerID != publisher.publishedWorkers[2] {
		t.Fatalf("task = %+v, published workers = %+v", task, publisher.publishedWorkers)
	}
	if got := countTaskActions(snapshot.Events, task.ID, "completion_publish_recovery", "completed"); got != 2 {
		t.Fatalf("completed publish recoveries = %d, want 2", got)
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskPlanned, task.ID, `"forcedConflictRepair":true`) {
		t.Fatalf("missing forced conflict repair metadata")
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskPlanned, task.ID, `"workspaceReusePolicy":"fresh"`) {
		t.Fatalf("missing fresh recovery workspace policy")
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskPlanned, task.ID, "Your only job in this turn is to produce a new candidate") {
		t.Fatalf("missing forced conflict repair prompt")
	}
	if eventPayloadContains(snapshot.Events, core.EventWorkerCreated, task.ID, `"spawnID":"post-repair-review"`) {
		t.Fatalf("finalization recovery should not run follow-up review spawns")
	}
	if len(brain.states) < 4 {
		t.Fatalf("replan states = %d, want initial plus two recovery replans", len(brain.states))
	}
	secondRecoveryState := brain.states[3]
	if got := secondRecoveryState.BlockedFinalCandidateIDs; len(got) != 2 {
		t.Fatalf("second recovery blocked final candidates = %+v, want two", got)
	}
}

func TestServicePublishRecoveryDoesNotRepublishBlockedCandidateAfterReplanError(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{
		errOnce: errors.New("remote patch has conflicts or no longer applies cleanly; patch does not apply"),
	}
	brain := &errorReplanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "make change",
		},
		err: errors.New("turn/start failed: Input exceeds the maximum length"),
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain:     brain,
		publisher: publisher,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Improve Long-Term Planning Intelligence",
		Prompt:   "Improve long-term planning intelligence.",
		Metadata: core.MustJSON(map[string]any{"completionMode": "github"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want only original failed publish", publisher.publishCalls)
	}
	task = snapshot.Tasks[0]
	if task.Error != "" {
		t.Fatalf("task error = %q, want waiting without fatal error", task.Error)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "completion_publish_recovery", "started") {
		t.Fatalf("missing started publish recovery action")
	}
	if !hasEvent(snapshot.Events, core.EventApprovalNeeded, task.ID, "") {
		t.Fatalf("missing dynamic replan error approval")
	}
}

func TestServiceUsesCompletionReviewBeforePublishingFinalCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	baseBrain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "produce the first intermediate result for an ongoing investigation",
		},
		decisions: []ReplanDecision{{
			Action:    "complete",
			Rationale: "Candidate is useful but only an intermediate artifact.",
		}, {
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "change",
				Prompt:     "continue toward a final result that satisfies the whole objective",
			},
		}, {
			Action:  "wait",
			Message: "continuing ongoing investigation",
		}},
	}
	brain := &completionReviewBrain{
		BrainProvider:  baseBrain,
		ReplanProvider: baseBrain,
		reviews: []CompletionReview{{
			Ready:  false,
			Reason: "the selected candidate is only an intermediate artifact for this open-ended task",
		}},
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: brain,
		runners: map[string]worker.Runner{
			"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "intermediate artifact produced; more work remains"}}},
		},
		changes: WorkspaceChanges{
			Dirty: true,
			ChangedFiles: []WorkspaceChangedFile{
				{Path: "tools/investigation_notes.md", Status: "added"},
			},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Improve Subsystem",
		Prompt: "Keep investigating possible improvements over multiple worker turns and open intermediate PRs when useful.",
		Metadata: core.MustJSON(map[string]any{
			"completionMode": "github",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want 0", publisher.publishCalls)
	}
	if brain.reviewCalls != 1 {
		t.Fatalf("review calls = %d, want 1", brain.reviewCalls)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "completion_publish_readiness_recovery", "started") {
		t.Fatalf("missing completion readiness recovery action")
	}
	if len(baseBrain.states) < 2 {
		t.Fatalf("replan states = %d, want rejection then continuation", len(baseBrain.states))
	}
	if got := baseBrain.states[1].BlockedFinalCandidateIDs; len(got) != 1 {
		t.Fatalf("blocked final candidates after rejection = %+v, want one blocked candidate", got)
	}
	if baseBrain.states[1].RecoveryHint == "" {
		t.Fatalf("missing recovery hint after rejected completion")
	}
}

func TestServiceCompletionReadinessRecoveryAcceptsValidationWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &completionValidationBrain{}
	publisher := &fakePullRequestPublisher{}
	workspaces := &sequencingWorkspaceManager{
		fakeWorkspaceManager: fakeWorkspaceManager{
			cwd:        t.TempDir(),
			sourceRoot: t.TempDir(),
		},
		changes: []WorkspaceChanges{
			{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
			},
			{},
		},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"change":   eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented cancellation fix"}}},
		"validate": eventRunner{kind: "validate", events: []worker.Event{{Kind: worker.EventResult, Text: "validated existing candidate without changes"}}},
	}, t.TempDir(), workspaces)
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Harden task cancellation",
		Prompt: "Fix cancellation and validate it.",
		Metadata: core.MustJSON(map[string]any{
			"completionMode": "github",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1; events: %s", publisher.publishCalls, taskEventSummary(snapshot.Events, task.ID))
	}
	if len(brain.states) < 3 {
		t.Fatalf("replan states = %d, want initial completion, validation, and completion", len(brain.states))
	}
	if got := brain.states[1].BlockedFinalCandidateIDs; len(got) != 1 || got[0] != publisher.publishedWorkers[0] {
		t.Fatalf("blocked candidates during validation = %+v, published = %+v", got, publisher.publishedWorkers)
	}
	if task := snapshot.Tasks[0]; task.FinalCandidateWorkerID != publisher.publishedWorkers[0] {
		t.Fatalf("final candidate = %q, published worker = %q", task.FinalCandidateWorkerID, publisher.publishedWorkers[0])
	}
	if hasEvent(snapshot.Events, core.EventApprovalNeeded, task.ID, "") {
		t.Fatalf("completion readiness recovery asked for approval instead of accepting validation")
	}
}

func TestValidatesBlockedCandidateRequiresLineageAndOrdering(t *testing.T) {
	results := []WorkerTurnResult{
		{
			WorkerID: "blocked-impl",
			Status:   core.WorkerSucceeded,
			Changes: WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
			},
		},
		{
			WorkerID: "unrelated-validation",
			Status:   core.WorkerSucceeded,
			Changes: WorkspaceChanges{
				DiffStat: "0 files changed, 0 insertions(+), 0 deletions(-)",
			},
		},
		{
			WorkerID:     "related-validation",
			BaseWorkerID: "blocked-impl",
			Status:       core.WorkerSucceeded,
			Changes: WorkspaceChanges{
				DiffStat: "0 files changed, 0 insertions(+), 0 deletions(-)",
			},
		},
	}

	if validatesBlockedCandidate(results, "unrelated-validation", "blocked-impl") {
		t.Fatalf("unrelated no-change worker validated blocked candidate without lineage")
	}
	if !validatesBlockedCandidate(results, "related-validation", "blocked-impl") {
		t.Fatalf("related no-change worker did not validate blocked candidate through BaseWorkerID lineage")
	}

	outOfOrder := append([]WorkerTurnResult{results[2]}, results[:2]...)
	if validatesBlockedCandidate(outOfOrder, "related-validation", "blocked-impl") {
		t.Fatalf("out-of-order no-change worker validated blocked candidate")
	}
}

func TestServicePublishesCompletionWhenCompletionReviewApprovesCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	baseBrain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "improve planner behavior for performance optimization work",
		},
		decisions: []ReplanDecision{{
			Action:    "complete",
			Rationale: "The candidate changes planner source code and adds regression coverage for performance-oriented planning decisions.",
		}},
	}
	brain := &completionReviewBrain{
		BrainProvider:  baseBrain,
		ReplanProvider: baseBrain,
		reviews: []CompletionReview{{
			Ready:  true,
			Reason: "candidate satisfies the bounded task objective",
		}},
	}
	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: brain,
		runners: map[string]worker.Runner{
			"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "planner source changes with tests"}}},
		},
		changes: WorkspaceChanges{
			Dirty: true,
			ChangedFiles: []WorkspaceChangedFile{
				{Path: "internal/orchestrator/service.go", Status: "modified"},
				{Path: "internal/orchestrator/service_test.go", Status: "modified"},
			},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Improve Long-Term Planning Intelligence",
		Prompt: "Review aged for opportunities to improve intelligence of planning of longer term or more complex tasks. Particularly interested in performance optimization finding. Make improvements if you can.",
		Metadata: core.MustJSON(map[string]any{
			"completionMode": "github",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want source candidate to publish", publisher.publishCalls)
	}
	if brain.reviewCalls != 1 {
		t.Fatalf("review calls = %d, want 1", brain.reviewCalls)
	}
	if hasTaskAction(snapshot.Events, task.ID, "replan_completion_rejected", "rejected") {
		t.Fatalf("source candidate completion was incorrectly rejected")
	}
}

func TestServicePlanActionPublishesIntermediatePullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service, publisher := newPRPublishingService(t, store, prPublishingServiceOptions{
		brain: fixedBrain{plan: Plan{
			WorkerKind: "change",
			Prompt:     "make change",
			Actions: []PlanAction{{
				Kind:   "publish_pull_request",
				When:   "after_success",
				Reason: "open a PR so review can happen while the objective continues",
				Inputs: map[string]any{
					"repo": "owner/repo",
					"base": "main",
					"body": "## Summary\n- Implement feature.\n\n## Validation\n- Worker completed successfully.",
				},
			}},
		}},
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Implement feature", Prompt: "Do it, open a PR, and babysit it."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForEvent(t, store, core.EventTaskArtifact, task.ID)
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status != core.TaskWaiting || task.ObjectiveStatus != core.ObjectiveWaitingExternal {
		t.Fatalf("task status = %q objective = %q", task.Status, task.ObjectiveStatus)
	}
	if !hasEvent(snapshot.Events, core.EventTaskAction, task.ID, "") {
		t.Fatalf("missing task action event")
	}
	if publisher.published.WorkerID == "" || publisher.published.WorkDir != taskWorkspaceCWD(snapshot, task.ID) {
		t.Fatalf("published from wrong worker workspace: %+v", publisher.published)
	}
	if !strings.Contains(publisher.published.Body, "Implement feature.") {
		t.Fatalf("publish action body was not forwarded: %+v", publisher.published)
	}
}

func TestServicePlanActionPublishWithoutCandidateWaitsForUser(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "inspect",
		Prompt:     "inspect the workspace",
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "publish a fix if the worker changed code",
			Inputs: map[string]any{
				"repo":  "denoland/deno",
				"base":  "main",
				"title": "fix(dx): preserve skill dotfiles",
				"body":  "Fixes denoland/deno#33922.",
			},
		}},
	}}, map[string]worker.Runner{
		"inspect": eventRunner{kind: "inspect", events: []worker.Event{{Kind: worker.EventResult, Text: "The execution workspace is nathanwhit/aged, not denoland/deno. No Deno sources are present."}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Fix Deno Issue 33922",
		Prompt: "reproduce and fix denoland/deno#33922",
		Metadata: core.MustJSON(map[string]any{
			"completionMode": "github",
			"projectId":      "default",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.ObjectiveStatus != core.ObjectiveWaitingUser || task.ObjectivePhase != "approval_needed" {
		t.Fatalf("objective = %q/%q, want waiting user approval", task.ObjectiveStatus, task.ObjectivePhase)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want none without candidate", publisher.publishCalls)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "waiting") {
		t.Fatalf("missing waiting publish_pull_request action")
	}
	if !eventPayloadContains(snapshot.Events, core.EventApprovalNeeded, task.ID, "missing_publish_candidate") {
		t.Fatalf("missing actionable approval-needed event")
	}
	if !eventPayloadContains(snapshot.Events, core.EventApprovalNeeded, task.ID, "not denoland/deno") {
		t.Fatalf("approval-needed event did not include worker blocker summary")
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskStatus, task.ID, `"status":"failed"`) {
		t.Fatalf("task failed instead of waiting for user action")
	}
}

func TestServicePlanActionAdoptsWorkerCreatedPullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: core.PullRequest{
		Repo:         "owner/repo",
		Number:       61,
		URL:          "https://github.com/owner/repo/pull/61",
		Branch:       "codex/ssh-checkout-root-health",
		Base:         "main",
		Title:        "Avoid SSH targets with invalid checkout roots",
		State:        "OPEN",
		ChecksStatus: "pending",
		MergeStatus:  "UNKNOWN",
		ReviewStatus: "REVIEW_REQUIRED",
	}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "make change",
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "open a PR so review can happen while the objective continues",
			Inputs: map[string]any{"repo": "owner/repo", "base": "main", "body": "Adopt the worker-created pull request."},
		}},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{
			worker.LogEvent("stdout", "https://github.com/owner/repo/pull/61"),
			{Kind: worker.EventResult, Text: "implemented and opened https://github.com/owner/repo/pull/61"},
		}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/ssh_target.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Avoid invalid SSH roots", Prompt: "Do it and open a PR."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "")
	}, func(snapshot core.Snapshot) string {
		return "missing completed publish action"
	})
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want worker-created PR to be adopted", publisher.publishCalls)
	}
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
	pr := snapshot.PullRequests[0]
	if pr.ID != "github:owner/repo#61" || pr.Number != 61 || pr.Branch != "codex/ssh-checkout-root-health" {
		t.Fatalf("adopted pr = %+v", pr)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "") {
		t.Fatalf("missing completed publish action")
	}
	if len(pr.Metadata) == 0 || !strings.Contains(string(pr.Metadata), `"workerCreated":true`) {
		t.Fatalf("pr metadata = %s", pr.Metadata)
	}
}

func TestServiceCompletionPublishAdoptsWorkerCreatedPullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: core.PullRequest{
		Repo:         "owner/repo",
		Number:       61,
		URL:          "https://github.com/owner/repo/pull/61",
		Branch:       "codex/ssh-checkout-root-health",
		Base:         "main",
		Title:        "Avoid SSH targets with invalid checkout roots",
		State:        "OPEN",
		ChecksStatus: "pending",
		MergeStatus:  "UNKNOWN",
		ReviewStatus: "REVIEW_REQUIRED",
	}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "make change",
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{
			worker.LogEvent("stdout", "Created pull request: https://github.com/owner/repo/pull/61"),
			{Kind: worker.EventResult, Text: "implemented"},
		}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/ssh_target.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Avoid invalid SSH roots",
		Prompt: "Implement the fix.",
		Metadata: core.MustJSON(map[string]any{
			"completionMode": "github",
			"repo":           "owner/repo",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want completion publish to adopt worker-created PR", publisher.publishCalls)
	}
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
	pr := snapshot.PullRequests[0]
	if pr.ID != "github:owner/repo#61" || pr.Number != 61 || pr.TaskID != task.ID {
		t.Fatalf("adopted pr = %+v", pr)
	}
}

func TestServiceRetriesExplicitPublishPullRequestActionAfterRecoverableSigningFailure(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{
		errOnce: errors.New(strings.Join([]string{
			"push jj bookmark: exit status 255",
			"sign_and_send_pubkey: signing failed for ED25519",
			"failed to fill whole buffer",
		}, "\n")),
	}
	brain := &sequenceBrain{plans: []Plan{{
		WorkerKind: "change",
		Prompt:     "make change",
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "open a PR so review can happen while the objective continues",
			Inputs: map[string]any{
				"repo":   "owner/repo",
				"base":   "release",
				"branch": "aged/retry-explicit-publish",
				"draft":  true,
				"title":  "Retry explicit publish",
				"body":   "Retry explicit publish.",
			},
		}},
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Implement feature", Prompt: "Do it, open a PR, and babysit it."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	snapshot = waitForEventCount(t, store, core.EventTaskAction, task.ID, 2)
	if publisher.publishCalls != 1 {
		t.Fatalf("initial publish calls = %d, want 1", publisher.publishCalls)
	}
	originalSpec := publisher.published
	if originalSpec.WorkerID == "" {
		t.Fatalf("initial publish did not retain worker id: %+v", originalSpec)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "waiting") {
		t.Fatalf("missing waiting publish_pull_request action")
	}
	if len(snapshot.PullRequests) != 0 {
		t.Fatalf("pull requests = %+v, want none before retry", snapshot.PullRequests)
	}

	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "SSH signing agent is fixed; retry publication."}); err != nil {
		t.Fatal(err)
	}
	snapshot = waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "")
	}, func(snapshot core.Snapshot) string {
		return "missing completed publish_pull_request action"
	})
	if publisher.publishCalls != 2 {
		t.Fatalf("publish calls = %d, want retry publish", publisher.publishCalls)
	}
	retrySpec := publisher.published
	if retrySpec.WorkerID != originalSpec.WorkerID {
		t.Fatalf("retried worker = %q, want retained action candidate %q", retrySpec.WorkerID, originalSpec.WorkerID)
	}
	if retrySpec.Repo != originalSpec.Repo || retrySpec.Base != originalSpec.Base || retrySpec.Branch != originalSpec.Branch || retrySpec.Title != originalSpec.Title || retrySpec.Draft != originalSpec.Draft {
		t.Fatalf("retried publish did not preserve action inputs:\ninitial=%+v\nretry=%+v", originalSpec, retrySpec)
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, task.ID) != 1 {
		t.Fatalf("feedback reran a worker; worker.created count = %d", countEvents(snapshot.Events, core.EventWorkerCreated, task.ID))
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 1 {
		t.Fatalf("feedback replanned task; task.planned count = %d", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
	if !hasEvent(snapshot.Events, core.EventApprovalDecided, task.ID, "") {
		t.Fatalf("missing approval.decided event")
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request", "") {
		t.Fatalf("missing completed publish_pull_request action")
	}
}

func TestServicePlanActionCanPublishPullRequestAndContinue(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "find the first optimization",
			Actions: []PlanAction{{
				Kind:   "publish_pull_request",
				When:   "after_success",
				Reason: "ship the first optimization and keep researching",
				Inputs: map[string]any{"repo": "owner/repo", "continueAfterPublish": true, "body": "Ship the first optimization."},
			}},
		},
		decisions: []ReplanDecision{{
			Action:    "continue",
			Rationale: "continue looking for the next optimization",
			Plan: &Plan{
				WorkerKind: "change",
				Prompt:     "find the second optimization",
				Metadata:   map[string]any{"baseWorkerID": "source"},
				Actions: []PlanAction{{
					Kind:   "publish_pull_request",
					When:   "after_success",
					Reason: "ship the second optimization and keep the task owning both PRs",
					Inputs: map[string]any{"repo": "owner/repo", "body": "Ship the second optimization."},
				}},
			},
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "optimized"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "fast.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Perf research",
		Prompt: "Keep producing optimization PRs.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 2)
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	snapshot = waitForEventCount(t, store, core.EventTaskAction, task.ID, 4)
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.ObjectiveStatus != core.ObjectiveWaitingExternal || task.ObjectivePhase != "pr_opened" {
		t.Fatalf("objective = %q phase %q", task.ObjectiveStatus, task.ObjectivePhase)
	}
	if len(publisher.publishedSpecs) != 2 {
		t.Fatalf("published specs = %d, want 2", len(publisher.publishedSpecs))
	}
	if countEvents(snapshot.Events, core.EventTaskAction, task.ID) != 4 {
		t.Fatalf("task action events = %d, want 4", countEvents(snapshot.Events, core.EventTaskAction, task.ID))
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, task.ID) != 1 {
		t.Fatalf("task.replanned events = %d, want 1", countEvents(snapshot.Events, core.EventTaskReplanned, task.ID))
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "baseWorkerID", "source") {
		t.Fatalf("missing source-base worker metadata")
	}
}

func TestServicePlanActionDoesNotPublishAfterBlockingReviewFinding(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "tighten the signing-agent classifier",
			Spawns: []SpawnRequest{{
				ID:         "review",
				Role:       "reviewer",
				Reason:     "Review whether the change is ready to publish.",
				WorkerKind: "reviewer",
			}},
			Actions: []PlanAction{{
				Kind:   "publish_pull_request",
				When:   "after_success",
				Reason: "publish the classifier fix",
				Inputs: map[string]any{"repo": "owner/repo", "base": "main", "body": "Publish the classifier fix."},
			}},
		},
		decisions: []ReplanDecision{{
			Action:  "wait",
			Message: "review feedback needs an implementation follow-up",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented classifier changes"}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: `## Findings
- Medium issue: internal/orchestrator/service.go still misclassifies signing-agent failures.

## Recommended Next Turns
- Tighten the signing-agent classifier before publishing.`}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Fix signing-agent classification",
		Prompt: "Implement, review, and publish only when ready.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want blocking review finding to suppress publish", publisher.publishCalls)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request_blocked_by_follow_up", "rejected") {
		t.Fatalf("missing blocked publication action")
	}
	if len(brain.states) != 1 {
		t.Fatalf("replan states = %d, want 1", len(brain.states))
	}
	if len(brain.states[0].Results) != 2 {
		t.Fatalf("replan results = %d, want implementation plus review", len(brain.states[0].Results))
	}
}

func TestServicePlanActionPublishesAfterCleanReviewFinding(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "tighten the signing-agent classifier",
		Spawns: []SpawnRequest{{
			ID:         "review",
			Role:       "reviewer",
			Reason:     "Review whether the change is ready to publish.",
			WorkerKind: "reviewer",
		}},
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "publish the classifier fix",
			Inputs: map[string]any{"repo": "owner/repo", "base": "main", "body": "Publish the classifier fix."},
		}},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented classifier changes"}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: `## Findings
- No findings.

## Recommended Next Turns
- Publish the pull request.`}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/service.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Fix signing-agent classification",
		Prompt: "Implement, review, and publish when ready.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want clean review to allow publish", publisher.publishCalls)
	}
	if hasTaskAction(snapshot.Events, task.ID, "publish_pull_request_blocked_by_follow_up", "rejected") {
		t.Fatalf("clean review should not block publication")
	}
}

func TestServicePlanActionDoesNotPublishRejectedCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	baseBrain := &replanningBrain{
		plan: Plan{
			WorkerKind: "change",
			Prompt:     "find and implement a real throughput optimization",
			Actions: []PlanAction{{
				Kind:   "publish_pull_request",
				When:   "after_success",
				Reason: "publish a useful optimization PR when one is ready",
				Inputs: map[string]any{"repo": "owner/repo", "base": "main", "body": "Publish the optimization when ready."},
			}},
		},
		decisions: []ReplanDecision{{
			Action:    "continue",
			Rationale: "The worker correctly reported this is not ready to publish yet.",
			Plan: &Plan{
				WorkerKind: "change",
				Prompt:     "continue until there is an actual task-relevant optimization",
			},
		}, {
			Action:  "wait",
			Message: "continuing broader investigation",
		}},
	}
	brain := &publicationReviewBrain{
		BrainProvider:  baseBrain,
		ReplanProvider: baseBrain,
		reviews: []PublicationReview{{
			Ready:  false,
			Reason: "worker result says the requested optimization is not done and only produced setup",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "I added setup notes, but the requested throughput optimization is not done yet."}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "bench/throughput.md", Status: "added"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Improve Deno Serve Throughput",
		Prompt: "Keep working until you find real throughput optimizations and open PRs as useful complete units become ready.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want rejected candidate to stay unpublished", publisher.publishCalls)
	}
	if brain.reviewCalls != 1 {
		t.Fatalf("publication review calls = %d, want 1", brain.reviewCalls)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "publish_pull_request_readiness_rejected", "rejected") {
		t.Fatalf("missing publication readiness rejection event")
	}
	if len(baseBrain.states) == 0 {
		t.Fatalf("replanner was not given a chance to continue after rejected publication")
	}
}

func TestServiceImmediatePlanActionWatchesExistingPullRequests(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "no worker should run",
		Actions: []PlanAction{{
			Kind:   "watch_pull_requests",
			When:   "immediate",
			Reason: "standalone PR babysitting task",
			Inputs: map[string]any{"repo": "owner/repo", "number": 42},
		}},
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "should not run"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Babysit PR", Prompt: "Watch owner/repo#42 until merged."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForEvent(t, store, core.EventTaskArtifact, task.ID)
	task, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatal("missing task")
	}
	if task.Status != core.TaskWaiting || task.ObjectivePhase != "watching_pull_requests" {
		t.Fatalf("task status = %q phase = %q", task.Status, task.ObjectivePhase)
	}
	if len(snapshot.Workers) != 0 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	if publisher.listSpec.Repo != "owner/repo" || publisher.listSpec.Number != 42 {
		t.Fatalf("list spec = %+v", publisher.listSpec)
	}
	if !hasMilestone(task.Milestones, "pull_requests_watched") || len(task.Artifacts) != 1 {
		t.Fatalf("milestones=%+v artifacts=%+v", task.Milestones, task.Artifacts)
	}
}

func TestServicePullRequestFollowUpSuppressesPlanSpawnsWhenReturningToWatch(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-pr-followup"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Repair PR",
			"prompt": "Fix the pull request and keep watching it.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":      "pr-1",
			"attempt": 1,
			"reason":  "pull_request_needs_work",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "repair dirty PR branch",
		Actions: []PlanAction{{
			Kind:   "watch_pull_requests",
			When:   "after_success",
			Reason: "return repaired PR to monitor",
			Inputs: map[string]any{"repo": "owner/repo", "number": 7},
		}},
		Spawns: []SpawnRequest{{
			ID:         "review-after-repair",
			Role:       "post-repair reviewer",
			Reason:     "review repaired PR",
			WorkerKind: "reviewer",
		}},
	}}, map[string]worker.Runner{
		"change":   eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "repaired"}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: "reviewed"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir(), sourceRoot: t.TempDir()})
	service.SetPullRequestPublisher(publisher)

	service.resumeWaitingTask(ctx, taskID, "GitHub pull request owner/repo#7 needs follow-up work.")

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if hasWorkerCreated(snapshot.Events, taskID, "reviewer") {
		t.Fatalf("pull request follow-up should not run plan spawns before returning to watch")
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskPlanned, taskID, `"spawnsSuppressedReason":"pull_request_followup_returns_to_github_monitor"`) {
		t.Fatalf("missing suppressed spawn metadata")
	}
	if publisher.listSpec.Repo != "owner/repo" || publisher.listSpec.Number != 7 {
		t.Fatalf("list spec = %+v", publisher.listSpec)
	}
}

func TestServicePullRequestFollowUpStartsWorkspaceFromPullRequestHead(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	remote := t.TempDir()
	runTestGit(t, remote, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "origin", remote)
	runTestGit(t, repo, "push", "-u", "origin", "main")
	runTestGit(t, repo, "checkout", "-b", "codex/aged-test")
	if err := os.WriteFile(filepath.Join(repo, "fix.txt"), []byte("pr head\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", "fix.txt")
	runTestGit(t, repo, "-c", "user.name=aged-test", "-c", "user.email=aged-test@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "pr head")
	runTestGit(t, repo, "push", "-u", "origin", "codex/aged-test")
	runTestGit(t, repo, "checkout", "main")

	taskID := "task-pr-head-followup"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Repair PR",
			"prompt": "Fix the pull request and keep watching it.",
			"metadata": map[string]any{
				"projectId": "repo",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-1",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":      "pr-1",
			"attempt": 1,
			"reason":  "pull_request_needs_work",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	projects, err := NewProjectRegistry([]core.Project{{
		ID:          "repo",
		Name:        "Repo",
		LocalPath:   repo,
		Repo:        "owner/repo",
		DefaultBase: "main",
	}}, "repo")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "repair dirty PR branch",
		Actions: []PlanAction{{
			Kind:   "watch_pull_requests",
			When:   "after_success",
			Reason: "return repaired PR to monitor",
			Inputs: map[string]any{"repo": "owner/repo", "number": 7},
		}},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "repaired"}}},
	}, repo, workspace)
	service.SetProjects(projects)
	service.SetPullRequestPublisher(&fakePullRequestPublisher{})

	service.resumeWaitingTask(ctx, taskID, "GitHub pull request owner/repo#7 needs follow-up work.")

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if workspace.baseRevision != "refs/remotes/origin/codex/aged-test" {
		t.Fatalf("workspace base revision = %q, want PR head", workspace.baseRevision)
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskPlanned, taskID, `"workspaceBaseRef":"codex/aged-test"`) {
		t.Fatalf("missing PR head workspace metadata")
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskPlanned, taskID, "Decide whether a GitHub PR comment is warranted") {
		t.Fatalf("missing PR comment instruction")
	}
}

func TestServicePullRequestFollowUpUpdatesExistingPullRequestBeforeWatching(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-pr-followup-update"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Repair PR",
			"prompt": "Fix the pull request and keep watching it.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-1",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Repair PR",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":      "pr-1",
			"attempt": 1,
			"reason":  "pull_request_needs_work",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	changed := WorkspaceChangedFile{Path: "internal/orchestrator/pull_request.go", Status: "modified"}
	workspace := &recordingWorkspaceManager{changes: WorkspaceChanges{
		Dirty:        true,
		ChangedFiles: []WorkspaceChangedFile{changed},
	}}
	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "repair dirty PR branch",
		Actions: []PlanAction{{
			Kind:   "watch_pull_requests",
			When:   "after_success",
			Reason: "return repaired PR to monitor",
			Inputs: map[string]any{"repo": "owner/repo", "number": 7},
		}},
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "repaired"}}},
	}, t.TempDir(), workspace)
	service.SetPullRequestPublisher(publisher)

	service.resumeWaitingTask(ctx, taskID, "GitHub pull request owner/repo#7 needs follow-up work.")

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if publisher.updateCalls != 1 {
		t.Fatalf("update calls = %d, want 1", publisher.updateCalls)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want existing PR update only", publisher.publishCalls)
	}
	if publisher.updatedPR.ID != "pr-1" || publisher.updated.Branch != "codex/aged-test" || publisher.updated.Base != "main" {
		t.Fatalf("updated PR=%+v spec=%+v", publisher.updatedPR, publisher.updated)
	}
	if publisher.updated.WorkerID == "" {
		t.Fatalf("update worker id was empty")
	}
	if !hasEvent(snapshot.Events, core.EventPRUpdated, taskID, "") {
		t.Fatalf("missing pull_request.updated event")
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskAction, taskID, `"kind":"update_pull_request"`) {
		t.Fatalf("missing deterministic update_pull_request action")
	}
	if publisher.listSpec.Repo != "owner/repo" || publisher.listSpec.Number != 7 {
		t.Fatalf("watch list spec = %+v", publisher.listSpec)
	}
}

func TestServiceCompleteTaskWithOpenPullRequestDoesNotRepublishCompletionCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-open-pr-completion"
	workerID := "worker-repair"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":    "Repair existing PR",
			"prompt":   "Fix review feedback on the open pull request.",
			"metadata": map[string]any{"completionMode": "github"},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-1",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Repair existing PR",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)

	err := service.completeTask(ctx, taskID, []WorkerTurnResult{{
		WorkerID: workerID,
		Status:   core.WorkerSucceeded,
		Kind:     "codex",
		Summary:  "repaired the pull request",
		Changes: WorkspaceChanges{
			Dirty: true,
			ChangedFiles: []WorkspaceChangedFile{{
				Path:   "internal/orchestrator/pull_request.go",
				Status: "modified",
			}},
		},
	}}, workerID, "ready")
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want existing PR to remain external objective", publisher.publishCalls)
	}
	if publisher.updateCalls != 0 {
		t.Fatalf("update calls = %d, want updates to be driven by explicit plan actions", publisher.updateCalls)
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskCandidate, taskID, `"workerId":"`+workerID+`"`) {
		t.Fatalf("missing final candidate event")
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskStatus, taskID, `"status":"failed"`) {
		t.Fatalf("task failed while open PR existed")
	}
}

func TestServiceRefreshPullRequestCanSatisfyTaskObjective(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{
		status: core.PullRequest{
			ID:           "pr-1",
			TaskID:       "task-1",
			Repo:         "owner/repo",
			Number:       12,
			URL:          "https://github.com/owner/repo/pull/12",
			Branch:       "codex/aged-test",
			Base:         "main",
			Title:        "Implement feature",
			State:        "MERGED",
			ChecksStatus: "success",
			MergeStatus:  "CLEAN",
			ReviewStatus: "APPROVED",
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "make change",
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Implement feature",
		Prompt:   "Do it.",
		Metadata: core.MustJSON(map[string]any{"completionMode": "github"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	pr := snapshot.PullRequests[0]
	publisher.status.TaskID = task.ID
	_, err = service.RefreshPullRequest(ctx, pr.ID)
	if err != nil {
		t.Fatal(err)
	}

	snapshot = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	task = snapshot.Tasks[0]
	if task.ObjectiveStatus != core.ObjectiveSatisfied || task.ObjectivePhase != "merged" {
		t.Fatalf("objective = %q phase %q", task.ObjectiveStatus, task.ObjectivePhase)
	}
	if !hasMilestone(task.Milestones, "pr_merged") {
		t.Fatalf("milestones = %+v", task.Milestones)
	}
}

func TestServiceRefreshPullRequestCompletesLegacyBabysitterTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{status: core.PullRequest{
		ID:           "github:owner/repo#7",
		Repo:         "owner/repo",
		Number:       7,
		URL:          "https://github.com/owner/repo/pull/7",
		Branch:       "codex/aged-test",
		Base:         "main",
		Title:        "Task",
		State:        "MERGED",
		ChecksStatus: "success",
		MergeStatus:  "UNKNOWN",
	}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "ready"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)
	for _, task := range []struct {
		id       string
		metadata map[string]any
	}{
		{id: "task-1"},
		{id: "babysitter-1", metadata: map[string]any{"pullRequestId": "github:owner/repo#7", "repo": "owner/repo", "number": 7}},
	} {
		if _, err := store.Append(ctx, core.Event{
			Type:   core.EventTaskCreated,
			TaskID: task.id,
			Payload: core.MustJSON(map[string]any{
				"title":    "Task",
				"prompt":   "Prompt",
				"metadata": task.metadata,
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:   core.EventTaskStatus,
			TaskID: task.id,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskWaiting,
			}),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":     "github:owner/repo#7",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Task",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := service.RefreshPullRequest(ctx, "github:owner/repo#7"); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, "babysitter-1", core.TaskSucceeded)
	babysitter, ok := findTask(snapshot, "babysitter-1")
	if !ok {
		t.Fatal("missing babysitter task")
	}
	if babysitter.ObjectiveStatus != core.ObjectiveSatisfied || babysitter.ObjectivePhase != "merged" {
		t.Fatalf("babysitter objective = %q phase %q", babysitter.ObjectiveStatus, babysitter.ObjectivePhase)
	}
	if !hasMilestone(babysitter.Milestones, "pr_merged") {
		t.Fatalf("babysitter milestones = %+v", babysitter.Milestones)
	}
}

func TestServiceReconcilesTerminalPullRequestLegacyBabysitterTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "ready"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	for _, task := range []struct {
		id       string
		metadata map[string]any
	}{
		{id: "task-1"},
		{id: "babysitter-1", metadata: map[string]any{"pullRequestId": "github:owner/repo#7", "repo": "owner/repo", "number": 7}},
	} {
		if _, err := store.Append(ctx, core.Event{
			Type:   core.EventTaskCreated,
			TaskID: task.id,
			Payload: core.MustJSON(map[string]any{
				"title":    "Task",
				"prompt":   "Prompt",
				"metadata": task.metadata,
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:   core.EventTaskStatus,
			TaskID: task.id,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskWaiting,
			}),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":     "github:owner/repo#7",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Task",
			"state":  "MERGED",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.ReconcilePullRequestTerminalTasks(ctx, "github:owner/repo#7"); err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, "babysitter-1", core.TaskSucceeded)
}

func TestServiceRoutesTaskToConfiguredProject(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectA := t.TempDir()
	projectB := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "a", Name: "A", LocalPath: projectA, Repo: "owner/a", DefaultBase: "main"},
		{ID: "b", Name: "B", LocalPath: projectB, Repo: "owner/b", DefaultBase: "trunk"},
	}, "a")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	runner := &recordingRunner{kind: "chosen"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "chosen",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"chosen": runner}, projectA, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		ProjectID: "b",
		Title:     "Project routed",
		Prompt:    "Run in project B.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if task.ProjectID != "b" {
		t.Fatalf("task project = %q, want b", task.ProjectID)
	}
	if workspace.workDir != projectB {
		t.Fatalf("workspace workDir = %q, want %q", workspace.workDir, projectB)
	}
	if runner.workDir != projectB {
		t.Fatalf("runner workDir = %q, want %q", runner.workDir, projectB)
	}
	if snapshot.Tasks[0].ProjectID == "" {
		t.Fatalf("snapshot task missing project id: %+v", snapshot.Tasks[0])
	}
}

func TestServiceStartsNewTaskWorkspaceFromProjectDefaultBase(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	mainCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", "HEAD"))
	upstream := t.TempDir()
	runTestGit(t, upstream, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "upstream", upstream)
	runTestGit(t, repo, "push", "-u", "upstream", "main")
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("feature\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "checkout", "-b", "feature")
	runTestGit(t, repo, "add", "file.txt")
	runTestGit(t, repo, "-c", "user.name=aged-test", "-c", "user.email=aged-test@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "unrelated feature")

	projects, err := NewProjectRegistry([]core.Project{{
		ID:           "repo",
		Name:         "Repo",
		LocalPath:    repo,
		Repo:         "fork/repo",
		UpstreamRepo: "owner/repo",
		DefaultBase:  "main",
	}}, "repo")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, repo, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "New task",
		Prompt: "Do unrelated work.",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if workspace.baseRevision != "refs/remotes/upstream/main" {
		t.Fatalf("workspace base revision = %q, want upstream default base", workspace.baseRevision)
	}
	gotCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", workspace.baseRevision))
	if gotCommit != mainCommit {
		t.Fatalf("workspace base revision commit = %q, want %q", gotCommit, mainCommit)
	}
}

func TestServiceStartsNewTaskWorkspaceFromFetchedProjectDefaultBase(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	fixture := prepareStaleOriginMainFixture(t, "remote update")
	repo := fixture.repo
	fixture.assertLocalOriginMainStale(t)
	projects, err := NewProjectRegistry([]core.Project{{
		ID:          "repo",
		Name:        "Repo",
		LocalPath:   repo,
		Repo:        "owner/repo",
		DefaultBase: "main",
	}}, "repo")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, repo, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "New task",
		Prompt: "Do unrelated work.",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if workspace.baseRevision != "refs/remotes/origin/main" {
		t.Fatalf("workspace base revision = %q, want origin default base", workspace.baseRevision)
	}
	gotCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", workspace.baseRevision))
	if gotCommit != fixture.remoteCommit {
		t.Fatalf("workspace base revision commit = %q, want %q", gotCommit, fixture.remoteCommit)
	}
}

func TestSyncedProjectWorkspaceBaseRevisionFetchesStaleBase(t *testing.T) {
	ctx := context.Background()
	fixture := prepareStaleOriginMainFixture(t, "remote update")
	repo := fixture.repo
	fixture.assertLocalOriginMainStale(t)
	ref, err := syncedProjectWorkspaceBaseRevision(ctx, core.Project{
		LocalPath:   repo,
		DefaultBase: "main",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ref != "refs/remotes/origin/main" {
		t.Fatalf("synced base ref = %q, want refs/remotes/origin/main", ref)
	}
	gotCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", ref))
	if gotCommit != fixture.remoteCommit {
		t.Fatalf("synced base commit = %q, want %q", gotCommit, fixture.remoteCommit)
	}
}

func TestSyncedProjectWorkspaceBaseRevisionUsesOriginFallbackWithoutBranchUpstream(t *testing.T) {
	ctx := context.Background()
	fixture := prepareStaleOriginMainFixture(t, "origin fallback update")
	repo := fixture.repo
	runTestGit(t, repo, "config", "--unset", "branch.main.remote")
	runTestGit(t, repo, "config", "--unset", "branch.main.merge")
	fixture.assertLocalOriginMainStale(t)
	ref, err := syncedProjectWorkspaceBaseRevision(ctx, core.Project{
		LocalPath:   repo,
		DefaultBase: "main",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ref != "refs/remotes/origin/main" {
		t.Fatalf("synced base ref = %q, want refs/remotes/origin/main", ref)
	}
	gotCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", ref))
	if gotCommit != fixture.remoteCommit {
		t.Fatalf("synced base commit = %q, want %q", gotCommit, fixture.remoteCommit)
	}
}

type staleOriginMainFixture struct {
	repo         string
	remoteCommit string
	remoteRef    string
}

func prepareStaleOriginMainFixture(t *testing.T, updateMessage string) staleOriginMainFixture {
	t.Helper()

	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	remote := t.TempDir()
	runTestGit(t, remote, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "origin", remote)
	runTestGit(t, repo, "push", "-u", "origin", "main")
	runTestGit(t, remote, "symbolic-ref", "HEAD", "refs/heads/main")

	updaterParent := t.TempDir()
	updater := filepath.Join(updaterParent, "updater")
	runTestGit(t, updaterParent, "clone", remote, updater)
	runTestGit(t, updater, "config", "user.name", "aged-test")
	runTestGit(t, updater, "config", "user.email", "aged-test@example.invalid")
	runTestGit(t, updater, "config", "commit.gpgsign", "false")
	if err := os.WriteFile(filepath.Join(updater, "file.txt"), []byte(updateMessage+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, updater, "add", "file.txt")
	runTestGit(t, updater, "commit", "-m", updateMessage)
	runTestGit(t, updater, "push", "origin", "main")

	return staleOriginMainFixture{
		repo:         repo,
		remoteCommit: strings.TrimSpace(runTestGit(t, updater, "rev-parse", "HEAD")),
		remoteRef:    "refs/remotes/origin/main",
	}
}

func (f staleOriginMainFixture) assertLocalOriginMainStale(t *testing.T) {
	t.Helper()

	staleCommit := strings.TrimSpace(runTestGit(t, f.repo, "rev-parse", f.remoteRef))
	if staleCommit == f.remoteCommit {
		t.Fatalf("test setup failed: local origin/main is already current")
	}
}

func TestSyncedProjectWorkspaceBaseRevisionFailsWithoutUpstream(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")

	_, err := syncedProjectWorkspaceBaseRevision(ctx, core.Project{
		LocalPath:   repo,
		DefaultBase: "main",
	})
	if err == nil {
		t.Fatal("syncedProjectWorkspaceBaseRevision succeeded; want missing upstream error")
	}
	if !strings.Contains(err.Error(), "upstream tracking branch is not configured") {
		t.Fatalf("error = %v, want upstream tracking blocker", err)
	}
}

func TestSyncedProjectWorkspaceBaseRevisionFailsWithoutResolvableRemoteTrackingFallback(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	remote := t.TempDir()
	runTestGit(t, remote, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "origin", remote)

	_, err := syncedProjectWorkspaceBaseRevision(ctx, core.Project{
		LocalPath:   repo,
		DefaultBase: "main",
	})
	if err == nil {
		t.Fatal("syncedProjectWorkspaceBaseRevision succeeded; want missing upstream error")
	}
	if !strings.Contains(err.Error(), "upstream tracking branch is not configured") {
		t.Fatalf("error = %v, want upstream tracking blocker", err)
	}
}

func TestSyncedProjectWorkspaceBaseRevisionFailsWithUnsupportedBranchUpstream(t *testing.T) {
	ctx := context.Background()
	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	remote := t.TempDir()
	runTestGit(t, remote, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "origin", remote)
	runTestGit(t, repo, "push", "-u", "origin", "main")
	runTestGit(t, repo, "config", "branch.main.merge", "refs/tags/main")

	_, err := syncedProjectWorkspaceBaseRevision(ctx, core.Project{
		LocalPath:   repo,
		DefaultBase: "main",
	})
	if err == nil {
		t.Fatal("syncedProjectWorkspaceBaseRevision succeeded; want unsupported upstream error")
	}
	if !strings.Contains(err.Error(), "unsupported upstream merge ref") {
		t.Fatalf("error = %v, want unsupported upstream blocker", err)
	}
}

func TestServicePublishedPRContainsWorkerChangesNotDaemonBranch(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	repo := initGitTestRepo(t)
	runTestGit(t, repo, "branch", "-M", "main")
	mainCommit := strings.TrimSpace(runTestGit(t, repo, "rev-parse", "HEAD"))
	upstream := t.TempDir()
	runTestGit(t, upstream, "init", "--bare")
	runTestGit(t, repo, "remote", "add", "upstream", upstream)
	runTestGit(t, repo, "push", "-u", "upstream", "main")
	runTestGit(t, repo, "checkout", "-b", "daemon-feature")
	if err := os.WriteFile(filepath.Join(repo, "unrelated.txt"), []byte("do not publish\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repo, "add", "unrelated.txt")
	runTestGit(t, repo, "-c", "user.name=aged-test", "-c", "user.email=aged-test@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "unrelated daemon branch work")

	projects, err := NewProjectRegistry([]core.Project{{
		ID:           "repo",
		Name:         "Repo",
		LocalPath:    repo,
		Repo:         "fork/repo",
		UpstreamRepo: "owner/repo",
		DefaultBase:  "main",
	}}, "repo")
	if err != nil {
		t.Fatal(err)
	}
	publisher := LocalPullRequestPublisher{
		exec: func(ctx context.Context, dir string, name string, args ...string) (string, error) {
			switch {
			case name == "git" && len(args) > 0 && args[0] == "push":
				return "", nil
			case name == "gh" && len(args) >= 2 && args[0] == "pr" && args[1] == "create":
				return "https://github.com/owner/repo/pull/22", nil
			case name == "gh" && len(args) >= 2 && args[0] == "pr" && args[1] == "view":
				return `{"number":22,"url":"https://github.com/owner/repo/pull/22","state":"OPEN","title":"CI","isDraft":false,"headRefName":"ci-branch","baseRefName":"main","mergeStateStatus":"CLEAN","statusCheckRollup":[],"reviewDecision":""}`, nil
			default:
				return runCommand(ctx, dir, name, args...)
			}
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "writer",
		Prompt:     "add workflow",
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "publish CI workflow",
			Inputs: map[string]any{"repo": "owner/repo", "base": "main", "branch": "ci-branch", "title": "CI", "body": "Body"},
		}},
	}}, map[string]worker.Runner{"writer": fileWritingRunner{
		kind: "writer",
		path: ".github/workflows/ci.yml",
		body: "name: CI\n",
	}}, repo, NewGitWorkspaceManager(WorkspaceModeIsolated, t.TempDir(), WorkspaceCleanupRetain))
	service.SetProjects(projects)
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Add CI",
		Prompt:   "Implement CI that checks formatting and runs all the tests.",
		Metadata: core.MustJSON(map[string]any{"completionMode": "github"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
	}
	if contents := runTestGit(t, repo, "show", "ci-branch:.github/workflows/ci.yml"); contents != "name: CI\n" {
		t.Fatalf("published branch missing worker workflow: %q", contents)
	}
	if _, err := runCommand(ctx, repo, "git", "cat-file", "-e", "ci-branch:unrelated.txt"); err == nil {
		t.Fatalf("published branch included unrelated daemon branch file")
	}
	if base := strings.TrimSpace(runTestGit(t, repo, "merge-base", "ci-branch", "refs/remotes/upstream/main")); base != mainCommit {
		t.Fatalf("branch merge-base = %q, want upstream main %q", base, mainCommit)
	}
}

func TestServiceLoadsProjectsFromSQLiteBeforeSeed(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	seedDir := t.TempDir()
	seed, err := NewProjectRegistry([]core.Project{{ID: "seed", Name: "Seed", LocalPath: seedDir}}, "seed")
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), seedDir)
	if err := service.LoadProjects(ctx, seed); err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	if _, err := service.CreateProject(ctx, core.Project{ID: "api", Name: "API", LocalPath: projectDir, Repo: "owner/api"}); err != nil {
		t.Fatal(err)
	}

	restarted := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), seedDir)
	if err := restarted.LoadProjects(ctx, seed); err != nil {
		t.Fatal(err)
	}
	snapshot, err := restarted.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Projects) != 2 {
		t.Fatalf("projects = %+v, want seed and api", snapshot.Projects)
	}
	if project, ok := restarted.projects.Get("api"); !ok || project.Repo != "owner/api" {
		t.Fatalf("loaded project = %+v, ok = %v", project, ok)
	}
}

func TestServiceDisablingRunnerPluginRemovesRuntimeRunner(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, StaticBrain{WorkerKind: "mock"}, map[string]worker.Runner{}, t.TempDir())
	plugin := core.Plugin{
		ID:       "runner:lint",
		Name:     "Lint",
		Kind:     "runner",
		Enabled:  true,
		Protocol: "aged-runner-v1",
		Command:  []string{"aged-lint"},
	}
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	if runner := service.runners["lint"]; runner == nil {
		t.Fatalf("runner plugin was not registered: %+v", service.runners)
	}

	plugin.Enabled = false
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	if runner, ok := service.runners["lint"]; ok {
		t.Fatalf("disabled runner plugin left stale runner: %+v", runner)
	}
}

func TestServiceClearingRunnerPluginCommandRemovesRuntimeRunner(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, StaticBrain{WorkerKind: "mock"}, map[string]worker.Runner{}, t.TempDir())
	plugin := core.Plugin{
		ID:       "runner:lint",
		Name:     "Lint",
		Kind:     "runner",
		Enabled:  true,
		Protocol: "aged-runner-v1",
		Command:  []string{"aged-lint"},
	}
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	if _, ok := service.runners["lint"]; !ok {
		t.Fatalf("runner plugin was not registered: %+v", service.runners)
	}

	plugin.Command = nil
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	if _, ok := service.runners["lint"]; ok {
		t.Fatalf("runner plugin with cleared command left stale runner: %+v", service.runners)
	}
}

func TestServiceRunnerPluginProtocolChangeRestoresStaticRunner(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	static := buildOnlyRunner{kind: "lint", command: []string{"static-lint"}}
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, map[string]worker.Runner{"lint": static}, t.TempDir())
	plugin := core.Plugin{
		ID:       "runner:lint",
		Name:     "Lint",
		Kind:     "runner",
		Enabled:  true,
		Protocol: "aged-runner-v1",
		Command:  []string{"aged-lint"},
	}
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(service.runners["lint"].BuildCommand(workerSpec("w1")), " "); got != "aged-lint run" {
		t.Fatalf("registered runner command = %q", got)
	}

	plugin.Protocol = "aged-plugin-v1"
	if _, err := service.RegisterPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	runner, ok := service.runners["lint"]
	if !ok {
		t.Fatalf("static runner was not restored after protocol change: %+v", service.runners)
	}
	if got := strings.Join(runner.BuildCommand(workerSpec("w1")), " "); got != "static-lint" {
		t.Fatalf("static runner was not restored, command = %q", got)
	}
}

func TestServiceMapsExternalRepoToProject(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectA := t.TempDir()
	projectB := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "a", Name: "A", LocalPath: projectA, Repo: "owner/a"},
		{ID: "b", Name: "B", LocalPath: projectB, Repo: "owner/b"},
	}, "a")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, projectA, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "GitHub issue owner/b#1",
		Prompt:   "Fix it.",
		Metadata: core.MustJSON(map[string]any{"repo": "owner/b"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if task.ProjectID != "b" {
		t.Fatalf("task project = %q, want b", task.ProjectID)
	}
	if workspace.workDir != projectB {
		t.Fatalf("workspace workDir = %q, want %q", workspace.workDir, projectB)
	}
}

func TestServiceRoutesGitHubIssueToExplicitUpstreamProject(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	upstreamCheckout := t.TempDir()
	forkCheckout := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "upstream", Name: "Upstream", LocalPath: upstreamCheckout, Repo: "owner/repo"},
		{ID: "fork", Name: "Fork", LocalPath: forkCheckout, Repo: "fork-owner/repo", UpstreamRepo: "owner/repo"},
	}, "upstream")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, upstreamCheckout, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "GitHub issue owner/repo#1",
		Prompt:   "Fix it.",
		Metadata: core.MustJSON(map[string]any{"source": "github-issue", "repo": "owner/repo"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if task.ProjectID != "fork" {
		t.Fatalf("task project = %q, want fork", task.ProjectID)
	}
	if workspace.workDir != forkCheckout {
		t.Fatalf("workspace workDir = %q, want %q", workspace.workDir, forkCheckout)
	}
}

func TestServiceRoutesGitHubIssueRepoDeterministically(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	firstForkCheckout := t.TempDir()
	secondForkCheckout := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "z-fork", Name: "Second Fork", LocalPath: secondForkCheckout, Repo: "second/repo", UpstreamRepo: "owner/repo"},
		{ID: "a-fork", Name: "First Fork", LocalPath: firstForkCheckout, Repo: "first/repo", UpstreamRepo: "owner/repo"},
	}, "z-fork")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, secondForkCheckout, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "GitHub issue owner/repo#1",
		Prompt:   "Fix it.",
		Metadata: core.MustJSON(map[string]any{"source": "github-issue", "repo": "owner/repo"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if task.ProjectID != "a-fork" {
		t.Fatalf("task project = %q, want a-fork", task.ProjectID)
	}
	if workspace.workDir != firstForkCheckout {
		t.Fatalf("workspace workDir = %q, want %q", workspace.workDir, firstForkCheckout)
	}
}

func TestServiceKeepsLocalRepoLookupWhenNotGitHubIssue(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	upstreamCheckout := t.TempDir()
	forkCheckout := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{
		{ID: "upstream", Name: "Upstream", LocalPath: upstreamCheckout, Repo: "owner/repo"},
		{ID: "fork", Name: "Fork", LocalPath: forkCheckout, Repo: "fork-owner/repo", UpstreamRepo: "owner/repo"},
	}, "fork")
	if err != nil {
		t.Fatal(err)
	}
	workspace := &recordingWorkspaceManager{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, forkCheckout, workspace)
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Local repo task",
		Prompt:   "Fix it.",
		Metadata: core.MustJSON(map[string]any{"repo": "owner/repo"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if task.ProjectID != "upstream" {
		t.Fatalf("task project = %q, want upstream", task.ProjectID)
	}
	if workspace.workDir != upstreamCheckout {
		t.Fatalf("workspace workDir = %q, want %q", workspace.workDir, upstreamCheckout)
	}
}

func TestServicePublishesPullRequestUsingProjectDefaults(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectRoot := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{{
		ID:          "repo",
		Name:        "Repo",
		LocalPath:   projectRoot,
		Repo:        "owner/repo",
		DefaultBase: "trunk",
		PullRequestPolicy: core.PullRequestPolicy{
			BranchPrefix: "aged/custom-",
			Draft:        true,
			AllowMerge:   true,
			AutoMerge:    false,
		},
	}}, "repo")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "make change",
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
	}, projectRoot, fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: projectRoot,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})
	service.SetProjects(projects)
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: "repo", Title: "Implement feature", Prompt: "Do it."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if _, err := service.PublishTaskPullRequest(ctx, task.ID, core.PublishPullRequestRequest{}); err != nil {
		t.Fatal(err)
	}
	if publisher.published.Repo != "owner/repo" {
		t.Fatalf("published repo = %q", publisher.published.Repo)
	}
	if publisher.published.Base != "trunk" {
		t.Fatalf("published base = %q", publisher.published.Base)
	}
	if publisher.published.BranchPrefix != "aged/custom-" {
		t.Fatalf("published branch prefix = %q", publisher.published.BranchPrefix)
	}
	if !publisher.published.Draft {
		t.Fatalf("published draft = false, want project policy draft")
	}
	if publisher.published.WorkDir != taskWorkspaceCWD(snapshot, task.ID) {
		t.Fatalf("published workDir = %q, want worker workspace", publisher.published.WorkDir)
	}
	if publisher.published.HeadRepoOwner != "" || publisher.published.PushRemote != "" {
		t.Fatalf("non-fork publish spec had fork fields: %+v", publisher.published)
	}
}

func TestServicePublishesForkPullRequestUsingProjectConfig(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	projectRoot := t.TempDir()
	projects, err := NewProjectRegistry([]core.Project{{
		ID:            "fork",
		Name:          "Fork",
		LocalPath:     projectRoot,
		Repo:          "fork-owner/repo",
		UpstreamRepo:  "owner/repo",
		HeadRepoOwner: "fork-owner",
		PushRemote:    "fork",
		DefaultBase:   "trunk",
	}}, "fork")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "make change",
	}}, map[string]worker.Runner{
		"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
	}, projectRoot, fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: projectRoot,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "README.md", Status: "modified"}},
		},
	})
	service.SetProjects(projects)
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: "fork", Title: "Implement feature", Prompt: "Do it."})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if _, err := service.PublishTaskPullRequest(ctx, task.ID, core.PublishPullRequestRequest{}); err != nil {
		t.Fatal(err)
	}
	if publisher.published.Repo != "owner/repo" {
		t.Fatalf("published repo = %q, want owner/repo", publisher.published.Repo)
	}
	if publisher.published.HeadRepoOwner != "fork-owner" {
		t.Fatalf("published head owner = %q, want fork-owner", publisher.published.HeadRepoOwner)
	}
	if publisher.published.PushRemote != "fork" {
		t.Fatalf("published push remote = %q, want fork", publisher.published.PushRemote)
	}
	if publisher.published.Base != "trunk" {
		t.Fatalf("published base = %q, want trunk", publisher.published.Base)
	}
}

func TestServiceRefreshesPullRequestStatus(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{
		status: core.PullRequest{
			State:            "OPEN",
			ChecksConclusion: "SUCCESS",
			Mergeable:        "MERGEABLE",
			ReviewStatus:     "APPROVED",
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "run"}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetPullRequestPublisher(publisher)
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"title":  "Task",
			"prompt": "Prompt",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-1",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Task",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	pr, err := service.RefreshPullRequest(ctx, "pr-1")
	if err != nil {
		t.Fatal(err)
	}
	if pr.ChecksStatus != "passing" || pr.ChecksConclusion != "SUCCESS" || pr.MergeStatus != "MERGEABLE" || pr.Mergeable != "MERGEABLE" || pr.ReviewStatus != "APPROVED" {
		t.Fatalf("refreshed pr = %+v", pr)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.PullRequests[0].ChecksStatus != "passing" || snapshot.PullRequests[0].ChecksConclusion != "SUCCESS" || snapshot.PullRequests[0].MergeStatus != "MERGEABLE" || snapshot.PullRequests[0].Mergeable != "MERGEABLE" {
		t.Fatalf("snapshot pr = %+v", snapshot.PullRequests[0])
	}
	if snapshot.Tasks[0].ObjectivePhase != "ready_to_merge" {
		t.Fatalf("objective phase = %q, want ready_to_merge", snapshot.Tasks[0].ObjectivePhase)
	}
}

func TestServiceAttachesPullRequestBabysittingToSourceTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "babysit",
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "ready"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"title":  "Task",
			"prompt": "Prompt",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: "task-1",
		Payload: core.MustJSON(map[string]any{
			"id":     "pr-1",
			"repo":   "owner/repo",
			"number": 7,
			"url":    "https://github.com/owner/repo/pull/7",
			"branch": "codex/aged-test",
			"base":   "main",
			"title":  "Task",
			"state":  "OPEN",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	task, err := service.StartPullRequestBabysitter(ctx, "pr-1")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if task.ID != "task-1" {
		t.Fatalf("babysitter task = %q, want source task", task.ID)
	}
	if !hasEvent(snapshot.Events, core.EventPRBabysitter, "task-1", "") {
		t.Fatalf("missing pr babysitter event")
	}
	var found bool
	for _, pr := range snapshot.PullRequests {
		if pr.ID == "pr-1" && pr.BabysitterTaskID == task.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("pull request did not point at babysitter task: %+v", snapshot.PullRequests)
	}
}

func TestServiceFailsCleanlyForUnknownBrainWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "missing",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskFailed)
	if !hasEvent(snapshot.Events, core.EventTaskPlanned, task.ID, "") {
		t.Fatalf("missing task.planned event before failure")
	}
}

func TestServiceRunsDurableLoopModeWithoutBrainPlanning(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventResult, Text: "loop iteration done"}},
			{{Kind: worker.EventNeedsInput, Text: "need user input"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Keep making bounded progress.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "loop",
			"loopIntervalSeconds": 0,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if calls := runner.callsValue(); calls != 2 {
		t.Fatalf("runner calls = %d, want 2", calls)
	}
	if count := countEvents(snapshot.Events, core.EventTaskPlanned, task.ID); count != 2 {
		t.Fatalf("task.planned count = %d, want 2", count)
	}
	if !strings.Contains(runner.promptValue(), "# Durable Agent Loop") {
		t.Fatalf("runner prompt missing loop context:\n%s", runner.promptValue())
	}
	if !strings.Contains(runner.promptValue(), "# Continuation Context") {
		t.Fatalf("runner prompt missing loop continuation context:\n%s", runner.promptValue())
	}
	if strings.Contains(runner.promptValue(), "previously failed or canceled") {
		t.Fatalf("runner prompt used retry wording for loop continuation:\n%s", runner.promptValue())
	}
	if !hasTaskAction(snapshot.Events, task.ID, "durable_loop", "waiting_for_input") {
		t.Fatalf("missing durable loop waiting action")
	}
	if hasTaskAction(snapshot.Events, task.ID, "durable_loop", "paused") {
		t.Fatalf("loop should only stop on worker input or cancelation")
	}
}

func TestServiceRecoveredRemoteWorkerContinuesDurableLoop(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventNeedsInput, Text: "need user input"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not replan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	taskID := "loop-task"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Loop",
			"prompt": "Keep looking for bugs.",
			"metadata": map[string]any{
				"executionMode":       "loop",
				"loopWorkerKind":      "loop",
				"loopIntervalSeconds": 0,
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskPlanned,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"workerKind": "loop",
			"prompt":     "iteration 1",
			"metadata": map[string]any{
				"executionMode":  "loop",
				"loopIteration":  1,
				"loopWorkerKind": "loop",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"nodeId":     "node-1",
			"workerId":   "worker-1",
			"workerKind": "loop",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"kind": "loop",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: "worker-1",
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "iteration 1 complete",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service.resumeRecoveredRemoteTask(ctx, taskID)

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if runner.callsValue() != 1 {
		t.Fatalf("runner calls = %d, want 1", runner.callsValue())
	}
	if !strings.Contains(runner.promptValue(), "iteration 2") {
		t.Fatalf("runner prompt did not resume at iteration 2:\n%s", runner.promptValue())
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, taskID) != 0 {
		t.Fatalf("loop recovery should not enter normal replan")
	}
	if countEvents(snapshot.Events, core.EventTaskStatus, taskID) == 0 || snapshot.Tasks[0].Status != core.TaskWaiting {
		t.Fatalf("task = %+v", snapshot.Tasks)
	}
	if !hasTaskAction(snapshot.Events, taskID, "durable_loop", "waiting_for_input") {
		t.Fatalf("missing durable loop waiting action")
	}
}

func TestServiceRetriesSucceededDurableLoopTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventNeedsInput, Text: "need user input"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not replan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	taskID := "loop-task"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Loop",
			"prompt": "Keep looking for bugs.",
			"metadata": map[string]any{
				"executionMode":       "loop",
				"loopWorkerKind":      "loop",
				"loopIntervalSeconds": 0,
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskPlanned,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"workerKind": "loop",
			"prompt":     "iteration 1",
			"metadata": map[string]any{
				"executionMode":  "loop",
				"loopIteration":  1,
				"loopWorkerKind": "loop",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskSucceeded,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := service.RetryTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if runner.callsValue() != 1 {
		t.Fatalf("runner calls = %d, want 1", runner.callsValue())
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, taskID) != 0 {
		t.Fatalf("loop retry should not enter normal replan")
	}
	if !hasTaskAction(snapshot.Events, taskID, "durable_loop", "waiting_for_input") {
		t.Fatalf("missing durable loop waiting action")
	}
}

func TestServiceUpdatesDurableLoopInterval(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventNeedsInput, Text: "pause after first iteration"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Keep making bounded progress.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "loop",
			"loopIntervalSeconds": 300,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskWaiting)

	updated, err := service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{LoopIntervalSeconds: ptrInt(30)})
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(updated.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if intMetadata(metadata, "loopIntervalSeconds") != 30 {
		t.Fatalf("loopIntervalSeconds = %v, want 30", metadata["loopIntervalSeconds"])
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "loop_config_updated", "updated") {
		t.Fatalf("missing loop config update action")
	}
}

func TestServiceUpdatesDurableLoopPrompt(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventNeedsInput, Text: "pause after first iteration"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Original durable objective.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "loop",
			"loopIntervalSeconds": 300,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskWaiting)

	updated, err := service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{LoopPrompt: ptrString("  Updated standing loop objective.  ")})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Prompt != "Original durable objective." {
		t.Fatalf("task prompt = %q, want original prompt preserved", updated.Prompt)
	}
	var metadata map[string]any
	if err := json.Unmarshal(updated.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if got := stringMetadataValue(metadata["loopPrompt"]); got != "Updated standing loop objective." {
		t.Fatalf("loopPrompt = %q, want updated standing objective", got)
	}
	config := durableLoopConfigFromTask(updated, service.runners)
	if config.Prompt != "Updated standing loop objective." {
		t.Fatalf("config prompt = %q, want updated loop prompt", config.Prompt)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "loop_config_updated", "updated") {
		t.Fatalf("missing loop config update action")
	}
}

func TestServiceUpdatesDurableLoopRequiredTargetID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventNeedsInput, Text: "pause after first iteration"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Keep making bounded progress.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "loop",
			"loopIntervalSeconds": 300,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskWaiting)

	updated, err := service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{RequiredTargetID: ptrString("  vm-fast  ")})
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(updated.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if got := stringMetadataValue(metadata["requiredTargetID"]); got != "vm-fast" {
		t.Fatalf("requiredTargetID = %q, want vm-fast", got)
	}

	updated, err = service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{RequiredTargetID: ptrString("")})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(updated.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if got := stringMetadataValue(metadata["requiredTargetID"]); got != "" {
		t.Fatalf("requiredTargetID after clear = %q, want empty", got)
	}
}

func TestDurableLoopUsesUpdatedConfigForNextIteration(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &sequenceEventRunner{
		kind: "loop",
		events: [][]worker.Event{
			{{Kind: worker.EventResult, Text: "iteration 1 done"}},
			{{Kind: worker.EventNeedsInput, Text: "pause after iteration 2"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"loop": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Original loop objective.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "loop",
			"loopIntervalSeconds": 10,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasIterationCompletedAction(snapshot.Events, task.ID, 1)
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("iteration 1 did not complete; events = %+v", snapshot.Events)
	})

	if _, err := service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{
		LoopPrompt: ptrString("Updated loop objective for next iteration."),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{
		LoopIntervalSeconds: ptrInt(0),
	}); err != nil {
		t.Fatal(err)
	}

	waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if runner.callsValue() != 2 {
		t.Fatalf("runner calls = %d, want 2", runner.callsValue())
	}
	if !strings.Contains(runner.promptValue(), "Updated loop objective for next iteration.") {
		t.Fatalf("iteration 2 prompt did not use updated loopPrompt:\n%s", runner.promptValue())
	}
}

func hasIterationCompletedAction(events []core.Event, taskID string, iteration int) bool {
	for _, event := range events {
		if event.Type != core.EventTaskAction || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Kind      string `json:"kind"`
			Status    string `json:"status"`
			Iteration int    `json:"iteration"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.Kind == "durable_loop" && payload.Status == "iteration_completed" && payload.Iteration == iteration {
			return true
		}
	}
	return false
}

func TestDurableLoopIntervalWaitObservesConfigUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{
		"loop": eventRunner{kind: "loop"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	taskID := "loop-task"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Loop",
			"prompt": "Keep making bounded progress.",
			"metadata": map[string]any{
				"executionMode":       "loop",
				"loopWorkerKind":      "loop",
				"loopIntervalSeconds": 30,
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskWaiting,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		done <- service.waitDurableLoopInterval(ctx, taskID, 30*time.Second)
	}()
	time.Sleep(50 * time.Millisecond)
	if _, err := service.UpdateTaskLoopConfig(ctx, taskID, core.UpdateLoopConfigRequest{LoopIntervalSeconds: ptrInt(0)}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("wait returned after %s, want it to observe the interval update promptly", elapsed)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestServiceRejectsLoopIntervalUpdateForNonLoopTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "do it",
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "One shot", Prompt: "Do it."})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.UpdateTaskLoopConfig(ctx, task.ID, core.UpdateLoopConfigRequest{LoopIntervalSeconds: ptrInt(30)})
	if err == nil || !strings.Contains(err.Error(), "not a durable loop") {
		t.Fatalf("error = %v, want durable loop rejection", err)
	}
}

func TestServiceFailsDurableLoopWithMissingExplicitRunner(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{err: errors.New("brain should not plan loop tasks")}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "should not run"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Loop",
		Prompt: "Keep making bounded progress.",
		Metadata: core.MustJSON(map[string]any{
			"executionMode":       "loop",
			"loopWorkerKind":      "codex",
			"loopIntervalSeconds": 0,
			"completionMode":      "local",
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskFailed)
	failed, ok := findTask(snapshot, task.ID)
	if !ok {
		t.Fatalf("missing task %s", task.ID)
	}
	if !strings.Contains(failed.Error, `loop worker kind "codex" is not configured`) {
		t.Fatalf("task error = %q", failed.Error)
	}
	if count := countEvents(snapshot.Events, core.EventWorkerCreated, task.ID); count != 0 {
		t.Fatalf("worker.created count = %d, want 0", count)
	}
}

func TestServiceRetriesFailedTaskFromPersistedPlan(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &flakyRunner{kind: "retryable"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "retryable",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"retryable": runner}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskFailed)

	retried, err := service.RetryTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.ID != task.ID {
		t.Fatalf("retry returned task %q, want %q", retried.ID, task.ID)
	}
	if retried.Error != "" || retried.ObjectiveStatus != core.ObjectiveActive || retried.ObjectivePhase != "retrying" {
		t.Fatalf("retried task did not reset failed objective state: %+v", retried)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if runner.callsValue() != 2 {
		t.Fatalf("runner calls = %d, want 2", runner.callsValue())
	}
	if countEvents(snapshot.Events, core.EventTaskCreated, task.ID) != 1 {
		t.Fatalf("retry created a new task")
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 2 {
		t.Fatalf("task.planned count = %d, want 2", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, task.ID) != 2 {
		t.Fatalf("worker.created count = %d, want 2", countEvents(snapshot.Events, core.EventWorkerCreated, task.ID))
	}
}

func TestServiceRetryFailsWhenExplicitTaskProjectWasDeleted(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	defaultProject := core.Project{ID: "default", Name: "Default", LocalPath: t.TempDir(), DefaultBase: "main"}
	deletedProject := core.Project{ID: "deleted", Name: "Deleted", LocalPath: t.TempDir(), DefaultBase: "main"}
	if _, err := store.SaveProject(ctx, defaultProject, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveProject(ctx, deletedProject, false); err != nil {
		t.Fatal(err)
	}
	projects, err := NewProjectRegistry([]core.Project{defaultProject, deletedProject}, defaultProject.ID)
	if err != nil {
		t.Fatal(err)
	}
	runner := &flakyRunner{kind: "retryable"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "retryable",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"retryable": runner}, defaultProject.LocalPath, fakeWorkspaceManager{cwd: t.TempDir()})
	service.SetProjects(projects)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		ProjectID: deletedProject.ID,
		Title:     "Do project work",
		Prompt:    "User request",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskFailed)
	if err := service.DeleteProject(ctx, deletedProject.ID); err != nil {
		t.Fatal(err)
	}

	_, err = service.RetryTask(ctx, task.ID)
	if err == nil || !strings.Contains(err.Error(), `unknown projectId "deleted"`) {
		t.Fatalf("retry err = %v, want missing explicit project", err)
	}
	if runner.callsValue() != 1 {
		t.Fatalf("runner calls = %d, want retry to stop before rerun", runner.callsValue())
	}
	if _, err := service.projectForTaskID(ctx, task.ID); err == nil || !strings.Contains(err.Error(), `unknown projectId "deleted"`) {
		t.Fatalf("projectForTaskID err = %v, want missing explicit project", err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, task.ID) != 1 {
		t.Fatalf("worker.created count = %d, want 1", countEvents(snapshot.Events, core.EventWorkerCreated, task.ID))
	}
}

func TestServiceRetriesCanceledTaskFromPersistedPlan(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-canceled"
	plan := Plan{WorkerKind: "retryable", Prompt: "resume canceled work"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Canceled work",
			"prompt": "Pick up where the canceled worker left off.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	runner := eventRunner{kind: "retryable", events: []worker.Event{{Kind: worker.EventResult, Text: "resumed"}}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: plan}, map[string]worker.Runner{"retryable": runner}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	retried, err := service.RetryTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.ID != taskID || retried.Status != core.TaskPlanning {
		t.Fatalf("retried = %+v", retried)
	}
	if retried.ObjectiveStatus != core.ObjectiveActive || retried.ObjectivePhase != "retrying" {
		t.Fatalf("retried objective = %q/%q, want active/retrying", retried.ObjectiveStatus, retried.ObjectivePhase)
	}

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if countEvents(snapshot.Events, core.EventTaskPlanned, taskID) != 2 {
		t.Fatalf("task.planned count = %d, want 2", countEvents(snapshot.Events, core.EventTaskPlanned, taskID))
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 1 {
		t.Fatalf("worker.created count = %d, want 1", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
}

func TestRecoverRemoteWorkersRetriesStartupCanceledLocalTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-startup-canceled"
	plan := Plan{WorkerKind: "retryable", Prompt: "resume startup-canceled work"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Startup canceled work",
			"prompt": "Pick up where the worker left off.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: "old-worker",
		Payload: core.MustJSON(map[string]any{
			"kind":   "retryable",
			"prompt": "old attempt",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: plan}, map[string]worker.Runner{
		"retryable": eventRunner{kind: "retryable", events: []worker.Event{{Kind: worker.EventResult, Text: "resumed"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if !hasTaskAction(snapshot.Events, taskID, "startup_auto_retry", "retrying") {
		t.Fatalf("missing startup auto-retry action")
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 2 {
		t.Fatalf("worker.created count = %d, want 2", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
	if countEvents(snapshot.Events, core.EventWorkerCompleted, taskID) != 2 {
		t.Fatalf("worker.completed count = %d, want 2", countEvents(snapshot.Events, core.EventWorkerCompleted, taskID))
	}
}

func TestRecoverRemoteWorkersDoesNotRetryManualCanceledTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-manual-canceled"
	plan := Plan{WorkerKind: "retryable", Prompt: "manual retry only"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Manual canceled work",
			"prompt": "Do not retry automatically.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: plan}, map[string]worker.Runner{
		"retryable": eventRunner{kind: "retryable", events: []worker.Event{{Kind: worker.EventResult, Text: "should not run"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		t.Fatalf("missing task %s", taskID)
	}
	if task.Status != core.TaskCanceled {
		t.Fatalf("task status = %s, want canceled", task.Status)
	}
	if hasTaskAction(snapshot.Events, taskID, "startup_auto_retry", "retrying") {
		t.Fatalf("manual cancellation was auto-retried")
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 0 {
		t.Fatalf("worker.created count = %d, want 0", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
}

func TestServiceRetriesFinalCandidateByPublishingWithoutRerunningWorker(t *testing.T) {
	for _, status := range []core.TaskStatus{core.TaskCanceled, core.TaskFailed} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			store := openTestStore(t)
			defer store.Close()

			publisher := &fakePullRequestPublisher{}
			service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
				WorkerKind: "codex",
				Prompt:     "implement the candidate",
			}}, map[string]worker.Runner{
				"codex": eventRunner{kind: "codex"},
			}, t.TempDir(), fakeWorkspaceManager{
				cwd:        t.TempDir(),
				sourceRoot: t.TempDir(),
				changes: WorkspaceChanges{
					Dirty:        true,
					ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
				},
			})
			service.SetPullRequestPublisher(publisher)

			task, err := service.CreateTask(ctx, core.CreateTaskRequest{
				Title:  "Retry finalization",
				Prompt: "Publish the existing candidate.",
				Metadata: core.MustJSON(map[string]any{
					"completionMode": "github",
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
			snapshot = waitForPullRequests(t, store, task.ID, 1)
			snapshot = waitForTaskStatusEventCount(t, store, task.ID, core.TaskWaiting, 2)
			if publisher.publishCalls != 1 {
				t.Fatalf("initial publish calls = %d, want 1", publisher.publishCalls)
			}
			workerID := publisher.published.WorkerID
			payload := map[string]any{
				"status": status,
			}
			if status == core.TaskFailed {
				payload["error"] = "publication failed"
			}
			if _, err := store.Append(ctx, core.Event{
				Type:    core.EventTaskStatus,
				TaskID:  task.ID,
				Payload: core.MustJSON(payload),
			}); err != nil {
				t.Fatal(err)
			}
			snapshot = waitForTaskStatus(t, store, task.ID, status)

			retried, err := service.RetryTask(ctx, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			if retried.Status != core.TaskPlanning || retried.ObjectiveStatus != core.ObjectiveActive || retried.ObjectivePhase != "retrying" {
				t.Fatalf("retried = %+v", retried)
			}
			snapshot = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
			if publisher.publishCalls != 2 || publisher.published.WorkerID != workerID {
				t.Fatalf("publish calls = %d, spec = %+v", publisher.publishCalls, publisher.published)
			}
			if countEvents(snapshot.Events, core.EventWorkerCreated, task.ID) != 1 {
				t.Fatalf("retry reran a worker; worker.created count = %d", countEvents(snapshot.Events, core.EventWorkerCreated, task.ID))
			}
		})
	}
}

func TestServiceRetriesDynamicReplanFailureFromCompletedGraph(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-retry-graph"
	workerID := "worker-done"
	initial := Plan{WorkerKind: "codex", Prompt: "implement the change"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Graph retry",
			"prompt": "Retry only replan.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(initial),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind": "codex",
			"metadata": map[string]any{
				"nodeID": "node-1",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "implemented",
			"workspaceChanges": WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskFailed,
			"error":  "dynamic replan failed: decode codex replan decision: invalid character '}' after top-level value",
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	retried, err := service.RetryTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Status != core.TaskPlanning {
		t.Fatalf("retry status = %q", retried.Status)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if snapshot.Tasks[0].FinalCandidateWorkerID != workerID {
		t.Fatalf("final candidate = %q, want %q", snapshot.Tasks[0].FinalCandidateWorkerID, workerID)
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 1 {
		t.Fatalf("retry reran a worker; worker.created count = %d", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
}

func TestServiceRetriesFinalCandidateSelectionFailureFromCompletedGraph(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-retry-final-candidate"
	workerID := "worker-impl"
	validationID := "worker-validation"
	initial := Plan{WorkerKind: "codex", Prompt: "implement the change"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Final candidate retry",
			"prompt": "Retry final candidate selection.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(initial)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{"kind": "codex", "metadata": map[string]any{"nodeID": "node-1"}}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "implemented",
			"workspaceChanges": WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: validationID,
		Payload: core.MustJSON(map[string]any{
			"kind":     "codex",
			"metadata": map[string]any{"nodeID": "node-2", "baseWorkerID": workerID, "spawnRole": "validation"},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: validationID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "validated",
			"workspaceChanges": WorkspaceChanges{
				DiffStat: "0 files changed, 0 insertions(+), 0 deletions(-)",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskFailed,
			"error":  `selected final candidate "worker-validation" is not a successful worker with candidate changes`,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	retried, err := service.RetryTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Status != core.TaskPlanning {
		t.Fatalf("retry status = %q", retried.Status)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if snapshot.Tasks[0].FinalCandidateWorkerID != workerID {
		t.Fatalf("final candidate = %q, want %q", snapshot.Tasks[0].FinalCandidateWorkerID, workerID)
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 2 {
		t.Fatalf("retry reran a worker; worker.created count = %d", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
}

func TestServiceRetriesFollowUpFailureFromCompletedGraph(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-retry-follow-up"
	workerID := "worker-impl"
	reviewID := "worker-review"
	initial := Plan{WorkerKind: "codex", Prompt: "implement the change"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Follow-up failure retry",
			"prompt": "Retry orchestration after review failure.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(initial)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{"kind": "codex", "metadata": map[string]any{"nodeID": "node-1"}}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "implemented",
			"workspaceChanges": WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: reviewID,
		Payload: core.MustJSON(map[string]any{
			"kind":     "claude",
			"metadata": map[string]any{"nodeID": "node-2", "baseWorkerID": workerID, "spawnRole": "review"},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: reviewID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerFailed,
			"error":  "worker command failed: exit status 1",
			"workspaceChanges": WorkspaceChanges{
				DiffStat: "0 files changed, 0 insertions(+), 0 deletions(-)",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskFailed,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	_, err := service.RetryTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if snapshot.Tasks[0].FinalCandidateWorkerID != workerID {
		t.Fatalf("final candidate = %q, want %q", snapshot.Tasks[0].FinalCandidateWorkerID, workerID)
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 2 {
		t.Fatalf("retry reran a worker; worker.created count = %d", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
}

func TestServiceRetryReusesCanceledWorkerWorkspaceAndSession(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-retry-resume"
	previousWorkerID := "worker-old"
	workspaceRoot := t.TempDir()
	sourceRoot := t.TempDir()
	freshWorkspaceRoot := t.TempDir()
	plan := Plan{WorkerKind: "codex", Prompt: "continue the partial implementation"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Resume canceled work",
			"prompt": "Continue the task.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":   previousWorkerID,
			"workerKind": "codex",
			"nodeId":     "node-old",
			"targetId":   "local",
			"targetKind": "local",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:          workspaceRoot,
			CWD:           workspaceRoot,
			SourceRoot:    sourceRoot,
			WorkspaceName: "aged-old",
			Mode:          string(WorkspaceModeIsolated),
			VCSType:       "jj",
			TaskID:        taskID,
			WorkerID:      previousWorkerID,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(worker.Event{
			Kind:   worker.EventLog,
			Stream: "stdout",
			Text:   `{"type":"thread.started","thread_id":"thread-1"}`,
			Raw:    json.RawMessage(`{"type":"thread.started","thread_id":"thread-1"}`),
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	runner := &recordingRunner{kind: "codex"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: plan}, map[string]worker.Runner{"codex": runner}, sourceRoot, fakeWorkspaceManager{
		cwd:        freshWorkspaceRoot,
		sourceRoot: sourceRoot,
	})

	if _, err := service.RetryTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if runner.workDir != workspaceRoot {
		t.Fatalf("runner workDir = %q, want retained workspace %q", runner.workDir, workspaceRoot)
	}
	if runner.resumeSessionID != "thread-1" {
		t.Fatalf("resume session = %q, want thread-1", runner.resumeSessionID)
	}
	if !strings.Contains(runner.prompt, "Previous worker ID: "+previousWorkerID) {
		t.Fatalf("runner prompt missing retry context:\n%s", runner.prompt)
	}
	if !strings.Contains(runner.prompt, "Run every command from this execution workspace:\n"+workspaceRoot) {
		t.Fatalf("runner prompt missing retained workspace:\n%s", runner.prompt)
	}
	if !eventPayloadContains(snapshot.Events, core.EventWorkerCreated, taskID, `"retryWorkspaceReused":true`) {
		t.Fatalf("missing retry workspace reuse metadata")
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, taskID, "retryResumeSessionID", "thread-1") {
		t.Fatalf("missing retry session metadata")
	}
}

func TestServiceGuardsWorkerPromptWithPreparedWorkspace(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	sourceRoot := filepath.Join(t.TempDir(), "source")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(sourceRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{kind: "codex"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "Inspect " + sourceRoot + " and make the requested edit.",
	}}, map[string]worker.Runner{"codex": runner}, sourceRoot, fakeWorkspaceManager{cwd: workspaceRoot, sourceRoot: sourceRoot})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do isolated work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if runner.workDir != workspaceRoot {
		t.Fatalf("runner workDir = %q, want %q", runner.workDir, workspaceRoot)
	}
	if !strings.Contains(runner.prompt, "Run every command from this execution workspace:\n"+workspaceRoot) {
		t.Fatalf("worker prompt did not name prepared workspace first:\n%s", runner.prompt)
	}
	if !strings.Contains(runner.prompt, "Do not edit the source checkout directly:\n"+sourceRoot) {
		t.Fatalf("worker prompt did not guard source checkout:\n%s", runner.prompt)
	}
	if !strings.Contains(runner.prompt, "Inspect "+sourceRoot+" and make the requested edit.") {
		t.Fatalf("worker prompt dropped original task:\n%s", runner.prompt)
	}
}

func TestRecoverRemoteWorkersCancelsStaleLocalWorkers(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-1"
	workerID := "worker-1"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Stale task",
			"prompt": "Was running before daemon restart",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":     "node-1",
			"workerId":   workerID,
			"workerKind": "codex",
			"targetId":   "local",
			"targetKind": "local",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind": "codex",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{}),
	}); err != nil {
		t.Fatal(err)
	}

	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[0].Status != core.TaskCanceled {
		t.Fatalf("task status = %q, want canceled", snapshot.Tasks[0].Status)
	}
	if snapshot.Workers[0].Status != core.WorkerCanceled {
		t.Fatalf("worker status = %q, want canceled", snapshot.Workers[0].Status)
	}
	if snapshot.ExecutionNodes[0].Status != core.WorkerCanceled {
		t.Fatalf("node status = %q, want canceled", snapshot.ExecutionNodes[0].Status)
	}
}

func TestRecoverRemoteWorkersResumesRunningTaskWithTerminalGraph(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-orphan-running-graph"
	workerID := "worker-impl"
	initial := Plan{WorkerKind: "codex", Prompt: "implement the cleanup"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Orphaned running graph",
			"prompt": "Recover after follow-up setup failure.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(initial)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":     "node-impl",
			"workerId":   workerID,
			"workerKind": "codex",
			"targetId":   "local",
			"targetKind": "local",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":     "codex",
			"metadata": map[string]any{"nodeID": "node-impl"},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "implemented",
			"workspaceChanges": WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "internal/cleanup.go", Status: "modified"}},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	followUp := Plan{
		WorkerKind: "codex",
		Prompt:     "review the cleanup",
		Metadata: map[string]any{
			"nodeID":       "node-review",
			"spawnID":      "review",
			"spawnRole":    "reviewer",
			"baseWorkerID": workerID,
		},
	}
	if _, err := store.Append(ctx, core.Event{Type: core.EventTaskPlanned, TaskID: taskID, Payload: core.MustJSON(followUp)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventExecutionStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"nodeId": "node-review",
			"status": core.WorkerFailed,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	brain := &replanningBrain{decisions: []ReplanDecision{{
		Action:    "complete",
		Rationale: "primary worker already produced the candidate",
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if !hasTaskAction(snapshot.Events, taskID, "startup_running_recovery", "resumed") {
		t.Fatalf("missing startup running recovery action")
	}
	if len(brain.states) != 1 || len(brain.states[0].Results) != 1 {
		t.Fatalf("replan states = %+v", brain.states)
	}
	if snapshot.Tasks[0].FinalCandidateWorkerID != workerID {
		t.Fatalf("final candidate = %q, want %q", snapshot.Tasks[0].FinalCandidateWorkerID, workerID)
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 1 {
		t.Fatalf("recovery reran a worker; worker.created count = %d", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
	}
}

func TestRecoverRemoteWorkersResumesOrphanedPullRequestFollowUpPlanning(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-dirty-pr"
	appendInterruptedPullRequestFollowUpPlanning(t, ctx, store, taskID)

	brain := &sequenceBrain{plans: []Plan{{
		WorkerKind: "repair",
		Prompt:     "repair dirty PR",
	}}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"repair": eventRunner{kind: "repair", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "repaired dirty PR branch",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/repair.go", Status: "modified"}},
		},
	})

	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForEvent(t, store, core.EventWorkerCreated, taskID)
	if !hasTaskAction(snapshot.Events, taskID, "startup_planning_recovery", "resumed") {
		t.Fatalf("missing startup planning recovery action")
	}
	if !hasWorkerCreated(snapshot.Events, taskID, "repair") {
		t.Fatalf("missing recovered repair worker")
	}
	if got := strings.Join(brain.steering, "\n"); !strings.Contains(got, "Merge status: DIRTY") {
		t.Fatalf("recovered planning did not preserve PR steering: %q", got)
	}
}

func TestRecoverRemoteWorkersMovesGenericOrphanedPlanningTaskToWaiting(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-orphan-planning"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Interrupted planning",
			"prompt": "Plan was interrupted by daemon restart.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskPlanning,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskWaiting)
	if !hasTaskAction(snapshot.Events, taskID, "startup_planning_recovery", "waiting") {
		t.Fatalf("missing startup planning recovery action")
	}
	if !hasEvent(snapshot.Events, core.EventApprovalNeeded, taskID, "") {
		t.Fatalf("missing approval-needed event")
	}
}

func TestCancelTaskCancelsPersistedActiveWorkers(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-replayed"
	workerIDs := []string{"worker-running", "worker-queued"}
	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Replayed task",
			"prompt": "Was active before daemon restart",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	for _, workerID := range workerIDs {
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"nodeId":     "node-" + workerID,
				"workerId":   workerID,
				"workerKind": "codex",
				"targetId":   "local",
				"targetKind": "local",
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
			}),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   taskID,
		WorkerID: "worker-running",
		Payload:  core.MustJSON(map[string]any{}),
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !taskHasActiveWorkers(snapshot, taskID) {
		t.Fatalf("taskHasActiveWorkers before cancel = false, want true")
	}

	if err := service.CancelTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if taskHasActiveWorkers(snapshot, taskID) {
		t.Fatalf("taskHasActiveWorkers after cancel = true, want false")
	}
	for _, worker := range snapshot.Workers {
		if worker.TaskID == taskID && worker.Status != core.WorkerCanceled {
			t.Fatalf("worker %s status = %q, want canceled", worker.ID, worker.Status)
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID == taskID && node.Status != core.WorkerCanceled {
			t.Fatalf("node %s status = %q, want canceled", node.ID, node.Status)
		}
	}
	if status := taskStatus(snapshot, taskID); status != core.TaskCanceled {
		t.Fatalf("task status = %q, want canceled", status)
	}
}

func TestCancelTaskAfterRestartReconstructsWorkersFromSnapshot(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-restarted"
	remoteWorkerID := "worker-recovered-remote"
	persistedWorkerID := "worker-persisted-local"
	remoteTarget := TargetConfig{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 1},
	}
	targets := NewTargetRegistry([]TargetConfig{remoteTarget})
	executor := &fakeRemoteExecutor{}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "run after restart",
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Restarted task",
			"prompt": "Was running before daemon restart",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	for _, spec := range []struct {
		workerID     string
		nodeID       string
		targetID     string
		kind         string
		workerEvents bool
		payload      map[string]any
	}{
		{
			workerID:     remoteWorkerID,
			nodeID:       "node-recovered-remote",
			targetID:     "vm-1",
			kind:         "ssh",
			workerEvents: true,
			payload: map[string]any{
				"remoteSession": "aged-recovered",
				"remoteRunDir":  "/runs/aged-recovered",
				"remoteWorkDir": "/repo",
			},
		},
		{
			workerID: persistedWorkerID,
			nodeID:   "node-persisted-local",
			targetID: "local",
			kind:     "local",
		},
	} {
		payload := map[string]any{
			"nodeId":     spec.nodeID,
			"workerId":   spec.workerID,
			"workerKind": "codex",
			"targetId":   spec.targetID,
			"targetKind": spec.kind,
		}
		for key, value := range spec.payload {
			payload[key] = value
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: spec.workerID,
			Payload:  core.MustJSON(payload),
		}); err != nil {
			t.Fatal(err)
		}
		if !spec.workerEvents {
			continue
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: spec.workerID,
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerStarted,
			TaskID:   taskID,
			WorkerID: spec.workerID,
			Payload:  core.MustJSON(map[string]any{}),
		}); err != nil {
			t.Fatal(err)
		}
	}

	liveCancelCalled := false
	service.cancels[remoteWorkerID] = func() {
		liveCancelCalled = true
	}
	service.remoteRuns[remoteWorkerID] = remoteRun{
		Target:   remoteTarget,
		Session:  "aged-recovered",
		RunDir:   "/runs/aged-recovered",
		WorkDir:  "/repo",
		TaskID:   taskID,
		WorkerID: remoteWorkerID,
		Status:   "running",
	}
	delete(service.tasks, remoteWorkerID)

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !taskHasActiveWorkers(snapshot, taskID) {
		t.Fatalf("taskHasActiveWorkers before cancel = false, want true")
	}
	workerIDs := activeTaskWorkerIDs(snapshot, taskID)
	if !reflect.DeepEqual(workerIDs, []string{persistedWorkerID, remoteWorkerID}) {
		t.Fatalf("activeTaskWorkerIDs before cancel = %+v, want persisted and remote worker IDs", workerIDs)
	}
	for _, worker := range snapshot.Workers {
		if worker.ID == persistedWorkerID {
			t.Fatalf("persisted worker unexpectedly has worker row before cancel: %+v", worker)
		}
	}

	if err := service.CancelTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	if !liveCancelCalled {
		t.Fatalf("recovered remote worker cancel func was not called")
	}
	foundKill := false
	for _, command := range executor.commands {
		joined := strings.Join(command, " ")
		if strings.Contains(joined, "kill-session") && strings.Contains(joined, "aged-recovered") {
			foundKill = true
			break
		}
	}
	if !foundKill {
		t.Fatalf("expected remote tmux kill command, got %+v", executor.commands)
	}

	snapshot, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, worker := range snapshot.Workers {
		if worker.TaskID == taskID && worker.Status != core.WorkerCanceled {
			t.Fatalf("worker %s status = %q, want canceled", worker.ID, worker.Status)
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID == taskID && node.Status != core.WorkerCanceled {
			t.Fatalf("node %s status = %q, want canceled", node.ID, node.Status)
		}
	}
	if status := taskStatus(snapshot, taskID); status != core.TaskCanceled {
		t.Fatalf("task status = %q, want canceled", status)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCompleted, taskID, "summary", "Worker was canceled from persisted execution node state.") {
		t.Fatalf("missing persisted execution-node cancellation event for worker without row")
	}
}

func TestCancelTaskUnknownTaskReturnsNotFoundWithoutCanceling(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, StaticBrain{WorkerKind: "mock"}, worker.DefaultRunners(), t.TempDir())
	taskCanceled := false
	workerCanceled := false
	service.taskCancels["missing-task"] = func() {
		taskCanceled = true
	}
	service.cancels["worker-orphan"] = func() {
		workerCanceled = true
	}
	service.tasks["worker-orphan"] = "missing-task"

	err := service.CancelTask(ctx, "missing-task")
	if !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("CancelTask error = %v, want ErrNotFound", err)
	}
	if taskCanceled {
		t.Fatalf("task cancel func was called for missing task")
	}
	if workerCanceled {
		t.Fatalf("worker cancel func was called for missing task")
	}

	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventTaskStatus, "missing-task") != 0 {
		t.Fatalf("missing task has %d task status events, want 0", countEvents(snapshot.Events, core.EventTaskStatus, "missing-task"))
	}
}

func TestCancelWorkerFallsBackToPersistedRemoteRun(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-remote"
	workerID := "worker-remote"
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1},
	}})
	executor := &fakeRemoteExecutor{}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote task",
			"prompt": "Was running before daemon restart",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":        "node-remote",
			"workerId":      workerID,
			"workerKind":    "codex",
			"targetId":      "vm-1",
			"targetKind":    "ssh",
			"remoteSession": "aged-worker",
			"remoteRunDir":  "/runs/aged-worker",
			"remoteWorkDir": "/repo",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind": "codex",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{"targetId": "vm-1", "session": "aged-worker"}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.CancelWorker(ctx, workerID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Workers[0].Status != core.WorkerCanceled {
		t.Fatalf("worker status = %q, want canceled", snapshot.Workers[0].Status)
	}
	foundKill := false
	for _, command := range executor.commands {
		joined := strings.Join(command, " ")
		if strings.Contains(joined, "kill-session") && strings.Contains(joined, "aged-worker") {
			foundKill = true
			break
		}
	}
	if !foundKill {
		t.Fatalf("expected remote tmux kill command, got %+v", executor.commands)
	}
}

func TestRemoteWorkerCallbackCreatesTaskThroughOriginalOrchestrator(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	callbackID := "create-task.20260511T000000Z.1.0"
	callbackOutput := "AGED-CALLBACK-FILE:" + callbackID + ".json\n" +
		`{"type":"create_task","promptBase64":"` + base64.StdEncoding.EncodeToString([]byte("follow up from remote")) + `","titleBase64":"` + base64.StdEncoding.EncodeToString([]byte("Remote follow-up")) + `","parentTaskIdBase64":"` + base64.StdEncoding.EncodeToString([]byte("task-parent")) + `","parentWorkerIdBase64":"` + base64.StdEncoding.EncodeToString([]byte("worker-parent")) + `"}` + "\n" +
		"AGED-CALLBACK-END\n"
	executor := &fakeRemoteExecutor{callbackOutput: callbackOutput}
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Parent", Prompt: "Run remote parent."})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found core.Task
	for _, candidate := range snapshot.Tasks {
		if candidate.Title == "Remote follow-up" {
			found = candidate
			break
		}
	}
	if found.ID == "" || found.Prompt != "follow up from remote" {
		t.Fatalf("missing created follow-up task: %+v", snapshot.Tasks)
	}
	source, externalID := taskExternalRef(found)
	if source != "remote-worker" || !strings.Contains(externalID, callbackID) {
		t.Fatalf("external ref = %q %q", source, externalID)
	}
	var metadata map[string]any
	if err := json.Unmarshal(found.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["completionMode"] != "github" {
		t.Fatalf("metadata = %+v, want remote follow-up to default to GitHub completion", metadata)
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "remote worker queued follow-up task") {
		t.Fatalf("missing parent worker callback event")
	}
}

func TestLocalWorkerCallbackCreatesTaskThroughOriginalOrchestrator(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	runner := &localCallbackRunner{kind: "callback"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "callback",
		Prompt:     "parent task creates a follow-up",
	}}, map[string]worker.Runner{"callback": runner}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Parent", Prompt: "Run local parent."})
	if err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if runner.parentWorkerID == "" {
		t.Fatalf("callback runner did not run; prompt:\n%s", runner.prompt)
	}
	var found core.Task
	for _, candidate := range snapshot.Tasks {
		if candidate.Title == "Local follow-up" {
			found = candidate
			break
		}
	}
	if found.ID == "" || found.Prompt != "follow up from local" {
		t.Fatalf("missing created follow-up task: %+v", snapshot.Tasks)
	}
	source, externalID := taskExternalRef(found)
	if source != "local-worker" || !strings.Contains(externalID, runner.parentWorkerID) {
		t.Fatalf("external ref = %q %q", source, externalID)
	}
	var metadata map[string]any
	if err := json.Unmarshal(found.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["parentTaskId"] != task.ID || metadata["parentWorkerId"] != runner.parentWorkerID {
		t.Fatalf("metadata = %+v, want parent ids %q %q", metadata, task.ID, runner.parentWorkerID)
	}
	if !strings.Contains(runner.prompt, "aged-create-task") {
		t.Fatalf("runner prompt missing task helper instructions:\n%s", runner.prompt)
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "local worker queued follow-up task") {
		t.Fatalf("missing parent worker callback event")
	}
}

func TestLocalWorkerCallbackPublishesPullRequestThroughOriginalOrchestrator(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	runner := &localPublishPRCallbackRunner{kind: "callback"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "callback",
		Prompt:     "publish an intermediate pull request",
	}}, map[string]worker.Runner{"callback": runner}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "loop.go", Status: "modified"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Parent",
		Prompt:   "Run local parent.",
		Metadata: core.MustJSON(map[string]any{"completionMode": "local"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return eventContains(snapshot.Events, core.EventWorkerOutput, "local worker published pull request")
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not record local worker PR publication; events = %+v", task.ID, snapshot.Events)
	})
	if runner.parentWorkerID == "" {
		t.Fatalf("callback runner did not run; prompt:\n%s", runner.prompt)
	}
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.publishCalls)
	}
	if publisher.published.WorkerID != runner.parentWorkerID {
		t.Fatalf("published worker = %q, want %q", publisher.published.WorkerID, runner.parentWorkerID)
	}
	if publisher.published.Title != "Local callback PR" || publisher.published.Body != "Callback PR body" || publisher.published.Repo != "owner/repo" {
		t.Fatalf("published spec = %+v", publisher.published)
	}
	if !strings.Contains(runner.prompt, "aged-publish-pr") {
		t.Fatalf("runner prompt missing publish helper instructions:\n%s", runner.prompt)
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "local worker published pull request") {
		t.Fatalf("missing parent worker publish event")
	}
}

func TestRemoteWorkerPublishPullRequestCallbackWithoutCandidateIsSkipped(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-callback-no-candidate"
	workerID := "worker-callback-no-candidate"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote parent",
			"prompt": "try to publish",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status":  core.WorkerSucceeded,
			"summary": "inspected the workspace but found no changes to publish",
			"workspaceChanges": WorkspaceChanges{
				Status: "The working copy has no changes.",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)

	err := service.handleRemoteWorkerCallbacks(ctx, remoteRun{
		TaskID:   taskID,
		WorkerID: workerID,
	}, []RemoteWorkerCallback{{
		ID:             "publish-pr.test",
		Type:           "publish_pull_request",
		ParentWorkerID: workerID,
		Title:          "Remote callback PR",
		Body:           "Callback PR body",
		Repo:           "owner/repo",
	}})
	if err != nil {
		t.Fatalf("handle remote callbacks returned error: %v", err)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want none without candidate changes", publisher.publishCalls)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTaskAction(snapshot.Events, taskID, "publish_pull_request", "skipped") {
		t.Fatalf("missing skipped publish_pull_request callback action")
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "remote worker skipped pull request publication") {
		t.Fatalf("missing worker output explaining skipped PR callback")
	}
	if eventPayloadContains(snapshot.Events, core.EventWorkerOutput, taskID, "failed to drain terminal remote worker callbacks") {
		t.Fatalf("callback was reported as a terminal drain failure")
	}
}

func TestRecoverRemoteWorkerResumesTaskAfterCompletion(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-recover-remote"
	workerID := "worker-recover-remote"
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: &fakeRemoteExecutor{}, PollInterval: time.Millisecond})

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote task",
			"prompt": "Was running before daemon restart",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(Plan{WorkerKind: "codex", Prompt: "run remotely"}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":        "node-remote",
			"workerId":      workerID,
			"workerKind":    "codex",
			"targetId":      "vm-1",
			"targetKind":    "ssh",
			"remoteSession": "aged-worker",
			"remoteRunDir":  "/runs/aged-worker",
			"remoteWorkDir": "/repo",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind": "codex",
			"metadata": map[string]any{
				"nodeID": "node-remote",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{"targetId": "vm-1", "session": "aged-worker"}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if snapshot.Tasks[0].FinalCandidateWorkerID != workerID {
		t.Fatalf("final candidate = %q, want %q", snapshot.Tasks[0].FinalCandidateWorkerID, workerID)
	}
}

func TestRecoverRemoteWorkerCancelDoesNotCancelTaskWithOtherActiveWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-recover-remote"
	canceledWorkerID := "worker-canceled"
	activeWorkerID := "worker-active"
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 1},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: &fakeRemoteExecutor{}, PollInterval: time.Millisecond})

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote task",
			"prompt": "Was running before daemon restart",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	for _, workerID := range []string{canceledWorkerID, activeWorkerID} {
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventExecutionPlanned,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"nodeId":        "node-" + workerID,
				"workerId":      workerID,
				"workerKind":    "codex",
				"targetId":      "vm-1",
				"targetKind":    "ssh",
				"remoteSession": "aged-" + workerID,
				"remoteRunDir":  "/runs/aged-" + workerID,
				"remoteWorkDir": "/repo",
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerCreated,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"kind": "codex",
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerStarted,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload:  core.MustJSON(map[string]any{"targetId": "vm-1", "session": "aged-" + workerID}),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: canceledWorkerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	service.resumeRecoveredRemoteTask(ctx, taskID)
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[0].Status != core.TaskRunning {
		t.Fatalf("task status = %q, want running while another worker remains active", snapshot.Tasks[0].Status)
	}
}

func TestRecoverRemoteWorkersReservesTargetCapacityDuringRecovery(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-reserve-capacity"
	workerID := "worker-reserve-capacity"
	targets := NewTargetRegistry([]TargetConfig{
		{
			ID:       "vm-1",
			Kind:     TargetKindSSH,
			Host:     "vm",
			WorkDir:  "/repo",
			WorkRoot: "/runs",
			Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1},
		},
		defaultLocalTargetConfig(),
	})
	executor := &gatedPollExecutor{}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "run remotely",
	}}, map[string]worker.Runner{"codex": eventRunner{kind: "codex"}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote task",
			"prompt": "Was running before daemon restart",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(Plan{WorkerKind: "codex", Prompt: "run remotely"}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskRunning,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":        "node-remote",
			"workerId":      workerID,
			"workerKind":    "codex",
			"targetId":      "vm-1",
			"targetKind":    "ssh",
			"remoteSession": "aged-worker",
			"remoteRunDir":  "/runs/aged-worker",
			"remoteWorkDir": "/repo",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":     "codex",
			"metadata": map[string]any{"nodeID": "node-remote"},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{"targetId": "vm-1", "session": "aged-worker"}),
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		t.Fatal(err)
	}

	waitForTargetRunning(t, targets, "vm-1", 1)
	if err := targets.Delete("vm-1"); err == nil || !strings.Contains(err.Error(), "running workers") {
		t.Fatalf("Delete during recovery err = %v, want \"running workers\"", err)
	}

	executor.complete()
	waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	waitForTargetRunning(t, targets, "vm-1", 0)
	if err := targets.Delete("vm-1"); err != nil {
		t.Fatalf("Delete after recovery err = %v, want nil", err)
	}
}

type gatedPollExecutor struct {
	mu   sync.Mutex
	done bool
}

func (e *gatedPollExecutor) complete() {
	e.mu.Lock()
	e.done = true
	e.mu.Unlock()
}

func (e *gatedPollExecutor) isDone() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.done
}

func (e *gatedPollExecutor) Run(_ context.Context, argv []string) (string, error) {
	joined := strings.Join(argv, " ")
	switch {
	case strings.Contains(joined, "status.json"):
		if e.isDone() {
			return `{"status":"succeeded","exit":0}`, nil
		}
		return `{"status":"running"}`, nil
	case strings.Contains(joined, "vcs.txt"):
		return "git\n", nil
	case strings.Contains(joined, "root.txt"):
		return "/repo\n", nil
	default:
		return "", nil
	}
}

func TestServiceAddsWorkerCompletionSummaryFromResultEvent(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	resultSummary := "implemented the requested change"
	changedFiles := []WorkspaceChangedFile{{Path: "internal/orchestrator/service_test.go", Status: "modified"}}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "summary",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"summary": eventRunner{
		kind: "summary",
		events: []worker.Event{
			worker.LogEvent("stdout", "starting work"),
			{
				Kind: worker.EventResult,
				Text: resultSummary,
			},
		},
	}}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: changedFiles,
			DiffStat:     "internal/orchestrator/service_test.go | 1 +",
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	payload := workerCompletedPayload(t, snapshot.Events, task.ID)
	if payload.Summary != resultSummary {
		t.Fatalf("summary = %q", payload.Summary)
	}
	if payload.LogCount != 1 {
		t.Fatalf("logCount = %d", payload.LogCount)
	}
	if len(payload.ChangedFiles) != 1 || payload.ChangedFiles[0] != changedFiles[0] {
		t.Fatalf("changedFiles = %+v", payload.ChangedFiles)
	}
	if !payload.WorkspaceChanges.Dirty {
		t.Fatalf("workspaceChanges.dirty = false")
	}
	if payload.Status != core.WorkerSucceeded {
		t.Fatalf("status = %q", payload.Status)
	}
}

func TestServiceMovesTaskToWaitingWhenWorkerNeedsInput(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "input",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"input": eventRunner{
		kind: "input",
		events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "approve dependency install?",
		}},
	}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	payload := workerCompletedPayload(t, snapshot.Events, task.ID)
	if payload.Status != core.WorkerWaiting {
		t.Fatalf("status = %q", payload.Status)
	}
	if !payload.NeedsInput {
		t.Fatalf("needsInput = false")
	}
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingUser || snapshot.Tasks[0].ObjectivePhase != "approval_needed" {
		t.Fatalf("objective = %q phase %q", snapshot.Tasks[0].ObjectiveStatus, snapshot.Tasks[0].ObjectivePhase)
	}
	if hasEvent(snapshot.Events, core.EventWorkerCleanup, task.ID, "") {
		t.Fatalf("waiting worker workspace should be retained")
	}
}

func TestServiceAutonomouslyContinuesWhenReplannerAnswersWorkerQuestion(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "ask",
			Prompt:     "ask for input",
		},
		decisions: []ReplanDecision{{
			Action:  "continue",
			Message: "Use the existing dependency.",
			Plan: &Plan{
				WorkerKind: "answer",
				Prompt:     "continue with autonomous answer",
			},
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"ask": eventRunner{kind: "ask", events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "Which dependency should I use?",
		}}},
		"answer": eventRunner{kind: "answer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "continued after orchestrator answer",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasEvent(snapshot.Events, core.EventApprovalNeeded, task.ID, "") {
		t.Fatalf("missing approval.needed event")
	}
	if !hasEvent(snapshot.Events, core.EventApprovalDecided, task.ID, "") {
		t.Fatalf("missing approval.decided event")
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "answer") {
		t.Fatalf("missing continuation worker")
	}
}

func TestServiceAutonomousQuestionContinuationRunsPlannedFollowUps(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "ask",
			Prompt:     "ask for input",
		},
		decisions: []ReplanDecision{{
			Action:  "continue",
			Message: "Use the existing dependency.",
			Plan: &Plan{
				WorkerKind: "answer",
				Prompt:     "continue with autonomous answer",
				Spawns: []SpawnRequest{{
					ID:         "review",
					Role:       "reviewer",
					Reason:     "Review the continuation output.",
					WorkerKind: "reviewer",
				}},
			},
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"ask": eventRunner{kind: "ask", events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "Which dependency should I use?",
		}}},
		"answer": eventRunner{kind: "answer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "continued after orchestrator answer",
		}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "reviewed continuation",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "answer") {
		t.Fatalf("missing continuation worker")
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "reviewer") {
		t.Fatalf("missing planned follow-up worker")
	}
}

func TestServiceResumesWaitingTaskWhenSteered(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &sequenceBrain{plans: []Plan{
		{WorkerKind: "ask", Prompt: "ask for input"},
		{WorkerKind: "answer", Prompt: "continue after feedback"},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"ask": eventRunner{kind: "ask", events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "Should I install a dependency?",
		}}},
		"answer": eventRunner{kind: "answer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "continued after user feedback",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "Use the existing package only."}); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasEvent(snapshot.Events, core.EventApprovalDecided, task.ID, "") {
		t.Fatalf("missing approval.decided event")
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "answer") {
		t.Fatalf("missing resumed worker")
	}
	if got := strings.Join(brain.steering, "\n"); !strings.Contains(got, "Should I install a dependency?") || !strings.Contains(got, "Use the existing package only.") {
		t.Fatalf("resume steering = %q", got)
	}
}

func TestServiceResumeWaitingTaskRunsPlannedFollowUps(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &sequenceBrain{plans: []Plan{
		{WorkerKind: "ask", Prompt: "ask for input"},
		{
			WorkerKind: "answer",
			Prompt:     "continue after feedback",
			Spawns: []SpawnRequest{{
				ID:         "review",
				Role:       "reviewer",
				Reason:     "Review the resumed output.",
				WorkerKind: "reviewer",
			}},
		},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"ask": eventRunner{kind: "ask", events: []worker.Event{{
			Kind: worker.EventNeedsInput,
			Text: "Should I install a dependency?",
		}}},
		"answer": eventRunner{kind: "answer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "continued after user feedback",
		}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "reviewed resumed work",
		}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "Use the existing package only."}); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "answer") {
		t.Fatalf("missing resumed worker")
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "reviewer") {
		t.Fatalf("missing planned follow-up worker")
	}
}

func TestServiceAskUserActionMovesTaskToWaiting(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "noop",
		Prompt:     "confirm profiling setup",
		Actions: []PlanAction{{
			Kind:   "ask_user",
			When:   "after_success",
			Reason: "perf setup is missing",
			Inputs: map[string]any{
				"question":   "Please install perf on the VM.",
				"summary":    "Profiling setup required.",
				"target":     "vm-a",
				"commands":   []any{"sudo apt-get install linux-perf"},
				"resumeHint": "Reply when perf works.",
			},
		}},
	}}, map[string]worker.Runner{"noop": eventRunner{
		kind: "noop",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "ready to profile",
		}},
	}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Profile", Prompt: "Run profiling"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingUser {
		t.Fatalf("objective = %q", snapshot.Tasks[0].ObjectiveStatus)
	}
	approval := latestEventOfType(snapshot.Events, core.EventApprovalNeeded, task.ID)
	if approval.ID == 0 {
		t.Fatalf("missing approval.needed event")
	}
	var payload map[string]any
	if err := json.Unmarshal(approval.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["reason"] != "ask_user" || payload["target"] != "vm-a" {
		t.Fatalf("approval payload = %+v", payload)
	}
	if commands, ok := payload["commands"].([]any); !ok || len(commands) != 1 {
		t.Fatalf("commands = %+v", payload["commands"])
	}
}

func TestServiceTreatsRecoverableWorkerFailureAsUserAction(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "fail",
		Prompt:     "run perf",
	}}, map[string]worker.Runner{"fail": failingRunner{
		kind: "fail",
		err:  errors.New("perf: command not found"),
	}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Profile", Prompt: "Run perf"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingUser {
		t.Fatalf("objective = %q", snapshot.Tasks[0].ObjectiveStatus)
	}
	approval := latestEventOfType(snapshot.Events, core.EventApprovalNeeded, task.ID)
	if approval.ID == 0 {
		t.Fatalf("missing approval.needed event")
	}
	var payload map[string]any
	if err := json.Unmarshal(approval.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["reason"] != "missing_tool" {
		t.Fatalf("reason = %v", payload["reason"])
	}
	if question, _ := payload["question"].(string); !strings.Contains(question, "perf: command not found") {
		t.Fatalf("question = %q", question)
	}
}

func TestServiceTreatsRecoverableDynamicReplanWorkerFailureAsUserAction(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &continueForTurnsBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "produce initial candidate",
		},
		continueTurns: maxConsecutiveUnproductiveReplanTurns + 10,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "initial candidate"}}},
		"follow": failingRunner{
			kind: "follow",
			err:  errors.New("unexpected status 401 Unauthorized: Missing bearer or basic authentication in header, url: https://api.openai.com/v1/responses"),
		},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Recover dynamic auth", Prompt: "Keep improving."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(brain.states) != 1 {
		t.Fatalf("replan states = %d, want 1", len(brain.states))
	}
	approval := latestEventOfType(snapshot.Events, core.EventApprovalNeeded, task.ID)
	if approval.ID == 0 {
		t.Fatalf("missing approval.needed event")
	}
	var payload map[string]any
	if err := json.Unmarshal(approval.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["reason"] != "worker_auth_required" {
		t.Fatalf("reason = %v", payload["reason"])
	}
	if hasTaskAction(snapshot.Events, task.ID, "worker_failure_recovery", "continued") {
		t.Fatalf("unexpected continued worker failure recovery")
	}
}

func TestServiceTreatsWorkflowScopePushRejectionAsRecoverable(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{
		errOnce: errors.New("push git branch: refusing to allow an OAuth App to create or update workflow `.github/workflows/ci.yml` without `workflow` scope"),
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "writer",
		Prompt:     "add CI workflow",
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "publish CI workflow",
			Inputs: map[string]any{"repo": "owner/repo", "base": "main", "body": "Publish CI workflow."},
		}},
	}}, map[string]worker.Runner{"writer": eventRunner{
		kind:   "writer",
		events: []worker.Event{{Kind: worker.EventResult, Text: "added CI workflow"}},
	}}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: ".github/workflows/ci.yml", Status: "added"}},
		},
	})
	service.SetPullRequestPublisher(publisher)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Add Formatting and Test CI",
		Prompt:   "Add GitHub Actions CI.",
		Metadata: core.MustJSON(map[string]any{"completionMode": "github"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if snapshot.Tasks[0].ObjectiveStatus != core.ObjectiveWaitingUser || snapshot.Tasks[0].ObjectivePhase != "approval_needed" {
		t.Fatalf("objective = %q/%q, want user approval needed", snapshot.Tasks[0].ObjectiveStatus, snapshot.Tasks[0].ObjectivePhase)
	}
	approval := latestEventOfType(snapshot.Events, core.EventApprovalNeeded, task.ID)
	if approval.ID == 0 {
		t.Fatalf("missing approval.needed event")
	}
	var payload map[string]any
	if err := json.Unmarshal(approval.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["reason"] != "github_workflow_scope_required" {
		t.Fatalf("approval reason = %v", payload["reason"])
	}
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want one blocked publish attempt", publisher.publishCalls)
	}
}

func TestServiceAppliesRetainedWorkerChanges(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	workspaceRoot := t.TempDir()
	changed := WorkspaceChangedFile{Path: "internal/example.txt", Status: "modified"}
	applyCalls := 0
	diffCalls := 0
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "writer",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"writer": fileWritingRunner{
		kind: "writer",
		path: changed.Path,
		body: "worker output\n",
	}}, t.TempDir(), fakeWorkspaceManager{
		cwd:        workspaceRoot,
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{changed},
		},
		applyCalls: &applyCalls,
		diff:       "diff --git a/internal/example.txt b/internal/example.txt\n",
		diffCalls:  &diffCalls,
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Do work",
		Prompt: "User request",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) != 1 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	review, err := service.ReviewWorkerChanges(ctx, snapshot.Workers[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if review.Changes.Diff == "" {
		t.Fatal("review diff is empty")
	}
	if diffCalls != 1 {
		t.Fatalf("diff calls = %d, want 1", diffCalls)
	}
	result, err := service.ApplyWorkerChanges(ctx, snapshot.Workers[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.AppliedFiles) != 1 || result.AppliedFiles[0] != changed {
		t.Fatalf("applied files = %+v", result.AppliedFiles)
	}
	if result.Method != "fake_merge" {
		t.Fatalf("method = %q", result.Method)
	}
	appliedSnapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(appliedSnapshot.Events, core.EventWorkerApplied, task.ID, snapshot.Workers[0].ID) {
		t.Fatalf("missing worker.changes_applied event")
	}
	if applyCalls != 1 {
		t.Fatalf("apply calls = %d, want 1", applyCalls)
	}
	if diffCalls != 1 {
		t.Fatalf("apply should not reread diff; diff calls = %d, want 1", diffCalls)
	}
	if _, err := service.ApplyWorkerChanges(ctx, snapshot.Workers[0].ID); err == nil {
		t.Fatal("second apply succeeded, want error")
	}
	if applyCalls != 1 {
		t.Fatalf("second apply changed apply calls to %d, want 1", applyCalls)
	}
}

func TestServiceRemoteApplyFailsWhenExplicitTaskProjectWasDeleted(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	defaultProject := core.Project{ID: "default", Name: "Default", LocalPath: t.TempDir(), DefaultBase: "main"}
	deletedProject := core.Project{ID: "deleted", Name: "Deleted", LocalPath: t.TempDir(), DefaultBase: "main"}
	if _, err := store.SaveProject(ctx, defaultProject, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveProject(ctx, deletedProject, false); err != nil {
		t.Fatal(err)
	}
	projects, err := NewProjectRegistry([]core.Project{defaultProject, deletedProject}, defaultProject.ID)
	if err != nil {
		t.Fatal(err)
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, defaultProject.LocalPath, fakeWorkspaceManager{})
	service.SetProjects(projects)

	taskID := "task-deleted-project"
	workerID := "worker-remote"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"projectId": deletedProject.ID,
			"title":     "Remote changes",
			"prompt":    "Apply remote patch.",
			"metadata":  map[string]any{"projectId": deletedProject.ID},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:       "/runs/remote",
			CWD:        "/checkouts/deleted",
			SourceRoot: "/checkouts/deleted",
			Mode:       "remote",
			VCSType:    "ssh",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerSucceeded,
			"workspaceChanges": WorkspaceChanges{
				Root:         "/runs/remote",
				CWD:          "/checkouts/deleted",
				Diff:         newFilePatch("remote.txt", "remote\n"),
				ChangedFiles: []WorkspaceChangedFile{{Path: "remote.txt", Status: "added"}},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskSucceeded,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	remoteApplyCalls := 0
	service.remoteApply = func(context.Context, core.Project, PreparedWorkspace, WorkspaceChanges) (WorkerApplyResult, error) {
		remoteApplyCalls++
		return WorkerApplyResult{}, nil
	}
	if err := service.DeleteProject(ctx, deletedProject.ID); err != nil {
		t.Fatal(err)
	}

	_, err = service.ApplyWorkerChanges(ctx, workerID)
	if err == nil || !strings.Contains(err.Error(), `unknown projectId "deleted"`) {
		t.Fatalf("apply err = %v, want missing explicit project", err)
	}
	if remoteApplyCalls != 0 {
		t.Fatalf("remote apply calls = %d, want 0", remoteApplyCalls)
	}
}

func TestServiceAppliesFinalTaskCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	changed := WorkspaceChangedFile{Path: "internal/example.txt", Status: "modified"}
	applyCalls := 0
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "writer",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"writer": fileWritingRunner{
		kind: "writer",
		path: changed.Path,
		body: "worker output\n",
	}}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{changed},
		},
		applyCalls: &applyCalls,
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if snapshot.Tasks[0].FinalCandidateWorkerID == "" {
		t.Fatalf("task final candidate was empty: %+v", snapshot.Tasks[0])
	}
	result, err := service.ApplyTaskResult(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkerID != snapshot.Tasks[0].FinalCandidateWorkerID {
		t.Fatalf("applied worker = %q, want final candidate %q", result.WorkerID, snapshot.Tasks[0].FinalCandidateWorkerID)
	}
	applied, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Tasks[0].AppliedWorkerID != snapshot.Tasks[0].FinalCandidateWorkerID {
		t.Fatalf("applied worker id = %q, want %q", applied.Tasks[0].AppliedWorkerID, snapshot.Tasks[0].FinalCandidateWorkerID)
	}
	if applyCalls != 1 {
		t.Fatalf("apply calls = %d, want 1", applyCalls)
	}
}

func TestServiceRepairsFinalTaskCandidateApplyConflict(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	changed := WorkspaceChangedFile{Path: "web/src/main.tsx", Status: "modified"}
	applyCalls := 0
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "writer",
			Prompt:     "worker prompt",
		},
		decisions: []ReplanDecision{{
			Action:    "complete",
			Rationale: "initial candidate is ready",
		}, {
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "writer",
				Prompt:     "repair local apply conflict against current checkout",
			},
		}, {
			Action:    "complete",
			Rationale: "repair worker is final",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{"writer": fileWritingRunner{
		kind: "writer",
		path: changed.Path,
		body: "worker output\n",
	}}, t.TempDir(), fakeWorkspaceManager{
		cwd:        t.TempDir(),
		sourceRoot: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{changed},
		},
		applyCalls:     &applyCalls,
		applyErr:       errors.New("merge git worker commit: conflict in web/src/main.tsx"),
		failApplyUntil: 1,
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Do work", Prompt: "User request"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	originalCandidate := snapshot.Tasks[0].FinalCandidateWorkerID
	if originalCandidate == "" {
		t.Fatalf("task final candidate was empty: %+v", snapshot.Tasks[0])
	}
	result, err := service.ApplyTaskResult(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !replanStatesContainResultError(brain.states, "local apply failed") {
		t.Fatalf("replan states did not include local apply failure context: %+v", brain.states)
	}
	if result.WorkerID == "" || result.WorkerID == originalCandidate {
		t.Fatalf("applied worker = %q, want repaired worker distinct from %q", result.WorkerID, originalCandidate)
	}
	applied, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Tasks[0].FinalCandidateWorkerID != result.WorkerID {
		t.Fatalf("final candidate = %q, want repaired worker %q", applied.Tasks[0].FinalCandidateWorkerID, result.WorkerID)
	}
	if applied.Tasks[0].AppliedWorkerID != result.WorkerID {
		t.Fatalf("applied worker id = %q, want %q", applied.Tasks[0].AppliedWorkerID, result.WorkerID)
	}
	if applyCalls != 2 {
		t.Fatalf("apply calls = %d, want failed original apply and repaired apply", applyCalls)
	}
	if !hasTaskAction(applied.Events, task.ID, "local_apply_recovery", "completed") {
		t.Fatalf("missing completed local apply recovery action")
	}
}

func TestServiceAppliesRemoteWorkerPatchArtifact(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	sourceRoot := t.TempDir()
	taskID := "task-remote"
	workerID := "worker-remote"
	changed := WorkspaceChangedFile{Path: "main.go", Status: "modified"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":    "Remote work",
			"prompt":   "Apply remote patch",
			"metadata": map[string]any{},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:          "/runs/" + workerID,
			CWD:           "/repo",
			SourceRoot:    "/repo",
			WorkspaceName: "aged-remote",
			Mode:          "remote",
			VCSType:       "ssh",
			TaskID:        taskID,
			WorkerID:      workerID,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerSucceeded,
			"workspaceChanges": WorkspaceChanges{
				Root:         "/runs/" + workerID,
				CWD:          "/repo",
				Mode:         "remote",
				VCSType:      "git",
				Dirty:        true,
				Diff:         "diff --git a/main.go b/main.go\n",
				ChangedFiles: []WorkspaceChangedFile{changed},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, sourceRoot, fakeWorkspaceManager{})
	applied := 0
	service.SetRemotePatchApplier(func(_ context.Context, project core.Project, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
		applied++
		if project.LocalPath != sourceRoot {
			t.Fatalf("project local path = %q, want %q", project.LocalPath, sourceRoot)
		}
		if workspace.VCSType != "ssh" || changes.Diff == "" || len(changes.ChangedFiles) != 1 {
			t.Fatalf("workspace=%+v changes=%+v", workspace, changes)
		}
		result := baseWorkerApplyResult(workspace, "remote_patch_apply")
		result.SourceRoot = project.LocalPath
		result.AppliedFiles = changes.ChangedFiles
		return result, nil
	})

	review, err := service.ReviewWorkerChanges(ctx, workerID)
	if err != nil {
		t.Fatal(err)
	}
	if review.Changes.Diff == "" || review.Changes.ChangedFiles[0] != changed {
		t.Fatalf("review changes = %+v", review.Changes)
	}
	result, err := service.ApplyWorkerChanges(ctx, workerID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Method != "remote_patch_apply" || result.SourceRoot != sourceRoot || len(result.AppliedFiles) != 1 {
		t.Fatalf("result = %+v", result)
	}
	if applied != 1 {
		t.Fatalf("applied calls = %d, want 1", applied)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(snapshot.Events, core.EventWorkerApplied, taskID, workerID) {
		t.Fatal("missing worker.changes_applied event")
	}
}

func TestServicePublishesRemoteWorkerPullRequestFromWorkerPatch(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	sourceRoot := t.TempDir()
	taskID := "task-remote-pr"
	workerID := "worker-remote-pr"
	changed := WorkspaceChangedFile{Path: "main.go", Status: "modified"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote PR",
			"prompt": "Publish remote patch",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:          "/runs/" + workerID,
			CWD:           "/repo",
			SourceRoot:    "/repo",
			WorkspaceName: "aged-remote",
			Mode:          "remote",
			VCSType:       "ssh",
			TaskID:        taskID,
			WorkerID:      workerID,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerSucceeded,
			"workspaceChanges": WorkspaceChanges{
				Root:         "/runs/" + workerID,
				CWD:          "/repo",
				Mode:         "remote",
				VCSType:      "git",
				Dirty:        true,
				Diff:         "diff --git a/main.go b/main.go\n",
				ChangedFiles: []WorkspaceChangedFile{changed},
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status":                 core.TaskSucceeded,
			"finalCandidateWorkerId": workerID,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, sourceRoot, fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)
	service.SetRemotePatchApplier(func(_ context.Context, project core.Project, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
		t.Fatalf("remote patch applier should not run while publishing an SSH worker PR: project=%+v workspace=%+v changes=%+v", project, workspace, changes)
		return WorkerApplyResult{}, nil
	})

	if _, err := service.PublishTaskPullRequest(ctx, taskID, core.PublishPullRequestRequest{
		Repo:     "owner/repo",
		Base:     "main",
		WorkerID: workerID,
	}); err != nil {
		t.Fatal(err)
	}
	if publisher.published.WorkDir != sourceRoot {
		t.Fatalf("published workDir = %q, want local source root %q", publisher.published.WorkDir, sourceRoot)
	}
	if !publisher.published.PatchFromBase {
		t.Fatal("published spec did not request patch-from-base publication")
	}
	if publisher.published.Patch != "diff --git a/main.go b/main.go\n" {
		t.Fatalf("published patch = %q", publisher.published.Patch)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if hasEvent(snapshot.Events, core.EventWorkerApplied, taskID, workerID) {
		t.Fatal("unexpected worker.changes_applied event")
	}
	if !hasEvent(snapshot.Events, core.EventPRPublished, taskID, "") {
		t.Fatal("missing pr.published event")
	}
}

func TestServiceSeparateTopLevelRemotePullRequestsStartFromProjectBase(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	sourceRoot := initGitTestRepo(t)
	runTestGit(t, sourceRoot, "branch", "-M", "main")
	remote := t.TempDir()
	runTestGit(t, remote, "init", "--bare")
	runTestGit(t, sourceRoot, "remote", "add", "origin", remote)
	runTestGit(t, sourceRoot, "push", "-u", "origin", "main")

	createCalls := 0
	publisher := LocalPullRequestPublisher{
		exec: func(ctx context.Context, dir string, name string, args ...string) (string, error) {
			switch {
			case name == "gh" && len(args) >= 2 && args[0] == "pr" && args[1] == "create":
				createCalls++
				if strings.Contains(strings.Join(args, " "), "first-pr") {
					return "https://github.com/owner/repo/pull/31", nil
				}
				return "https://github.com/owner/repo/pull/32", nil
			case name == "gh" && len(args) >= 3 && args[0] == "pr" && args[1] == "view":
				if strings.Contains(args[2], "/31") {
					return `{"number":31,"url":"https://github.com/owner/repo/pull/31","state":"OPEN","title":"First","isDraft":false,"headRefName":"first-pr","baseRefName":"main","mergeStateStatus":"CLEAN","statusCheckRollup":[],"reviewDecision":""}`, nil
				}
				return `{"number":32,"url":"https://github.com/owner/repo/pull/32","state":"OPEN","title":"Second","isDraft":false,"headRefName":"second-pr","baseRefName":"main","mergeStateStatus":"CLEAN","statusCheckRollup":[],"reviewDecision":""}`, nil
			default:
				return runCommand(ctx, dir, name, args...)
			}
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, sourceRoot, fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)

	appendRemotePublishCandidate := func(taskID string, workerID string, title string, diff string, changedPath string) {
		t.Helper()
		if _, err := store.Append(ctx, core.Event{
			Type:   core.EventTaskCreated,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"title":  title,
				"prompt": title,
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerWorkspace,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(PreparedWorkspace{
				Root:          "/runs/" + workerID,
				CWD:           "/repo",
				SourceRoot:    "/repo",
				WorkspaceName: "aged-remote",
				Mode:          "remote",
				VCSType:       "ssh",
				TaskID:        taskID,
				WorkerID:      workerID,
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   taskID,
			WorkerID: workerID,
			Payload: core.MustJSON(map[string]any{
				"status": core.WorkerSucceeded,
				"workspaceChanges": WorkspaceChanges{
					Root:    "/runs/" + workerID,
					CWD:     "/repo",
					Mode:    "remote",
					VCSType: "git",
					Dirty:   true,
					Diff:    diff,
					ChangedFiles: []WorkspaceChangedFile{{
						Path:   changedPath,
						Status: "added",
					}},
				},
			}),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(ctx, core.Event{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status":                 core.TaskSucceeded,
				"finalCandidateWorkerId": workerID,
			}),
		}); err != nil {
			t.Fatal(err)
		}
	}

	appendRemotePublishCandidate("task-one", "worker-one", "First task", newFilePatch("first.txt", "first\n"), "first.txt")
	if _, err := service.PublishTaskPullRequest(ctx, "task-one", core.PublishPullRequestRequest{
		Repo:     "owner/repo",
		Base:     "main",
		Branch:   "first-pr",
		WorkerID: "worker-one",
	}); err != nil {
		t.Fatal(err)
	}
	if contents := runTestGit(t, sourceRoot, "show", "first-pr:first.txt"); contents != "first\n" {
		t.Fatalf("first branch content = %q", contents)
	}
	if _, err := runCommand(ctx, sourceRoot, "git", "cat-file", "-e", "HEAD:first.txt"); err == nil {
		t.Fatal("source checkout still contains first task change after publishing")
	}

	appendRemotePublishCandidate("task-two", "worker-two", "Second task", newFilePatch("second.txt", "second\n"), "second.txt")
	if _, err := service.PublishTaskPullRequest(ctx, "task-two", core.PublishPullRequestRequest{
		Repo:     "owner/repo",
		Base:     "main",
		Branch:   "second-pr",
		WorkerID: "worker-two",
	}); err != nil {
		t.Fatal(err)
	}
	if createCalls != 2 {
		t.Fatalf("gh pr create calls = %d, want 2", createCalls)
	}
	if contents := runTestGit(t, sourceRoot, "show", "second-pr:second.txt"); contents != "second\n" {
		t.Fatalf("second branch content = %q", contents)
	}
	if _, err := runCommand(ctx, sourceRoot, "git", "cat-file", "-e", "second-pr:first.txt"); err == nil {
		t.Fatal("second top-level PR branch included the first task change")
	}
}

func newFilePatch(path string, body string) string {
	var builder strings.Builder
	builder.WriteString("diff --git a/")
	builder.WriteString(path)
	builder.WriteString(" b/")
	builder.WriteString(path)
	builder.WriteString("\nnew file mode 100644\n--- /dev/null\n+++ b/")
	builder.WriteString(path)
	builder.WriteString("\n@@ -0,0 +1 @@\n+")
	builder.WriteString(strings.TrimSuffix(body, "\n"))
	builder.WriteString("\n")
	return builder.String()
}

func TestServiceRecordsBenchmarkResultArtifact(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "benchmark_compare",
		Prompt:     "baseline: 10\ncandidate: 12\nthreshold_percent: 5\nhigher_is_better: true",
	}}, map[string]worker.Runner{
		"benchmark_compare": worker.BenchmarkCompareRunner{},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Bench", Prompt: "Compare benchmark result."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	task = snapshot.Tasks[0]
	if len(task.Artifacts) != 1 || task.Artifacts[0].Kind != "benchmark_report" {
		t.Fatalf("artifacts = %+v", task.Artifacts)
	}
	if !strings.Contains(string(task.Artifacts[0].Metadata), "deltaPercent") {
		t.Fatalf("artifact metadata = %s", task.Artifacts[0].Metadata)
	}
}

func TestServiceRunsSpawnedFollowUpWorkerWithPriorResultContext(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	implementationSummary := "implemented the first refactor slice"
	changed := WorkspaceChangedFile{Path: "internal/refactor.go", Status: "modified"}
	reviewer := &recordingEventRunner{
		kind: "claude",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "reviewed implementation",
		}},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Rationale:  "large refactor should start with one bounded implementation turn",
		Steps: []PlanStep{{
			Title:       "Implement slice",
			Description: "Make the first scoped code change.",
		}},
		Spawns: []SpawnRequest{{
			Role:   "reviewer",
			Reason: "Review the implementation output and recommend required follow-up fixes.",
		}},
	}}, map[string]worker.Runner{
		"codex": eventRunner{
			kind: "codex",
			events: []worker.Event{{
				Kind: worker.EventResult,
				Text: implementationSummary,
			}},
		},
		"claude": reviewer,
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{changed},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Large refactor",
		Prompt: "Refactor the subsystem and have another worker review it.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) != 2 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "codex") {
		t.Fatalf("missing initial codex worker")
	}
	if !hasWorkerCreated(snapshot.Events, task.ID, "claude") {
		t.Fatalf("missing follow-up claude reviewer worker")
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 2 {
		t.Fatalf("task.planned count = %d, want 2", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}

	prompt := reviewer.promptValue()
	for _, want := range []string{
		"Follow-up role:\nreviewer",
		implementationSummary,
		"modified internal/refactor.go",
		"Review the implementation output",
		"Benchmark Results",
		"Recommended Next Turns",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("follow-up prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestServiceContinuesAfterFailedFollowUpWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	reviewer := &flakyRunner{kind: "reviewer"}
	brain := &replanningBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Spawns: []SpawnRequest{{
			ID:         "review",
			Role:       "reviewer",
			Reason:     "Review the implementation output.",
			WorkerKind: "reviewer",
		}},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":    eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"reviewer": reviewer,
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/refactor.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Recover failed review",
		Prompt: "Implement, then review the candidate.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if snapshot.Tasks[0].FinalCandidateWorkerID == "" {
		t.Fatalf("missing final candidate: %+v", snapshot.Tasks[0])
	}
	if len(brain.states) != 1 || len(brain.states[0].Results) != 2 {
		t.Fatalf("replan states = %+v", brain.states)
	}
	if brain.states[0].Results[1].Status != core.WorkerFailed {
		t.Fatalf("follow-up status = %q, want failed", brain.states[0].Results[1].Status)
	}
	if reviewer.callsValue() != 1 {
		t.Fatalf("reviewer calls = %d, want 1", reviewer.callsValue())
	}
}

func TestServiceContinuesAfterFollowUpSetupError(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	prepareCalls := 0
	workspaceErr := errors.New("apply base worker patch in local workspace: corrupt patch")
	brain := &replanningBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Spawns: []SpawnRequest{{
			ID:         "review",
			Role:       "reviewer",
			Reason:     "Review the implementation output.",
			WorkerKind: "reviewer",
		}},
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":    eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: "reviewed"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:              t.TempDir(),
		prepareCalls:     &prepareCalls,
		failPrepareAfter: 1,
		prepareErr:       workspaceErr,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/refactor.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Recover failed setup",
		Prompt: "Implement, then review the candidate.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(brain.states) != 1 || len(brain.states[0].Results) != 2 {
		t.Fatalf("replan states = %+v", brain.states)
	}
	followUp := brain.states[0].Results[1]
	if followUp.Status != core.WorkerFailed || !strings.Contains(followUp.Error, "corrupt patch") {
		t.Fatalf("follow-up result = %+v", followUp)
	}
	if countEvents(snapshot.Events, core.EventTaskStatus, task.ID) == 0 {
		t.Fatalf("missing task status events")
	}
}

func TestServiceReplansAfterInitialWorkerSetupError(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	prepareCalls := 0
	workspaceErr := errors.New("prepare workspace: apply base worker patch in local workspace: corrupt patch")
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement the first attempt",
		},
		decisions: []ReplanDecision{{
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "codex",
				Prompt:     "retry with a repaired workspace handoff",
			},
		}},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented after recovery"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd:              t.TempDir(),
		prepareCalls:     &prepareCalls,
		failPrepareUntil: 1,
		prepareErr:       workspaceErr,
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/recovered.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Recover primary setup",
		Prompt: "Implement despite a setup failure.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(brain.states) != 2 {
		t.Fatalf("replan states = %+v", brain.states)
	}
	firstFailure := brain.states[0].Results[0]
	if firstFailure.Status != core.WorkerFailed || !strings.Contains(firstFailure.Error, "corrupt patch") {
		t.Fatalf("first failure = %+v", firstFailure)
	}
	if snapshot.Tasks[0].FinalCandidateWorkerID == "" {
		t.Fatalf("missing final candidate: %+v", snapshot.Tasks[0])
	}
	if !hasTaskAction(snapshot.Events, task.ID, "worker_failure_recovery", "started") {
		t.Fatalf("missing worker failure recovery action")
	}
}

func TestServiceBasesFollowUpWorkspaceOnLatestCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	workspace := &recordingWorkspaceManager{
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/refactor.go", Status: "modified"}},
		},
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Spawns: []SpawnRequest{{
			ID:         "review",
			Role:       "reviewer",
			Reason:     "Review the implementation output.",
			WorkerKind: "reviewer",
		}},
	}}, map[string]worker.Runner{
		"codex":    eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"reviewer": eventRunner{kind: "reviewer", events: []worker.Event{{Kind: worker.EventResult, Text: "reviewed"}}},
	}, t.TempDir(), workspace)

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Candidate review",
		Prompt: "Implement, then review the candidate.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if workspace.baseWorkDir == "" || workspace.baseRevision != "shared@" {
		t.Fatalf("follow-up base workdir=%q baseRevision=%q, want candidate workspace base", workspace.baseWorkDir, workspace.baseRevision)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "baseWorkerID", snapshot.Workers[0].ID) {
		t.Fatalf("missing baseWorkerID metadata on follow-up worker")
	}
}

func TestServiceRunsIndependentSpawnedWorkersInParallel(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan string, 2)
	release := make(chan struct{})
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Spawns: []SpawnRequest{
			{
				ID:         "review",
				Role:       "reviewer",
				Reason:     "Review the implementation output.",
				WorkerKind: "left",
			},
			{
				ID:         "test",
				Role:       "tester",
				Reason:     "Validate the implementation output.",
				WorkerKind: "right",
			},
		},
	}}, map[string]worker.Runner{
		"codex": eventRunner{
			kind: "codex",
			events: []worker.Event{{
				Kind: worker.EventResult,
				Text: "implemented the first slice",
			}},
		},
		"left":  &blockingEventRunner{kind: "left", started: started, release: release, summary: "left done"},
		"right": &blockingEventRunner{kind: "right", started: started, release: release, summary: "right done"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Parallel review",
		Prompt: "Implement, then review and test in parallel.",
	})
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	deadline := time.After(500 * time.Millisecond)
	for len(got) < 2 {
		select {
		case kind := <-started:
			got[kind] = true
		case <-deadline:
			t.Fatalf("spawned workers did not start in parallel; started = %+v", got)
		}
	}
	close(release)

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "left") || !hasWorkerCreated(snapshot.Events, task.ID, "right") {
		t.Fatalf("missing parallel spawned workers")
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 3 {
		t.Fatalf("task.planned count = %d, want 3", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
}

func TestServiceRunsInitialWorkersInParallel(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan string, 2)
	release := make(chan struct{})
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		Rationale: "independent initial investigation can run in parallel",
		Workers: []WorkerRequest{
			{
				ID:              "audit",
				Role:            "auditor",
				Reason:          "Inspect one side of the task.",
				WorkerKind:      "left",
				Prompt:          "Audit the left side.",
				ReasoningEffort: "low",
			},
			{
				ID:              "test",
				Role:            "tester",
				Reason:          "Inspect another side of the task.",
				WorkerKind:      "right",
				Prompt:          "Audit the right side.",
				ReasoningEffort: "low",
			},
		},
	}}, map[string]worker.Runner{
		"left":  &blockingEventRunner{kind: "left", started: started, release: release, summary: "left done"},
		"right": &blockingEventRunner{kind: "right", started: started, release: release, summary: "right done"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Parallel initial work",
		Prompt: "Run independent audits in parallel.",
	})
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	deadline := time.After(500 * time.Millisecond)
	for len(got) < 2 {
		select {
		case kind := <-started:
			got[kind] = true
		case <-deadline:
			t.Fatalf("initial workers did not start in parallel; started = %+v", got)
		}
	}
	close(release)

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "left") || !hasWorkerCreated(snapshot.Events, task.ID, "right") {
		t.Fatalf("missing initial workers")
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 1 {
		t.Fatalf("task.planned count = %d, want 1", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.SpawnID != "audit" && node.SpawnID != "test" {
			t.Fatalf("unexpected initial worker node spawn id: %+v", node)
		}
	}
}

func TestServiceHonorsInitialWorkerDependencies(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	firstStarted := make(chan string, 1)
	secondStarted := make(chan string, 1)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	second := &blockingEventRunner{kind: "second", started: secondStarted, release: secondRelease, summary: "second done"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		Rationale: "initial worker graph has a dependency",
		Workers: []WorkerRequest{
			{
				ID:         "inspect",
				Role:       "inspector",
				Reason:     "Inspect the current implementation.",
				WorkerKind: "first",
				Prompt:     "Inspect first.",
			},
			{
				ID:         "repair",
				Role:       "implementer",
				Reason:     "Repair issues found by inspection.",
				WorkerKind: "second",
				Prompt:     "Repair after inspection.",
				DependsOn:  []string{"inspect"},
			},
		},
	}}, map[string]worker.Runner{
		"first":  &blockingEventRunner{kind: "first", started: firstStarted, release: firstRelease, summary: "inspection summary"},
		"second": second,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Dependent initial graph",
		Prompt: "Inspect, then repair.",
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first initial worker did not start")
	}
	select {
	case <-secondStarted:
		t.Fatal("dependent initial worker started before dependency completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(firstRelease)
	select {
	case <-secondStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("dependent initial worker did not start after dependency completed")
	}
	close(secondRelease)

	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !strings.Contains(second.promptValue(), "inspection summary") {
		t.Fatalf("dependent prompt missing dependency summary:\n%s", second.promptValue())
	}
}

func TestServiceHonorsSpawnDependencies(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	firstStarted := make(chan string, 1)
	secondStarted := make(chan string, 1)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	second := &blockingEventRunner{kind: "second", started: secondStarted, release: secondRelease, summary: "second done"}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement the first bounded refactor slice",
		Spawns: []SpawnRequest{
			{
				ID:         "review",
				Role:       "reviewer",
				Reason:     "Review the implementation output.",
				WorkerKind: "first",
			},
			{
				ID:         "incorporate",
				Role:       "implementer",
				Reason:     "Incorporate required review feedback.",
				WorkerKind: "second",
				DependsOn:  []string{"review"},
			},
		},
	}}, map[string]worker.Runner{
		"codex": eventRunner{
			kind: "codex",
			events: []worker.Event{{
				Kind: worker.EventResult,
				Text: "implemented the first slice",
			}},
		},
		"first":  &blockingEventRunner{kind: "first", started: firstStarted, release: firstRelease, summary: "review summary"},
		"second": second,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Dependent follow-up",
		Prompt: "Implement, review, then incorporate feedback.",
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first spawned worker did not start")
	}
	select {
	case <-secondStarted:
		t.Fatal("dependent worker started before dependency completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(firstRelease)
	select {
	case <-secondStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("dependent worker did not start after dependency completed")
	}
	close(secondRelease)

	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !strings.Contains(second.promptValue(), "review summary") {
		t.Fatalf("dependent prompt missing dependency summary:\n%s", second.promptValue())
	}
}

func TestServiceDynamicallyReplansAfterFollowUpWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	implementer := &recordingEventRunner{
		kind: "codex",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "implemented the first slice",
		}},
	}
	reviewer := &recordingEventRunner{
		kind: "claude",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "review found a missing edge case",
		}},
	}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement first slice",
			Rationale:  "start with implementation",
			Spawns: []SpawnRequest{{
				Role:   "reviewer",
				Reason: "Review the initial implementation.",
			}},
		},
		decisions: []ReplanDecision{
			{
				Action:    "continue",
				Rationale: "review requested an incorporation turn",
				Plan: &Plan{
					WorkerKind: "codex",
					Prompt:     "incorporate reviewer feedback about the missing edge case",
					Rationale:  "review found a missing edge case",
					Steps: []PlanStep{{
						Title:       "Incorporate feedback",
						Description: "Fix the reviewed edge case.",
					}},
					RequiredApprovals: []ApprovalRequest{},
					Spawns:            []SpawnRequest{},
				},
			},
			{
				Action:    "complete",
				Rationale: "incorporation turn completed",
			},
		},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  implementer,
		"claude": reviewer,
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "internal/refactor.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Large refactor",
		Prompt: "Implement, review, then incorporate review feedback.",
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) != 3 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 3 {
		t.Fatalf("task.planned count = %d, want 3", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, task.ID) != 2 {
		t.Fatalf("task.replanned count = %d, want 2", countEvents(snapshot.Events, core.EventTaskReplanned, task.ID))
	}
	if !strings.Contains(implementer.promptValue(), "incorporate reviewer feedback") {
		t.Fatalf("last implementer prompt = %q", implementer.promptValue())
	}
	if len(brain.states) != 2 {
		t.Fatalf("replan states = %d, want 2", len(brain.states))
	}
	if len(brain.states[0].Results) != 2 {
		t.Fatalf("first replan results = %d, want 2", len(brain.states[0].Results))
	}
	if len(brain.states[1].Results) != 3 {
		t.Fatalf("second replan results = %d, want 3", len(brain.states[1].Results))
	}
}

func TestServiceDynamicReplanFollowUpHandsOffLocalBaseToRemoteTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	localRunner := &recordingEventRunner{
		kind: "codex",
		events: []worker.Event{{
			Kind: worker.EventResult,
			Text: "local candidate ready",
		}},
	}
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "produce the local candidate",
			Metadata: map[string]any{
				"retryTargetID": "local",
			},
		},
		decisions: []ReplanDecision{
			{
				Action:    "continue",
				Rationale: "validate on top of the candidate",
				Plan: &Plan{
					WorkerKind: "codex",
					Prompt:     "validate the local candidate",
				},
			},
			{
				Action:    "complete",
				Rationale: "validation completed",
			},
		},
	}
	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm-fast", Kind: TargetKindSSH, Host: "vm-fast", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
	})
	remoteExecutor := &fakeRemoteExecutor{}
	service := NewServiceWithWorkspaceManagerAndTargets(store, brain, map[string]worker.Runner{
		"codex": localRunner,
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "candidate.go", Status: "modified"}},
		},
		diff: "diff --git a/candidate.go b/candidate.go\n--- a/candidate.go\n+++ b/candidate.go\n@@ -1 +1 @@\n-old\n+new\n",
	}, targets, SSHRunner{Executor: remoteExecutor, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Dependent target inheritance",
		Prompt: "Run a dependent follow-up after a local candidate.",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 2 {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	if snapshot.ExecutionNodes[0].TargetID != "local" || snapshot.ExecutionNodes[0].TargetKind != "local" {
		t.Fatalf("first node should run locally; nodes = %+v", snapshot.ExecutionNodes)
	}
	if snapshot.ExecutionNodes[1].TargetID != "vm-fast" || snapshot.ExecutionNodes[1].TargetKind != "ssh" {
		t.Fatalf("dependent follow-up should move to remote target; nodes = %+v", snapshot.ExecutionNodes)
	}
	joinedCommands := strings.Join(flattenCommands(remoteExecutor.commands), "\n")
	if !strings.Contains(joinedCommands, "base.patch") || !strings.Contains(joinedCommands, "git apply") {
		t.Fatalf("remote handoff did not upload/apply base patch: %+v", remoteExecutor.commands)
	}
	if remoteExecutor.input == "" || !strings.Contains(remoteExecutor.input, "candidate.go") {
		t.Fatalf("uploaded base patch = %q", remoteExecutor.input)
	}
	if !eventPayloadContains(snapshot.Events, core.EventWorkerCreated, task.ID, `"baseHandoff":"patch"`) {
		t.Fatalf("missing base handoff metadata")
	}
}

func TestServiceCompletesWithFallbackWhenReplannerErrorsAfterSingleCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &errorReplanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement the change",
		},
		err: errors.New("decode codex replan decision: invalid character '}' after top-level value"),
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Fallback complete", Prompt: "Do it."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if snapshot.Tasks[0].FinalCandidateWorkerID == "" {
		t.Fatalf("missing final candidate: %+v", snapshot.Tasks[0])
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, task.ID) != 1 {
		t.Fatalf("task.replanned count = %d, want 1", countEvents(snapshot.Events, core.EventTaskReplanned, task.ID))
	}
}

func TestServiceSelectsLatestLeafWhenReplannerErrorsWithAmbiguousCandidates(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &errorReplanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement baseline",
			Spawns: []SpawnRequest{
				{ID: "left", Role: "left", Reason: "Try A.", WorkerKind: "left"},
				{ID: "right", Role: "right", Reason: "Try B.", WorkerKind: "right"},
			},
		},
		err: errors.New("decode codex replan decision: invalid character '}' after top-level value"),
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "baseline"}}},
		"left":  fileWritingRunner{kind: "left", path: "a.txt", body: "a"},
		"right": fileWritingRunner{kind: "right", path: "b.txt", body: "b"},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "candidate.txt", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Fallback wait", Prompt: "Try alternatives."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if snapshot.Tasks[0].FinalCandidateWorkerID == "" {
		t.Fatalf("missing final candidate: %+v", snapshot.Tasks[0])
	}
	var selectedRole string
	for _, node := range snapshot.ExecutionNodes {
		if node.WorkerID == snapshot.Tasks[0].FinalCandidateWorkerID {
			selectedRole = node.Role
			break
		}
	}
	if selectedRole != "right" {
		t.Fatalf("selected role = %q, want latest candidate leaf right; final=%q", selectedRole, snapshot.Tasks[0].FinalCandidateWorkerID)
	}
	if hasEvent(snapshot.Events, core.EventApprovalNeeded, task.ID, "") {
		t.Fatalf("unexpected approval-needed event")
	}
}

func TestServiceDoesNotExhaustTurnLimitWhileReplannerMakesProgress(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &continueForTurnsBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement initial slice",
		},
		continueTurns: maxConsecutiveUnproductiveReplanTurns + 1,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "initial"}}},
		"follow": eventRunner{kind: "follow", events: []worker.Event{{Kind: worker.EventResult, Text: "follow-up patch"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Replan limit", Prompt: "Keep improving the candidate."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(brain.states) != maxConsecutiveUnproductiveReplanTurns+2 {
		t.Fatalf("replan states = %d, want %d", len(brain.states), maxConsecutiveUnproductiveReplanTurns+2)
	}
	if snapshot.Tasks[0].FinalCandidateWorkerID == "" {
		t.Fatalf("missing final candidate: %+v", snapshot.Tasks[0])
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, task.ID) != maxConsecutiveUnproductiveReplanTurns+2 {
		t.Fatalf("task.replanned count = %d, want %d", countEvents(snapshot.Events, core.EventTaskReplanned, task.ID), maxConsecutiveUnproductiveReplanTurns+2)
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"fallback":true`) {
		t.Fatalf("unexpected fallback replanned event")
	}
	var finalKind string
	for _, worker := range snapshot.Workers {
		if worker.ID == snapshot.Tasks[0].FinalCandidateWorkerID {
			finalKind = worker.Kind
			break
		}
	}
	if finalKind != "follow" {
		t.Fatalf("final worker kind = %q, want latest dynamic follow worker", finalKind)
	}
}

func TestServiceCompletesWithFallbackWhenDynamicReplanningStallsPastLimit(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &continueForTurnsBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement initial candidate",
		},
		continueTurns: maxConsecutiveUnproductiveReplanTurns + 10,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "initial implementation"}}},
		"follow": failingRunner{kind: "follow", err: errors.New("no useful follow-up progress")},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Stalled replan", Prompt: "Keep trying follow-ups."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(brain.states) != maxConsecutiveUnproductiveReplanTurns {
		t.Fatalf("replan states = %d, want %d", len(brain.states), maxConsecutiveUnproductiveReplanTurns)
	}
	if countEvents(snapshot.Events, core.EventTaskReplanned, task.ID) != maxConsecutiveUnproductiveReplanTurns+1 {
		t.Fatalf("task.replanned count = %d, want %d", countEvents(snapshot.Events, core.EventTaskReplanned, task.ID), maxConsecutiveUnproductiveReplanTurns+1)
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"fallback":true`) {
		t.Fatalf("missing fallback replanned event")
	}
	if snapshot.Tasks[0].FinalCandidateWorkerID == "" {
		t.Fatalf("missing fallback final candidate: %+v", snapshot.Tasks[0])
	}
	var finalKind string
	for _, worker := range snapshot.Workers {
		if worker.ID == snapshot.Tasks[0].FinalCandidateWorkerID {
			finalKind = worker.Kind
			break
		}
	}
	if finalKind != "codex" {
		t.Fatalf("final worker kind = %q, want original candidate", finalKind)
	}
}

func TestServiceWaitsWhenDynamicReplanningStallsWithoutCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &continueForTurnsBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "attempt initial implementation",
		},
		continueTurns: maxConsecutiveUnproductiveReplanTurns + 10,
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  failingRunner{kind: "codex", err: errors.New("missing API token")},
		"follow": failingRunner{kind: "follow", err: errors.New("cannot run worker as root")},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Stalled no candidate", Prompt: "Keep trying until fixed."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(brain.states) != maxConsecutiveUnproductiveReplanTurns {
		t.Fatalf("replan states = %d, want %d", len(brain.states), maxConsecutiveUnproductiveReplanTurns)
	}
	if snapshot.Tasks[0].FinalCandidateWorkerID != "" {
		t.Fatalf("final candidate = %q, want empty", snapshot.Tasks[0].FinalCandidateWorkerID)
	}
	if !eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"fallback":true`) {
		t.Fatalf("missing fallback replanned event")
	}
	if !hasEvent(snapshot.Events, core.EventApprovalNeeded, task.ID, "") {
		t.Fatalf("missing approval-needed event")
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskReplanned, task.ID, `"action":"complete"`) {
		t.Fatalf("unexpected fallback completion")
	}
}

func TestServiceRunsSpawnedWorkersFromDynamicReplan(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan string, 2)
	release := make(chan struct{})
	brain := &replanningBrain{
		plan: Plan{
			WorkerKind: "codex",
			Prompt:     "implement initial slice",
		},
		decisions: []ReplanDecision{
			{
				Action: "continue",
				Plan: &Plan{
					WorkerKind: "codex",
					Prompt:     "incorporate the first result",
					Rationale:  "initial result needs review and validation",
					Spawns: []SpawnRequest{
						{
							ID:         "review",
							Role:       "reviewer",
							Reason:     "Review the incorporated result.",
							WorkerKind: "reviewer",
						},
						{
							ID:         "test",
							Role:       "tester",
							Reason:     "Validate the incorporated result.",
							WorkerKind: "tester",
						},
					},
				},
			},
			{
				Action:    "complete",
				Rationale: "implementation and spawned verification completed",
			},
		},
	}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{
			kind: "codex",
			events: []worker.Event{{
				Kind: worker.EventResult,
				Text: "codex turn done",
			}},
		},
		"reviewer": &blockingEventRunner{kind: "reviewer", started: started, release: release, summary: "review passed"},
		"tester":   &blockingEventRunner{kind: "tester", started: started, release: release, summary: "tests passed"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Dynamic spawn",
		Prompt: "Use dynamic replanning to schedule parallel verification.",
	})
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	deadline := time.After(500 * time.Millisecond)
	for len(got) < 2 {
		select {
		case kind := <-started:
			got[kind] = true
		case <-deadline:
			t.Fatalf("replanned spawned workers did not start in parallel; started = %+v", got)
		}
	}
	close(release)

	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if !hasWorkerCreated(snapshot.Events, task.ID, "reviewer") || !hasWorkerCreated(snapshot.Events, task.ID, "tester") {
		t.Fatalf("missing replanned spawned workers")
	}
	if countEvents(snapshot.Events, core.EventTaskPlanned, task.ID) != 4 {
		t.Fatalf("task.planned count = %d, want 4", countEvents(snapshot.Events, core.EventTaskPlanned, task.ID))
	}
	if len(brain.states) != 2 {
		t.Fatalf("replan states = %d, want 2", len(brain.states))
	}
	if len(brain.states[1].Results) != 4 {
		t.Fatalf("second replan results = %d, want 4", len(brain.states[1].Results))
	}
}

func TestServiceCompletesWithWorkerCreatedDuringDynamicReplan(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &continueThenSelectLatestBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement initial slice",
	}}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex":  eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "initial"}}},
		"follow": eventRunner{kind: "follow", events: []worker.Event{{Kind: worker.EventResult, Text: "follow-up patch"}}},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Dynamic final candidate", Prompt: "Patch then select the patch."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	var followWorkerID string
	for _, worker := range snapshot.Workers {
		if worker.Kind == "follow" {
			followWorkerID = worker.ID
			break
		}
	}
	if followWorkerID == "" {
		t.Fatalf("missing follow-up worker: %+v", snapshot.Workers)
	}
	if snapshot.Tasks[0].FinalCandidateWorkerID != followWorkerID {
		t.Fatalf("final candidate = %q, want dynamic worker %q", snapshot.Tasks[0].FinalCandidateWorkerID, followWorkerID)
	}
	if len(brain.states) != 2 || len(brain.states[1].Results) != 2 {
		t.Fatalf("replan states = %+v", brain.states)
	}
}

func TestServiceEmitsExecutionGraphNodes(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement first slice",
		Spawns: []SpawnRequest{{
			ID:         "review",
			Role:       "reviewer",
			Reason:     "Review the first slice.",
			WorkerKind: "claude",
		}},
	}}, map[string]worker.Runner{
		"codex":  eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		"claude": eventRunner{kind: "claude", events: []worker.Event{{Kind: worker.EventResult, Text: "reviewed"}}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Graph", Prompt: "Run graph task."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 2 {
		t.Fatalf("execution nodes = %+v", snapshot.ExecutionNodes)
	}
	if snapshot.ExecutionNodes[0].WorkerKind != "codex" || snapshot.ExecutionNodes[0].Status != core.WorkerSucceeded {
		t.Fatalf("primary node = %+v", snapshot.ExecutionNodes[0])
	}
	if snapshot.ExecutionNodes[1].SpawnID != "review" || snapshot.ExecutionNodes[1].ParentNodeID != snapshot.ExecutionNodes[0].ID {
		t.Fatalf("follow-up node = %+v, primary = %+v", snapshot.ExecutionNodes[1], snapshot.ExecutionNodes[0])
	}
}

func TestServiceDeliversSteeringToRunningWorker(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan struct{})
	gotSteering := make(chan string, 1)
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "steerable",
		Prompt:     "wait for steering",
	}}, map[string]worker.Runner{
		"steerable": steeringRunner{started: started, got: gotSteering},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Steer", Prompt: "Start and wait."})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "adjust course"}); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-gotSteering:
		if message != "adjust course" {
			t.Fatalf("steering = %q", message)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker did not receive steering")
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
}

func TestServiceSteerTaskMissingTaskReturnsNotFoundWithoutEvent(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	err := service.SteerTask(ctx, "missing-task", core.SteeringRequest{Message: "adjust course"})
	if !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("SteerTask error = %v, want ErrNotFound", err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEvents(snapshot.Events, core.EventTaskSteered, "missing-task") != 0 {
		t.Fatalf("task.steered events = %d, want 0", countEvents(snapshot.Events, core.EventTaskSteered, "missing-task"))
	}
}

func TestServiceRestartsNonSteerableRunningWorkerWithSteering(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan struct{})
	runner := &restartOnSteeringRunner{started: started}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "continue the investigation",
	}}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Steer restart", Prompt: "Start and wait."})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "adjust course"}); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if calls := runner.callsValue(); calls < 2 {
		t.Fatalf("runner calls = %d, want at least 2", calls)
	}
	if runner.resumeSessionIDValue() != "thread-1" {
		t.Fatalf("resume session id = %q", runner.resumeSessionIDValue())
	}
	prompt := runner.promptValue()
	if !strings.Contains(prompt, "Apply this user steering on the resumed turn") || !strings.Contains(prompt, "adjust course") {
		t.Fatalf("retry prompt did not include steering: %q", prompt)
	}
	if !hasTaskAction(snapshot.Events, task.ID, "steering_restart", "started") {
		t.Fatalf("missing started steering restart action")
	}
	if !hasTaskAction(snapshot.Events, task.ID, "steering_restart", "resumed") {
		t.Fatalf("missing resumed steering restart action")
	}
}

func TestServiceDeduplicatesConcurrentNonSteerableSteeringRestarts(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan struct{})
	retryStarted := make(chan struct{}, 2)
	retryRelease := make(chan struct{})
	runner := &restartOnSteeringRunner{
		started:      started,
		retryStarted: retryStarted,
		retryRelease: retryRelease,
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "continue the investigation",
	}}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Duplicate steer restart", Prompt: "Start and wait."})
	if err != nil {
		t.Fatal(err)
	}
	<-started

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "continue"})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-retryStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("retry worker did not start")
	}
	select {
	case <-retryStarted:
		t.Fatal("duplicate steering restart launched a second retry worker")
	case <-time.After(100 * time.Millisecond):
	}

	close(retryRelease)
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if calls := runner.callsValue(); calls != 2 {
		t.Fatalf("runner calls = %d, want 2", calls)
	}
	if created := countEvents(snapshot.Events, core.EventWorkerCreated, task.ID); created != 2 {
		t.Fatalf("worker.created count = %d, want 2", created)
	}
	if countTaskActions(snapshot.Events, task.ID, "steering_restart", "started") != 1 {
		t.Fatalf("steering restart started actions = %d, want 1", countTaskActions(snapshot.Events, task.ID, "steering_restart", "started"))
	}
	if countTaskActions(snapshot.Events, task.ID, "steering_restart", "skipped") != 1 {
		t.Fatalf("steering restart skipped actions = %d, want 1", countTaskActions(snapshot.Events, task.ID, "steering_restart", "skipped"))
	}
}

func TestServiceCancelTaskStopsPendingSteeringRestart(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	started := make(chan struct{})
	firstCancelSeen := make(chan struct{})
	firstCancelRelease := make(chan struct{})
	retryStarted := make(chan struct{}, 1)
	runner := &restartOnSteeringRunner{
		started:            started,
		firstCancelSeen:    firstCancelSeen,
		firstCancelRelease: firstCancelRelease,
		retryStarted:       retryStarted,
	}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "continue the investigation",
	}}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Cancel steering restart", Prompt: "Start and wait."})
	if err != nil {
		t.Fatal(err)
	}
	<-started

	if err := service.SteerTask(ctx, task.ID, core.SteeringRequest{Message: "continue"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstCancelSeen:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("initial worker was not canceled")
	}
	if err := service.CancelTask(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	close(firstCancelRelease)

	snapshot := waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasTaskAction(snapshot.Events, task.ID, "steering_restart", "skipped")
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("steering restart did not skip after task cancel; events = %+v", snapshot.Events)
	})
	if status := taskStatus(snapshot, task.ID); status != core.TaskCanceled {
		t.Fatalf("task status = %q, want canceled", status)
	}
	if created := countEvents(snapshot.Events, core.EventWorkerCreated, task.ID); created != 1 {
		t.Fatalf("worker.created count = %d, want 1", created)
	}
	select {
	case <-retryStarted:
		t.Fatal("steering restart launched a retry worker after task cancel")
	default:
	}
}

func TestServiceRecommendsFinalApplyPolicyForSelectedCandidate(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &finalSelectingBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement baseline",
		Spawns: []SpawnRequest{
			{ID: "opt-a", Role: "optimizer", Reason: "Try optimization A.", WorkerKind: "left"},
			{ID: "opt-b", Role: "optimizer", Reason: "Try optimization B.", WorkerKind: "right"},
		},
	}, role: "optimizer"}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "baseline"}}},
		"left":  fileWritingRunner{kind: "left", path: "a.txt", body: "a"},
		"right": fileWritingRunner{kind: "right", path: "b.txt", body: "b"},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "candidate.txt", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Apply policy", Prompt: "Try alternatives."})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	policy, err := service.RecommendApplyPolicy(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Strategy != "apply_final" {
		t.Fatalf("strategy = %q, policy = %+v", policy.Strategy, policy)
	}
	if len(policy.Candidates) < 2 {
		t.Fatalf("candidates = %+v", policy.Candidates)
	}
}

func TestServiceRecommendApplyPolicyMissingTaskDoesNotRecordEvent(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewService(store, fixedBrain{}, nil, t.TempDir())
	_, err := service.RecommendApplyPolicy(ctx, "missing-task")
	if !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if hasEvent(snapshot.Events, core.EventApplyPolicy, "missing-task", "") {
		t.Fatalf("recorded apply-policy event for missing task")
	}
}

func TestServiceWaitsOnAmbiguousCompetingCandidatesWithoutFinalSelection(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement baseline",
		Spawns: []SpawnRequest{
			{ID: "opt-a", Role: "optimizer", Reason: "Try optimization A.", WorkerKind: "left"},
			{ID: "opt-b", Role: "optimizer", Reason: "Try optimization B.", WorkerKind: "right"},
		},
	}}, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "baseline"}}},
		"left":  fileWritingRunner{kind: "left", path: "a.txt", body: "a"},
		"right": fileWritingRunner{kind: "right", path: "b.txt", body: "b"},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "candidate.txt", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Ambiguous candidates", Prompt: "Try alternatives."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if snapshot.Tasks[0].FinalCandidateWorkerID != "" {
		t.Fatalf("final candidate = %q, want empty", snapshot.Tasks[0].FinalCandidateWorkerID)
	}
	if !hasEvent(snapshot.Events, core.EventApprovalNeeded, task.ID, "") {
		t.Fatalf("missing approval-needed event")
	}
}

func TestLatestCandidateLeafExcludingSkipsBlockedCandidates(t *testing.T) {
	results := []WorkerTurnResult{{
		WorkerID: "base",
		Status:   core.WorkerSucceeded,
		Changes:  WorkspaceChanges{Dirty: true},
	}, {
		WorkerID:     "blocked-repair",
		BaseWorkerID: "base",
		Status:       core.WorkerSucceeded,
		Changes:      WorkspaceChanges{Dirty: true},
	}, {
		WorkerID: "alternative",
		Status:   core.WorkerSucceeded,
		Changes:  WorkspaceChanges{Dirty: true},
	}}

	workerID, _ := latestCandidateLeafExcluding(results, map[string]string{
		"blocked-repair": "publish conflict",
	})
	if workerID != "alternative" {
		t.Fatalf("workerID = %q, want alternative", workerID)
	}

	workerID, _ = latestCandidateLeafExcluding(results, map[string]string{
		"blocked-repair": "publish conflict",
		"alternative":    "publish conflict",
	})
	if workerID != "" {
		t.Fatalf("workerID = %q, want no unblocked candidate leaf", workerID)
	}
}

func TestServiceUsesExplicitReplanFinalCandidateForCompetingBranches(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	brain := &finalSelectingBrain{plan: Plan{
		WorkerKind: "codex",
		Prompt:     "implement baseline",
		Spawns: []SpawnRequest{
			{ID: "opt-a", Role: "left", Reason: "Try optimization A.", WorkerKind: "left"},
			{ID: "opt-b", Role: "right", Reason: "Try optimization B.", WorkerKind: "right"},
		},
	}, role: "right"}
	service := NewServiceWithWorkspaceManager(store, brain, map[string]worker.Runner{
		"codex": eventRunner{kind: "codex", events: []worker.Event{{Kind: worker.EventResult, Text: "baseline"}}},
		"left":  fileWritingRunner{kind: "left", path: "a.txt", body: "a"},
		"right": fileWritingRunner{kind: "right", path: "b.txt", body: "b"},
	}, t.TempDir(), fakeWorkspaceManager{
		cwd: t.TempDir(),
		changes: WorkspaceChanges{
			Dirty:        true,
			ChangedFiles: []WorkspaceChangedFile{{Path: "candidate.txt", Status: "modified"}},
		},
	})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Selected candidate", Prompt: "Try alternatives and choose one."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if snapshot.Tasks[0].FinalCandidateWorkerID == "" {
		t.Fatalf("missing final candidate: %+v", snapshot.Tasks[0])
	}
	var selected WorkerTurnResult
	for _, result := range brain.states[0].Results {
		if result.WorkerID == snapshot.Tasks[0].FinalCandidateWorkerID {
			selected = result
			break
		}
	}
	if selected.Role != "right" {
		t.Fatalf("selected role = %q, want right; final=%q results=%+v", selected.Role, snapshot.Tasks[0].FinalCandidateWorkerID, brain.states[0].Results)
	}
}

func TestResolveFinalCandidateUsesSingleChangedLineageWhenSelectionIsEmpty(t *testing.T) {
	workerID, reason, err := resolveFinalCandidate([]WorkerTurnResult{
		{
			WorkerID: "impl",
			Status:   core.WorkerSucceeded,
			Changes: WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
			},
		},
		{
			WorkerID:     "validation",
			Status:       core.WorkerSucceeded,
			BaseWorkerID: "impl",
			Changes: WorkspaceChanges{
				DiffStat: "0 files changed, 0 insertions(+), 0 deletions(-)",
			},
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if workerID != "impl" {
		t.Fatalf("workerID = %q, want impl", workerID)
	}
	if !strings.Contains(reason, "only successful worker with candidate changes") {
		t.Fatalf("reason = %q", reason)
	}
}

func TestResolveFinalCandidateDoesNotPublishAncestorForExplicitNoChangeSelection(t *testing.T) {
	workerID, reason, err := resolveFinalCandidate([]WorkerTurnResult{
		{
			WorkerID: "impl",
			Status:   core.WorkerSucceeded,
			Changes: WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "main.go", Status: "modified"}},
			},
		},
		{
			WorkerID:     "validation",
			Status:       core.WorkerSucceeded,
			BaseWorkerID: "impl",
			Summary:      "The current repository already contains the fix. The final worktree diff is empty.",
			Changes: WorkspaceChanges{
				DiffStat: "0 files changed, 0 insertions(+), 0 deletions(-)",
			},
		},
	}, "validation")
	if err != nil {
		t.Fatal(err)
	}
	if workerID != "" {
		t.Fatalf("workerID = %q, want no publishable candidate", workerID)
	}
	if !strings.Contains(reason, "no candidate changes") {
		t.Fatalf("reason = %q", reason)
	}
}

func TestServiceGithubCompletionWithSelectedNoChangeWorkerDoesNotPublishAncestor(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-no-change-final"
	implID := "worker-impl"
	finalID := "worker-no-change"
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Already fixed",
			"prompt": "Confirm whether issue 107 needs any new code changes.",
			"metadata": map[string]any{
				"completionMode": "github",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskReplanned,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"turn": 2,
			"decision": ReplanDecision{
				Action:                 "complete",
				FinalCandidateWorkerID: finalID,
				PullRequestBody:        "No new changes are needed; the fix is already present.",
				Rationale:              "The follow-up worker found a clean workspace.",
			},
		}),
	}); err != nil {
		t.Fatal(err)
	}

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{}, map[string]worker.Runner{}, t.TempDir(), fakeWorkspaceManager{})
	service.SetPullRequestPublisher(publisher)

	err := service.completeTask(ctx, taskID, []WorkerTurnResult{
		{
			WorkerID: implID,
			Status:   core.WorkerSucceeded,
			Changes: WorkspaceChanges{
				Dirty:        true,
				ChangedFiles: []WorkspaceChangedFile{{Path: "internal/orchestrator/plugins.go", Status: "modified"}},
			},
		},
		{
			WorkerID:     finalID,
			Status:       core.WorkerSucceeded,
			BaseWorkerID: implID,
			Summary:      "The intended fix is already present in HEAD and the final worktree diff is empty.",
			Changes: WorkspaceChanges{
				DiffStat: "0 files changed, 0 insertions(+), 0 deletions(-)",
			},
		},
	}, finalID, "The follow-up worker found a clean workspace.")
	if err != nil {
		t.Fatal(err)
	}

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want no PR for selected no-change final worker", publisher.publishCalls)
	}
	if eventPayloadContains(snapshot.Events, core.EventTaskCandidate, taskID, `"workerId":"`+implID+`"`) {
		t.Fatalf("older changed ancestor was recorded as final candidate")
	}
}

func TestServiceRunsWorkerOnSSHTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Labels:   map[string]string{"role": "remote"},
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 4},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "remote",
		Prompt:     "run remotely",
		Metadata: map[string]any{
			"targetLabels": map[string]any{"role": "remote"},
		},
	}}, map[string]worker.Runner{
		"remote": buildOnlyRunner{kind: "remote", command: []string{"sh", "-lc", "echo remote output"}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: &fakeRemoteExecutor{}, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Remote", Prompt: "Run on VM."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	node := snapshot.ExecutionNodes[0]
	if node.TargetID != "vm-1" || node.TargetKind != "ssh" || node.RemoteSession == "" {
		t.Fatalf("node = %+v", node)
	}
	if len(snapshot.Workers) != 1 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	remoteWorker := snapshot.Workers[0]
	wantPrompt := remoteWorkerExecutionPrompt("run remotely", PreparedWorkspace{CWD: "/repo/default", Mode: "remote", VCSType: "ssh"})
	if remoteWorker.Prompt != wantPrompt {
		t.Fatalf("worker prompt = %q, want %q", remoteWorker.Prompt, wantPrompt)
	}
	if !strings.Contains(remoteWorker.Prompt, "do not ask the follow-up task to open a draft pull request unless the user explicitly requested a draft PR") {
		t.Fatalf("worker prompt missing draft PR guard:\n%s", remoteWorker.Prompt)
	}
	if remoteWorker.PromptPath != "/runs/"+remoteWorker.ID+"/prompt.txt" {
		t.Fatalf("worker prompt path = %q", remoteWorker.PromptPath)
	}
	if !hasEvent(snapshot.Events, core.EventWorkerOutput, task.ID, remoteWorker.ID) {
		t.Fatalf("missing remote worker output")
	}
}

func TestServiceRemoteClaudeWorkerUploadsPromptForPrintStdin(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	executor := &fakeRemoteExecutor{}
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 4},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "claude",
		Prompt:     "review remotely",
	}}, map[string]worker.Runner{
		"claude": worker.DefaultRunners()["claude"],
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Remote Claude", Prompt: "Run Claude on VM."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) != 1 {
		t.Fatalf("workers = %+v", snapshot.Workers)
	}
	if got, want := executor.input, snapshot.Workers[0].Prompt; got != want {
		t.Fatalf("uploaded prompt = %q, want worker prompt %q", got, want)
	}
	if !strings.Contains(executor.input, "review remotely") {
		t.Fatalf("uploaded prompt missing plan prompt: %q", executor.input)
	}
}

func TestServiceRemotePluginWorkerUploadsRunnerSpecStdin(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	executor := &fakeRemoteExecutor{}
	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 4},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind:      "review-plugin",
		Prompt:          "review remotely",
		ReasoningEffort: "high",
	}}, map[string]worker.Runner{
		"review-plugin": worker.NewPluginRunner("review-plugin", []string{"aged-review-plugin"}),
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Remote plugin", Prompt: "Run plugin on VM."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.Workers) != 1 || len(snapshot.ExecutionNodes) != 1 {
		t.Fatalf("workers = %+v nodes = %+v", snapshot.Workers, snapshot.ExecutionNodes)
	}
	remoteWorker := snapshot.Workers[0]
	node := snapshot.ExecutionNodes[0]
	expected, err := worker.PluginRunnerStdin(worker.Spec{
		ID:              remoteWorker.ID,
		TaskID:          task.ID,
		Kind:            "review-plugin",
		Prompt:          remoteWorker.Prompt,
		WorkDir:         node.RemoteWorkDir,
		ReasoningEffort: "high",
	})
	if err != nil {
		t.Fatal(err)
	}
	if executor.input != expected {
		t.Fatalf("uploaded plugin stdin = %q, want %q", executor.input, expected)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(executor.input), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["prompt"] != remoteWorker.Prompt || !strings.Contains(remoteWorker.Prompt, "review remotely") {
		t.Fatalf("plugin stdin prompt = %v, worker prompt = %q", payload["prompt"], remoteWorker.Prompt)
	}
	if payload["workDir"] != node.RemoteWorkDir {
		t.Fatalf("plugin stdin workDir = %v, want %q", payload["workDir"], node.RemoteWorkDir)
	}
	joinedCommands := strings.Join(flattenCommands(executor.commands), "\n")
	if !strings.Contains(joinedCommands, "aged-review-plugin") || !strings.Contains(joinedCommands, "run") {
		t.Fatalf("remote command did not start plugin runner: %+v", executor.commands)
	}
}

func TestServiceRemoteWorkerUsesProjectCheckoutOverride(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-1",
		Kind:     TargetKindSSH,
		Host:     "vm",
		WorkDir:  "/repo-root",
		WorkRoot: "/runs",
		Labels:   map[string]string{"role": "remote"},
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 4},
	}})
	executor := &fakeRemoteExecutor{}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "remote",
		Prompt:     "run remotely",
		Metadata: map[string]any{
			"targetLabels": map[string]any{"role": "remote"},
		},
	}}, map[string]worker.Runner{
		"remote": buildOnlyRunner{kind: "remote", command: []string{"sh", "-lc", "echo remote output"}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	if _, err := service.CreateProject(ctx, core.Project{
		ID:              "node",
		LocalPath:       t.TempDir(),
		Repo:            "owner/node",
		RemoteCheckouts: map[string]string{"vm-1": "/custom/node"},
		TargetLabels:    map[string]string{"role": "remote"},
	}); err != nil {
		t.Fatal(err)
	}
	task, err := service.CreateTask(ctx, core.CreateTaskRequest{ProjectID: "node", Title: "Remote", Prompt: "Run on VM."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	if snapshot.ExecutionNodes[0].RemoteWorkDir != "/custom/node" {
		t.Fatalf("remote workdir = %q, want override", snapshot.ExecutionNodes[0].RemoteWorkDir)
	}
	joinedCommands := strings.Join(flattenCommands(executor.commands), "\n")
	if !strings.Contains(joinedCommands, "/custom/node") {
		t.Fatalf("remote commands did not use checkout override: %+v", executor.commands)
	}
}

func TestServiceRetryReusesRemoteWorkerTargetWorkspaceAndSession(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	taskID := "task-remote-retry"
	previousWorkerID := "worker-remote-old"
	plan := Plan{WorkerKind: "codex", Prompt: "continue remote work"}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Remote retry",
			"prompt": "Continue the remote task.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":      previousWorkerID,
			"workerKind":    "codex",
			"nodeId":        "node-remote-old",
			"targetId":      "vm-old",
			"targetKind":    "ssh",
			"remoteSession": "aged-old",
			"remoteRunDir":  "/runs/old",
			"remoteWorkDir": "/repo-old",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(PreparedWorkspace{
			Root:          "/runs/old",
			CWD:           "/repo-old",
			SourceRoot:    "/repo-old",
			WorkspaceName: "aged-old",
			Mode:          "remote",
			VCSType:       "ssh",
			TaskID:        taskID,
			WorkerID:      previousWorkerID,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(worker.Event{
			Kind:   worker.EventLog,
			Stream: "stdout",
			Text:   `{"type":"thread.started","thread_id":"thread-remote"}`,
			Raw:    json.RawMessage(`{"type":"thread.started","thread_id":"thread-remote"}`),
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   taskID,
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"status": core.WorkerCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
		}),
	}); err != nil {
		t.Fatal(err)
	}

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-new", Kind: TargetKindSSH, Host: "new-vm", WorkDir: "/repo-new", WorkRoot: "/runs", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 10}},
		{ID: "vm-old", Kind: TargetKindSSH, Host: "old-vm", WorkDir: "/repo-default", WorkRoot: "/runs", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	runner := &recordingBuildRunner{kind: "codex"}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: plan}, map[string]worker.Runner{
		"codex": runner,
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: &fakeRemoteExecutor{}, PollInterval: time.Millisecond})

	if _, err := service.RetryTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if runner.spec.WorkDir != "/repo-old" {
		t.Fatalf("remote retry work dir = %q, want /repo-old", runner.spec.WorkDir)
	}
	if runner.spec.ResumeSessionID != "thread-remote" {
		t.Fatalf("remote retry session = %q, want thread-remote", runner.spec.ResumeSessionID)
	}
	if !strings.Contains(runner.spec.Prompt, "Previous worker ID: "+previousWorkerID) {
		t.Fatalf("remote retry prompt missing context:\n%s", runner.spec.Prompt)
	}
	newNode := latestExecutionNodeForTask(snapshot, taskID, previousWorkerID)
	if newNode.TargetID != "vm-old" || newNode.RemoteWorkDir != "/repo-old" {
		t.Fatalf("new execution node = %+v", newNode)
	}
	if newNode.RemoteRunDir == "/runs/old" || newNode.RemoteSession == "aged-old" {
		t.Fatalf("remote retry should allocate a fresh run/session for logs: %+v", newNode)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, taskID, "retryResumeSessionID", "thread-remote") {
		t.Fatalf("missing remote retry session metadata")
	}
	if !eventPayloadContains(snapshot.Events, core.EventWorkerCreated, taskID, `"retryWorkspaceReused":true`) {
		t.Fatalf("missing remote retry workspace reuse metadata")
	}
}

func TestServiceIgnoresSchedulerTargetLabels(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewLocalTargetRegistry()
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run local work",
		Metadata: map[string]any{
			"targetLabels": map[string]any{"role": "frontend"},
		},
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Local", Prompt: "Run locally."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].TargetID != "local" {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	if !eventContains(snapshot.Events, core.EventWorkerCreated, "ignoredTargetLabels") {
		t.Fatalf("missing ignored target label metadata")
	}
}

func TestServiceIgnoresSchedulerRequiredTargetID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 100}},
		{ID: "pinned", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run normal work",
		Metadata: map[string]any{
			"requiredTargetID": "pinned",
		},
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Normal", Prompt: "Run normally."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].TargetID != "local" {
		t.Fatalf("nodes = %+v, want scheduler requiredTargetID ignored and local selected", snapshot.ExecutionNodes)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "ignoredRequiredTargetID", "pinned") {
		t.Fatalf("missing ignored required target metadata")
	}
}

func TestServiceTaskRequiredTargetIDWinsOverSchedulerRequiredTargetID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 100}},
		{ID: "scheduler-pinned", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "task-pinned", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run pinned work",
		Metadata: map[string]any{
			"requiredTargetID": "scheduler-pinned",
		},
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Pinned",
		Prompt:   "Run on task-pinned.",
		Metadata: core.MustJSON(map[string]any{"requiredTargetID": "task-pinned"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].TargetID != "task-pinned" {
		t.Fatalf("nodes = %+v, want task requiredTargetID to win", snapshot.ExecutionNodes)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "ignoredRequiredTargetID", "scheduler-pinned") {
		t.Fatalf("missing ignored scheduler required target metadata")
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "requiredTargetID", "task-pinned") {
		t.Fatalf("missing task required target metadata")
	}
}

func TestServiceUsesTaskTargetLabels(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Labels: map[string]string{"location": "local"}, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "frontend", Kind: TargetKindLocal, Labels: map[string]string{"role": "frontend"}, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run frontend work",
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:    "Frontend",
		Prompt:   "Run on frontend target.",
		Metadata: core.MustJSON(map[string]any{"targetLabels": map[string]any{"role": "frontend"}}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].TargetID != "frontend" {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
}

func TestServiceFollowUpCanMoveAwayFromBaseWorkerTarget(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	baseWorkerID := "base-worker"
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-target-inheritance",
		WorkerID: baseWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":   baseWorkerID,
			"workerKind": "codex",
			"nodeId":     "node-base",
			"targetId":   "local",
			"targetKind": "local",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm-fast", Kind: TargetKindSSH, Host: "vm-fast", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	target, err := service.selectExecutionTarget(ctx, Plan{
		WorkerKind: "mock",
		Prompt:     "follow up",
		Metadata: map[string]any{
			"baseWorkerID": baseWorkerID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "vm-fast" {
		t.Fatalf("target = %q, want vm-fast", target.ID)
	}
}

func TestServiceRetryInheritsPreviousWorkerTargetWithoutRetryTargetID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	previousWorkerID := "previous-worker"
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-retry-target-inheritance",
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":   previousWorkerID,
			"workerKind": "codex",
			"nodeId":     "node-previous",
			"targetId":   "vm-previous",
			"targetKind": "ssh",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
		{ID: "vm-previous", Kind: TargetKindSSH, Host: "vm-previous", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	target, err := service.selectExecutionTarget(ctx, Plan{
		WorkerKind: "mock",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryFromWorkerID": previousWorkerID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "vm-previous" {
		t.Fatalf("target = %q, want vm-previous", target.ID)
	}
}

func TestServiceRetryFallsBackWhenExplicitRetryTargetIsUnhealthy(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-bad", Kind: TargetKindSSH, Host: "vm-bad", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm-good", Kind: TargetKindSSH, Host: "vm-good", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-bad", core.TargetHealth{Status: "unhealthy"}, core.TargetResources{})
	targets.UpdateHealth("vm-good", core.TargetHealth{Status: "ok", Reachable: true, Tmux: true, RepoPresent: true}, core.TargetResources{CPUCount: 4, Load1: 0.1, MemoryAvailableMB: 8192})

	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "mock",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryTargetID": "vm-bad",
		},
	}
	target, err := service.selectExecutionTarget(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "vm-good" {
		t.Fatalf("target = %q, want vm-good", target.ID)
	}
	if plan.Metadata["retryTargetFallbackFromID"] != "vm-bad" {
		t.Fatalf("fallback from = %v, want vm-bad", plan.Metadata["retryTargetFallbackFromID"])
	}
	if plan.Metadata["retryTargetFallbackToID"] != "vm-good" {
		t.Fatalf("fallback to = %v, want vm-good", plan.Metadata["retryTargetFallbackToID"])
	}
	if reason, _ := plan.Metadata["retryTargetFallbackReason"].(string); reason == "" {
		t.Fatalf("missing retryTargetFallbackReason")
	}
}

func TestServiceRetryTargetIDFallsBackWhenTargetLacksRequestedWorkerTool(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
		{ID: "vm-pinned", Kind: TargetKindSSH, Host: "vm-pinned", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-pinned", core.TargetHealth{
		Status:    "ok",
		Reachable: true,
		Tmux:      true,
		Tools:     map[string]bool{"codex": false},
	}, core.TargetResources{})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "codex",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryTargetID": "vm-pinned",
		},
	}
	target, err := service.selectExecutionTarget(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "local" {
		t.Fatalf("target = %q, want local", target.ID)
	}
	if plan.Metadata["retryTargetFallbackFromID"] != "vm-pinned" {
		t.Fatalf("fallback from = %v, want vm-pinned", plan.Metadata["retryTargetFallbackFromID"])
	}
	if reason, _ := plan.Metadata["retryTargetFallbackReason"].(string); !strings.Contains(reason, `execution target "vm-pinned" does not support worker kind "codex"`) {
		t.Fatalf("fallback reason = %q, want unsupported worker kind", reason)
	}
}

func TestServiceSelectsAnotherTargetAfterWorkerKindMarkedUnavailable(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-new", Kind: TargetKindSSH, Host: "vm-new", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 10}},
		{ID: "vm-old", Kind: TargetKindSSH, Host: "vm-old", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	if !targets.MarkWorkerKindUnavailable("vm-new", "codex", "missing auth") {
		t.Fatalf("failed to mark target worker kind unavailable")
	}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	target, err := service.selectExecutionTarget(ctx, Plan{
		WorkerKind: "codex",
		Prompt:     "retry elsewhere",
	})
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "vm-old" {
		t.Fatalf("target = %q, want vm-old", target.ID)
	}
}

func TestServiceSelectsLocalAfterAllRemoteWorkerKindsMarkedUnavailable(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-a", Kind: TargetKindSSH, Host: "vm-a", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 10}},
		{ID: "vm-b", Kind: TargetKindSSH, Host: "vm-b", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.MarkWorkerKindUnavailable("vm-a", "codex", "missing auth")
	targets.MarkWorkerKindUnavailable("vm-b", "codex", "missing auth")
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "codex",
		Prompt:     "retry locally",
		Metadata:   map[string]any{},
	}
	target, err := service.selectExecutionTarget(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "local" || target.Kind != TargetKindLocal {
		t.Fatalf("target = %+v, want local", target)
	}
	if plan.Metadata["retryTargetFallbackToID"] != "local" {
		t.Fatalf("fallback to = %v, want local", plan.Metadata["retryTargetFallbackToID"])
	}
}

func TestServiceRetryFallsBackWhenPreviousWorkerTargetIsUnhealthy(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	previousWorkerID := "previous-worker-fallback"
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-retry-target-fallback",
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":   previousWorkerID,
			"workerKind": "mock",
			"nodeId":     "node-previous",
			"targetId":   "vm-bad",
			"targetKind": "ssh",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-bad", Kind: TargetKindSSH, Host: "vm-bad", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm-good", Kind: TargetKindSSH, Host: "vm-good", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-bad", core.TargetHealth{Status: "unhealthy"}, core.TargetResources{})
	targets.UpdateHealth("vm-good", core.TargetHealth{Status: "ok", Reachable: true, Tmux: true, RepoPresent: true}, core.TargetResources{CPUCount: 4, Load1: 0.1, MemoryAvailableMB: 8192})

	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "mock",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryFromWorkerID": previousWorkerID,
		},
	}
	target, err := service.selectExecutionTarget(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "vm-good" {
		t.Fatalf("target = %q, want vm-good", target.ID)
	}
	if plan.Metadata["retryTargetFallbackFromID"] != "vm-bad" {
		t.Fatalf("fallback from = %v, want vm-bad", plan.Metadata["retryTargetFallbackFromID"])
	}
	if plan.Metadata["retryTargetFallbackToID"] != "vm-good" {
		t.Fatalf("fallback to = %v, want vm-good", plan.Metadata["retryTargetFallbackToID"])
	}
}

func TestServiceRetryTargetReuseFallsBackWhenTargetLacksRequestedWorkerTool(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	previousWorkerID := "previous-worker"
	if _, err := store.Append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   "task-retry-target-tool",
		WorkerID: previousWorkerID,
		Payload: core.MustJSON(map[string]any{
			"workerId":   previousWorkerID,
			"workerKind": "codex",
			"nodeId":     "node-previous",
			"targetId":   "vm-previous",
			"targetKind": "ssh",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
		{ID: "vm-previous", Kind: TargetKindSSH, Host: "vm-previous", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-previous", core.TargetHealth{
		Status:    "ok",
		Reachable: true,
		Tmux:      true,
		Tools:     map[string]bool{"codex": false},
	}, core.TargetResources{})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "codex",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryFromWorkerID": previousWorkerID,
		},
	}
	target, err := service.selectExecutionTarget(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "local" {
		t.Fatalf("target = %q, want local", target.ID)
	}
	if plan.Metadata["retryTargetFallbackFromID"] != "vm-previous" {
		t.Fatalf("fallback from = %v, want vm-previous", plan.Metadata["retryTargetFallbackFromID"])
	}
	if reason, _ := plan.Metadata["retryTargetFallbackReason"].(string); !strings.Contains(reason, `execution target "vm-previous" does not support worker kind "codex"`) {
		t.Fatalf("fallback reason = %q, want unsupported worker kind", reason)
	}
}

func TestServiceSelectExecutionTargetEnforcesRequiredTargetID(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 100}},
		{ID: "pinned", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	target, err := service.selectExecutionTarget(ctx, Plan{
		WorkerKind: "mock",
		Prompt:     "must run pinned",
		Metadata:   map[string]any{"requiredTargetID": "pinned"},
	})
	if err != nil {
		t.Fatalf("selectExecutionTarget err = %v, want nil", err)
	}
	if target.ID != "pinned" {
		t.Fatalf("target = %q, want pinned", target.ID)
	}
}

func TestServiceSelectExecutionTargetFailsWhenRequiredTargetUnknown(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 100}},
	})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	_, err := service.selectExecutionTarget(ctx, Plan{
		WorkerKind: "mock",
		Prompt:     "must run pinned",
		Metadata:   map[string]any{"requiredTargetID": "missing"},
	})
	if err == nil {
		t.Fatal("selectExecutionTarget with unknown requiredTargetID succeeded, want hard error (no local fallback)")
	}
	if !strings.Contains(err.Error(), `required execution target "missing"`) {
		t.Fatalf("err = %v, want error mentioning required target", err)
	}
}

func TestServiceRetryReturnsErrorWhenNoFallbackTargetEligible(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-only", Kind: TargetKindSSH, Host: "vm-only", WorkDir: "/repo", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-only", core.TargetHealth{Status: "unhealthy"}, core.TargetResources{})

	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "mock",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryTargetID": "vm-only",
		},
	}
	_, err := service.selectExecutionTarget(ctx, plan)
	if err == nil {
		t.Fatal("expected error when no fallback target is eligible")
	}
	if !strings.Contains(err.Error(), "vm-only") {
		t.Fatalf("error should mention the original target; err = %v", err)
	}
	if _, fellBack := plan.Metadata["retryTargetFallbackToID"]; fellBack {
		t.Fatalf("plan should not record fallback metadata when no eligible target exists: %+v", plan.Metadata)
	}
}

func TestServiceRetryFallbackRespectsTargetLabels(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{
		{ID: "vm-bad", Kind: TargetKindSSH, Host: "vm-bad", WorkDir: "/repo", Labels: map[string]string{"role": "gpu"}, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm-other", Kind: TargetKindSSH, Host: "vm-other", WorkDir: "/repo", Labels: map[string]string{"role": "cpu"}, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
	})
	targets.UpdateHealth("vm-bad", core.TargetHealth{Status: "unhealthy"}, core.TargetResources{})
	targets.UpdateHealth("vm-other", core.TargetHealth{Status: "ok", Reachable: true, Tmux: true, RepoPresent: true}, core.TargetResources{CPUCount: 4, Load1: 0.1, MemoryAvailableMB: 8192})

	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{})

	plan := Plan{
		WorkerKind: "mock",
		Prompt:     "retry",
		Metadata: map[string]any{
			"retryTargetID": "vm-bad",
			"targetLabels":  map[string]string{"role": "gpu"},
		},
	}
	_, err := service.selectExecutionTarget(ctx, plan)
	if err == nil {
		t.Fatal("expected error: no healthy target matches the required labels")
	}
}

func TestServiceRegisterTargetProbesImmediately(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	executor := &fakeRemoteExecutor{probeOutput: strings.Join([]string{
		"checkoutRootOK=true",
		"tmux=false",
		"repoPresent=false",
		"cpuCount=4",
		"load1=0.3",
	}, "\n")}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{WorkerKind: "mock", Prompt: "noop"}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, NewLocalTargetRegistry(), SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	_, err := service.RegisterTarget(ctx, core.TargetConfig{
		ID:       "vm-1",
		Kind:     "ssh",
		Host:     "vm.local",
		WorkDir:  "/repo",
		WorkRoot: "/runs",
		Capacity: core.TargetCapacity{MaxWorkers: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range snapshot.Targets {
		if target.ID == "vm-1" {
			if target.Health.Status != "unhealthy" || !strings.Contains(target.Health.Error, "tmux") || target.Resources.CPUCount != 4 {
				t.Fatalf("target health = %+v resources = %+v", target.Health, target.Resources)
			}
			return
		}
	}
	t.Fatalf("missing registered target: %+v", snapshot.Targets)
}

func appendInterruptedPullRequestFollowUpPlanning(t *testing.T, ctx context.Context, store eventstore.Store, taskID string) {
	t.Helper()
	if _, err := store.Append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"title":  "Repair dirty PR",
			"prompt": "Fix the dirty pull request branch.",
		}),
	}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []core.Event{
		{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskWaiting,
			}),
		},
		{
			Type:   core.EventPRPublished,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":           "pr-1",
				"repo":         "owner/repo",
				"number":       7,
				"url":          "https://github.com/owner/repo/pull/7",
				"branch":       "codex/aged-test",
				"base":         "main",
				"title":        "Repair dirty PR",
				"state":        "OPEN",
				"checksStatus": "passing",
				"mergeStatus":  "DIRTY",
				"reviewStatus": "",
				"metadata":     map[string]any{"workerId": "worker-original"},
			}),
		},
		{
			Type:   core.EventPRFollowUp,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"id":      "pr-1",
				"attempt": 1,
				"reason":  "pull_request_needs_work",
			}),
		},
		{
			Type:   core.EventTaskSteered,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"message": pullRequestFollowUpPrompt(core.PullRequest{
					ID:           "pr-1",
					TaskID:       taskID,
					Repo:         "owner/repo",
					Number:       7,
					URL:          "https://github.com/owner/repo/pull/7",
					Branch:       "codex/aged-test",
					Base:         "main",
					State:        "OPEN",
					ChecksStatus: "passing",
					MergeStatus:  "DIRTY",
				}),
			}),
		},
		{
			Type:   core.EventApprovalDecided,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"approved": true,
				"answer":   "resume dirty PR follow-up",
				"reason":   "user_feedback",
			}),
		},
		{
			Type:   core.EventTaskStatus,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskPlanning,
			}),
		},
	} {
		if _, err := store.Append(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
}

func ptrInt(value int) *int {
	return &value
}

func ptrString(value string) *string {
	return &value
}

type fixedBrain struct {
	plan Plan
	err  error
}

func (b fixedBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return b.plan, b.err
}

type fixedAssistantBrain struct {
	fixedBrain
	answer string
}

func (b fixedAssistantBrain) Ask(_ context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	return core.AssistantResponse{
		ConversationID: req.ConversationID,
		Message:        b.answer,
		Metadata:       core.MustJSON(map[string]any{"brain": "test"}),
	}, nil
}

type recordingAssistant struct {
	requests []core.AssistantRequest
}

func (a *recordingAssistant) Ask(_ context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	a.requests = append(a.requests, req)
	sessionID := nonEmpty(req.ProviderSessionID, "session-1")
	return core.AssistantResponse{
		ConversationID:    req.ConversationID,
		Message:           "answer",
		Provider:          "codex",
		ProviderSessionID: sessionID,
		Metadata:          core.MustJSON(map[string]any{"assistant": "codex", "providerSessionId": sessionID}),
	}, nil
}

type fakePullRequestPublisher struct {
	published        PullRequestPublishSpec
	publishedSpecs   []PullRequestPublishSpec
	publishedWorkers []string
	publishCalls     int
	updated          PullRequestPublishSpec
	updatedPR        core.PullRequest
	updateCalls      int
	errOnce          error
	errCount         int
	status           core.PullRequest
	inspectCalls     int
	list             []core.PullRequest
	listSpec         PullRequestListSpec
}

type prPublishingServiceOptions struct {
	brain      BrainProvider
	runners    map[string]worker.Runner
	publisher  *fakePullRequestPublisher
	workDir    string
	cwd        string
	sourceRoot string
	changes    WorkspaceChanges
	applyCalls *int
}

func newPRPublishingService(t *testing.T, store eventstore.Store, opts prPublishingServiceOptions) (*Service, *fakePullRequestPublisher) {
	t.Helper()
	publisher := opts.publisher
	if publisher == nil {
		publisher = &fakePullRequestPublisher{}
	}
	if opts.runners == nil {
		opts.runners = map[string]worker.Runner{
			"change": eventRunner{kind: "change", events: []worker.Event{{Kind: worker.EventResult, Text: "implemented"}}},
		}
	}
	if opts.workDir == "" {
		opts.workDir = t.TempDir()
	}
	if opts.cwd == "" {
		opts.cwd = t.TempDir()
	}
	if opts.sourceRoot == "" {
		opts.sourceRoot = t.TempDir()
	}
	service := NewServiceWithWorkspaceManager(store, opts.brain, opts.runners, opts.workDir, fakeWorkspaceManager{
		cwd:        opts.cwd,
		sourceRoot: opts.sourceRoot,
		changes:    opts.changes,
		applyCalls: opts.applyCalls,
	})
	service.SetPullRequestPublisher(publisher)
	return service, publisher
}

type fakeTitleGenerator struct {
	title string
	err   error
}

func (g fakeTitleGenerator) GenerateTitle(context.Context, string) (string, error) {
	return g.title, g.err
}

func (p *fakePullRequestPublisher) Publish(_ context.Context, spec PullRequestPublishSpec) (core.PullRequest, error) {
	p.published = spec
	p.publishCalls++
	p.publishedSpecs = append(p.publishedSpecs, spec)
	p.publishedWorkers = append(p.publishedWorkers, spec.WorkerID)
	if p.errCount > 0 && p.publishCalls <= p.errCount {
		return core.PullRequest{}, errors.New("remote patch has conflicts or no longer applies cleanly; patch does not apply")
	}
	if p.errOnce != nil && p.publishCalls == 1 {
		return core.PullRequest{}, p.errOnce
	}
	branch := strings.TrimSpace(spec.Branch)
	if branch == "" {
		branch = defaultPRBranch(spec)
	}
	return core.PullRequest{
		ID:           fmt.Sprintf("pr-%d", p.publishCalls),
		TaskID:       spec.TaskID,
		Repo:         spec.Repo,
		Number:       11 + p.publishCalls,
		URL:          fmt.Sprintf("https://github.com/%s/pull/%d", spec.Repo, 11+p.publishCalls),
		Branch:       branch,
		Base:         nonEmpty(spec.Base, "main"),
		Title:        spec.Title,
		State:        "OPEN",
		Draft:        spec.Draft,
		ChecksStatus: "pending",
		MergeStatus:  "UNKNOWN",
		ReviewStatus: "REVIEW_REQUIRED",
		Metadata:     core.MustJSON(spec.Metadata),
	}, nil
}

func (p *fakePullRequestPublisher) Update(_ context.Context, pr core.PullRequest, spec PullRequestPublishSpec) (core.PullRequest, error) {
	p.updated = spec
	p.updatedPR = pr
	p.updateCalls++
	if p.errCount > 0 && p.updateCalls <= p.errCount {
		return core.PullRequest{}, errors.New("remote patch has conflicts or no longer applies cleanly; patch does not apply")
	}
	updated := pr
	if spec.Repo != "" {
		updated.Repo = spec.Repo
	}
	if spec.Branch != "" {
		updated.Branch = spec.Branch
	}
	if spec.Base != "" {
		updated.Base = spec.Base
	}
	if spec.Title != "" {
		updated.Title = spec.Title
	}
	if len(spec.Metadata) > 0 {
		updated.Metadata = core.MustJSON(spec.Metadata)
	}
	if updated.State == "" {
		updated.State = "OPEN"
	}
	if updated.ChecksStatus == "" {
		updated.ChecksStatus = "pending"
	}
	return updated, nil
}

func (p *fakePullRequestPublisher) Inspect(_ context.Context, pr core.PullRequest) (core.PullRequest, error) {
	p.inspectCalls++
	if p.status.ID == "" {
		p.status.ID = pr.ID
	}
	if p.status.TaskID == "" {
		p.status.TaskID = pr.TaskID
	}
	if p.status.Repo == "" {
		p.status.Repo = pr.Repo
	}
	if p.status.Number == 0 {
		p.status.Number = pr.Number
	}
	if p.status.URL == "" {
		p.status.URL = pr.URL
	}
	if p.status.Branch == "" {
		p.status.Branch = pr.Branch
	}
	if p.status.Base == "" {
		p.status.Base = pr.Base
	}
	if p.status.Title == "" {
		p.status.Title = pr.Title
	}
	return p.status, nil
}

func (p *fakePullRequestPublisher) List(_ context.Context, spec PullRequestListSpec) ([]core.PullRequest, error) {
	p.listSpec = spec
	if len(p.list) > 0 {
		out := make([]core.PullRequest, len(p.list))
		copy(out, p.list)
		for index := range out {
			if out[index].TaskID == "" {
				out[index].TaskID = spec.TaskID
			}
			if out[index].Repo == "" {
				out[index].Repo = spec.Repo
			}
			if len(out[index].Metadata) == 0 {
				out[index].Metadata = core.MustJSON(spec.Metadata)
			}
		}
		return out, nil
	}
	return []core.PullRequest{{
		ID:           "pr-watch-1",
		TaskID:       spec.TaskID,
		Repo:         spec.Repo,
		Number:       12,
		URL:          "https://github.com/" + spec.Repo + "/pull/12",
		Branch:       "feature",
		Base:         "main",
		Title:        "Watch me",
		State:        "OPEN",
		ChecksStatus: "pending",
		MergeStatus:  "UNKNOWN",
		ReviewStatus: "REVIEW_REQUIRED",
		Metadata:     core.MustJSON(spec.Metadata),
	}}, nil
}

type replanningBrain struct {
	plan      Plan
	decisions []ReplanDecision
	states    []OrchestrationState
}

func (b *replanningBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return b.plan, nil
}

func (b *replanningBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	if len(b.decisions) == 0 {
		return ReplanDecision{Action: "complete"}, nil
	}
	decision := b.decisions[0]
	b.decisions = b.decisions[1:]
	return decision, nil
}

type completionReviewBrain struct {
	BrainProvider
	ReplanProvider
	reviews     []CompletionReview
	reviewCalls int
}

func (b *completionReviewBrain) ReviewCompletion(context.Context, core.Task, WorkerTurnResult, string) (CompletionReview, error) {
	b.reviewCalls++
	if len(b.reviews) == 0 {
		return CompletionReview{Ready: true}, nil
	}
	review := b.reviews[0]
	b.reviews = b.reviews[1:]
	return review, nil
}

type completionValidationBrain struct {
	states      []OrchestrationState
	reviewCalls int
}

func (b *completionValidationBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return Plan{WorkerKind: "change", Prompt: "make change"}, nil
}

func (b *completionValidationBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	switch len(b.states) {
	case 1:
		return ReplanDecision{Action: "complete", Rationale: "initial implementation is ready"}, nil
	case 2:
		return ReplanDecision{
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "validate",
				Prompt:     "validate the existing candidate without making changes",
			},
			Rationale: "validate the blocked candidate",
		}, nil
	default:
		latest := latestWorkerResult(state.Results)
		return ReplanDecision{
			Action:                 "complete",
			FinalCandidateWorkerID: latest.WorkerID,
			Rationale:              "validation worker confirmed the base candidate",
			PullRequestBody:        "## Summary\n- Implement cancellation fix.\n\n## Validation\n- go test ./internal/orchestrator",
		}, nil
	}
}

func (b *completionValidationBrain) ReviewCompletion(context.Context, core.Task, WorkerTurnResult, string) (CompletionReview, error) {
	b.reviewCalls++
	if b.reviewCalls == 1 {
		return CompletionReview{Ready: false, Reason: "candidate needs independent validation"}, nil
	}
	return CompletionReview{Ready: true}, nil
}

type publicationReviewBrain struct {
	BrainProvider
	ReplanProvider
	reviews     []PublicationReview
	reviewCalls int
}

func (b *publicationReviewBrain) ReviewPublication(context.Context, core.Task, WorkerTurnResult, PlanAction) (PublicationReview, error) {
	b.reviewCalls++
	if len(b.reviews) == 0 {
		return PublicationReview{Ready: true}, nil
	}
	review := b.reviews[0]
	b.reviews = b.reviews[1:]
	return review, nil
}

type continueThenSelectLatestBrain struct {
	plan   Plan
	states []OrchestrationState
}

func (b *continueThenSelectLatestBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return b.plan, nil
}

func (b *continueThenSelectLatestBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	if state.Turn == 1 {
		return ReplanDecision{
			Action: "continue",
			Plan: &Plan{
				WorkerKind: "follow",
				Prompt:     "patch the candidate",
			},
		}, nil
	}
	return ReplanDecision{
		Action:                 "complete",
		FinalCandidateWorkerID: latestCandidateWorkerID(state.Results),
		Rationale:              "select latest dynamic candidate",
	}, nil
}

type continueForTurnsBrain struct {
	plan          Plan
	continueTurns int
	states        []OrchestrationState
}

func (b *continueForTurnsBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return b.plan, nil
}

func (b *continueForTurnsBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	if state.Turn > b.continueTurns {
		return ReplanDecision{
			Action:                 "complete",
			FinalCandidateWorkerID: latestCandidateWorkerID(state.Results),
			Rationale:              "select latest dynamic candidate after continued progress",
		}, nil
	}
	return ReplanDecision{
		Action: "continue",
		Plan: &Plan{
			WorkerKind: "follow",
			Prompt:     "continue improving the candidate",
		},
		Rationale: "more work remains",
	}, nil
}

type errorReplanningBrain struct {
	plan Plan
	err  error
}

func (b *errorReplanningBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return b.plan, nil
}

func (b *errorReplanningBrain) Replan(context.Context, core.Task, OrchestrationState) (ReplanDecision, error) {
	return ReplanDecision{}, b.err
}

type finalSelectingBrain struct {
	plan   Plan
	role   string
	states []OrchestrationState
}

func (b *finalSelectingBrain) Plan(context.Context, core.Task, []string) (Plan, error) {
	return b.plan, nil
}

func (b *finalSelectingBrain) Replan(_ context.Context, _ core.Task, state OrchestrationState) (ReplanDecision, error) {
	b.states = append(b.states, state)
	for i := len(state.Results) - 1; i >= 0; i-- {
		result := state.Results[i]
		if result.Role == b.role && resultHasCandidateChanges(result) {
			return ReplanDecision{
				Action:                 "complete",
				FinalCandidateWorkerID: result.WorkerID,
				Rationale:              "selected " + b.role + " candidate",
			}, nil
		}
	}
	return ReplanDecision{Action: "complete", Rationale: "no matching candidate"}, nil
}

type sequenceBrain struct {
	mu       sync.Mutex
	plans    []Plan
	steering []string
}

func (b *sequenceBrain) Plan(_ context.Context, _ core.Task, steering []string) (Plan, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.steering = append(b.steering[:0], steering...)
	if len(b.plans) == 0 {
		return Plan{}, errors.New("no plans left")
	}
	plan := b.plans[0]
	b.plans = b.plans[1:]
	return plan, nil
}

type recordingRunner struct {
	kind            string
	prompt          string
	workDir         string
	resumeSessionID string
	reasoningEffort string
}

type eventRunner struct {
	kind   string
	events []worker.Event
}

type failingRunner struct {
	kind string
	err  error
}

type fileWritingRunner struct {
	kind string
	path string
	body string
}

type recordingEventRunner struct {
	mu      sync.Mutex
	kind    string
	events  []worker.Event
	prompt  string
	workDir string
	calls   int
}

type sequenceEventRunner struct {
	mu      sync.Mutex
	kind    string
	events  [][]worker.Event
	prompt  string
	workDir string
	calls   int
}

type flakyRunner struct {
	mu    sync.Mutex
	kind  string
	calls int
}

type blockingEventRunner struct {
	mu      sync.Mutex
	kind    string
	started chan<- string
	release <-chan struct{}
	summary string
	prompt  string
}

type restartOnSteeringRunner struct {
	mu                 sync.Mutex
	started            chan<- struct{}
	firstCancelSeen    chan<- struct{}
	firstCancelRelease <-chan struct{}
	retryStarted       chan<- struct{}
	retryRelease       <-chan struct{}
	calls              int
	prompt             string
	resumeSessionID    string
}

type steeringRunner struct {
	started chan<- struct{}
	got     chan<- string
}

type buildOnlyRunner struct {
	kind    string
	command []string
}

type recordingBuildRunner struct {
	kind string
	spec worker.Spec
}

func (r eventRunner) Kind() string {
	return r.kind
}

func (r eventRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r eventRunner) Run(ctx context.Context, _ worker.Spec, sink worker.Sink) error {
	for _, event := range r.events {
		if err := sink.Event(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (r failingRunner) Kind() string {
	return r.kind
}

func (r failingRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r failingRunner) Run(context.Context, worker.Spec, worker.Sink) error {
	return r.err
}

func (r *recordingEventRunner) Kind() string {
	return r.kind
}

func (r *recordingEventRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *recordingEventRunner) Run(ctx context.Context, spec worker.Spec, sink worker.Sink) error {
	r.mu.Lock()
	r.prompt = spec.Prompt
	r.workDir = spec.WorkDir
	r.calls++
	r.mu.Unlock()
	for _, event := range r.events {
		if err := sink.Event(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (r *recordingEventRunner) promptValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prompt
}

func (r *recordingEventRunner) callsValue() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *sequenceEventRunner) Kind() string {
	return r.kind
}

func (r *sequenceEventRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *sequenceEventRunner) Run(ctx context.Context, spec worker.Spec, sink worker.Sink) error {
	r.mu.Lock()
	r.prompt = spec.Prompt
	r.workDir = spec.WorkDir
	r.calls++
	call := r.calls
	r.mu.Unlock()
	events := []worker.Event{{Kind: worker.EventResult, Text: "ok"}}
	if call > 0 && call <= len(r.events) {
		events = r.events[call-1]
	} else if len(r.events) > 0 {
		events = r.events[len(r.events)-1]
	}
	for _, event := range events {
		if err := sink.Event(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (r *sequenceEventRunner) promptValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prompt
}

func (r *sequenceEventRunner) callsValue() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *flakyRunner) Kind() string {
	return r.kind
}

func (r *flakyRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *flakyRunner) Run(ctx context.Context, _ worker.Spec, sink worker.Sink) error {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()
	if call == 1 {
		return errors.New("transient worker failure")
	}
	return sink.Event(ctx, worker.Event{Kind: worker.EventResult, Text: "retry succeeded"})
}

func (r *flakyRunner) callsValue() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *blockingEventRunner) Kind() string {
	return r.kind
}

func (r *blockingEventRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *blockingEventRunner) Run(ctx context.Context, spec worker.Spec, sink worker.Sink) error {
	r.mu.Lock()
	r.prompt = spec.Prompt
	r.mu.Unlock()
	r.started <- r.kind
	select {
	case <-r.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if r.summary != "" {
		return sink.Event(ctx, worker.Event{Kind: worker.EventResult, Text: r.summary})
	}
	return nil
}

func (r *blockingEventRunner) promptValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prompt
}

func (r *restartOnSteeringRunner) Kind() string {
	return "codex"
}

func (r *restartOnSteeringRunner) Capabilities() worker.Capabilities {
	return worker.Capabilities{ResumeSession: true}
}

func (r *restartOnSteeringRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *restartOnSteeringRunner) Run(ctx context.Context, spec worker.Spec, sink worker.Sink) error {
	r.mu.Lock()
	r.calls++
	call := r.calls
	if call > 1 {
		r.prompt = spec.Prompt
		r.resumeSessionID = spec.ResumeSessionID
	}
	r.mu.Unlock()
	if call == 1 {
		if err := sink.Event(ctx, worker.Event{
			Kind: worker.EventLog,
			Raw:  json.RawMessage(`{"type":"thread.started","thread_id":"thread-1"}`),
		}); err != nil {
			return err
		}
		close(r.started)
		<-ctx.Done()
		if r.firstCancelSeen != nil {
			r.firstCancelSeen <- struct{}{}
		}
		if r.firstCancelRelease != nil {
			<-r.firstCancelRelease
		}
		return ctx.Err()
	}
	if r.retryStarted != nil {
		r.retryStarted <- struct{}{}
	}
	if r.retryRelease != nil {
		select {
		case <-r.retryRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return sink.Event(ctx, worker.Event{Kind: worker.EventResult, Text: "resumed with steering"})
}

func (r *restartOnSteeringRunner) callsValue() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *restartOnSteeringRunner) promptValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prompt
}

func (r *restartOnSteeringRunner) resumeSessionIDValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.resumeSessionID
}

func (r steeringRunner) Kind() string {
	return "steerable"
}

func (r steeringRunner) SupportsSteering() bool {
	return true
}

func (r steeringRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r steeringRunner) Run(ctx context.Context, spec worker.Spec, sink worker.Sink) error {
	close(r.started)
	select {
	case message := <-spec.Steering:
		r.got <- message
		return sink.Event(ctx, worker.Event{Kind: worker.EventResult, Text: "received steering"})
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r buildOnlyRunner) Kind() string {
	return r.kind
}

func (r buildOnlyRunner) BuildCommand(worker.Spec) []string {
	return r.command
}

func (r buildOnlyRunner) Run(context.Context, worker.Spec, worker.Sink) error {
	return errors.New("build-only runner should not run locally")
}

func (r *recordingBuildRunner) Kind() string {
	return r.kind
}

func (r *recordingBuildRunner) Capabilities() worker.Capabilities {
	return worker.Capabilities{ResumeSession: true}
}

func (r *recordingBuildRunner) BuildCommand(spec worker.Spec) []string {
	r.spec = spec
	return []string{"worker", spec.WorkDir, spec.ResumeSessionID}
}

func (r *recordingBuildRunner) Run(context.Context, worker.Spec, worker.Sink) error {
	return errors.New("recording build runner should not run locally")
}

func (r fileWritingRunner) Kind() string {
	return r.kind
}

func (r fileWritingRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r fileWritingRunner) Run(_ context.Context, spec worker.Spec, _ worker.Sink) error {
	target := filepath.Join(spec.WorkDir, r.path)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	return os.WriteFile(target, []byte(r.body), 0o644)
}

type localCallbackRunner struct {
	kind           string
	prompt         string
	parentWorkerID string
}

func (r *localCallbackRunner) Kind() string {
	return r.kind
}

func (r *localCallbackRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *localCallbackRunner) Run(_ context.Context, spec worker.Spec, _ worker.Sink) error {
	r.prompt = spec.Prompt
	if r.parentWorkerID != "" {
		return nil
	}
	r.parentWorkerID = spec.ID
	callbackDir := filepath.Join(os.TempDir(), "aged-worker-callbacks", spec.ID, "callbacks")
	if err := os.MkdirAll(callbackDir, 0o755); err != nil {
		return err
	}
	body := `{"type":"create_task","promptBase64":"` + base64.StdEncoding.EncodeToString([]byte("follow up from local")) + `","titleBase64":"` + base64.StdEncoding.EncodeToString([]byte("Local follow-up")) + `","parentTaskIdBase64":"` + base64.StdEncoding.EncodeToString([]byte(spec.TaskID)) + `","parentWorkerIdBase64":"` + base64.StdEncoding.EncodeToString([]byte(spec.ID)) + `"}`
	return os.WriteFile(filepath.Join(callbackDir, "create-task.local.json"), []byte(body), 0o644)
}

type localPublishPRCallbackRunner struct {
	kind           string
	prompt         string
	parentWorkerID string
}

func (r *localPublishPRCallbackRunner) Kind() string {
	return r.kind
}

func (r *localPublishPRCallbackRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *localPublishPRCallbackRunner) Run(_ context.Context, spec worker.Spec, _ worker.Sink) error {
	r.prompt = spec.Prompt
	if r.parentWorkerID != "" {
		return nil
	}
	r.parentWorkerID = spec.ID
	callbackDir := filepath.Join(os.TempDir(), "aged-worker-callbacks", spec.ID, "callbacks")
	if err := os.MkdirAll(callbackDir, 0o755); err != nil {
		return err
	}
	body := `{"type":"publish_pull_request","bodyBase64":"` + base64.StdEncoding.EncodeToString([]byte("Callback PR body")) + `","titleBase64":"` + base64.StdEncoding.EncodeToString([]byte("Local callback PR")) + `","repoBase64":"` + base64.StdEncoding.EncodeToString([]byte("owner/repo")) + `","parentTaskIdBase64":"` + base64.StdEncoding.EncodeToString([]byte(spec.TaskID)) + `","parentWorkerIdBase64":"` + base64.StdEncoding.EncodeToString([]byte(spec.ID)) + `","continueAfterPublish":true}`
	return os.WriteFile(filepath.Join(callbackDir, "publish-pr.local.json"), []byte(body), 0o644)
}

func (r *recordingRunner) Kind() string {
	return r.kind
}

func (r *recordingRunner) Capabilities() worker.Capabilities {
	return worker.Capabilities{ResumeSession: true}
}

func (r *recordingRunner) BuildCommand(worker.Spec) []string {
	return nil
}

func (r *recordingRunner) Run(_ context.Context, spec worker.Spec, _ worker.Sink) error {
	r.prompt = spec.Prompt
	r.workDir = spec.WorkDir
	r.resumeSessionID = spec.ResumeSessionID
	r.reasoningEffort = spec.ReasoningEffort
	return nil
}

type fakeWorkspaceManager struct {
	cwd          string
	sourceRoot   string
	baseWorkDir  string
	baseRevision string
	changes      WorkspaceChanges
	diff         string
	applyCalls   *int
	diffCalls    *int
	prepareCalls *int
	prepareErr   error
	applyErr     error

	failPrepareAfter int
	failPrepareUntil int
	failApplyUntil   int
}

type sequencingWorkspaceManager struct {
	fakeWorkspaceManager
	mu      sync.Mutex
	changes []WorkspaceChanges
	calls   int
}

type recordingWorkspaceManager struct {
	workDir      string
	baseWorkDir  string
	baseRevision string
	changes      WorkspaceChanges
}

func (m *recordingWorkspaceManager) Prepare(_ context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	m.workDir = spec.WorkDir
	m.baseWorkDir = spec.BaseWorkDir
	m.baseRevision = spec.BaseRevision
	return PreparedWorkspace{
		Root:          spec.WorkDir,
		CWD:           spec.WorkDir,
		SourceRoot:    spec.WorkDir,
		WorkspaceName: "shared",
		Change:        "@ fake",
		Status:        "The working copy has no changes.",
		Mode:          string(WorkspaceModeShared),
		VCSType:       "jj",
		WorkerID:      spec.WorkerID,
		TaskID:        spec.TaskID,
	}, nil
}

func (m *recordingWorkspaceManager) Cleanup(_ context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error) {
	return WorkspaceCleanup{
		Root:    workspace.Root,
		CWD:     workspace.CWD,
		Mode:    workspace.Mode,
		VCSType: workspace.VCSType,
		Policy:  workspace.CleanupPolicy,
		Result:  result,
		Reason:  "fake cleanup retained workspace",
	}, nil
}

func (m *recordingWorkspaceManager) DescribeChanges(_ context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	if m.changes.Root != "" || m.changes.CWD != "" || m.changes.Dirty || len(m.changes.ChangedFiles) > 0 {
		changes := m.changes
		if changes.Root == "" {
			changes.Root = workspace.Root
		}
		if changes.CWD == "" {
			changes.CWD = workspace.CWD
		}
		if changes.Mode == "" {
			changes.Mode = workspace.Mode
		}
		if changes.VCSType == "" {
			changes.VCSType = workspace.VCSType
		}
		return changes, nil
	}
	return WorkspaceChanges{
		Root:    workspace.Root,
		CWD:     workspace.CWD,
		Mode:    workspace.Mode,
		VCSType: workspace.VCSType,
		Status:  workspace.Status,
	}, nil
}

func (m *recordingWorkspaceManager) ApplyChanges(_ context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	return WorkerApplyResult{
		SourceRoot:    workspace.SourceRoot,
		WorkspaceRoot: workspace.Root,
		Method:        "fake_merge",
		AppliedFiles:  changes.ChangedFiles,
	}, nil
}

func (m fakeWorkspaceManager) Prepare(_ context.Context, spec WorkspaceSpec) (PreparedWorkspace, error) {
	if m.prepareCalls != nil {
		*m.prepareCalls = *m.prepareCalls + 1
		if m.prepareErr != nil {
			if m.failPrepareUntil > 0 && *m.prepareCalls <= m.failPrepareUntil {
				return PreparedWorkspace{}, m.prepareErr
			}
			if m.failPrepareAfter > 0 && *m.prepareCalls > m.failPrepareAfter {
				return PreparedWorkspace{}, m.prepareErr
			}
		}
	}
	sourceRoot := m.sourceRoot
	mode := string(WorkspaceModeShared)
	if sourceRoot == "" {
		sourceRoot = m.cwd
	} else if sourceRoot != m.cwd {
		mode = string(WorkspaceModeIsolated)
	}
	return PreparedWorkspace{
		Root:       m.cwd,
		CWD:        m.cwd,
		SourceRoot: sourceRoot,
		Change:     "@ fake",
		Status:     "The working copy has no changes.",
		Mode:       mode,
		VCSType:    "jj",
		Dirty:      false,
		WorkerID:   spec.WorkerID,
		TaskID:     spec.TaskID,
	}, nil
}

func (m fakeWorkspaceManager) Cleanup(_ context.Context, workspace PreparedWorkspace, result WorkspaceResult) (WorkspaceCleanup, error) {
	return WorkspaceCleanup{
		Root:          workspace.Root,
		CWD:           workspace.CWD,
		WorkspaceName: workspace.WorkspaceName,
		Mode:          workspace.Mode,
		VCSType:       workspace.VCSType,
		Policy:        workspace.CleanupPolicy,
		Result:        result,
		Reason:        "fake cleanup retained workspace",
	}, nil
}

func (m fakeWorkspaceManager) DescribeChanges(_ context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	changes := m.changes
	if changes.Root == "" {
		changes.Root = workspace.Root
	}
	if changes.CWD == "" {
		changes.CWD = workspace.CWD
	}
	if changes.WorkspaceName == "" {
		changes.WorkspaceName = workspace.WorkspaceName
	}
	if changes.Mode == "" {
		changes.Mode = workspace.Mode
	}
	if changes.VCSType == "" {
		changes.VCSType = workspace.VCSType
	}
	return changes, nil
}

func (m *sequencingWorkspaceManager) DescribeChanges(ctx context.Context, workspace PreparedWorkspace) (WorkspaceChanges, error) {
	m.mu.Lock()
	index := m.calls
	m.calls++
	m.mu.Unlock()
	if index < len(m.changes) {
		base := m.fakeWorkspaceManager
		base.changes = m.changes[index]
		return base.DescribeChanges(ctx, workspace)
	}
	return m.fakeWorkspaceManager.DescribeChanges(ctx, workspace)
}

func (m fakeWorkspaceManager) DescribeDiff(context.Context, PreparedWorkspace) (string, error) {
	if m.diffCalls != nil {
		*m.diffCalls = *m.diffCalls + 1
	}
	return m.diff, nil
}

func (m fakeWorkspaceManager) ApplyChanges(_ context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	if m.applyCalls != nil {
		*m.applyCalls = *m.applyCalls + 1
		if m.applyErr != nil && m.failApplyUntil > 0 && *m.applyCalls <= m.failApplyUntil {
			return WorkerApplyResult{}, m.applyErr
		}
	}
	return WorkerApplyResult{
		SourceRoot:    workspace.SourceRoot,
		WorkspaceRoot: workspace.Root,
		Method:        "fake_merge",
		AppliedFiles:  changes.ChangedFiles,
	}, nil
}

func openTestStore(t *testing.T) *eventstore.SQLiteStore {
	t.Helper()
	store, err := eventstore.OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "aged.db"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func waitForTaskStatus(t *testing.T, store eventstore.Store, taskID string, status core.TaskStatus) core.Snapshot {
	t.Helper()
	return waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		for _, task := range snapshot.Tasks {
			if task.ID == taskID && task.Status == status {
				return true
			}
		}
		return false
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not reach %s; snapshot = %+v", taskID, status, snapshot.Tasks)
	})
}

func waitForPullRequests(t *testing.T, store eventstore.Store, taskID string, count int) core.Snapshot {
	t.Helper()
	return waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		found := 0
		for _, pr := range snapshot.PullRequests {
			if pr.TaskID == taskID {
				found++
			}
		}
		return found >= count
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not publish %d pull requests; pull requests = %+v", taskID, count, snapshot.PullRequests)
	})
}

func waitForTaskStatusEventCount(t *testing.T, store eventstore.Store, taskID string, status core.TaskStatus, count int) core.Snapshot {
	t.Helper()
	return waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		found := 0
		for _, event := range snapshot.Events {
			if event.Type != core.EventTaskStatus || event.TaskID != taskID {
				continue
			}
			var payload struct {
				Status core.TaskStatus `json:"status"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Status == status {
				found++
			}
		}
		return found >= count
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not record %d %s status events; events = %+v", taskID, count, status, snapshot.Events)
	})
}

func waitForEvent(t *testing.T, store eventstore.Store, eventType core.EventType, taskID string) core.Snapshot {
	t.Helper()
	return waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return hasEvent(snapshot.Events, eventType, taskID, "")
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not record event %s; events = %+v", taskID, eventType, snapshot.Events)
	})
}

func waitForEventCount(t *testing.T, store eventstore.Store, eventType core.EventType, taskID string, count int) core.Snapshot {
	t.Helper()
	return waitForSnapshot(t, store, func(snapshot core.Snapshot) bool {
		return countEvents(snapshot.Events, eventType, taskID) >= count
	}, func(snapshot core.Snapshot) string {
		return fmt.Sprintf("task %s did not record %d events of type %s; events = %+v", taskID, count, eventType, snapshot.Events)
	})
}

func waitForSnapshot(t *testing.T, store eventstore.Store, ready func(core.Snapshot) bool, failure func(core.Snapshot) string) core.Snapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := store.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if ready(snapshot) {
			return snapshot
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapshot, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("%s", failure(snapshot))
	return core.Snapshot{}
}

func taskWorkspaceCWD(snapshot core.Snapshot, taskID string) string {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventWorkerWorkspace || event.TaskID != taskID {
			continue
		}
		var workspace PreparedWorkspace
		if err := json.Unmarshal(event.Payload, &workspace); err == nil {
			return workspace.CWD
		}
	}
	return ""
}

func hasEvent(events []core.Event, eventType core.EventType, taskID string, workerID string) bool {
	for _, event := range events {
		if event.Type == eventType && event.TaskID == taskID && (workerID == "" || event.WorkerID == workerID) {
			return true
		}
	}
	return false
}

func hasTaskAction(events []core.Event, taskID string, kind string, status string) bool {
	return countTaskActions(events, taskID, kind, status) > 0
}

func countTaskActions(events []core.Event, taskID string, kind string, status string) int {
	count := 0
	for _, event := range events {
		if event.Type != core.EventTaskAction || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Kind   string `json:"kind"`
			Status string `json:"status"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.Kind == kind && payload.Status == status {
			count++
		}
	}
	return count
}

func resultErrorContains(results []WorkerTurnResult, needle string) bool {
	for _, result := range results {
		if strings.Contains(result.Error, needle) {
			return true
		}
	}
	return false
}

func replanStatesContainResultError(states []OrchestrationState, needle string) bool {
	for _, state := range states {
		if resultErrorContains(state.Results, needle) {
			return true
		}
	}
	return false
}

func latestEventOfType(events []core.Event, eventType core.EventType, taskID string) core.Event {
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.Type == eventType && event.TaskID == taskID {
			return event
		}
	}
	return core.Event{}
}

func taskEventSummary(events []core.Event, taskID string) string {
	var parts []string
	for _, event := range events {
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventTaskAction, core.EventTaskReplanned, core.EventApprovalNeeded, core.EventTaskStatus, core.EventTaskCandidate:
			parts = append(parts, fmt.Sprintf("%s:%s", event.Type, truncateStringForPrompt(string(event.Payload), 400)))
		}
	}
	return strings.Join(parts, " | ")
}

func hasMilestone(milestones []core.TaskMilestone, name string) bool {
	for _, milestone := range milestones {
		if milestone.Name == name {
			return true
		}
	}
	return false
}

func countEvents(events []core.Event, eventType core.EventType, taskID string) int {
	count := 0
	for _, event := range events {
		if event.Type == eventType && event.TaskID == taskID {
			count++
		}
	}
	return count
}

func latestExecutionNodeForTask(snapshot core.Snapshot, taskID string, excludeWorkerID string) core.ExecutionNode {
	for i := len(snapshot.ExecutionNodes) - 1; i >= 0; i-- {
		node := snapshot.ExecutionNodes[i]
		if node.TaskID == taskID && node.WorkerID != excludeWorkerID {
			return node
		}
	}
	return core.ExecutionNode{}
}

func eventPayloadContains(events []core.Event, eventType core.EventType, taskID string, needle string) bool {
	for _, event := range events {
		if event.Type == eventType && event.TaskID == taskID && strings.Contains(string(event.Payload), needle) {
			return true
		}
	}
	return false
}

func flattenCommands(commands [][]string) []string {
	flattened := make([]string, 0, len(commands))
	for _, command := range commands {
		flattened = append(flattened, strings.Join(command, " "))
	}
	return flattened
}

func hasEventPayloadValue(events []core.Event, eventType core.EventType, taskID string, key string, want string) bool {
	for _, event := range events {
		if event.Type != eventType || event.TaskID != taskID {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if stringMetadataValue(payload[key]) == want {
			return true
		}
		if metadata, ok := payload["metadata"].(map[string]any); ok && stringMetadataValue(metadata[key]) == want {
			return true
		}
	}
	return false
}

func hasWorkerCreated(events []core.Event, taskID string, kind string) bool {
	for _, event := range events {
		if event.Type != core.EventWorkerCreated || event.TaskID != taskID {
			continue
		}
		if string(event.Payload) == "" {
			continue
		}
		if strings.Contains(string(event.Payload), `"kind":"`+kind+`"`) {
			return true
		}
	}
	return false
}

func workerCompletedPayload(t *testing.T, events []core.Event, taskID string) struct {
	Status           core.WorkerStatus      `json:"status"`
	Summary          string                 `json:"summary"`
	NeedsInput       bool                   `json:"needsInput"`
	LogCount         int                    `json:"logCount"`
	ChangedFiles     []WorkspaceChangedFile `json:"changedFiles"`
	WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges"`
} {
	t.Helper()
	for _, event := range events {
		if event.Type != core.EventWorkerCompleted || event.TaskID != taskID {
			continue
		}
		var payload struct {
			Status           core.WorkerStatus      `json:"status"`
			Summary          string                 `json:"summary"`
			NeedsInput       bool                   `json:"needsInput"`
			LogCount         int                    `json:"logCount"`
			ChangedFiles     []WorkspaceChangedFile `json:"changedFiles"`
			WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		return payload
	}
	t.Fatalf("missing worker.completed for task %s", taskID)
	return struct {
		Status           core.WorkerStatus      `json:"status"`
		Summary          string                 `json:"summary"`
		NeedsInput       bool                   `json:"needsInput"`
		LogCount         int                    `json:"logCount"`
		ChangedFiles     []WorkspaceChangedFile `json:"changedFiles"`
		WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges"`
	}{}
}
