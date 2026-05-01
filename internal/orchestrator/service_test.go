package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

func TestServiceInfersGitHubCompletionModeFromPrompt(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "worker prompt",
	}}, map[string]worker.Runner{"mock": eventRunner{kind: "mock", events: []worker.Event{{Kind: worker.EventResult, Text: "done"}}}}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{
		Title:  "Fix TODOs",
		Prompt: "Take a look at TODOs in the code, fix them, open PR and make sure it gets merged.",
	})
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["completionMode"] != "github" || metadata["completionModeInferred"] != true {
		t.Fatalf("metadata = %+v", metadata)
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
	publisher := &fakePullRequestPublisher{}
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
		applyCalls: &applyCalls,
	})
	service.SetPullRequestPublisher(publisher)

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
	publisher := &fakePullRequestPublisher{}
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
		applyCalls: &applyCalls,
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
	_ = waitForPullRequests(t, store, task.ID, 1)
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if len(snapshot.PullRequests) != 1 {
		t.Fatalf("pull requests = %+v", snapshot.PullRequests)
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
	if applyCalls != 0 {
		t.Fatalf("apply calls = %d, want 0", applyCalls)
	}
}

func TestServicePlanActionPublishesIntermediatePullRequest(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	publisher := &fakePullRequestPublisher{}
	service := NewServiceWithWorkspaceManager(store, fixedBrain{plan: Plan{
		WorkerKind: "change",
		Prompt:     "make change",
		Actions: []PlanAction{{
			Kind:   "publish_pull_request",
			When:   "after_success",
			Reason: "open a PR so review can happen while the objective continues",
			Inputs: map[string]any{"repo": "owner/repo", "base": "main"},
		}},
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

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Implement feature", Prompt: "Do it, open a PR, and babysit it."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForPullRequests(t, store, task.ID, 1)
	snapshot = waitForEvent(t, store, core.EventTaskArtifact, task.ID)
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
			State:        "OPEN",
			ChecksStatus: "failing",
			MergeStatus:  "BLOCKED",
			ReviewStatus: "CHANGES_REQUESTED",
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
	if pr.ChecksStatus != "failing" || pr.MergeStatus != "BLOCKED" || pr.ReviewStatus != "CHANGES_REQUESTED" {
		t.Fatalf("refreshed pr = %+v", pr)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.PullRequests[0].ChecksStatus != "failing" {
		t.Fatalf("snapshot pr = %+v", snapshot.PullRequests[0])
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

	snapshot := waitForTaskStatus(t, store, taskID, core.TaskSucceeded)
	if countEvents(snapshot.Events, core.EventTaskPlanned, taskID) != 2 {
		t.Fatalf("task.planned count = %d, want 2", countEvents(snapshot.Events, core.EventTaskPlanned, taskID))
	}
	if countEvents(snapshot.Events, core.EventWorkerCreated, taskID) != 1 {
		t.Fatalf("worker.created count = %d, want 1", countEvents(snapshot.Events, core.EventWorkerCreated, taskID))
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

func TestServicePublishesRemoteWorkerPullRequestAfterApplyingPatch(t *testing.T) {
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
	applied := 0
	service.SetRemotePatchApplier(func(_ context.Context, project core.Project, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
		applied++
		if project.LocalPath != sourceRoot {
			t.Fatalf("project local path = %q, want %q", project.LocalPath, sourceRoot)
		}
		if workspace.VCSType != "ssh" || workspace.CWD != "/repo" || changes.Diff == "" {
			t.Fatalf("workspace=%+v changes=%+v", workspace, changes)
		}
		result := baseWorkerApplyResult(workspace, "remote_patch_apply")
		result.SourceRoot = project.LocalPath
		result.AppliedFiles = changes.ChangedFiles
		return result, nil
	})

	if _, err := service.PublishTaskPullRequest(ctx, taskID, core.PublishPullRequestRequest{
		Repo:     "owner/repo",
		Base:     "main",
		WorkerID: workerID,
	}); err != nil {
		t.Fatal(err)
	}
	if applied != 1 {
		t.Fatalf("applied calls = %d, want 1", applied)
	}
	if publisher.published.WorkDir != sourceRoot {
		t.Fatalf("published workDir = %q, want local source root %q", publisher.published.WorkDir, sourceRoot)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(snapshot.Events, core.EventWorkerApplied, taskID, workerID) {
		t.Fatal("missing worker.changes_applied event")
	}
	if !hasEvent(snapshot.Events, core.EventPRPublished, taskID, "") {
		t.Fatal("missing pr.published event")
	}
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
	case <-time.After(500 * time.Millisecond):
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
	case <-time.After(500 * time.Millisecond):
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

func TestServiceWaitsWhenReplannerErrorsWithAmbiguousCandidates(t *testing.T) {
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
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskWaiting)
	if !hasEvent(snapshot.Events, core.EventApprovalNeeded, task.ID, "") {
		t.Fatalf("missing approval-needed event")
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

func TestResolveFinalCandidateUsesSelectedWorkerCandidateAncestor(t *testing.T) {
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
	}, "validation")
	if err != nil {
		t.Fatal(err)
	}
	if workerID != "impl" {
		t.Fatalf("workerID = %q, want impl", workerID)
	}
	if !strings.Contains(reason, "nearest changed candidate ancestor") {
		t.Fatalf("reason = %q", reason)
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
	if !hasEvent(snapshot.Events, core.EventWorkerOutput, task.ID, snapshot.Workers[0].ID) {
		t.Fatalf("missing remote worker output")
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

func TestServiceRegisterTargetProbesImmediately(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	executor := &fakeRemoteExecutor{probeOutput: strings.Join([]string{
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
	published PullRequestPublishSpec
	status    core.PullRequest
	list      []core.PullRequest
	listSpec  PullRequestListSpec
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
	branch := strings.TrimSpace(spec.Branch)
	if branch == "" {
		branch = defaultPRBranch(spec)
	}
	return core.PullRequest{
		ID:           "pr-1",
		TaskID:       spec.TaskID,
		Repo:         spec.Repo,
		Number:       12,
		URL:          "https://github.com/" + spec.Repo + "/pull/12",
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

func (p *fakePullRequestPublisher) Inspect(_ context.Context, pr core.PullRequest) (core.PullRequest, error) {
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

func (r *recordingRunner) Kind() string {
	return r.kind
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

	failPrepareAfter int
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
		if m.prepareErr != nil && m.failPrepareAfter > 0 && *m.prepareCalls > m.failPrepareAfter {
			return PreparedWorkspace{}, m.prepareErr
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

func (m fakeWorkspaceManager) DescribeDiff(context.Context, PreparedWorkspace) (string, error) {
	if m.diffCalls != nil {
		*m.diffCalls = *m.diffCalls + 1
	}
	return m.diff, nil
}

func (m fakeWorkspaceManager) ApplyChanges(_ context.Context, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	if m.applyCalls != nil {
		*m.applyCalls = *m.applyCalls + 1
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
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := store.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range snapshot.Tasks {
			if task.ID == taskID && task.Status == status {
				return snapshot
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapshot, _ := store.Snapshot(context.Background())
	t.Fatalf("task %s did not reach %s; snapshot = %+v", taskID, status, snapshot.Tasks)
	return core.Snapshot{}
}

func waitForPullRequests(t *testing.T, store eventstore.Store, taskID string, count int) core.Snapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := store.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		found := 0
		for _, pr := range snapshot.PullRequests {
			if pr.TaskID == taskID {
				found++
			}
		}
		if found >= count {
			return snapshot
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapshot, _ := store.Snapshot(context.Background())
	t.Fatalf("task %s did not publish %d pull requests; pull requests = %+v", taskID, count, snapshot.PullRequests)
	return core.Snapshot{}
}

func waitForEvent(t *testing.T, store eventstore.Store, eventType core.EventType, taskID string) core.Snapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := store.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if hasEvent(snapshot.Events, eventType, taskID, "") {
			return snapshot
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapshot, _ := store.Snapshot(context.Background())
	t.Fatalf("task %s did not record event %s; events = %+v", taskID, eventType, snapshot.Events)
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

func latestEventOfType(events []core.Event, eventType core.EventType, taskID string) core.Event {
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.Type == eventType && event.TaskID == taskID {
			return event
		}
	}
	return core.Event{}
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
