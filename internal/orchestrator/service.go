package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"

	"github.com/google/uuid"
)

type WorkerChangesReview struct {
	WorkerID  string            `json:"workerId"`
	Workspace PreparedWorkspace `json:"workspace"`
	Changes   WorkspaceChanges  `json:"changes"`
}

type WorkerApplyResult struct {
	WorkerID      string                 `json:"workerId"`
	SourceRoot    string                 `json:"sourceRoot"`
	WorkspaceRoot string                 `json:"workspaceRoot"`
	Method        string                 `json:"method"`
	AppliedFiles  []WorkspaceChangedFile `json:"appliedFiles"`
	SkippedFiles  []WorkspaceChangedFile `json:"skippedFiles,omitempty"`
}

type ApplyPolicyRecommendation struct {
	TaskID     string           `json:"taskId"`
	Strategy   string           `json:"strategy"`
	Reason     string           `json:"reason"`
	Candidates []ApplyCandidate `json:"candidates"`
}

type ApplyCandidate struct {
	WorkerID     string                 `json:"workerId"`
	NodeID       string                 `json:"nodeId,omitempty"`
	WorkerKind   string                 `json:"workerKind"`
	Summary      string                 `json:"summary,omitempty"`
	ChangedFiles []WorkspaceChangedFile `json:"changedFiles,omitempty"`
	Applied      bool                   `json:"applied"`
}

type ClearTasksResult struct {
	Cleared []string `json:"cleared"`
}

type Service struct {
	store       eventstore.Store
	broker      *Broker
	brain       BrainProvider
	assistant   AssistantProvider
	titles      TitleGenerator
	runners     map[string]worker.Runner
	workDir     string
	projects    *ProjectRegistry
	plugins     *PluginRegistry
	pluginCtx   context.Context
	workspaces  WorkspaceManager
	targets     *TargetRegistry
	sshRunner   SSHRunner
	prPublisher PullRequestPublisher
	remoteApply func(context.Context, core.Project, PreparedWorkspace, WorkspaceChanges) (WorkerApplyResult, error)

	mu         sync.Mutex
	cancels    map[string]context.CancelFunc
	tasks      map[string]string
	steering   map[string]chan string
	remoteRuns map[string]remoteRun
}

const maxDynamicReplanTurns = 4

func workerExecutionPrompt(prompt string, workspace PreparedWorkspace) string {
	cwd := strings.TrimSpace(workspace.CWD)
	sourceRoot := strings.TrimSpace(workspace.SourceRoot)
	if cwd == "" {
		return prompt
	}

	var b strings.Builder
	b.WriteString("# Execution Workspace\n\n")
	b.WriteString("Run every command from this execution workspace:\n")
	b.WriteString(cwd)
	b.WriteString("\n\n")
	b.WriteString("Edit only files under the execution workspace. ")
	if sourceRoot != "" && sourceRoot != cwd {
		b.WriteString("Do not edit the source checkout directly:\n")
		b.WriteString(sourceRoot)
		b.WriteString("\n\n")
		b.WriteString("If the worker task below names the source checkout or another local checkout path, treat that path as context only and translate the work to the execution workspace.\n\n")
	} else {
		b.WriteString("Use the current working directory as the repository root.\n\n")
	}
	b.WriteString("# Worker Task\n\n")
	b.WriteString(strings.TrimSpace(prompt))
	return b.String()
}

func retryWorkerExecutionPrompt(prompt string, previousWorkerID string, resumeSessionID string) string {
	var b strings.Builder
	b.WriteString("# Retry Context\n\n")
	b.WriteString("This is a retry of a previously failed or canceled worker turn.\n")
	b.WriteString("Previous worker ID: ")
	b.WriteString(previousWorkerID)
	b.WriteString("\n")
	if strings.TrimSpace(resumeSessionID) != "" {
		b.WriteString("The worker provider session is being resumed when supported.\n")
	}
	b.WriteString("The execution workspace may already contain partial changes from that worker. Inspect the current workspace state first, preserve useful existing work, and continue from there instead of starting over.\n\n")
	b.WriteString(prompt)
	return b.String()
}

type WorkerTurnResult struct {
	WorkerID     string            `json:"workerId"`
	NodeID       string            `json:"nodeId,omitempty"`
	Status       core.WorkerStatus `json:"status"`
	Kind         string            `json:"kind"`
	Role         string            `json:"role,omitempty"`
	SpawnID      string            `json:"spawnId,omitempty"`
	BaseWorkerID string            `json:"baseWorkerId,omitempty"`
	Summary      string            `json:"summary,omitempty"`
	Error        string            `json:"error,omitempty"`
	Changes      WorkspaceChanges  `json:"changes"`
}

func NewService(store eventstore.Store, brain BrainProvider, runners map[string]worker.Runner, workDir string) *Service {
	return NewServiceWithWorkspaceManager(store, brain, runners, workDir, NewWorkspaceManager(WorkspaceVCSAuto, WorkspaceModeIsolated, "", WorkspaceCleanupRetain))
}

func NewServiceWithWorkspaceManager(store eventstore.Store, brain BrainProvider, runners map[string]worker.Runner, workDir string, workspaces WorkspaceManager) *Service {
	return NewServiceWithWorkspaceManagerAndTargets(store, brain, runners, workDir, workspaces, NewLocalTargetRegistry(), NewSSHRunner())
}

func NewServiceWithWorkspaceManagerAndTargets(store eventstore.Store, brain BrainProvider, runners map[string]worker.Runner, workDir string, workspaces WorkspaceManager, targets *TargetRegistry, sshRunner SSHRunner) *Service {
	if workspaces == nil {
		workspaces = NewWorkspaceManager(WorkspaceVCSAuto, WorkspaceModeIsolated, "", WorkspaceCleanupRetain)
	}
	if targets == nil {
		targets = NewLocalTargetRegistry()
	}
	projects, err := NewDefaultProjectRegistry(workDir)
	if err != nil {
		projects, _ = NewProjectRegistry([]core.Project{{
			ID:          "default",
			Name:        "default",
			LocalPath:   workDir,
			VCS:         "auto",
			DefaultBase: "main",
		}}, "default")
	}
	return &Service{
		store:       store,
		broker:      NewBroker(),
		brain:       brain,
		runners:     runners,
		workDir:     workDir,
		projects:    projects,
		plugins:     NewPluginRegistry(builtinPlugins()),
		workspaces:  workspaces,
		targets:     targets,
		sshRunner:   sshRunner,
		prPublisher: NewLocalPullRequestPublisher(),
		remoteApply: applyRemotePatch,
		cancels:     map[string]context.CancelFunc{},
		tasks:       map[string]string{},
		steering:    map[string]chan string{},
		remoteRuns:  map[string]remoteRun{},
	}
}

func (s *Service) SetAssistant(assistant AssistantProvider) {
	s.assistant = assistant
	if assistant != nil && s.titles == nil {
		s.titles = AssistantTitleGenerator{Assistant: assistant}
	}
}

func (s *Service) SetTitleGenerator(generator TitleGenerator) {
	s.titles = generator
}

func (s *Service) SetProjects(projects *ProjectRegistry) {
	if projects != nil {
		s.projects = projects
		s.workDir = projects.Default().LocalPath
	}
}

func (s *Service) LoadProjects(ctx context.Context, seed *ProjectRegistry) error {
	if seed == nil {
		return errors.New("project seed registry is not configured")
	}
	projects, defaultID, err := s.store.ListProjects(ctx)
	if err != nil {
		return err
	}
	if len(projects) == 0 {
		defaultProject := seed.Default()
		for _, project := range seed.Snapshot() {
			if _, err := s.store.SaveProject(ctx, project, project.ID == defaultProject.ID); err != nil {
				return err
			}
		}
		projects, defaultID, err = s.store.ListProjects(ctx)
		if err != nil {
			return err
		}
	}
	registry, err := NewProjectRegistry(projects, defaultID)
	if err != nil {
		return err
	}
	s.SetProjects(registry)
	return nil
}

func (s *Service) CreateProject(ctx context.Context, project core.Project) (core.Project, error) {
	normalized, err := normalizeProject(project)
	if err != nil {
		return core.Project{}, err
	}
	if s.projects != nil {
		if _, exists := s.projects.Get(normalized.ID); exists {
			return core.Project{}, fmt.Errorf("project %q already exists", normalized.ID)
		}
	}
	saved, err := s.store.CreateProject(ctx, normalized)
	if err != nil {
		return core.Project{}, err
	}
	if s.projects == nil {
		registry, err := NewProjectRegistry([]core.Project{saved}, saved.ID)
		if err != nil {
			return core.Project{}, err
		}
		s.SetProjects(registry)
		return saved, nil
	}
	if _, err := s.projects.Add(saved); err != nil {
		return core.Project{}, err
	}
	return saved, nil
}

func (s *Service) UpdateProject(ctx context.Context, id string, project core.Project) (core.Project, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return core.Project{}, errors.New("project id is required")
	}
	project.ID = id
	normalized, err := normalizeProject(project)
	if err != nil {
		return core.Project{}, err
	}
	if s.projects == nil {
		return core.Project{}, errors.New("project registry is not configured")
	}
	if _, exists := s.projects.Get(id); !exists {
		return core.Project{}, eventstore.ErrNotFound
	}
	saved, err := s.store.SaveProject(ctx, normalized, false)
	if err != nil {
		return core.Project{}, err
	}
	if _, err := s.projects.Update(saved); err != nil {
		return core.Project{}, err
	}
	return saved, nil
}

func (s *Service) DeleteProject(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("project id is required")
	}
	if s.projects == nil {
		return errors.New("project registry is not configured")
	}
	if _, exists := s.projects.Get(id); !exists {
		return eventstore.ErrNotFound
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, task := range snapshot.Tasks {
		if task.ProjectID == id && !isTerminalTaskStatus(task.Status) {
			return fmt.Errorf("cannot delete project %q while task %q is nonterminal", id, task.ID)
		}
	}
	if err := s.store.DeleteProject(ctx, id); err != nil {
		return err
	}
	return s.projects.Delete(id)
}

func (s *Service) ProjectHealth(ctx context.Context, id string) (core.ProjectHealth, error) {
	if s.projects == nil {
		return core.ProjectHealth{}, errors.New("project registry is not configured")
	}
	project, ok := s.projects.Get(id)
	if !ok {
		return core.ProjectHealth{}, eventstore.ErrNotFound
	}
	health := core.ProjectHealth{
		ProjectID:    project.ID,
		OK:           true,
		PathStatus:   "ok",
		VCSStatus:    "unknown",
		GitHubStatus: "not_configured",
		TargetStatus: "ok",
		CheckedAt:    time.Now().UTC(),
	}
	addError := func(message string) {
		health.OK = false
		health.Errors = append(health.Errors, message)
	}
	info, err := os.Stat(project.LocalPath)
	if err != nil {
		health.PathStatus = "missing"
		addError(err.Error())
		return health, nil
	}
	if !info.IsDir() {
		health.PathStatus = "not_directory"
		addError("localPath is not a directory")
		return health, nil
	}
	detectedVCS := detectProjectVCS(ctx, project.LocalPath)
	health.DetectedVCS = detectedVCS
	switch {
	case detectedVCS == "":
		health.VCSStatus = "not_detected"
		addError("no jj or git checkout detected")
	case project.VCS == "" || project.VCS == "auto" || project.VCS == detectedVCS:
		health.VCSStatus = "ok"
	default:
		health.VCSStatus = "mismatch"
		addError(fmt.Sprintf("configured vcs %q does not match detected %q", project.VCS, detectedVCS))
	}
	health.DetectedRepo = detectGitHubRepo(ctx, project.LocalPath)
	if project.Repo != "" || project.UpstreamRepo != "" {
		if health.DetectedRepo == "" {
			health.GitHubStatus = "repo_not_detected"
		} else {
			health.GitHubStatus = "repo_detected"
		}
		if _, err := runCommand(ctx, project.LocalPath, "gh", "auth", "status", "--hostname", "github.com"); err != nil {
			health.GitHubStatus = "auth_not_ready"
		} else if health.GitHubStatus == "repo_detected" {
			health.GitHubStatus = "ok"
		} else {
			health.GitHubStatus = "auth_ok"
		}
	}
	health.DetectedBase = detectDefaultBase(ctx, project.LocalPath, project.Repo)
	if project.DefaultBase == "" {
		health.DefaultBaseStatus = "missing"
		addError("defaultBase is not configured")
	} else if health.DetectedBase == "" || health.DetectedBase == project.DefaultBase {
		health.DefaultBaseStatus = "ok"
	} else {
		health.DefaultBaseStatus = "mismatch"
	}
	if len(project.TargetLabels) > 0 && s.targets != nil {
		_, err := s.targets.Select(Plan{Metadata: map[string]any{"targetLabels": project.TargetLabels}})
		if err != nil {
			health.TargetStatus = "no_matching_target"
			addError(err.Error())
		}
	}
	return health, nil
}

func (s *Service) SetPlugins(plugins *PluginRegistry) {
	if plugins != nil {
		s.plugins = plugins
	}
}

func (s *Service) SetPluginRuntimeContext(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.pluginCtx = ctx
}

func (s *Service) LoadRegisteredTargets(ctx context.Context) error {
	targets, err := s.store.ListTargets(ctx)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if _, err := s.registerTargetRuntime(targetConfigFromCore(target)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) RegisterTarget(ctx context.Context, target core.TargetConfig) (core.TargetConfig, error) {
	registered, err := s.registerTargetRuntime(targetConfigFromCore(target))
	if err != nil {
		return core.TargetConfig{}, err
	}
	out := coreTargetConfig(registered)
	if _, err := s.store.SaveTarget(ctx, out); err != nil {
		return core.TargetConfig{}, err
	}
	s.RefreshTargetHealthFor(ctx, registered.ID)
	return out, nil
}

func (s *Service) DeleteTarget(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("target id is required")
	}
	if err := s.targets.Delete(id); err != nil {
		return err
	}
	if err := s.store.DeleteTarget(ctx, id); err != nil && !errors.Is(err, eventstore.ErrNotFound) {
		return err
	}
	return nil
}

func (s *Service) registerTargetRuntime(target TargetConfig) (TargetConfig, error) {
	if s.targets == nil {
		s.targets = NewLocalTargetRegistry()
	}
	return s.targets.Register(target)
}

func (s *Service) LoadRegisteredPlugins(ctx context.Context) error {
	plugins, err := s.store.ListPlugins(ctx)
	if err != nil {
		return err
	}
	for _, plugin := range plugins {
		if s.plugins != nil && s.plugins.IsBuiltIn(plugin.ID) {
			continue
		}
		if _, err := s.registerPluginRuntime(plugin, false); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) RegisterPlugin(ctx context.Context, plugin core.Plugin) (core.Plugin, error) {
	registered, err := s.registerPluginRuntime(plugin, true)
	if err != nil {
		return core.Plugin{}, err
	}
	return s.store.SavePlugin(ctx, registered)
}

func (s *Service) DeletePlugin(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("plugin id is required")
	}
	if err := s.store.DeletePlugin(ctx, id); err != nil && !errors.Is(err, eventstore.ErrNotFound) {
		return err
	}
	if err := s.plugins.Delete(id); err != nil {
		return err
	}
	delete(s.runners, strings.TrimPrefix(id, "runner:"))
	return nil
}

func (s *Service) registerPluginRuntime(plugin core.Plugin, probe bool) (core.Plugin, error) {
	if s.plugins == nil {
		s.plugins = NewPluginRegistry(nil)
	}
	registered, err := s.plugins.Register(plugin)
	if err != nil {
		return core.Plugin{}, err
	}
	if probe {
		s.plugins.Probe(context.Background())
		for _, current := range s.plugins.Snapshot() {
			if current.ID == registered.ID {
				registered = current
				break
			}
		}
	}
	for kind, runner := range s.plugins.RunnerPlugins() {
		s.runners[kind] = runner
	}
	if registered.Kind == "driver" && registered.Enabled {
		ctx := s.pluginCtx
		if ctx == nil {
			ctx = context.Background()
		}
		s.plugins.StartDrivers(ctx)
	}
	return registered, nil
}

func (s *Service) SetPullRequestPublisher(publisher PullRequestPublisher) {
	s.prPublisher = publisher
}

func (s *Service) SetRemotePatchApplier(applier func(context.Context, core.Project, PreparedWorkspace, WorkspaceChanges) (WorkerApplyResult, error)) {
	if applier != nil {
		s.remoteApply = applier
	}
}

func (s *Service) StartTargetProbes(ctx context.Context, interval time.Duration) {
	if s == nil || s.targets == nil {
		return
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go func() {
		s.RefreshTargetHealth(ctx)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.RefreshTargetHealth(ctx)
			}
		}
	}()
}

func (s *Service) RefreshTargetHealth(ctx context.Context) {
	if s == nil || s.targets == nil {
		return
	}
	for _, target := range s.targets.Configs() {
		s.refreshTargetHealth(ctx, target)
	}
}

func (s *Service) RefreshTargetHealthFor(ctx context.Context, id string) {
	if s == nil || s.targets == nil {
		return
	}
	target, ok := s.targets.Get(id)
	if !ok {
		return
	}
	s.refreshTargetHealth(ctx, target)
}

func (s *Service) refreshTargetHealth(ctx context.Context, target TargetConfig) {
	if target.Kind == TargetKindLocal {
		s.targets.UpdateHealth(target.ID, core.TargetHealth{
			Status:      "ok",
			CheckedAt:   time.Now().UTC(),
			Reachable:   true,
			Tmux:        true,
			RepoPresent: true,
		}, core.TargetResources{})
		return
	}
	if target.Kind != TargetKindSSH {
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	health, resources := s.sshRunner.Probe(probeCtx, target)
	s.targets.UpdateHealth(target.ID, health, resources)
}

func (s *Service) Snapshot(ctx context.Context) (core.Snapshot, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Snapshot{}, err
	}
	if s.targets != nil {
		snapshot.Targets = s.targets.Snapshot()
	}
	if s.projects != nil {
		snapshot.Projects = s.projects.Snapshot()
	}
	if s.plugins != nil {
		snapshot.Plugins = s.plugins.Snapshot()
	}
	return snapshot, nil
}

func (s *Service) RecoverRemoteWorkers(ctx context.Context) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := s.cancelStaleLocalWorkers(ctx, snapshot); err != nil {
		return err
	}
	completed := map[string]bool{}
	for _, event := range snapshot.Events {
		if event.Type == core.EventWorkerCompleted {
			completed[event.WorkerID] = true
		}
	}
	for _, node := range snapshot.ExecutionNodes {
		if node.TargetKind != string(TargetKindSSH) || node.WorkerID == "" || completed[node.WorkerID] {
			continue
		}
		if node.Status != core.WorkerRunning && node.Status != core.WorkerQueued {
			continue
		}
		target, ok := s.targets.Get(node.TargetID)
		if !ok {
			continue
		}
		run := remoteRun{
			Target:  target,
			Session: node.RemoteSession,
			RunDir:  node.RemoteRunDir,
			WorkDir: node.RemoteWorkDir,
			Status:  "running",
		}
		go s.recoverRemoteWorker(context.Background(), node, run)
	}
	return nil
}

func (s *Service) cancelStaleLocalWorkers(ctx context.Context, snapshot core.Snapshot) error {
	nodesByWorker := map[string]core.ExecutionNode{}
	for _, node := range snapshot.ExecutionNodes {
		if node.WorkerID != "" {
			nodesByWorker[node.WorkerID] = node
		}
	}
	for _, worker := range snapshot.Workers {
		if isTerminalWorkerStatus(worker.Status) {
			continue
		}
		node := nodesByWorker[worker.ID]
		if node.TargetKind == string(TargetKindSSH) {
			continue
		}
		_, err := s.append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   worker.TaskID,
			WorkerID: worker.ID,
			Payload: core.MustJSON(map[string]any{
				"status":  core.WorkerCanceled,
				"summary": "Local worker was marked canceled during daemon startup recovery.",
				"error":   "local worker did not have a recoverable process handle after daemon restart",
			}),
		})
		if err != nil {
			return err
		}
		if _, err := s.append(ctx, core.Event{
			Type:   core.EventTaskStatus,
			TaskID: worker.TaskID,
			Payload: core.MustJSON(map[string]any{
				"status": core.TaskCanceled,
			}),
		}); err != nil {
			return err
		}
	}
	return nil
}

func isTerminalWorkerStatus(status core.WorkerStatus) bool {
	return status == core.WorkerSucceeded || status == core.WorkerFailed || status == core.WorkerCanceled
}

func (s *Service) recoverRemoteWorker(ctx context.Context, node core.ExecutionNode, run remoteRun) {
	runState := &workerRunState{}
	sink := eventSink{service: s, taskID: node.TaskID, workerID: node.WorkerID, state: runState}
	status, err := s.sshRunner.Poll(ctx, run, worker.ParserForKind(node.WorkerKind), sink)
	workerStatus, statusErr := remoteStatusToWorkerStatus(status)
	if err != nil && !errors.Is(err, context.Canceled) {
		statusErr = err
		workerStatus = core.WorkerFailed
	}
	changes := WorkspaceChanges{
		Root:     run.RunDir,
		CWD:      run.WorkDir,
		Mode:     "remote",
		VCSType:  "ssh",
		DiffStat: "remote worker changes are reported through worker output",
	}
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   node.TaskID,
		WorkerID: node.WorkerID,
		Payload:  core.MustJSON(runState.completionPayload(workerStatus, statusErr, changes)),
	})
}

func (s *Service) Events(ctx context.Context, afterID int64, limit int) ([]core.Event, error) {
	return s.store.ListEvents(ctx, afterID, limit)
}

func (s *Service) Subscribe() (int, <-chan core.Event) {
	return s.broker.Subscribe()
}

func (s *Service) Unsubscribe(id int) {
	s.broker.Unsubscribe(id)
}

func (s *Service) CreateTask(ctx context.Context, req core.CreateTaskRequest) (core.Task, error) {
	if req.Prompt == "" {
		return core.Task{}, errors.New("prompt is required")
	}
	title := strings.TrimSpace(req.Title)
	metadata, err := createTaskMetadata(req)
	if err != nil {
		return core.Task{}, err
	}
	if stringMetadataValue(metadata["completionMode"]) == "" && promptRequestsGitHubCompletion(req.Prompt) {
		metadata["completionMode"] = "github"
		metadata["completionModeInferred"] = true
	}
	if title == "" {
		title = s.generateTaskTitle(ctx, req.Prompt)
		metadata["titleGenerated"] = true
	}
	project, err := s.projects.Resolve(req)
	if err != nil {
		return core.Task{}, err
	}
	metadata["projectId"] = project.ID
	if req.Source != "" || req.ExternalID != "" {
		if req.Source == "" || req.ExternalID == "" {
			return core.Task{}, errors.New("source and externalId must be provided together")
		}
		if existing, ok, err := s.FindTaskByExternalID(ctx, req.Source, req.ExternalID); err != nil {
			return core.Task{}, err
		} else if ok {
			return existing, nil
		}
	}

	taskID := uuid.NewString()
	created, err := s.append(ctx, core.Event{
		Type:   core.EventTaskCreated,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"projectId": project.ID,
			"title":     title,
			"prompt":    req.Prompt,
			"metadata":  metadata,
		}),
	})
	if err != nil {
		return core.Task{}, err
	}

	task := core.Task{
		ID:        taskID,
		ProjectID: project.ID,
		Title:     title,
		Prompt:    req.Prompt,
		Status:    core.TaskQueued,
		CreatedAt: created.At,
		UpdatedAt: created.At,
		Metadata:  core.MustJSON(metadata),
	}

	go s.runTask(context.Background(), task)
	return task, nil
}

func (s *Service) generateTaskTitle(ctx context.Context, prompt string) string {
	if s.titles != nil {
		if title, err := s.titles.GenerateTitle(ctx, prompt); err == nil && strings.TrimSpace(title) != "" {
			return title
		}
	}
	return fallbackTaskTitle(prompt)
}

func (s *Service) Ask(ctx context.Context, req core.AssistantRequest) (core.AssistantResponse, error) {
	req.Message = strings.TrimSpace(req.Message)
	if req.Message == "" {
		return core.AssistantResponse{}, errors.New("message is required")
	}
	if strings.TrimSpace(req.ConversationID) == "" {
		req.ConversationID = uuid.NewString()
	}
	if session := s.assistantSession(ctx, req.ConversationID); session.ProviderSessionID != "" {
		req.Provider = session.Provider
		req.ProviderSessionID = session.ProviderSessionID
	}
	if _, err := s.append(ctx, core.Event{
		Type: core.EventAssistantAsked,
		Payload: core.MustJSON(map[string]any{
			"conversationId":    req.ConversationID,
			"message":           req.Message,
			"context":           req.Context,
			"workDir":           req.WorkDir,
			"provider":          req.Provider,
			"providerSessionId": req.ProviderSessionID,
		}),
	}); err != nil {
		return core.AssistantResponse{}, err
	}
	assistant := s.assistant
	if assistant == nil {
		var ok bool
		assistant, ok = s.brain.(AssistantProvider)
		if !ok {
			return core.AssistantResponse{}, errors.New("assistant brain is not configured")
		}
	}
	response, err := assistant.Ask(ctx, req)
	if err != nil {
		return core.AssistantResponse{}, err
	}
	if strings.TrimSpace(response.ConversationID) == "" {
		response.ConversationID = req.ConversationID
	}
	metadata := assistantResponseMetadata(response)
	if _, err := s.append(ctx, core.Event{
		Type: core.EventAssistantAnswered,
		Payload: core.MustJSON(map[string]any{
			"conversationId":    response.ConversationID,
			"message":           response.Message,
			"provider":          response.Provider,
			"providerSessionId": response.ProviderSessionID,
			"metadata":          metadata,
		}),
	}); err != nil {
		return core.AssistantResponse{}, err
	}
	response.Metadata = metadata
	return response, nil
}

type assistantSession struct {
	Provider          string
	ProviderSessionID string
}

func (s *Service) assistantSession(ctx context.Context, conversationID string) assistantSession {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return assistantSession{}
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventAssistantAnswered {
			continue
		}
		var payload struct {
			ConversationID    string          `json:"conversationId"`
			Provider          string          `json:"provider"`
			ProviderSessionID string          `json:"providerSessionId"`
			Metadata          json.RawMessage `json:"metadata"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.ConversationID != conversationID {
			continue
		}
		provider := payload.Provider
		sessionID := payload.ProviderSessionID
		if sessionID == "" && len(payload.Metadata) > 0 {
			var metadata map[string]any
			if err := json.Unmarshal(payload.Metadata, &metadata); err == nil {
				provider = nonEmpty(provider, stringMetadataValue(metadata["assistant"]), stringMetadataValue(metadata["brain"]))
				sessionID = stringMetadataValue(metadata["providerSessionId"])
			}
		}
		if sessionID != "" {
			return assistantSession{Provider: provider, ProviderSessionID: sessionID}
		}
	}
	return assistantSession{}
}

func assistantResponseMetadata(response core.AssistantResponse) json.RawMessage {
	metadata := map[string]any{}
	if len(response.Metadata) > 0 && string(response.Metadata) != "null" {
		_ = json.Unmarshal(response.Metadata, &metadata)
	}
	if response.Provider != "" {
		metadata["assistant"] = response.Provider
	}
	if response.ProviderSessionID != "" {
		metadata["providerSessionId"] = response.ProviderSessionID
	}
	return core.MustJSON(metadata)
}

func (s *Service) PublishTaskPullRequest(ctx context.Context, taskID string, req core.PublishPullRequestRequest) (core.PullRequest, error) {
	if s.prPublisher == nil {
		return core.PullRequest{}, errors.New("pull request publisher is not configured")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return core.PullRequest{}, eventstore.ErrNotFound
	}
	workerID := strings.TrimSpace(req.WorkerID)
	if !canPublishPullRequestForTask(task) && workerID == "" {
		return core.PullRequest{}, errors.New("provide workerId when publishing before task completion")
	}
	project := s.projectForTask(task)
	sourceRoot := project.LocalPath
	if workerID == "" {
		if task.FinalCandidateWorkerID != "" {
			workerID = task.FinalCandidateWorkerID
		} else {
			candidates := applyCandidates(snapshot, taskID)
			unapplied := unappliedCandidates(candidates)
			switch len(unapplied) {
			case 0:
				if latest := latestAppliedWorker(snapshot, taskID); latest != "" {
					workerID = latest
				}
			case 1:
				workerID = unapplied[0].WorkerID
			default:
				return core.PullRequest{}, errors.New("multiple unapplied worker changes exist; provide workerId")
			}
		}
	}
	if workerID != "" {
		if !workerBelongsToTask(snapshot, workerID, taskID) {
			return core.PullRequest{}, errors.New("worker does not belong to task")
		}
		sourceRoot, err = s.pullRequestSourceRootForWorker(ctx, snapshot, workerID, project)
		if err != nil {
			return core.PullRequest{}, err
		}
	}
	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = task.Title
	}
	body := strings.TrimSpace(req.Body)
	if body == "" {
		body = fmt.Sprintf("Task: `%s`\n\n%s", task.ID, task.Prompt)
	}
	pr, err := s.prPublisher.Publish(ctx, PullRequestPublishSpec{
		TaskID:        taskID,
		WorkerID:      workerID,
		WorkDir:       sourceRoot,
		Repo:          pullRequestTargetRepo(req, project, task),
		Base:          nonEmpty(req.Base, project.DefaultBase),
		Branch:        req.Branch,
		HeadRepoOwner: pullRequestHeadRepoOwner(project),
		PushRemote:    project.PushRemote,
		BranchPrefix:  project.PullRequestPolicy.BranchPrefix,
		Title:         title,
		Body:          body,
		Draft:         req.Draft || project.PullRequestPolicy.Draft,
		Metadata: map[string]any{
			"workerId":          workerID,
			"workDir":           sourceRoot,
			"projectId":         project.ID,
			"branchPrefix":      project.PullRequestPolicy.BranchPrefix,
			"mergeAllowed":      project.PullRequestPolicy.AllowMerge,
			"autoMerge":         project.PullRequestPolicy.AutoMerge,
			"pullRequestPolicy": project.PullRequestPolicy,
		},
	})
	if err != nil {
		return core.PullRequest{}, err
	}
	if pr.ID == "" {
		pr.ID = uuid.NewString()
	}
	pr.TaskID = taskID
	if err := s.recordTaskMilestone(ctx, taskID, "pr_opened", "pr_opened", "Pull request opened.", map[string]any{
		"pullRequestId": pr.ID,
		"url":           pr.URL,
		"repo":          pr.Repo,
		"number":        pr.Number,
		"branch":        pr.Branch,
	}); err != nil {
		return core.PullRequest{}, err
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingExternal, "pr_opened", "Pull request opened; objective continues until the PR reaches its terminal condition."); err != nil {
		return core.PullRequest{}, err
	}
	if err := s.setTaskStatus(ctx, taskID, core.TaskWaiting); err != nil {
		return core.PullRequest{}, err
	}
	if err := s.recordPullRequestPublished(ctx, pr); err != nil {
		return core.PullRequest{}, err
	}
	if err := s.recordPullRequestArtifact(ctx, pr); err != nil {
		return core.PullRequest{}, err
	}
	return pr, nil
}

func (s *Service) pullRequestSourceRootForWorker(ctx context.Context, snapshot core.Snapshot, workerID string, project core.Project) (string, error) {
	if appliedRoot := appliedWorkerSourceRoot(snapshot, workerID); appliedRoot != "" {
		return appliedRoot, nil
	}
	workspace, err := s.workspaceForWorker(ctx, workerID)
	if err != nil {
		return "", err
	}
	if workspace.VCSType != "ssh" {
		if workspace.CWD != "" {
			return workspace.CWD, nil
		}
		return project.LocalPath, nil
	}
	result, err := s.ApplyWorkerChanges(ctx, workerID)
	if err != nil {
		if appliedRoot := appliedWorkerSourceRootFromStore(ctx, s.store, workerID); appliedRoot != "" {
			return appliedRoot, nil
		}
		return "", err
	}
	return nonEmpty(result.SourceRoot, project.LocalPath), nil
}

func (s *Service) WatchPullRequests(ctx context.Context, taskID string, req core.WatchPullRequestsRequest) ([]core.PullRequest, error) {
	if s.prPublisher == nil {
		return nil, errors.New("pull request publisher is not configured")
	}
	lister, ok := s.prPublisher.(PullRequestLister)
	if !ok {
		return nil, errors.New("pull request publisher cannot list existing pull requests")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	project := s.projectForTask(task)
	repo := strings.TrimSpace(req.Repo)
	if repo == "" {
		repo = project.UpstreamRepo
	}
	if repo == "" {
		repo = project.Repo
	}
	if repo == "" && strings.TrimSpace(req.URL) != "" {
		parsedRepo, _ := parsePullRequestURL(req.URL)
		repo = parsedRepo
	}
	metadata := map[string]any{
		"watch": true,
		"repo":  repo,
		"state": nonEmpty(req.State, "open"),
	}
	if req.Number > 0 {
		metadata["number"] = req.Number
	}
	if req.URL != "" {
		metadata["url"] = req.URL
	}
	if req.Author != "" {
		metadata["author"] = req.Author
	}
	if req.HeadBranch != "" {
		metadata["headBranch"] = req.HeadBranch
	}
	prs, err := lister.List(ctx, PullRequestListSpec{
		TaskID:     taskID,
		Repo:       repo,
		Number:     req.Number,
		URL:        req.URL,
		State:      req.State,
		Author:     req.Author,
		HeadBranch: req.HeadBranch,
		Limit:      req.Limit,
		Metadata:   metadata,
	})
	if err != nil {
		return nil, err
	}
	if len(prs) == 0 {
		return nil, errors.New("no pull requests matched watch request")
	}
	if err := s.recordTaskMilestone(ctx, taskID, "pull_requests_watched", "waiting_external", fmt.Sprintf("Watching %d existing pull request(s).", len(prs)), map[string]any{
		"count": len(prs),
		"repo":  repo,
	}); err != nil {
		return nil, err
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingExternal, "watching_pull_requests", fmt.Sprintf("Watching %d pull request(s) for GitHub state changes.", len(prs))); err != nil {
		return nil, err
	}
	if err := s.setTaskStatus(ctx, taskID, core.TaskWaiting); err != nil {
		return nil, err
	}
	for _, pr := range prs {
		pr.ID = watchedPullRequestID(pr)
		pr.TaskID = taskID
		if pr.Repo == "" {
			pr.Repo = repo
		}
		if len(pr.Metadata) == 0 {
			pr.Metadata = core.MustJSON(metadata)
		}
		if err := s.recordPullRequestPublished(ctx, pr); err != nil {
			return nil, err
		}
		if err := s.recordPullRequestArtifact(ctx, pr); err != nil {
			return nil, err
		}
	}
	return prs, nil
}

func watchedPullRequestID(pr core.PullRequest) string {
	repo := strings.TrimSpace(pr.Repo)
	if repo != "" && pr.Number > 0 {
		return "github:" + repo + "#" + fmt.Sprint(pr.Number)
	}
	if strings.TrimSpace(pr.URL) != "" {
		repo, number := parsePullRequestURL(pr.URL)
		if repo != "" && number > 0 {
			return "github:" + repo + "#" + fmt.Sprint(number)
		}
	}
	if strings.TrimSpace(pr.ID) != "" {
		return pr.ID
	}
	return newPullRequestID()
}

func (s *Service) RefreshPullRequest(ctx context.Context, prID string) (core.PullRequest, error) {
	if s.prPublisher == nil {
		return core.PullRequest{}, errors.New("pull request publisher is not configured")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, err
	}
	var pr core.PullRequest
	var ok bool
	for _, candidate := range snapshot.PullRequests {
		if candidate.ID == prID {
			pr = candidate
			ok = true
			break
		}
	}
	if !ok {
		return core.PullRequest{}, eventstore.ErrNotFound
	}
	checked, err := s.prPublisher.Inspect(ctx, pr)
	if err != nil {
		return core.PullRequest{}, err
	}
	checked.ID = pr.ID
	checked.TaskID = pr.TaskID
	event, err := s.append(ctx, core.Event{
		Type:   core.EventPRStatusChecked,
		TaskID: pr.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":           checked.ID,
			"state":        checked.State,
			"draft":        checked.Draft,
			"checksStatus": checked.ChecksStatus,
			"mergeStatus":  checked.MergeStatus,
			"reviewStatus": checked.ReviewStatus,
			"metadata":     checked.Metadata,
		}),
	})
	if err != nil {
		return core.PullRequest{}, err
	}
	checked.UpdatedAt = event.At
	if err := s.recordPullRequestArtifact(ctx, checked); err != nil {
		return core.PullRequest{}, err
	}
	status, phase := objectiveForPullRequest(checked)
	if phase != "" {
		if err := s.updateTaskObjective(ctx, checked.TaskID, status, phase, pullRequestObjectiveSummary(checked, phase)); err != nil {
			return core.PullRequest{}, err
		}
	}
	if strings.EqualFold(checked.State, "MERGED") {
		if err := s.recordTaskMilestone(ctx, checked.TaskID, "pr_merged", "merged", "Pull request merged.", map[string]any{
			"pullRequestId": checked.ID,
			"url":           checked.URL,
			"repo":          checked.Repo,
			"number":        checked.Number,
		}); err != nil {
			return core.PullRequest{}, err
		}
		if err := s.setTaskStatus(ctx, checked.TaskID, core.TaskSucceeded); err != nil {
			return core.PullRequest{}, err
		}
		if err := s.completeRelatedPullRequestTasks(ctx, snapshot, checked, core.TaskSucceeded, core.ObjectiveSatisfied, "pr_merged", "merged", "Pull request merged."); err != nil {
			return core.PullRequest{}, err
		}
	} else if strings.EqualFold(checked.State, "CLOSED") {
		if err := s.recordTaskMilestone(ctx, checked.TaskID, "pr_closed", "pr_closed", "Pull request closed without merge.", map[string]any{
			"pullRequestId": checked.ID,
			"url":           checked.URL,
			"repo":          checked.Repo,
			"number":        checked.Number,
		}); err != nil {
			return core.PullRequest{}, err
		}
		if err := s.setTaskStatus(ctx, checked.TaskID, core.TaskCanceled); err != nil {
			return core.PullRequest{}, err
		}
		if err := s.completeRelatedPullRequestTasks(ctx, snapshot, checked, core.TaskCanceled, core.ObjectiveAbandoned, "pr_closed", "pr_closed", "Pull request closed without merge."); err != nil {
			return core.PullRequest{}, err
		}
	}
	return checked, nil
}

func (s *Service) ReconcilePullRequestTerminalTasks(ctx context.Context, prID string) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	var pr core.PullRequest
	var ok bool
	for _, candidate := range snapshot.PullRequests {
		if candidate.ID == prID {
			pr = candidate
			ok = true
			break
		}
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	switch {
	case strings.EqualFold(pr.State, "MERGED"):
		return s.completeRelatedPullRequestTasks(ctx, snapshot, pr, core.TaskSucceeded, core.ObjectiveSatisfied, "pr_merged", "merged", "Pull request merged.")
	case strings.EqualFold(pr.State, "CLOSED"):
		return s.completeRelatedPullRequestTasks(ctx, snapshot, pr, core.TaskCanceled, core.ObjectiveAbandoned, "pr_closed", "pr_closed", "Pull request closed without merge.")
	default:
		return nil
	}
}

func (s *Service) completeRelatedPullRequestTasks(ctx context.Context, snapshot core.Snapshot, pr core.PullRequest, taskStatus core.TaskStatus, objectiveStatus core.ObjectiveStatus, milestone string, phase string, summary string) error {
	for _, task := range snapshot.Tasks {
		if task.ID == pr.TaskID || isTerminalTaskStatus(task.Status) || !taskWatchesPullRequest(task, pr) {
			continue
		}
		if err := s.updateTaskObjective(ctx, task.ID, objectiveStatus, phase, summary); err != nil {
			return err
		}
		if err := s.recordTaskMilestone(ctx, task.ID, milestone, phase, summary, map[string]any{
			"pullRequestId": pr.ID,
			"url":           pr.URL,
			"repo":          pr.Repo,
			"number":        pr.Number,
		}); err != nil {
			return err
		}
		if err := s.setTaskStatus(ctx, task.ID, taskStatus); err != nil {
			return err
		}
	}
	return nil
}

func taskWatchesPullRequest(task core.Task, pr core.PullRequest) bool {
	if len(task.Metadata) == 0 {
		return false
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return false
	}
	if pullRequestID := strings.TrimSpace(stringMetadataValue(metadata["pullRequestId"])); pullRequestID != "" && pullRequestID == pr.ID {
		return true
	}
	repo := strings.TrimSpace(stringMetadataValue(metadata["repo"]))
	number := intMetadata(metadata, "number")
	return repo != "" && pr.Repo != "" && strings.EqualFold(repo, pr.Repo) && number > 0 && number == pr.Number
}

func (s *Service) StartPullRequestBabysitter(ctx context.Context, prID string) (core.Task, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Task{}, err
	}
	var pr core.PullRequest
	var ok bool
	for _, candidate := range snapshot.PullRequests {
		if candidate.ID == prID {
			pr = candidate
			ok = true
			break
		}
	}
	if !ok {
		return core.Task{}, eventstore.ErrNotFound
	}
	task, ok := findTask(snapshot, pr.TaskID)
	if !ok {
		return core.Task{}, eventstore.ErrNotFound
	}
	if isTerminalTaskStatus(task.Status) && !strings.EqualFold(pr.State, "OPEN") {
		return task, nil
	}
	if _, err := s.append(ctx, core.Event{
		Type:   core.EventPRBabysitter,
		TaskID: pr.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":               pr.ID,
			"babysitterTaskId": pr.TaskID,
		}),
	}); err != nil {
		return core.Task{}, err
	}
	if !isTerminalTaskStatus(task.Status) {
		if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveWaitingExternal, "pr_open", "Pull request is open; waiting on external GitHub state."); err != nil {
			return core.Task{}, err
		}
		if task.Status != core.TaskWaiting {
			if err := s.setTaskStatus(ctx, task.ID, core.TaskWaiting); err != nil {
				return core.Task{}, err
			}
		}
	}
	return task, nil
}

func (s *Service) ContinueTaskForPullRequest(ctx context.Context, prID string) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	var pr core.PullRequest
	var ok bool
	for _, candidate := range snapshot.PullRequests {
		if candidate.ID == prID {
			pr = candidate
			ok = true
			break
		}
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	task, ok := findTask(snapshot, pr.TaskID)
	if !ok {
		return eventstore.ErrNotFound
	}
	if task.Status == core.TaskRunning || task.Status == core.TaskPlanning || task.Status == core.TaskQueued {
		return nil
	}
	if isTerminalTaskStatus(task.Status) {
		return nil
	}
	if pullRequestFollowUpStartedAfterLatestStatus(snapshot, pr.ID) {
		return nil
	}
	attempt := pullRequestFollowUpAttempt(snapshot, pr.ID) + 1
	if _, err := s.append(ctx, core.Event{
		Type:   core.EventPRFollowUp,
		TaskID: pr.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":      pr.ID,
			"attempt": attempt,
			"reason":  "pull_request_needs_work",
		}),
	}); err != nil {
		return err
	}
	if err := s.recordTaskMilestone(ctx, pr.TaskID, fmt.Sprintf("pr_followup_%d", attempt), "pr_needs_work", "Pull request needs follow-up work.", map[string]any{
		"pullRequestId": pr.ID,
		"url":           pr.URL,
		"repo":          pr.Repo,
		"number":        pr.Number,
		"attempt":       attempt,
	}); err != nil {
		return err
	}
	if err := s.updateTaskObjective(ctx, pr.TaskID, core.ObjectiveActive, "pr_needs_work", "Pull request needs follow-up work from checks or review."); err != nil {
		return err
	}
	return s.SteerTask(ctx, pr.TaskID, core.SteeringRequest{Message: pullRequestFollowUpPrompt(pr)})
}

func activePullRequestBabysitter(snapshot core.Snapshot, pr core.PullRequest) core.Task {
	if pr.BabysitterTaskID == "" {
		return core.Task{}
	}
	task, ok := findTask(snapshot, pr.BabysitterTaskID)
	if !ok || isTerminalTaskStatus(task.Status) {
		return core.Task{}
	}
	return task
}

func pullRequestBabysitterAttempt(snapshot core.Snapshot, prID string) int {
	attempt := 0
	for _, event := range snapshot.Events {
		if event.Type != core.EventPRBabysitter {
			continue
		}
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.ID == prID {
			attempt++
		}
	}
	return attempt
}

func pullRequestFollowUpAttempt(snapshot core.Snapshot, prID string) int {
	attempt := 0
	for _, event := range snapshot.Events {
		if event.Type != core.EventPRFollowUp {
			continue
		}
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.ID == prID {
			attempt++
		}
	}
	return attempt
}

func pullRequestFollowUpStartedAfterLatestStatus(snapshot core.Snapshot, prID string) bool {
	latestStatusEvent := int64(0)
	latestFollowUpEvent := int64(0)
	for _, event := range snapshot.Events {
		var payload struct {
			ID string `json:"id"`
		}
		switch event.Type {
		case core.EventPRStatusChecked:
			if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.ID == prID {
				latestStatusEvent = event.ID
			}
		case core.EventPRFollowUp:
			if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.ID == prID {
				latestFollowUpEvent = event.ID
			}
		}
	}
	return latestFollowUpEvent > 0 && latestFollowUpEvent >= latestStatusEvent
}

func (s *Service) recordPullRequestArtifact(ctx context.Context, pr core.PullRequest) error {
	name := pr.Title
	if name == "" {
		name = fmt.Sprintf("%s#%d", pr.Repo, pr.Number)
	}
	return s.recordTaskArtifact(ctx, pr.TaskID, pr.ID, "github_pull_request", name, pr.URL, pr.Branch, map[string]any{
		"repo":         pr.Repo,
		"number":       pr.Number,
		"state":        pr.State,
		"draft":        pr.Draft,
		"checksStatus": pr.ChecksStatus,
		"mergeStatus":  pr.MergeStatus,
		"reviewStatus": pr.ReviewStatus,
	})
}

func (s *Service) recordPullRequestPublished(ctx context.Context, pr core.PullRequest) error {
	_, err := s.append(ctx, core.Event{
		Type:   core.EventPRPublished,
		TaskID: pr.TaskID,
		Payload: core.MustJSON(map[string]any{
			"id":           pr.ID,
			"repo":         pr.Repo,
			"number":       pr.Number,
			"url":          pr.URL,
			"branch":       pr.Branch,
			"base":         pr.Base,
			"title":        pr.Title,
			"state":        pr.State,
			"draft":        pr.Draft,
			"checksStatus": pr.ChecksStatus,
			"mergeStatus":  pr.MergeStatus,
			"reviewStatus": pr.ReviewStatus,
			"metadata":     pr.Metadata,
		}),
	})
	return err
}

func objectiveForPullRequest(pr core.PullRequest) (core.ObjectiveStatus, string) {
	switch strings.ToUpper(strings.TrimSpace(pr.State)) {
	case "MERGED":
		return core.ObjectiveSatisfied, "merged"
	case "CLOSED":
		return core.ObjectiveAbandoned, "pr_closed"
	}
	if strings.EqualFold(pr.ChecksStatus, "failing") || strings.EqualFold(pr.ReviewStatus, "CHANGES_REQUESTED") {
		return core.ObjectiveActive, "pr_needs_work"
	}
	if strings.EqualFold(pr.ChecksStatus, "success") && (pr.ReviewStatus == "" || strings.EqualFold(pr.ReviewStatus, "APPROVED")) {
		return core.ObjectiveWaitingExternal, "ready_to_merge"
	}
	return core.ObjectiveWaitingExternal, "pr_open"
}

func pullRequestObjectiveSummary(pr core.PullRequest, phase string) string {
	switch phase {
	case "merged":
		return "Pull request merged."
	case "pr_closed":
		return "Pull request closed without merge."
	case "pr_needs_work":
		return "Pull request needs follow-up work from checks or review."
	case "ready_to_merge":
		return "Pull request is ready for merge."
	default:
		return "Pull request is open; waiting on external GitHub state."
	}
}

func (s *Service) FindTaskByExternalID(ctx context.Context, source string, externalID string) (core.Task, bool, error) {
	source = strings.TrimSpace(source)
	externalID = strings.TrimSpace(externalID)
	if source == "" || externalID == "" {
		return core.Task{}, false, errors.New("source and externalId are required")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Task{}, false, err
	}
	for _, task := range snapshot.Tasks {
		taskSource, taskExternalID := taskExternalRef(task)
		if taskSource == source && taskExternalID == externalID {
			return task, true, nil
		}
	}
	return core.Task{}, false, nil
}

func (s *Service) SteerTask(ctx context.Context, taskID string, req core.SteeringRequest) error {
	if req.Message == "" {
		return errors.New("message is required")
	}
	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskSteered,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"message": req.Message,
		}),
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	for workerID, ch := range s.steering {
		if s.tasks[workerID] != taskID {
			continue
		}
		select {
		case ch <- req.Message:
		default:
		}
	}
	s.mu.Unlock()
	snapshot, snapshotErr := s.store.Snapshot(ctx)
	if snapshotErr == nil && taskStatus(snapshot, taskID) == core.TaskWaiting {
		go s.resumeWaitingTask(context.Background(), taskID, req.Message)
	}
	return err
}

func (s *Service) RetryTask(ctx context.Context, taskID string) (core.Task, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Task{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return core.Task{}, eventstore.ErrNotFound
	}
	if task.Status != core.TaskFailed && task.Status != core.TaskCanceled {
		return core.Task{}, errors.New("can only retry failed or canceled tasks")
	}
	if task.Status == core.TaskFailed {
		initial, results, graphErr := retryGraphStateForTask(snapshot, taskID)
		if graphErr == nil && taskFailureRecoverableFromGraph(snapshot, taskID, results) {
			if err := s.setTaskStatus(ctx, taskID, core.TaskPlanning); err != nil {
				return core.Task{}, err
			}
			task.Status = core.TaskPlanning
			go s.retryGraphTask(context.Background(), task, initial, results)
			return task, nil
		}
	}
	if task.Status == core.TaskFailed && taskFailedDuringDynamicReplan(snapshot, taskID) {
		initial, results, err := retryGraphStateForTask(snapshot, taskID)
		if err != nil {
			return core.Task{}, err
		}
		if err := s.setTaskStatus(ctx, taskID, core.TaskPlanning); err != nil {
			return core.Task{}, err
		}
		task.Status = core.TaskPlanning
		go s.retryGraphTask(context.Background(), task, initial, results)
		return task, nil
	}
	plan, err := retryPlanForTask(snapshot, taskID)
	if err != nil {
		return core.Task{}, err
	}
	if err := s.setTaskStatus(ctx, taskID, core.TaskPlanning); err != nil {
		return core.Task{}, err
	}
	task.Status = core.TaskPlanning
	go s.retryTask(context.Background(), task, plan)
	return task, nil
}

func (s *Service) CancelWorker(ctx context.Context, workerID string) error {
	s.mu.Lock()
	cancel := s.cancels[workerID]
	remote := s.remoteRuns[workerID]
	s.mu.Unlock()
	if cancel == nil {
		return eventstore.ErrNotFound
	}
	if remote.Session != "" {
		_ = s.sshRunner.Cancel(ctx, remote)
	}
	cancel()
	return nil
}

func (s *Service) CancelTask(ctx context.Context, taskID string) error {
	s.mu.Lock()
	for workerID, cancel := range s.cancels {
		if s.tasks[workerID] == taskID {
			cancel()
		}
	}
	s.mu.Unlock()

	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskCanceled,
		}),
	})
	return err
}

func (s *Service) ClearTask(ctx context.Context, taskID string) error {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	for _, task := range snapshot.Tasks {
		if task.ID != taskID {
			continue
		}
		if !isTerminalTaskStatus(task.Status) {
			return errors.New("can only clear terminal tasks")
		}
		_, err := s.append(ctx, core.Event{
			Type:   core.EventTaskCleared,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"reason": "user_cleared",
			}),
		})
		return err
	}
	return eventstore.ErrNotFound
}

func (s *Service) ClearTerminalTasks(ctx context.Context) (ClearTasksResult, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return ClearTasksResult{}, err
	}
	result := ClearTasksResult{Cleared: []string{}}
	for _, task := range snapshot.Tasks {
		if !isTerminalTaskStatus(task.Status) {
			continue
		}
		if _, err := s.append(ctx, core.Event{
			Type:   core.EventTaskCleared,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"reason": "user_cleared_terminal",
			}),
		}); err != nil {
			return result, err
		}
		result.Cleared = append(result.Cleared, task.ID)
	}
	return result, nil
}

func isTerminalTaskStatus(status core.TaskStatus) bool {
	return status == core.TaskSucceeded || status == core.TaskFailed || status == core.TaskCanceled
}

func canPublishPullRequestForTask(task core.Task) bool {
	return isTerminalTaskStatus(task.Status) || strings.TrimSpace(task.FinalCandidateWorkerID) != ""
}

func (s *Service) ReviewWorkerChanges(ctx context.Context, workerID string) (WorkerChangesReview, error) {
	return s.reviewWorkerChanges(ctx, workerID, true)
}

func (s *Service) reviewWorkerChanges(ctx context.Context, workerID string, includeDiff bool) (WorkerChangesReview, error) {
	workspace, err := s.workspaceForWorker(ctx, workerID)
	if err != nil {
		return WorkerChangesReview{}, err
	}
	if workspace.VCSType == "ssh" {
		changes, err := s.completedWorkspaceChanges(ctx, workerID)
		if err != nil {
			return WorkerChangesReview{}, err
		}
		return WorkerChangesReview{
			WorkerID:  workerID,
			Workspace: workspace,
			Changes:   changes,
		}, nil
	}
	changes := s.describeWorkspaceChanges(ctx, workspace)
	if includeDiff && changes.Error == "" {
		if completed, err := s.completedWorkspaceChanges(ctx, workerID); err == nil && strings.TrimSpace(completed.Diff) != "" {
			changes.Diff = completed.Diff
		} else if diff, err := s.describeWorkspaceDiff(ctx, workspace); err != nil {
			changes.Error = err.Error()
		} else {
			changes.Diff = strings.TrimSpace(diff)
		}
	}
	return WorkerChangesReview{
		WorkerID:  workerID,
		Workspace: workspace,
		Changes:   changes,
	}, nil
}

func (s *Service) ApplyWorkerChanges(ctx context.Context, workerID string) (WorkerApplyResult, error) {
	if applied, err := s.workerChangesApplied(ctx, workerID); err != nil {
		return WorkerApplyResult{}, err
	} else if applied {
		return WorkerApplyResult{}, fmt.Errorf("worker changes already applied: %s", workerID)
	}
	review, err := s.reviewWorkerChanges(ctx, workerID, false)
	if err != nil {
		return WorkerApplyResult{}, err
	}
	var result WorkerApplyResult
	if review.Workspace.VCSType == "ssh" {
		project, err := s.projectForTaskID(ctx, review.Workspace.TaskID)
		if err != nil {
			return WorkerApplyResult{}, err
		}
		result, err = s.remoteApply(ctx, project, review.Workspace, review.Changes)
		if err != nil {
			return WorkerApplyResult{}, err
		}
	} else {
		result, err = s.workspaces.ApplyChanges(ctx, review.Workspace, review.Changes)
		if err != nil {
			return WorkerApplyResult{}, err
		}
	}
	result.WorkerID = workerID
	_, err = s.append(ctx, core.Event{
		Type:     core.EventWorkerApplied,
		TaskID:   review.Workspace.TaskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(result),
	})
	return result, err
}

func (s *Service) ApplyTaskResult(ctx context.Context, taskID string) (WorkerApplyResult, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return WorkerApplyResult{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return WorkerApplyResult{}, eventstore.ErrNotFound
	}
	if !isTerminalTaskStatus(task.Status) {
		return WorkerApplyResult{}, errors.New("can only apply terminal task results")
	}
	if task.FinalCandidateWorkerID == "" {
		return WorkerApplyResult{}, errors.New("task has no final candidate to apply")
	}
	return s.ApplyWorkerChanges(ctx, task.FinalCandidateWorkerID)
}

func (s *Service) RecommendApplyPolicy(ctx context.Context, taskID string) (ApplyPolicyRecommendation, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return ApplyPolicyRecommendation{}, err
	}
	candidates := applyCandidates(snapshot, taskID)
	recommendation := ApplyPolicyRecommendation{
		TaskID:     taskID,
		Strategy:   "none",
		Reason:     "no unapplied successful workers with source changes",
		Candidates: candidates,
	}
	if task, ok := findTask(snapshot, taskID); ok && task.FinalCandidateWorkerID != "" {
		for _, candidate := range candidates {
			if candidate.WorkerID == task.FinalCandidateWorkerID {
				if candidate.Applied {
					recommendation.Strategy = "already_applied"
					recommendation.Reason = "final task candidate has already been applied"
				} else {
					recommendation.Strategy = "apply_final"
					recommendation.Reason = "orchestrator selected a final task candidate"
				}
				return s.recordApplyPolicy(ctx, taskID, recommendation)
			}
		}
	}
	unapplied := 0
	for _, candidate := range candidates {
		if !candidate.Applied {
			unapplied++
		}
	}
	switch {
	case unapplied == 1:
		recommendation.Strategy = "apply_single"
		recommendation.Reason = "exactly one unapplied successful worker has source changes"
	case unapplied > 1:
		recommendation.Strategy = "manual_select"
		recommendation.Reason = "multiple unapplied successful workers have source changes; select one or schedule a review/benchmark comparison before applying"
	}
	return s.recordApplyPolicy(ctx, taskID, recommendation)
}

func (s *Service) recordApplyPolicy(ctx context.Context, taskID string, recommendation ApplyPolicyRecommendation) (ApplyPolicyRecommendation, error) {
	_, err := s.append(ctx, core.Event{
		Type:    core.EventApplyPolicy,
		TaskID:  taskID,
		Payload: core.MustJSON(recommendation),
	})
	return recommendation, err
}

func applyCandidates(snapshot core.Snapshot, taskID string) []ApplyCandidate {
	workers := map[string]core.Worker{}
	for _, worker := range snapshot.Workers {
		if worker.TaskID == taskID {
			workers[worker.ID] = worker
		}
	}
	nodesByWorker := map[string]string{}
	for _, node := range snapshot.ExecutionNodes {
		if node.TaskID == taskID && node.WorkerID != "" {
			nodesByWorker[node.WorkerID] = node.ID
		}
	}
	applied := map[string]bool{}
	for _, event := range snapshot.Events {
		if event.Type == core.EventWorkerApplied {
			applied[event.WorkerID] = true
		}
	}
	var candidates []ApplyCandidate
	for _, event := range snapshot.Events {
		if event.Type != core.EventWorkerCompleted || event.TaskID != taskID {
			continue
		}
		worker := workers[event.WorkerID]
		if worker.ID == "" {
			continue
		}
		var payload struct {
			Status           core.WorkerStatus      `json:"status"`
			Summary          string                 `json:"summary,omitempty"`
			ChangedFiles     []WorkspaceChangedFile `json:"changedFiles,omitempty"`
			WorkspaceChanges WorkspaceChanges       `json:"workspaceChanges,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		changedFiles := payload.ChangedFiles
		if len(changedFiles) == 0 {
			changedFiles = payload.WorkspaceChanges.ChangedFiles
		}
		if payload.Status != core.WorkerSucceeded || len(changedFiles) == 0 {
			continue
		}
		candidates = append(candidates, ApplyCandidate{
			WorkerID:     event.WorkerID,
			NodeID:       nodesByWorker[event.WorkerID],
			WorkerKind:   worker.Kind,
			Summary:      payload.Summary,
			ChangedFiles: changedFiles,
			Applied:      applied[event.WorkerID],
		})
	}
	return candidates
}

func unappliedCandidates(candidates []ApplyCandidate) []ApplyCandidate {
	var out []ApplyCandidate
	for _, candidate := range candidates {
		if !candidate.Applied {
			out = append(out, candidate)
		}
	}
	return out
}

func latestAppliedWorker(snapshot core.Snapshot, taskID string) string {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type == core.EventWorkerApplied && event.TaskID == taskID {
			return event.WorkerID
		}
	}
	return ""
}

func appliedWorkerSourceRoot(snapshot core.Snapshot, workerID string) string {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventWorkerApplied || event.WorkerID != workerID {
			continue
		}
		var payload struct {
			SourceRoot string `json:"sourceRoot"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil {
			return payload.SourceRoot
		}
	}
	return ""
}

func appliedWorkerSourceRootFromStore(ctx context.Context, store eventstore.Store, workerID string) string {
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		return ""
	}
	return appliedWorkerSourceRoot(snapshot, workerID)
}

func (s *Service) workerChangesApplied(ctx context.Context, workerID string) (bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	for _, event := range snapshot.Events {
		if event.Type == core.EventWorkerApplied && event.WorkerID == workerID {
			return true, nil
		}
	}
	return false, nil
}

func workerBelongsToTask(snapshot core.Snapshot, workerID string, taskID string) bool {
	for _, worker := range snapshot.Workers {
		if worker.ID == workerID && worker.TaskID == taskID {
			return true
		}
	}
	for _, event := range snapshot.Events {
		if event.WorkerID == workerID && event.TaskID == taskID {
			return true
		}
	}
	return false
}

func (s *Service) findPullRequest(ctx context.Context, prID string) (core.PullRequest, bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, false, err
	}
	for _, pr := range snapshot.PullRequests {
		if pr.ID == prID {
			return pr, true, nil
		}
	}
	return core.PullRequest{}, false, nil
}

func pullRequestBabysitterPrompt(pr core.PullRequest) string {
	return fmt.Sprintf(`Monitor GitHub pull request %s#%d until it is ready to merge.

Pull request URL: %s
Branch: %s
Base: %s

Repeatedly inspect CI status, review comments, and mergeability. If checks fail or review comments request changes, diagnose the issue, make the required code changes in the repo, and report what changed. If the PR is green and no action is needed, report that it is ready. Do not merge unless the user explicitly asks for merge.
`, pr.Repo, pr.Number, pr.URL, pr.Branch, pr.Base)
}

func pullRequestFollowUpPrompt(pr core.PullRequest) string {
	comment := pullRequestCommentPromptContext(pr)
	if comment != "" {
		comment = "\nLatest PR conversation comment:\n" + comment + "\n"
	}
	return fmt.Sprintf(`GitHub pull request %s#%d needs follow-up work on the existing task.

Pull request URL: %s
Branch: %s
Base: %s
State: %s
Checks: %s
Merge status: %s
Review status: %s
%s

Inspect the current PR state, CI failures, review comments, and mergeability. Schedule the next bounded worker turn needed to fix the PR or report that it is ready. Keep this as the same long-running task objective; do not start a separate babysitter task.
`, pr.Repo, pr.Number, pr.URL, pr.Branch, pr.Base, pr.State, pr.ChecksStatus, pr.MergeStatus, pr.ReviewStatus, comment)
}

func pullRequestCommentPromptContext(pr core.PullRequest) string {
	var metadata map[string]any
	if len(pr.Metadata) == 0 {
		return ""
	}
	if err := json.Unmarshal(pr.Metadata, &metadata); err != nil {
		return ""
	}
	body := strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentBody"]))
	if body == "" {
		return ""
	}
	author := strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentAuthor"]))
	createdAt := strings.TrimSpace(stringMetadataValue(metadata["latestConversationCommentCreatedAt"]))
	prefix := ""
	if author != "" {
		prefix = "@" + author
	}
	if createdAt != "" {
		if prefix != "" {
			prefix += " "
		}
		prefix += "(" + createdAt + ")"
	}
	if prefix == "" {
		return body
	}
	return prefix + ":\n" + body
}

func (s *Service) runTask(ctx context.Context, task core.Task) {
	if err := s.setTaskStatus(ctx, task.ID, core.TaskPlanning); err != nil {
		return
	}

	plan, err := s.brain.Plan(ctx, task, nil)
	if err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if err := plan.Validate(); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	normalizePlanReasoning(&plan)
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  task.ID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if ok, err := s.runImmediatePlanActions(ctx, task, plan); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	} else if !ok {
		return
	}

	results := []WorkerTurnResult{}
	result, err := s.runPlannedWorker(ctx, task, plan)
	if err != nil {
		if s.waitForRecoverableError(ctx, task.ID, "", err) {
			return
		}
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	results = append(results, result)
	if result.Status == core.WorkerWaiting {
		s.handleWorkerQuestion(ctx, task, plan, results, result)
		return
	}
	if !s.finishOrContinueTask(ctx, task.ID, result) {
		return
	}

	results, ok, err := s.runFollowUpWorkers(ctx, task, plan, results, result.NodeID)
	if err != nil {
		if s.waitForRecoverableError(ctx, task.ID, "", err) {
			return
		}
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if !ok {
		return
	}
	if ok, err := s.runPlanActions(ctx, task, plan, results); err != nil {
		if s.waitForRecoverableError(ctx, task.ID, "", err) {
			return
		}
		_ = s.failTask(ctx, task.ID, err)
		return
	} else if !ok {
		return
	}

	replanOK, finalCandidateWorkerID, finalCandidateReason, results := s.replanLoop(ctx, task, plan, results)
	if !replanOK {
		return
	}

	_ = s.completeTask(ctx, task.ID, results, finalCandidateWorkerID, finalCandidateReason)
}

func (s *Service) handleWorkerQuestion(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult, waiting WorkerTurnResult) {
	question := nonEmpty(waiting.Summary, waiting.Error, "worker requested orchestrator input")
	_ = s.recordUserActionNeeded(ctx, task.ID, waiting.WorkerID, "worker_needs_input", question, map[string]any{
		"summary": waiting.Summary,
		"error":   waiting.Error,
	})
	replanner, ok := s.brain.(ReplanProvider)
	if !ok {
		_ = s.updateTaskObjective(ctx, task.ID, core.ObjectiveWaitingUser, "approval_needed", question)
		_ = s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
		return
	}
	decision, err := replanner.Replan(ctx, task, OrchestrationState{
		InitialPlan: initial,
		Results:     results,
		Turn:        1,
	})
	if err != nil {
		_ = s.failTask(ctx, task.ID, fmt.Errorf("question replan failed: %w", err))
		return
	}
	if err := decision.Validate(); err != nil {
		_ = s.failTask(ctx, task.ID, fmt.Errorf("invalid question replan decision: %w", err))
		return
	}
	switch decision.Action {
	case "continue":
		_, _ = s.append(ctx, core.Event{
			Type:   core.EventApprovalDecided,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"approved": true,
				"answer":   nonEmpty(decision.Message, decision.Rationale),
				"reason":   "autonomous_replan",
				"workerId": waiting.WorkerID,
			}),
		})
		if decision.Plan.Metadata == nil {
			decision.Plan.Metadata = map[string]any{}
		}
		decision.Plan.Metadata["parentNodeID"] = waiting.NodeID
		decision.Plan.Metadata["questionWorkerID"] = waiting.WorkerID
		if stringMetadata(decision.Plan.Metadata, "baseWorkerID") == "" {
			if baseWorkerID := latestCandidateWorkerID(results); baseWorkerID != "" {
				decision.Plan.Metadata["baseWorkerID"] = baseWorkerID
			}
		}
		_, _ = s.append(ctx, core.Event{
			Type:   core.EventTaskReplanned,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"action":    decision.Action,
				"rationale": decision.Rationale,
				"message":   decision.Message,
				"turn":      1,
			}),
		})
		_, _ = s.append(ctx, core.Event{
			Type:    core.EventTaskPlanned,
			TaskID:  task.ID,
			Payload: core.MustJSON(decision.Plan),
		})
		if result, err := s.runPlannedWorker(ctx, task, *decision.Plan); err != nil {
			if s.waitForRecoverableError(ctx, task.ID, waiting.WorkerID, err) {
				return
			}
			_ = s.failTask(ctx, task.ID, err)
		} else if result.Status == core.WorkerWaiting {
			s.handleWorkerQuestion(ctx, task, *decision.Plan, append(results, result), result)
		} else if s.finishOrContinueTask(ctx, task.ID, result) {
			nextResults := append(results, result)
			nextResults, ok, err := s.runFollowUpWorkers(ctx, task, *decision.Plan, nextResults, result.NodeID)
			if err != nil {
				if s.waitForRecoverableError(ctx, task.ID, result.WorkerID, err) {
					return
				}
				_ = s.failTask(ctx, task.ID, err)
				return
			}
			if !ok {
				return
			}
			if ok, err := s.runPlanActions(ctx, task, *decision.Plan, nextResults); err != nil {
				if s.waitForRecoverableError(ctx, task.ID, result.WorkerID, err) {
					return
				}
				_ = s.failTask(ctx, task.ID, err)
			} else if ok {
				replanOK, finalCandidateWorkerID, finalCandidateReason, nextResults := s.replanLoop(ctx, task, *decision.Plan, nextResults)
				if !replanOK {
					return
				}
				_ = s.completeTask(ctx, task.ID, nextResults, nonEmpty(decision.FinalCandidateWorkerID, finalCandidateWorkerID), nonEmpty(decision.Rationale, finalCandidateReason))
			}
		}
	case "wait":
		_ = s.waitForUserAction(ctx, task.ID, waiting.WorkerID, "orchestrator_wait", nonEmpty(decision.Message, decision.Rationale, question), map[string]any{
			"rationale": decision.Rationale,
		})
	case "complete":
		_ = s.completeTask(ctx, task.ID, results, decision.FinalCandidateWorkerID, decision.Rationale)
	case "fail":
		_ = s.failTask(ctx, task.ID, errors.New(nonEmpty(decision.Message, decision.Rationale, "worker question could not be answered")))
	}
}

func (s *Service) resumeWaitingTask(ctx context.Context, taskID string, feedback string) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return
	}
	task, ok := findTask(snapshot, taskID)
	if !ok || task.Status != core.TaskWaiting {
		return
	}
	waitingWorkerID, question := latestWorkerQuestion(snapshot, taskID)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventApprovalDecided,
		TaskID:   taskID,
		WorkerID: waitingWorkerID,
		Payload: core.MustJSON(map[string]any{
			"approved": true,
			"answer":   feedback,
			"question": question,
			"reason":   "user_feedback",
		}),
	})
	if err := s.setTaskStatus(ctx, taskID, core.TaskPlanning); err != nil {
		return
	}
	steering := taskSteering(snapshot, taskID)
	steering = append(steering, fmt.Sprintf("Worker question: %s\nFeedback: %s", question, feedback))
	plan, err := s.brain.Plan(ctx, task, steering)
	if err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if err := plan.Validate(); err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Metadata["questionWorkerID"] = waitingWorkerID
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  taskID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	result, err := s.runPlannedWorker(ctx, task, plan)
	if err != nil {
		_ = s.failTask(ctx, taskID, err)
		return
	}
	if result.Status == core.WorkerWaiting {
		s.handleWorkerQuestion(ctx, task, plan, []WorkerTurnResult{result}, result)
		return
	}
	if s.finishOrContinueTask(ctx, taskID, result) {
		results := []WorkerTurnResult{result}
		results, ok, err := s.runFollowUpWorkers(ctx, task, plan, results, result.NodeID)
		if err != nil {
			if s.waitForRecoverableError(ctx, taskID, result.WorkerID, err) {
				return
			}
			_ = s.failTask(ctx, taskID, err)
			return
		}
		if !ok {
			return
		}
		if ok, err := s.runPlanActions(ctx, task, plan, results); err != nil {
			if s.waitForRecoverableError(ctx, taskID, result.WorkerID, err) {
				return
			}
			_ = s.failTask(ctx, taskID, err)
		} else if ok {
			replanOK, finalCandidateWorkerID, finalCandidateReason, results := s.replanLoop(ctx, task, plan, results)
			if !replanOK {
				return
			}
			_ = s.completeTask(ctx, taskID, results, finalCandidateWorkerID, finalCandidateReason)
		}
	}
}

func (s *Service) retryTask(ctx context.Context, task core.Task, plan Plan) {
	if _, err := s.append(ctx, core.Event{
		Type:    core.EventTaskPlanned,
		TaskID:  task.ID,
		Payload: core.MustJSON(plan),
	}); err != nil {
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	result, err := s.runPlannedWorker(ctx, task, plan)
	if err != nil {
		if s.waitForRecoverableError(ctx, task.ID, "", err) {
			return
		}
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if result.Status == core.WorkerWaiting {
		s.handleWorkerQuestion(ctx, task, plan, []WorkerTurnResult{result}, result)
		return
	}
	if !s.finishOrContinueTask(ctx, task.ID, result) {
		return
	}
	results := []WorkerTurnResult{result}
	results, ok, err := s.runFollowUpWorkers(ctx, task, plan, results, result.NodeID)
	if err != nil {
		if s.waitForRecoverableError(ctx, task.ID, "", err) {
			return
		}
		_ = s.failTask(ctx, task.ID, err)
		return
	}
	if !ok {
		return
	}
	if ok, err := s.runPlanActions(ctx, task, plan, results); err != nil {
		if s.waitForRecoverableError(ctx, task.ID, "", err) {
			return
		}
		_ = s.failTask(ctx, task.ID, err)
		return
	} else if !ok {
		return
	}
	replanOK, finalCandidateWorkerID, finalCandidateReason, results := s.replanLoop(ctx, task, plan, results)
	if !replanOK {
		return
	}
	_ = s.completeTask(ctx, task.ID, results, finalCandidateWorkerID, finalCandidateReason)
}

func (s *Service) retryGraphTask(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult) {
	replanOK, finalCandidateWorkerID, finalCandidateReason, results := s.replanLoop(ctx, task, initial, results)
	if !replanOK {
		return
	}
	_ = s.completeTask(ctx, task.ID, results, finalCandidateWorkerID, finalCandidateReason)
}

func (s *Service) runPlannedWorker(ctx context.Context, task core.Task, plan Plan) (WorkerTurnResult, error) {
	runner := s.runners[plan.WorkerKind]
	if runner == nil {
		return WorkerTurnResult{}, fmt.Errorf("unknown worker kind %q", plan.WorkerKind)
	}
	normalizePlanReasoning(&plan)
	project := s.projectForTask(task)
	plan.Metadata["projectId"] = project.ID
	if requested := targetLabels(plan.Metadata); len(requested) > 0 {
		plan.Metadata["ignoredTargetLabels"] = requested
		plan.Metadata["targetSelectionPolicy"] = "scheduler target labels are ignored; placement is selected by task or project policy"
		delete(plan.Metadata, "targetLabels")
	}
	if labels := taskTargetLabels(task); len(labels) > 0 {
		plan.Metadata["targetLabels"] = labels
		plan.Metadata["targetSelectionSource"] = "task"
	} else if len(project.TargetLabels) > 0 {
		plan.Metadata["targetLabels"] = project.TargetLabels
		plan.Metadata["targetSelectionSource"] = "project"
	}
	target, err := s.selectExecutionTarget(ctx, plan)
	if err != nil {
		return WorkerTurnResult{}, err
	}
	s.targets.Begin(target.ID)
	defer s.targets.Finish(target.ID)
	plan.Metadata["targetID"] = target.ID
	plan.Metadata["targetKind"] = string(target.Kind)
	if target.Kind == TargetKindSSH {
		return s.runSSHPlannedWorker(ctx, task, plan, runner, target)
	}

	workerID := uuid.NewString()
	nodeID := stringMetadata(plan.Metadata, "nodeID")
	if nodeID == "" {
		nodeID = uuid.NewString()
		plan.Metadata["nodeID"] = nodeID
	}
	planID := stringMetadata(plan.Metadata, "planID")
	if planID == "" {
		planID = uuid.NewString()
		plan.Metadata["planID"] = planID
	}
	retryFromWorkerID := stringMetadata(plan.Metadata, "retryFromWorkerID")
	resumeSessionID := stringMetadata(plan.Metadata, "retryResumeSessionID")
	workspace, reusedWorkspace, workspaceErr := s.retryWorkspace(ctx, task.ID, workerID, retryFromWorkerID)
	if workspaceErr != nil {
		plan.Metadata["retryWorkspaceReused"] = false
		plan.Metadata["retryWorkspaceError"] = workspaceErr.Error()
	}
	if reusedWorkspace {
		plan.Metadata["retryWorkspaceReused"] = true
		plan.Metadata["retryWorkspaceCWD"] = workspace.CWD
	} else if retryFromWorkerID != "" {
		plan.Metadata["retryWorkspaceReused"] = false
	}
	workspaceSpec := WorkspaceSpec{
		TaskID:   task.ID,
		WorkerID: workerID,
		WorkDir:  project.LocalPath,
	}
	if !reusedWorkspace {
		if baseWorkerID := stringMetadata(plan.Metadata, "baseWorkerID"); baseWorkerID != "" {
			baseSpec, err := s.baseWorkspaceSpec(ctx, workspaceSpec, baseWorkerID)
			if err != nil {
				_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
				return WorkerTurnResult{}, err
			}
			workspaceSpec = baseSpec
			plan.Metadata["baseWorkspaceCWD"] = workspaceSpec.BaseWorkDir
			plan.Metadata["baseRevision"] = workspaceSpec.BaseRevision
		}
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":       nodeID,
			"workerId":     workerID,
			"workerKind":   plan.WorkerKind,
			"planId":       planID,
			"parentNodeId": stringMetadata(plan.Metadata, "parentNodeID"),
			"spawnId":      stringMetadata(plan.Metadata, "spawnID"),
			"role":         stringMetadata(plan.Metadata, "spawnRole"),
			"reason":       stringMetadata(plan.Metadata, "spawnReason"),
			"targetId":     target.ID,
			"targetKind":   string(target.Kind),
			"dependsOn":    stringSliceMetadata(plan.Metadata, "dependsOn"),
			"metadata":     planMetadata(plan),
		}),
	}); err != nil {
		return WorkerTurnResult{}, err
	}
	if !reusedWorkspace {
		workspace, err = s.workspaces.Prepare(ctx, WorkspaceSpec{
			TaskID:       workspaceSpec.TaskID,
			WorkerID:     workspaceSpec.WorkerID,
			WorkDir:      workspaceSpec.WorkDir,
			BaseWorkDir:  workspaceSpec.BaseWorkDir,
			BaseRevision: workspaceSpec.BaseRevision,
		})
		if err != nil {
			_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
			return WorkerTurnResult{}, err
		}
		if baseWorkerID := stringMetadata(plan.Metadata, "baseWorkerID"); baseWorkerID != "" && workspaceSpec.BaseWorkDir == "" && workspaceSpec.BaseRevision == "" {
			patch, baseChanges, err := s.workerHandoffPatch(ctx, baseWorkerID)
			if err != nil {
				_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
				return WorkerTurnResult{}, err
			}
			if strings.TrimSpace(patch) != "" {
				if err := applyGitPatchToWorkspace(ctx, workspace.CWD, patch); err != nil {
					_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
					return WorkerTurnResult{}, fmt.Errorf("apply base worker patch in local workspace: %w", err)
				}
				plan.Metadata["baseHandoff"] = "patch"
				plan.Metadata["basePatchApplied"] = true
				plan.Metadata["baseChangedFiles"] = len(baseChanges.ChangedFiles)
			} else {
				plan.Metadata["baseHandoff"] = "empty_patch"
			}
		}
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(workspace),
	}); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}
	var steering chan string
	if runnerSupportsSteering(runner) {
		steering = make(chan string, 16)
	}
	prompt := workerExecutionPrompt(plan.Prompt, workspace)
	if reusedWorkspace {
		prompt = retryWorkerExecutionPrompt(prompt, retryFromWorkerID, resumeSessionID)
	} else {
		resumeSessionID = ""
		delete(plan.Metadata, "retryResumeSessionID")
	}
	spec := worker.Spec{
		ID:              workerID,
		TaskID:          task.ID,
		Kind:            plan.WorkerKind,
		Prompt:          prompt,
		WorkDir:         workspace.CWD,
		ResumeSessionID: resumeSessionID,
		ReasoningEffort: plan.ReasoningEffort,
		Steering:        steering,
	}
	command := runner.BuildCommand(spec)
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":     plan.WorkerKind,
			"command":  command,
			"metadata": planMetadata(plan),
		}),
	}); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}

	workerCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancels[workerID] = cancel
	s.tasks[workerID] = task.ID
	if steering != nil {
		s.steering[workerID] = steering
	}
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		if ch := s.steering[workerID]; ch != nil {
			close(ch)
		}
		delete(s.cancels, workerID)
		delete(s.tasks, workerID)
		delete(s.steering, workerID)
		s.mu.Unlock()
	}()

	_ = s.setTaskStatus(ctx, task.ID, core.TaskRunning)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{}),
	})

	runState := &workerRunState{}
	err = runner.Run(workerCtx, spec, eventSink{service: s, taskID: task.ID, workerID: workerID, state: runState})
	if err != nil {
		status := core.WorkerFailed
		workspaceResult := WorkspaceResultFailed
		if errors.Is(workerCtx.Err(), context.Canceled) {
			status = core.WorkerCanceled
			workspaceResult = WorkspaceResultCanceled
		}
		changes := s.describeWorkspaceChangesForCompletion(ctx, workspace)
		_, _ = s.append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   task.ID,
			WorkerID: workerID,
			Payload:  core.MustJSON(runState.completionPayload(status, err, changes)),
		})
		_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
		_ = s.cleanupWorkspace(ctx, task.ID, workerID, workspace, workspaceResult)
		return runState.turnResult(workerID, plan, status, err, changes), nil
	}

	if runState.isWaitingForInput() {
		changes := s.describeWorkspaceChangesForCompletion(ctx, workspace)
		_, _ = s.append(ctx, core.Event{
			Type:     core.EventWorkerCompleted,
			TaskID:   task.ID,
			WorkerID: workerID,
			Payload:  core.MustJSON(runState.completionPayload(core.WorkerWaiting, nil, changes)),
		})
		_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
		return runState.turnResult(workerID, plan, core.WorkerWaiting, nil, changes), nil
	}

	changes := s.describeWorkspaceChangesForCompletion(ctx, workspace)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(runState.completionPayload(core.WorkerSucceeded, nil, changes)),
	})
	_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
	_ = s.cleanupWorkspace(ctx, task.ID, workerID, workspace, WorkspaceResultSucceeded)
	return runState.turnResult(workerID, plan, core.WorkerSucceeded, nil, changes), nil
}

func (s *Service) selectExecutionTarget(ctx context.Context, plan Plan) (TargetConfig, error) {
	if retryTargetID := stringMetadata(plan.Metadata, "retryTargetID"); retryTargetID != "" {
		return s.targets.SelectID(retryTargetID)
	}
	if retryFromWorkerID := stringMetadata(plan.Metadata, "retryFromWorkerID"); retryFromWorkerID != "" {
		if target, ok, err := s.executionTargetForWorker(ctx, retryFromWorkerID); err != nil || ok {
			return target, err
		}
	}
	return s.targets.Select(plan)
}

func (s *Service) executionTargetForWorker(ctx context.Context, workerID string) (TargetConfig, bool, error) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return TargetConfig{}, false, nil
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return TargetConfig{}, false, err
	}
	for i := len(snapshot.ExecutionNodes) - 1; i >= 0; i-- {
		node := snapshot.ExecutionNodes[i]
		if node.WorkerID != workerID || strings.TrimSpace(node.TargetID) == "" {
			continue
		}
		target, err := s.targets.SelectID(node.TargetID)
		if err != nil {
			return TargetConfig{}, true, err
		}
		return target, true, nil
	}
	info := workerExecutionInfo(snapshot, workerID)
	if info == nil {
		return TargetConfig{}, false, nil
	}
	targetID, _ := info["targetId"].(string)
	if strings.TrimSpace(targetID) == "" {
		targetID, _ = info["targetID"].(string)
	}
	if strings.TrimSpace(targetID) == "" {
		return TargetConfig{}, false, nil
	}
	target, err := s.targets.SelectID(targetID)
	return target, true, err
}

func (s *Service) runSSHPlannedWorker(ctx context.Context, task core.Task, plan Plan, runner worker.Runner, target TargetConfig) (WorkerTurnResult, error) {
	workerID := uuid.NewString()
	nodeID := stringMetadata(plan.Metadata, "nodeID")
	if nodeID == "" {
		nodeID = uuid.NewString()
		plan.Metadata["nodeID"] = nodeID
	}
	planID := stringMetadata(plan.Metadata, "planID")
	if planID == "" {
		planID = uuid.NewString()
		plan.Metadata["planID"] = planID
	}
	project := s.projectForTask(task)
	remoteWorkDir := nonEmpty(target.WorkDir, project.LocalPath)
	retryFromWorkerID := stringMetadata(plan.Metadata, "retryFromWorkerID")
	resumeSessionID := stringMetadata(plan.Metadata, "retryResumeSessionID")
	reusedWorkspace := false
	if retryFromWorkerID != "" {
		if retryWorkDir, err := s.remoteRetryWorkDir(ctx, target, retryFromWorkerID); err != nil {
			plan.Metadata["retryWorkspaceReused"] = false
			plan.Metadata["retryWorkspaceError"] = err.Error()
			resumeSessionID = ""
			delete(plan.Metadata, "retryResumeSessionID")
		} else {
			remoteWorkDir = retryWorkDir
			reusedWorkspace = true
			plan.Metadata["retryWorkspaceReused"] = true
			plan.Metadata["retryWorkspaceCWD"] = remoteWorkDir
		}
	}
	baseWorkerID := stringMetadata(plan.Metadata, "baseWorkerID")
	if !reusedWorkspace && baseWorkerID != "" {
		if sameTarget, _ := s.workerRanOnTarget(ctx, baseWorkerID, target.ID); sameTarget {
			if baseWorkDir, err := s.remoteRetryWorkDir(ctx, target, baseWorkerID); err == nil {
				remoteWorkDir = baseWorkDir
				reusedWorkspace = true
				plan.Metadata["baseWorkspaceCWD"] = remoteWorkDir
			} else {
				plan.Metadata["baseWorkspaceReuseError"] = err.Error()
			}
		}
	}
	remoteRun := NewRemoteRun(target, worker.Spec{ID: workerID, WorkDir: remoteWorkDir})
	if !reusedWorkspace {
		checkoutLog, err := s.sshRunner.PrepareCheckout(ctx, target, RemoteCheckoutSpec{
			RepoURL:     projectCloneURL(project),
			WorkDir:     remoteWorkDir,
			DefaultBase: project.DefaultBase,
		})
		if err != nil {
			return WorkerTurnResult{}, fmt.Errorf("prepare remote checkout: %w: %s", err, checkoutLog)
		}
		if checkoutLog != "" {
			plan.Metadata["remoteCheckout"] = checkoutLog
		}
		if baseWorkerID != "" {
			patch, baseChanges, err := s.workerHandoffPatch(ctx, baseWorkerID)
			if err != nil {
				return WorkerTurnResult{}, err
			}
			if strings.TrimSpace(patch) != "" {
				if err := s.sshRunner.ApplyPatch(ctx, target, remoteWorkDir, remoteRun.RunDir, patch); err != nil {
					return WorkerTurnResult{}, fmt.Errorf("apply base worker patch on remote target: %w", err)
				}
				plan.Metadata["baseHandoff"] = "patch"
				plan.Metadata["basePatchApplied"] = true
				plan.Metadata["baseChangedFiles"] = len(baseChanges.ChangedFiles)
			} else {
				plan.Metadata["baseHandoff"] = "empty_patch"
			}
		}
	}
	workspace := PreparedWorkspace{
		Root:       remoteWorkDir,
		CWD:        remoteWorkDir,
		SourceRoot: remoteWorkDir,
		Mode:       "remote",
		VCSType:    "ssh",
		WorkerID:   workerID,
		TaskID:     task.ID,
	}
	steering := make(chan string, 16)
	spec := worker.Spec{
		ID:              workerID,
		TaskID:          task.ID,
		Kind:            plan.WorkerKind,
		Prompt:          workerExecutionPrompt(plan.Prompt, workspace),
		WorkDir:         remoteWorkDir,
		ResumeSessionID: resumeSessionID,
		ReasoningEffort: plan.ReasoningEffort,
		Steering:        steering,
	}
	if reusedWorkspace {
		spec.Prompt = retryWorkerExecutionPrompt(spec.Prompt, retryFromWorkerID, resumeSessionID)
	}
	command := runner.BuildCommand(spec)
	workspace.Root = remoteRun.RunDir
	workspace.CWD = remoteRun.WorkDir
	workspace.SourceRoot = remoteRun.WorkDir
	workspace.WorkspaceName = remoteRun.Session
	plan.Metadata["remoteSession"] = remoteRun.Session
	plan.Metadata["remoteRunDir"] = remoteRun.RunDir
	plan.Metadata["remoteWorkDir"] = remoteRun.WorkDir

	if _, err := s.append(ctx, core.Event{
		Type:     core.EventExecutionPlanned,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"nodeId":        nodeID,
			"workerId":      workerID,
			"workerKind":    plan.WorkerKind,
			"planId":        planID,
			"parentNodeId":  stringMetadata(plan.Metadata, "parentNodeID"),
			"spawnId":       stringMetadata(plan.Metadata, "spawnID"),
			"role":          stringMetadata(plan.Metadata, "spawnRole"),
			"reason":        stringMetadata(plan.Metadata, "spawnReason"),
			"targetId":      target.ID,
			"targetKind":    string(target.Kind),
			"remoteSession": remoteRun.Session,
			"remoteRunDir":  remoteRun.RunDir,
			"remoteWorkDir": remoteRun.WorkDir,
			"dependsOn":     stringSliceMetadata(plan.Metadata, "dependsOn"),
			"metadata":      planMetadata(plan),
		}),
	}); err != nil {
		return WorkerTurnResult{}, err
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerWorkspace,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(workspace),
	}); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}
	if _, err := s.append(ctx, core.Event{
		Type:     core.EventWorkerCreated,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload: core.MustJSON(map[string]any{
			"kind":     plan.WorkerKind,
			"command":  command,
			"metadata": planMetadata(plan),
		}),
	}); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}

	workerCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancels[workerID] = cancel
	s.tasks[workerID] = task.ID
	s.steering[workerID] = steering
	s.remoteRuns[workerID] = remoteRun
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		close(s.steering[workerID])
		delete(s.cancels, workerID)
		delete(s.tasks, workerID)
		delete(s.steering, workerID)
		delete(s.remoteRuns, workerID)
		s.mu.Unlock()
	}()

	_ = s.setTaskStatus(ctx, task.ID, core.TaskRunning)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerStarted,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(map[string]any{"targetId": target.ID, "session": remoteRun.Session}),
	})
	runState := &workerRunState{}
	sink := eventSink{service: s, taskID: task.ID, workerID: workerID, state: runState}
	stdin := ""
	if worker.CommandUsesPromptStdin(command) {
		stdin = spec.Prompt
	}
	if err := s.sshRunner.Start(workerCtx, remoteRun, command, stdin); err != nil {
		_ = s.setExecutionNodeStatus(ctx, task.ID, nodeID, core.WorkerFailed)
		return WorkerTurnResult{}, err
	}
	status, err := s.sshRunner.Poll(workerCtx, remoteRun, worker.ParserForKind(plan.WorkerKind), sink)
	workerStatus, statusErr := remoteStatusToWorkerStatus(status)
	if err != nil && !errors.Is(err, context.Canceled) {
		statusErr = err
		workerStatus = core.WorkerFailed
	}
	if errors.Is(workerCtx.Err(), context.Canceled) {
		workerStatus = core.WorkerCanceled
		statusErr = context.Canceled
	}
	changes := s.sshRunner.DescribeChanges(ctx, remoteRun)
	_, _ = s.append(ctx, core.Event{
		Type:     core.EventWorkerCompleted,
		TaskID:   task.ID,
		WorkerID: workerID,
		Payload:  core.MustJSON(runState.completionPayload(workerStatus, statusErr, changes)),
	})
	_ = s.recordWorkerArtifacts(ctx, task.ID, workerID, plan.WorkerKind, runState, changes)
	return runState.turnResult(workerID, plan, workerStatus, statusErr, changes), nil
}

func (s *Service) finishOrContinueTask(ctx context.Context, taskID string, result WorkerTurnResult) bool {
	switch result.Status {
	case core.WorkerSucceeded:
		return true
	case core.WorkerWaiting:
		_ = s.setTaskStatus(ctx, taskID, core.TaskWaiting)
	case core.WorkerCanceled:
		_ = s.setTaskStatus(ctx, taskID, core.TaskCanceled)
	default:
		if blocker, ok := classifyUserRecoverableBlocker(nonEmpty(result.Error, result.Summary)); ok {
			_ = s.waitForUserAction(ctx, taskID, result.WorkerID, blocker.Reason, blocker.Question, map[string]any{
				"summary":    blocker.Summary,
				"workerKind": result.Kind,
				"resumeHint": "After fixing the environment or setup issue, respond on this task with what changed.",
				"error":      result.Error,
			})
			return false
		}
		_ = s.setTaskStatus(ctx, taskID, core.TaskFailed)
	}
	return false
}

func (s *Service) completeTask(ctx context.Context, taskID string, results []WorkerTurnResult, selectedWorkerID string, reason string) error {
	candidateWorkerID, candidateReason, err := resolveFinalCandidate(results, selectedWorkerID)
	if err != nil {
		return s.waitForFinalCandidateResolution(ctx, taskID, err)
	}
	if candidateWorkerID != "" {
		if strings.TrimSpace(reason) == "" {
			reason = candidateReason
		}
		if _, err := s.append(ctx, core.Event{
			Type:   core.EventTaskCandidate,
			TaskID: taskID,
			Payload: core.MustJSON(map[string]any{
				"workerId": candidateWorkerID,
				"reason":   reason,
			}),
		}); err != nil {
			return err
		}
		if err := s.recordTaskMilestone(ctx, taskID, "candidate_ready", "candidate_ready", "Final candidate selected.", map[string]any{
			"workerId": candidateWorkerID,
			"reason":   reason,
		}); err != nil {
			return err
		}
	}
	if candidateWorkerID != "" && s.taskCompletionMode(ctx, taskID) == "github" {
		if _, err := s.PublishTaskPullRequest(ctx, taskID, core.PublishPullRequestRequest{WorkerID: candidateWorkerID}); err != nil {
			_ = s.failTask(ctx, taskID, fmt.Errorf("publish completion pull request: %w", err))
			return err
		}
		return s.setTaskStatus(ctx, taskID, core.TaskWaiting)
	}
	if pr, ok := s.openPullRequestForTask(ctx, taskID); ok {
		if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingExternal, "pr_open", pullRequestObjectiveSummary(pr, "pr_open")); err != nil {
			return err
		}
		return s.setTaskStatus(ctx, taskID, core.TaskWaiting)
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveSatisfied, "satisfied", "Local task result is complete."); err != nil {
		return err
	}
	return s.setTaskStatus(ctx, taskID, core.TaskSucceeded)
}

func (s *Service) openPullRequestForTask(ctx context.Context, taskID string) (core.PullRequest, bool) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.PullRequest{}, false
	}
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID != taskID {
			continue
		}
		if strings.EqualFold(pr.State, "MERGED") || strings.EqualFold(pr.State, "CLOSED") {
			continue
		}
		return pr, true
	}
	return core.PullRequest{}, false
}

func (s *Service) runImmediatePlanActions(ctx context.Context, task core.Task, plan Plan) (bool, error) {
	for _, action := range plan.Actions {
		if strings.TrimSpace(action.When) != "immediate" {
			continue
		}
		keepGoing, err := s.executePlanAction(ctx, task, action, nil)
		if err != nil || !keepGoing {
			return keepGoing, err
		}
	}
	return true, nil
}

func (s *Service) runPlanActions(ctx context.Context, task core.Task, plan Plan, results []WorkerTurnResult) (bool, error) {
	for _, action := range plan.Actions {
		if strings.TrimSpace(action.When) == "immediate" {
			continue
		}
		keepGoing, err := s.executePlanAction(ctx, task, action, results)
		if err != nil || !keepGoing {
			return keepGoing, err
		}
	}
	return true, nil
}

func (s *Service) executePlanAction(ctx context.Context, task core.Task, action PlanAction, results []WorkerTurnResult) (bool, error) {
	switch strings.TrimSpace(action.Kind) {
	case "publish_pull_request":
		workerID := strings.TrimSpace(action.WorkerID)
		if workerID == "" {
			workerID = latestCandidateWorkerID(results)
		}
		if workerID == "" {
			return false, errors.New("publish_pull_request action requires a candidate worker")
		}
		req := publishPullRequestRequestFromAction(action)
		req.WorkerID = workerID
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":     action.Kind,
			"when":     nonEmpty(action.When, "after_success"),
			"reason":   action.Reason,
			"workerId": workerID,
			"status":   "started",
		}); err != nil {
			return false, err
		}
		pr, err := s.PublishTaskPullRequest(ctx, task.ID, req)
		if err != nil {
			return false, err
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":          action.Kind,
			"when":          nonEmpty(action.When, "after_success"),
			"reason":        action.Reason,
			"workerId":      workerID,
			"pullRequestId": pr.ID,
			"url":           pr.URL,
		}); err != nil {
			return false, err
		}
		return false, s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
	case "watch_pull_requests":
		req := watchPullRequestsRequestFromAction(action)
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":   action.Kind,
			"when":   nonEmpty(action.When, "after_success"),
			"reason": action.Reason,
			"inputs": action.Inputs,
			"status": "started",
		}); err != nil {
			return false, err
		}
		prs, err := s.WatchPullRequests(ctx, task.ID, req)
		if err != nil {
			return false, err
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":             action.Kind,
			"when":             nonEmpty(action.When, "after_success"),
			"reason":           action.Reason,
			"inputs":           action.Inputs,
			"pullRequestCount": len(prs),
		}); err != nil {
			return false, err
		}
		return false, nil
	case "wait_external":
		phase := stringMetadata(action.Inputs, "phase")
		if phase == "" {
			phase = "waiting_external"
		}
		summary := stringMetadata(action.Inputs, "summary")
		if summary == "" {
			summary = action.Reason
		}
		if err := s.updateTaskObjective(ctx, task.ID, core.ObjectiveWaitingExternal, phase, summary); err != nil {
			return false, err
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":   action.Kind,
			"when":   nonEmpty(action.When, "after_success"),
			"reason": action.Reason,
			"inputs": action.Inputs,
		}); err != nil {
			return false, err
		}
		return false, s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
	case "ask_user":
		question := stringMetadata(action.Inputs, "question")
		if question == "" {
			question = stringMetadata(action.Inputs, "summary")
		}
		if question == "" {
			question = action.Reason
		}
		if err := s.recordTaskAction(ctx, task.ID, map[string]any{
			"kind":   action.Kind,
			"when":   nonEmpty(action.When, "after_success"),
			"reason": action.Reason,
			"inputs": action.Inputs,
		}); err != nil {
			return false, err
		}
		return false, s.waitForUserAction(ctx, task.ID, strings.TrimSpace(action.WorkerID), "ask_user", question, map[string]any{
			"summary":    stringMetadata(action.Inputs, "summary"),
			"target":     stringMetadata(action.Inputs, "target"),
			"project":    stringMetadata(action.Inputs, "project"),
			"resumeHint": stringMetadata(action.Inputs, "resumeHint"),
			"commands":   stringSliceMetadata(action.Inputs, "commands"),
		})
	default:
		return true, nil
	}
}

func (s *Service) recordTaskAction(ctx context.Context, taskID string, payload map[string]any) error {
	_, err := s.append(ctx, core.Event{
		Type:    core.EventTaskAction,
		TaskID:  taskID,
		Payload: core.MustJSON(payload),
	})
	return err
}

func (s *Service) waitForUserAction(ctx context.Context, taskID string, workerID string, reason string, question string, metadata map[string]any) error {
	if err := s.recordUserActionNeeded(ctx, taskID, workerID, reason, question, metadata); err != nil {
		return err
	}
	if err := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingUser, "approval_needed", question); err != nil {
		return err
	}
	return s.setTaskStatus(ctx, taskID, core.TaskWaiting)
}

func (s *Service) recordUserActionNeeded(ctx context.Context, taskID string, workerID string, reason string, question string, metadata map[string]any) error {
	payload := map[string]any{
		"question": nonEmpty(question, "The orchestrator needs user input before continuing."),
		"reason":   nonEmpty(reason, "user_input_required"),
	}
	for key, value := range metadata {
		switch typed := value.(type) {
		case string:
			if strings.TrimSpace(typed) != "" {
				payload[key] = typed
			}
		case []string:
			if len(typed) > 0 {
				payload[key] = typed
			}
		default:
			if value != nil {
				payload[key] = value
			}
		}
	}
	_, err := s.append(ctx, core.Event{
		Type:     core.EventApprovalNeeded,
		TaskID:   taskID,
		WorkerID: strings.TrimSpace(workerID),
		Payload:  core.MustJSON(payload),
	})
	return err
}

func (s *Service) waitForRecoverableError(ctx context.Context, taskID string, workerID string, err error) bool {
	if err == nil {
		return false
	}
	blocker, ok := classifyUserRecoverableBlocker(err.Error())
	if !ok {
		return false
	}
	_ = s.waitForUserAction(ctx, taskID, workerID, blocker.Reason, blocker.Question, map[string]any{
		"summary":    blocker.Summary,
		"resumeHint": "After fixing the environment or setup issue, respond on this task with what changed.",
		"error":      err.Error(),
	})
	return true
}

type userRecoverableBlocker struct {
	Reason   string
	Summary  string
	Question string
}

func classifyUserRecoverableBlocker(text string) (userRecoverableBlocker, bool) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return userRecoverableBlocker{}, false
	}
	lower := strings.ToLower(trimmed)
	checks := []struct {
		reason  string
		summary string
		any     []string
	}{
		{
			reason:  "missing_tool",
			summary: "A required command or tool is missing from the execution environment.",
			any:     []string{"command not found", "executable file not found", "no execution target matches labels", "no such file or directory: \"perf\"", "exec: \"perf\"", "exec: \"go\"", "exec: \"npm\"", "exec: \"deno\"", "exec: \"cargo\""},
		},
		{
			reason:  "permission_denied",
			summary: "The worker is blocked by operating-system or sandbox permissions.",
			any:     []string{"permission denied", "operation not permitted", "not permitted", "requires root", "must be root", "sudo:"},
		},
		{
			reason:  "profiler_setup_required",
			summary: "Profiling is blocked by VM or kernel profiling configuration.",
			any:     []string{"perf_event_paranoid", "kernel.perf_event", "perf_event_open", "failed to open perf", "debug symbols", "dwarf"},
		},
		{
			reason:  "ssh_setup_required",
			summary: "Remote execution is blocked by SSH authentication or host setup.",
			any:     []string{"permission denied (publickey)", "host key verification failed", "no route to host", "could not resolve hostname", "connection refused"},
		},
		{
			reason:  "repo_setup_required",
			summary: "Repository checkout or access is missing on the target environment.",
			any:     []string{"repository not found", "not a git repository", "workdir is not inside a supported vcs workspace", "missing repository", "clone"},
		},
	}
	for _, check := range checks {
		for _, needle := range check.any {
			if strings.Contains(lower, needle) {
				return userRecoverableBlocker{
					Reason:   check.reason,
					Summary:  check.summary,
					Question: fmt.Sprintf("%s\n\nBlocked error:\n%s\n\nPlease fix this setup issue in the target environment, then reply with what changed so the orchestrator can continue.", check.summary, trimmed),
				}, true
			}
		}
	}
	return userRecoverableBlocker{}, false
}

func publishPullRequestRequestFromAction(action PlanAction) core.PublishPullRequestRequest {
	inputs := action.Inputs
	return core.PublishPullRequestRequest{
		Title:  stringMetadata(inputs, "title"),
		Body:   stringMetadata(inputs, "body"),
		Repo:   stringMetadata(inputs, "repo"),
		Base:   stringMetadata(inputs, "base"),
		Branch: stringMetadata(inputs, "branch"),
		Draft:  boolMetadata(inputs, "draft"),
	}
}

func watchPullRequestsRequestFromAction(action PlanAction) core.WatchPullRequestsRequest {
	inputs := action.Inputs
	return core.WatchPullRequestsRequest{
		Repo:       stringMetadata(inputs, "repo"),
		Number:     intMetadata(inputs, "number"),
		URL:        stringMetadata(inputs, "url"),
		State:      stringMetadata(inputs, "state"),
		Author:     stringMetadata(inputs, "author"),
		HeadBranch: stringMetadata(inputs, "headBranch"),
		Limit:      intMetadata(inputs, "limit"),
	}
}

func (s *Service) waitForFinalCandidateResolution(ctx context.Context, taskID string, err error) error {
	_, _ = s.append(ctx, core.Event{
		Type:   core.EventApprovalNeeded,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"question": "Final candidate selection is ambiguous or points at a non-applyable worker. Retry after steering the orchestrator to consolidate or select the intended candidate.",
			"reason":   "final_candidate_selection",
			"error":    err.Error(),
		}),
	})
	if statusErr := s.setTaskStatus(ctx, taskID, core.TaskWaiting); statusErr != nil {
		return statusErr
	}
	if objectiveErr := s.updateTaskObjective(ctx, taskID, core.ObjectiveWaitingUser, "approval_needed", "Final candidate selection needs user or orchestrator approval."); objectiveErr != nil {
		return objectiveErr
	}
	return err
}

func resolveFinalCandidate(results []WorkerTurnResult, selectedWorkerID string) (string, string, error) {
	candidates := candidateResults(results)
	selectedWorkerID = strings.TrimSpace(selectedWorkerID)
	if selectedWorkerID != "" {
		for _, candidate := range candidates {
			if candidate.WorkerID == selectedWorkerID {
				return selectedWorkerID, "orchestrator selected final candidate explicitly", nil
			}
		}
		if ancestorID := selectedCandidateAncestor(results, selectedWorkerID); ancestorID != "" {
			return ancestorID, fmt.Sprintf("orchestrator selected worker %s; using nearest changed candidate ancestor", selectedWorkerID), nil
		}
		if fallbackID, fallbackReason, err := resolveFinalCandidate(results, ""); err == nil && fallbackID != "" {
			return fallbackID, fmt.Sprintf("selected final candidate %q was not applyable; fallback selected %s", selectedWorkerID, fallbackReason), nil
		}
		return "", "", fmt.Errorf("selected final candidate %q is not a successful worker with candidate changes", selectedWorkerID)
	}
	switch len(candidates) {
	case 0:
		return "", "", nil
	case 1:
		return candidates[0].WorkerID, "only successful worker with candidate changes", nil
	}
	leaves := candidateLeaves(candidates)
	if len(leaves) == 1 {
		return leaves[0].WorkerID, "only remaining candidate leaf after dependency lineage", nil
	}
	ids := make([]string, 0, len(leaves))
	for _, leaf := range leaves {
		ids = append(ids, leaf.WorkerID)
	}
	return "", "", fmt.Errorf("multiple competing final candidates remain (%s); the orchestrator must select finalCandidateWorkerId or schedule a consolidation/validation worker", strings.Join(ids, ", "))
}

func selectedCandidateAncestor(results []WorkerTurnResult, selectedWorkerID string) string {
	byID := map[string]WorkerTurnResult{}
	for _, result := range results {
		byID[result.WorkerID] = result
	}
	current, ok := byID[selectedWorkerID]
	if !ok || current.Status != core.WorkerSucceeded {
		return ""
	}
	seen := map[string]bool{selectedWorkerID: true}
	for strings.TrimSpace(current.BaseWorkerID) != "" {
		parentID := current.BaseWorkerID
		if seen[parentID] {
			return ""
		}
		seen[parentID] = true
		parent, ok := byID[parentID]
		if !ok {
			return ""
		}
		if parent.Status == core.WorkerSucceeded && resultHasCandidateChanges(parent) {
			return parent.WorkerID
		}
		current = parent
	}
	return ""
}

func candidateResults(results []WorkerTurnResult) []WorkerTurnResult {
	candidates := []WorkerTurnResult{}
	for _, result := range results {
		if result.Status == core.WorkerSucceeded && resultHasCandidateChanges(result) {
			candidates = append(candidates, result)
		}
	}
	return candidates
}

func candidateLeaves(candidates []WorkerTurnResult) []WorkerTurnResult {
	candidateIDs := map[string]bool{}
	for _, candidate := range candidates {
		candidateIDs[candidate.WorkerID] = true
	}
	hasCandidateChild := map[string]bool{}
	for _, candidate := range candidates {
		if candidateIDs[candidate.BaseWorkerID] {
			hasCandidateChild[candidate.BaseWorkerID] = true
		}
	}
	leaves := []WorkerTurnResult{}
	for _, candidate := range candidates {
		if !hasCandidateChild[candidate.WorkerID] {
			leaves = append(leaves, candidate)
		}
	}
	return leaves
}

func latestCandidateWorkerID(results []WorkerTurnResult) string {
	for i := len(results) - 1; i >= 0; i-- {
		result := results[i]
		if result.Status == core.WorkerSucceeded && resultHasCandidateChanges(result) {
			return result.WorkerID
		}
	}
	return ""
}

func resultHasCandidateChanges(result WorkerTurnResult) bool {
	return result.Changes.Dirty || len(result.Changes.ChangedFiles) > 0 || strings.TrimSpace(result.Changes.Diff) != ""
}

func (s *Service) taskCompletionMode(ctx context.Context, taskID string) string {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return "local"
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return "local"
	}
	return taskCompletionModeFromTask(task)
}

func taskCompletionModeFromTask(task core.Task) string {
	var metadata map[string]any
	if len(task.Metadata) > 0 {
		_ = json.Unmarshal(task.Metadata, &metadata)
	}
	switch strings.ToLower(strings.TrimSpace(stringMetadataValue(metadata["completionMode"]))) {
	case "github":
		return "github"
	default:
		return "local"
	}
}

func (s *Service) replanLoop(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult) (bool, string, string, []WorkerTurnResult) {
	replanner, ok := s.brain.(ReplanProvider)
	if !ok {
		return true, "", "", results
	}
	for turn := 1; turn <= maxDynamicReplanTurns; turn++ {
		decision, err := replanner.Replan(ctx, task, OrchestrationState{
			InitialPlan: initial,
			Results:     results,
			Turn:        turn,
		})
		if err != nil {
			return s.recoverReplanError(ctx, task, turn, results, fmt.Errorf("dynamic replan failed: %w", err))
		}
		if err := decision.Validate(); err != nil {
			return s.recoverReplanError(ctx, task, turn, results, fmt.Errorf("invalid dynamic replan decision: %w", err))
		}
		if _, err := s.append(ctx, core.Event{
			Type:   core.EventTaskReplanned,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"turn":     turn,
				"decision": decision,
			}),
		}); err != nil {
			_ = s.failTask(ctx, task.ID, err)
			return false, "", "", results
		}
		switch decision.Action {
		case "complete":
			return true, decision.FinalCandidateWorkerID, decision.Rationale, results
		case "wait":
			_ = s.waitForUserAction(ctx, task.ID, "", "orchestrator_wait", nonEmpty(decision.Message, decision.Rationale, "The orchestrator needs user input before continuing."), map[string]any{
				"turn":      turn,
				"rationale": decision.Rationale,
			})
			return false, "", "", results
		case "fail":
			_ = s.failTask(ctx, task.ID, errors.New(nonEmpty(decision.Message, decision.Rationale, "dynamic replan failed task")))
			return false, "", "", results
		case "continue":
			next := *decision.Plan
			if next.Metadata == nil {
				next.Metadata = map[string]any{}
			}
			next.Metadata["dynamicReplanTurn"] = turn
			if stringMetadata(next.Metadata, "baseWorkerID") == "" {
				if baseWorkerID := latestCandidateWorkerID(results); baseWorkerID != "" {
					next.Metadata["baseWorkerID"] = baseWorkerID
				}
			}
			normalizePlanReasoning(&next)
			if _, err := s.append(ctx, core.Event{
				Type:    core.EventTaskPlanned,
				TaskID:  task.ID,
				Payload: core.MustJSON(next),
			}); err != nil {
				_ = s.failTask(ctx, task.ID, err)
				return false, "", "", results
			}
			result, err := s.runPlannedWorker(ctx, task, next)
			if err != nil {
				if s.waitForRecoverableError(ctx, task.ID, "", err) {
					return false, "", "", results
				}
				_ = s.failTask(ctx, task.ID, err)
				return false, "", "", results
			}
			results = append(results, result)
			if !s.finishOrContinueTask(ctx, task.ID, result) {
				return false, "", "", results
			}
			var ok bool
			results, ok, err = s.runFollowUpWorkers(ctx, task, next, results, result.NodeID)
			if err != nil {
				if s.waitForRecoverableError(ctx, task.ID, result.WorkerID, err) {
					return false, "", "", results
				}
				_ = s.failTask(ctx, task.ID, err)
				return false, "", "", results
			}
			if !ok {
				return false, "", "", results
			}
			if ok, err := s.runPlanActions(ctx, task, next, results); err != nil {
				if s.waitForRecoverableError(ctx, task.ID, result.WorkerID, err) {
					return false, "", "", results
				}
				_ = s.failTask(ctx, task.ID, err)
				return false, "", "", results
			} else if !ok {
				return false, "", "", results
			}
		}
	}
	_ = s.failTask(ctx, task.ID, fmt.Errorf("dynamic replanning exceeded %d turns", maxDynamicReplanTurns))
	return false, "", "", results
}

func (s *Service) recoverReplanError(ctx context.Context, task core.Task, turn int, results []WorkerTurnResult, replanErr error) (bool, string, string, []WorkerTurnResult) {
	candidateWorkerID, candidateReason, candidateErr := resolveFinalCandidate(results, "")
	if candidateErr == nil {
		reason := "fallback completion after replanner error: " + replanErr.Error()
		if candidateReason != "" {
			reason += "; " + candidateReason
		}
		_, _ = s.append(ctx, core.Event{
			Type:   core.EventTaskReplanned,
			TaskID: task.ID,
			Payload: core.MustJSON(map[string]any{
				"turn": turn,
				"decision": ReplanDecision{
					Action:                 "complete",
					FinalCandidateWorkerID: candidateWorkerID,
					Rationale:              reason,
					Message:                "The replanner returned an invalid decision, so aged used the deterministic final-candidate fallback.",
				},
				"fallback": true,
				"error":    replanErr.Error(),
			}),
		})
		return true, candidateWorkerID, reason, results
	}
	_, _ = s.append(ctx, core.Event{
		Type:   core.EventTaskReplanned,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"turn": turn,
			"decision": ReplanDecision{
				Action:    "wait",
				Rationale: "replanner returned an invalid decision and deterministic final-candidate fallback is ambiguous",
				Message:   replanErr.Error(),
			},
			"fallback":       true,
			"error":          replanErr.Error(),
			"candidateError": candidateErr.Error(),
		}),
	})
	_, _ = s.append(ctx, core.Event{
		Type:   core.EventApprovalNeeded,
		TaskID: task.ID,
		Payload: core.MustJSON(map[string]any{
			"question": "Dynamic replanning failed and final candidate selection is ambiguous. Provide steering or retry after resolving the competing candidates.",
			"reason":   "dynamic_replan_error",
			"error":    replanErr.Error(),
		}),
	})
	_ = s.updateTaskObjective(ctx, task.ID, core.ObjectiveWaitingUser, "approval_needed", "Dynamic replanning needs user steering before continuing.")
	_ = s.setTaskStatus(ctx, task.ID, core.TaskWaiting)
	return false, "", "", results
}

type followUpNode struct {
	id    string
	index int
	spawn SpawnRequest
	deps  []string
}

func (s *Service) runFollowUpWorkers(ctx context.Context, task core.Task, initial Plan, results []WorkerTurnResult, parentNodeID string) ([]WorkerTurnResult, bool, error) {
	if len(initial.Spawns) == 0 {
		return results, true, nil
	}
	pending, err := followUpNodes(initial.Spawns)
	if err != nil {
		return results, false, err
	}
	completed := map[string]WorkerTurnResult{}
	for len(pending) > 0 {
		ready := readyFollowUps(pending, completed)
		if len(ready) == 0 {
			return results, false, fmt.Errorf("spawn dependency cycle or missing dependency")
		}
		waveResults, ok, err := s.runFollowUpWave(ctx, task, initial, ready, results, parentNodeID)
		results = append(results, waveResults...)
		for index, result := range waveResults {
			completed[ready[index].id] = result
			delete(pending, ready[index].id)
		}
		if err != nil {
			return results, false, err
		}
		if !ok {
			return results, false, nil
		}
	}
	return results, true, nil
}

func followUpNodes(spawns []SpawnRequest) (map[string]followUpNode, error) {
	nodes := map[string]followUpNode{}
	for index, spawn := range spawns {
		id := spawnID(spawn, index)
		if _, ok := nodes[id]; ok {
			return nil, fmt.Errorf("duplicate spawn id %q", id)
		}
		deps := make([]string, 0, len(spawn.DependsOn))
		for _, dep := range spawn.DependsOn {
			dep = strings.TrimSpace(dep)
			if dep != "" {
				deps = append(deps, dep)
			}
		}
		nodes[id] = followUpNode{id: id, index: index, spawn: spawn, deps: deps}
	}
	for _, node := range nodes {
		for _, dep := range node.deps {
			if _, ok := nodes[dep]; !ok {
				return nil, fmt.Errorf("spawn %q depends on unknown spawn %q", node.id, dep)
			}
			if dep == node.id {
				return nil, fmt.Errorf("spawn %q depends on itself", node.id)
			}
		}
	}
	return nodes, nil
}

func spawnID(spawn SpawnRequest, index int) string {
	if strings.TrimSpace(spawn.ID) != "" {
		return strings.TrimSpace(spawn.ID)
	}
	return fmt.Sprintf("spawn-%d", index+1)
}

func readyFollowUps(pending map[string]followUpNode, completed map[string]WorkerTurnResult) []followUpNode {
	ready := []followUpNode{}
	for _, node := range pending {
		blocked := false
		for _, dep := range node.deps {
			if _, ok := completed[dep]; !ok {
				blocked = true
				break
			}
		}
		if !blocked {
			ready = append(ready, node)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		return ready[i].index < ready[j].index
	})
	return ready
}

func (s *Service) runFollowUpWave(ctx context.Context, task core.Task, initial Plan, nodes []followUpNode, priorResults []WorkerTurnResult, parentNodeID string) ([]WorkerTurnResult, bool, error) {
	waveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type outcome struct {
		index  int
		plan   Plan
		result WorkerTurnResult
		err    error
	}
	outcomes := make(chan outcome, len(nodes))
	for index, node := range nodes {
		followUp := s.followUpPlan(task, initial, node.spawn, priorResults, node.index+2, node.id, node.deps, parentNodeID)
		if followUp.Metadata == nil {
			followUp.Metadata = map[string]any{}
		}
		if stringMetadata(followUp.Metadata, "nodeID") == "" {
			followUp.Metadata["nodeID"] = uuid.NewString()
		}
		if stringMetadata(followUp.Metadata, "planID") == "" {
			followUp.Metadata["planID"] = uuid.NewString()
		}
		if _, err := s.append(ctx, core.Event{
			Type:    core.EventTaskPlanned,
			TaskID:  task.ID,
			Payload: core.MustJSON(followUp),
		}); err != nil {
			return nil, false, err
		}
		go func(index int, plan Plan) {
			result, err := s.runPlannedWorker(waveCtx, task, plan)
			outcomes <- outcome{index: index, plan: plan, result: result, err: err}
		}(index, followUp)
	}

	ordered := make([]WorkerTurnResult, len(nodes))
	for range nodes {
		outcome := <-outcomes
		if outcome.err != nil {
			ordered[outcome.index] = failedFollowUpResult(outcome.plan, outcome.err)
			continue
		}
		ordered[outcome.index] = outcome.result
	}
	return ordered, true, nil
}

func failedFollowUpResult(plan Plan, err error) WorkerTurnResult {
	result := WorkerTurnResult{
		NodeID:       stringMetadata(plan.Metadata, "nodeID"),
		Status:       core.WorkerFailed,
		Kind:         plan.WorkerKind,
		Role:         stringMetadata(plan.Metadata, "spawnRole"),
		SpawnID:      stringMetadata(plan.Metadata, "spawnID"),
		BaseWorkerID: stringMetadata(plan.Metadata, "baseWorkerID"),
		Summary:      "Follow-up worker setup failed before execution.",
		Error:        err.Error(),
	}
	return result
}

func (s *Service) followUpPlan(task core.Task, initial Plan, spawn SpawnRequest, results []WorkerTurnResult, turn int, spawnID string, dependsOn []string, parentNodeID string) Plan {
	workerKind := s.workerKindForSpawn(spawn, initial.WorkerKind)
	prompt := buildFollowUpPrompt(task, spawn, results)
	reasoningEffort := normalizeReasoningEffort(nonEmpty(spawn.ReasoningEffort, initial.ReasoningEffort))
	baseWorkerID := latestCandidateWorkerID(results)
	plan := Plan{
		WorkerKind:      workerKind,
		Prompt:          prompt,
		ReasoningEffort: reasoningEffort,
		Rationale:       "follow-up worker scheduled from initial plan: " + spawn.Reason,
		Steps: []PlanStep{{
			Title:       "Run " + spawn.Role,
			Description: spawn.Reason,
		}},
		RequiredApprovals: []ApprovalRequest{},
		Spawns:            []SpawnRequest{},
		Metadata: map[string]any{
			"brain":           "orchestrator",
			"scheduler":       "orchestrator",
			"turn":            turn,
			"spawnID":         spawnID,
			"spawnRole":       spawn.Role,
			"spawnReason":     spawn.Reason,
			"dependsOn":       dependsOn,
			"parentNodeID":    parentNodeID,
			"parentRationale": initial.Rationale,
		},
	}
	if baseWorkerID != "" {
		plan.Metadata["baseWorkerID"] = baseWorkerID
	}
	if reasoningEffort != "" {
		plan.Metadata["reasoningEffort"] = reasoningEffort
	}
	return plan
}

func (s *Service) workerKindForSpawn(spawn SpawnRequest, fallback string) string {
	if strings.TrimSpace(spawn.WorkerKind) != "" {
		if _, ok := s.runners[spawn.WorkerKind]; ok {
			return spawn.WorkerKind
		}
	}
	role := strings.ToLower(spawn.Role + " " + spawn.Reason)
	if strings.Contains(role, "review") || strings.Contains(role, "feedback") || strings.Contains(role, "critique") {
		if _, ok := s.runners["claude"]; ok {
			return "claude"
		}
	}
	if _, ok := s.runners["codex"]; ok {
		return "codex"
	}
	if _, ok := s.runners[fallback]; ok {
		return fallback
	}
	if _, ok := s.runners["mock"]; ok {
		return "mock"
	}
	for kind := range s.runners {
		return kind
	}
	return fallback
}

func buildFollowUpPrompt(task core.Task, spawn SpawnRequest, results []WorkerTurnResult) string {
	var builder strings.Builder
	builder.WriteString("# Orchestrator Follow-up Worker Prompt\n\n")
	builder.WriteString("Task: ")
	builder.WriteString(task.Title)
	builder.WriteString("\n\nOriginal user request:\n")
	builder.WriteString(task.Prompt)
	builder.WriteString("\n\nFollow-up role:\n")
	builder.WriteString(spawn.Role)
	builder.WriteString("\n\nReason for this follow-up:\n")
	builder.WriteString(spawn.Reason)
	builder.WriteString("\n\nPrior worker results:\n")
	for index, result := range results {
		builder.WriteString(fmt.Sprintf("\n%d. Worker %s status: %s\n", index+1, result.WorkerID, result.Status))
		if result.Summary != "" {
			builder.WriteString("Summary: ")
			builder.WriteString(result.Summary)
			builder.WriteString("\n")
		}
		if result.Error != "" {
			builder.WriteString("Error: ")
			builder.WriteString(result.Error)
			builder.WriteString("\n")
		}
		if len(result.Changes.ChangedFiles) > 0 {
			builder.WriteString("Changed files:\n")
			for _, file := range result.Changes.ChangedFiles {
				builder.WriteString("- ")
				if file.Status != "" {
					builder.WriteString(file.Status)
					builder.WriteString(" ")
				}
				builder.WriteString(file.Path)
				builder.WriteString("\n")
			}
		}
	}
	builder.WriteString("\nExecute only this follow-up role. Do not apply changes unless this role explicitly requires implementation.\n")
	builder.WriteString("\nReport with these markdown sections when applicable:\n")
	builder.WriteString("- Findings\n")
	builder.WriteString("- Commands Run\n")
	builder.WriteString("- Benchmark Results\n")
	builder.WriteString("- Changed Files\n")
	builder.WriteString("- Blockers\n")
	builder.WriteString("- Recommended Next Turns\n")
	builder.WriteString("\nFor benchmark or profiler work, include exact commands, baseline numbers, candidate numbers, sample count when known, and confidence in whether the change is a real improvement.\n")
	return builder.String()
}

func nonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func pullRequestTargetRepo(req core.PublishPullRequestRequest, project core.Project, task core.Task) string {
	var metadata map[string]any
	if len(task.Metadata) > 0 {
		_ = json.Unmarshal(task.Metadata, &metadata)
	}
	return nonEmpty(req.Repo, project.UpstreamRepo, project.Repo, stringMetadataValue(metadata["repo"]))
}

func pullRequestHeadRepoOwner(project core.Project) string {
	if owner := strings.TrimSpace(project.HeadRepoOwner); owner != "" {
		return owner
	}
	if strings.TrimSpace(project.UpstreamRepo) == "" || strings.EqualFold(strings.TrimSpace(project.UpstreamRepo), strings.TrimSpace(project.Repo)) {
		return ""
	}
	return repoOwner(project.Repo)
}

func taskStatus(snapshot core.Snapshot, taskID string) core.TaskStatus {
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return ""
	}
	return task.Status
}

func (s *Service) projectForTask(task core.Task) core.Project {
	if s.projects == nil {
		return core.Project{ID: "default", Name: "default", LocalPath: s.workDir, VCS: "auto", DefaultBase: "main"}
	}
	if task.ProjectID != "" {
		if project, ok := s.projects.Get(task.ProjectID); ok {
			return project
		}
	}
	if len(task.Metadata) > 0 {
		var metadata map[string]any
		if err := json.Unmarshal(task.Metadata, &metadata); err == nil {
			if projectID := stringMetadataValue(metadata["projectId"]); projectID != "" {
				if project, ok := s.projects.Get(projectID); ok {
					return project
				}
			}
			if repo := stringMetadataValue(metadata["repo"]); repo != "" {
				if project, ok := s.projects.findByMetadataRepo(metadata, repo); ok {
					return project
				}
			}
		}
	}
	return s.projects.Default()
}

func projectCloneURL(project core.Project) string {
	repo := strings.TrimSpace(nonEmpty(project.Repo, project.UpstreamRepo))
	if repo == "" {
		return ""
	}
	if strings.Contains(repo, "://") || strings.HasPrefix(repo, "git@") || strings.HasSuffix(repo, ".git") {
		return repo
	}
	if strings.Count(repo, "/") == 1 {
		return "https://github.com/" + repo + ".git"
	}
	return repo
}

func createTaskMetadata(req core.CreateTaskRequest) (map[string]any, error) {
	metadata := map[string]any{}
	if len(req.Metadata) > 0 {
		if err := json.Unmarshal(req.Metadata, &metadata); err != nil {
			return nil, fmt.Errorf("metadata must be a JSON object: %w", err)
		}
		if metadata == nil {
			metadata = map[string]any{}
		}
	}
	if req.Source != "" {
		metadata["source"] = strings.TrimSpace(req.Source)
	}
	if req.ExternalID != "" {
		metadata["externalId"] = strings.TrimSpace(req.ExternalID)
	}
	if req.ProjectID != "" {
		metadata["projectId"] = strings.TrimSpace(req.ProjectID)
	}
	return metadata, nil
}

func promptRequestsGitHubCompletion(prompt string) bool {
	lower := strings.ToLower(prompt)
	return strings.Contains(lower, "open pr") ||
		strings.Contains(lower, "open a pr") ||
		strings.Contains(lower, "open pull request") ||
		strings.Contains(lower, "open a pull request") ||
		strings.Contains(lower, "create pr") ||
		strings.Contains(lower, "create a pr") ||
		strings.Contains(lower, "create pull request") ||
		strings.Contains(lower, "make sure it gets merged") ||
		strings.Contains(lower, "until merged") ||
		strings.Contains(lower, "babysit")
}

func taskTargetLabels(task core.Task) map[string]string {
	if len(task.Metadata) == 0 {
		return nil
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return nil
	}
	return targetLabels(metadata)
}

func taskExternalRef(task core.Task) (string, string) {
	if len(task.Metadata) == 0 {
		return "", ""
	}
	var metadata map[string]any
	if err := json.Unmarshal(task.Metadata, &metadata); err != nil {
		return "", ""
	}
	return stringMetadataValue(metadata["source"]), stringMetadataValue(metadata["externalId"])
}

func stringMetadataValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}

func findTask(snapshot core.Snapshot, taskID string) (core.Task, bool) {
	for _, task := range snapshot.Tasks {
		if task.ID == taskID {
			return task, true
		}
	}
	return core.Task{}, false
}

func taskSteering(snapshot core.Snapshot, taskID string) []string {
	var out []string
	for _, event := range snapshot.Events {
		if event.TaskID != taskID || event.Type != core.EventTaskSteered {
			continue
		}
		var payload struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err == nil && strings.TrimSpace(payload.Message) != "" {
			out = append(out, payload.Message)
		}
	}
	return out
}

func retryPlanForTask(snapshot core.Snapshot, taskID string) (Plan, error) {
	var plans []Plan
	var workerIDs []string
	terminalWorkerID := ""
	for _, event := range snapshot.Events {
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventTaskPlanned:
			var plan Plan
			if err := json.Unmarshal(event.Payload, &plan); err != nil {
				return Plan{}, fmt.Errorf("decode task plan: %w", err)
			}
			plans = append(plans, plan)
		case core.EventExecutionPlanned:
			var payload struct {
				WorkerID string `json:"workerId"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return Plan{}, fmt.Errorf("decode execution plan: %w", err)
			}
			workerIDs = append(workerIDs, nonEmpty(payload.WorkerID, event.WorkerID))
		case core.EventWorkerCompleted:
			var payload struct {
				Status core.WorkerStatus `json:"status"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return Plan{}, fmt.Errorf("decode worker completion: %w", err)
			}
			if payload.Status == core.WorkerFailed || payload.Status == core.WorkerCanceled {
				terminalWorkerID = event.WorkerID
			}
		}
	}
	if len(plans) == 0 {
		return Plan{}, errors.New("task has no persisted plan to retry")
	}
	if terminalWorkerID != "" {
		for i := len(workerIDs) - 1; i >= 0; i-- {
			if workerIDs[i] == terminalWorkerID && i < len(plans) {
				return retryPlanWithResume(snapshot, plans[i], terminalWorkerID), nil
			}
		}
	}
	return retryPlanWithResume(snapshot, plans[len(plans)-1], terminalWorkerID), nil
}

func taskFailedDuringDynamicReplan(snapshot core.Snapshot, taskID string) bool {
	return latestTaskFailureMatches(snapshot, taskID, func(errorText string) bool {
		return strings.Contains(errorText, "dynamic replan")
	})
}

func taskFailureRecoverableFromGraph(snapshot core.Snapshot, taskID string, results []WorkerTurnResult) bool {
	if len(candidateResults(results)) == 0 {
		return false
	}
	if latestTaskFailureMatches(snapshot, taskID, func(errorText string) bool {
		return errorText == "" ||
			strings.Contains(errorText, "dynamic replan") ||
			strings.Contains(errorText, "final candidate") ||
			strings.Contains(errorText, "selected final candidate") ||
			strings.Contains(errorText, "multiple competing final candidates") ||
			strings.Contains(errorText, "worker command failed")
	}) {
		return true
	}
	latest := latestWorkerResult(results)
	return latest.Status == core.WorkerFailed || latest.Status == core.WorkerCanceled
}

func latestTaskFailureMatches(snapshot core.Snapshot, taskID string, match func(string) bool) bool {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.TaskID != taskID || event.Type != core.EventTaskStatus {
			continue
		}
		var payload struct {
			Status core.TaskStatus `json:"status"`
			Error  string          `json:"error"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return false
		}
		return payload.Status == core.TaskFailed && match(strings.ToLower(strings.TrimSpace(payload.Error)))
	}
	return false
}

func latestWorkerResult(results []WorkerTurnResult) WorkerTurnResult {
	if len(results) == 0 {
		return WorkerTurnResult{}
	}
	return results[len(results)-1]
}

func retryGraphStateForTask(snapshot core.Snapshot, taskID string) (Plan, []WorkerTurnResult, error) {
	var initial Plan
	haveInitial := false
	workerMetadata := map[string]map[string]any{}
	results := []WorkerTurnResult{}
	for _, event := range snapshot.Events {
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventTaskPlanned:
			var plan Plan
			if err := json.Unmarshal(event.Payload, &plan); err != nil {
				return Plan{}, nil, fmt.Errorf("decode task plan: %w", err)
			}
			if !haveInitial {
				initial = plan
				haveInitial = true
			}
		case core.EventWorkerCreated:
			var payload struct {
				Kind     string         `json:"kind"`
				Metadata map[string]any `json:"metadata"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return Plan{}, nil, fmt.Errorf("decode worker created: %w", err)
			}
			metadata := map[string]any{}
			for key, value := range payload.Metadata {
				metadata[key] = value
			}
			if payload.Kind != "" {
				metadata["workerKind"] = payload.Kind
			}
			workerMetadata[event.WorkerID] = metadata
		case core.EventWorkerCompleted:
			var payload struct {
				Status           core.WorkerStatus `json:"status"`
				Summary          string            `json:"summary"`
				Error            string            `json:"error"`
				WorkspaceChanges WorkspaceChanges  `json:"workspaceChanges"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return Plan{}, nil, fmt.Errorf("decode worker completion: %w", err)
			}
			metadata := workerMetadata[event.WorkerID]
			result := WorkerTurnResult{
				WorkerID: event.WorkerID,
				Status:   payload.Status,
				Kind:     stringMetadata(metadata, "workerKind"),
				Summary:  payload.Summary,
				Error:    payload.Error,
				Changes:  payload.WorkspaceChanges,
			}
			result.NodeID = stringMetadata(metadata, "nodeID")
			result.Role = stringMetadata(metadata, "spawnRole")
			result.SpawnID = stringMetadata(metadata, "spawnID")
			result.BaseWorkerID = stringMetadata(metadata, "baseWorkerID")
			results = append(results, result)
		}
	}
	if !haveInitial {
		return Plan{}, nil, errors.New("task has no persisted initial plan to retry graph")
	}
	if len(results) == 0 {
		return Plan{}, nil, errors.New("task has no completed worker results to retry graph")
	}
	return initial, results, nil
}

func retryPlanWithResume(snapshot core.Snapshot, plan Plan, workerID string) Plan {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return plan
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.Metadata["retryFromWorkerID"] = workerID
	if execution := workerExecutionInfo(snapshot, workerID); len(execution) > 0 {
		if targetID := stringMetadataValue(execution["targetId"]); targetID != "" {
			plan.Metadata["retryTargetID"] = targetID
		}
		if targetKind := stringMetadataValue(execution["targetKind"]); targetKind != "" {
			plan.Metadata["retryTargetKind"] = targetKind
		}
		if remoteSession := stringMetadataValue(execution["remoteSession"]); remoteSession != "" {
			plan.Metadata["retryRemoteSession"] = remoteSession
		}
		if remoteRunDir := stringMetadataValue(execution["remoteRunDir"]); remoteRunDir != "" {
			plan.Metadata["retryRemoteRunDir"] = remoteRunDir
		}
		if remoteWorkDir := stringMetadataValue(execution["remoteWorkDir"]); remoteWorkDir != "" {
			plan.Metadata["retryRemoteWorkDir"] = remoteWorkDir
		}
	}
	if sessionID := workerProviderSessionID(snapshot, workerID, plan.WorkerKind); sessionID != "" {
		plan.Metadata["retryResumeSessionID"] = sessionID
	}
	return plan
}

func workerProviderSessionID(snapshot core.Snapshot, workerID string, kind string) string {
	for _, event := range snapshot.Events {
		if event.WorkerID != workerID || event.Type != core.EventWorkerOutput {
			continue
		}
		var payload struct {
			Raw json.RawMessage `json:"raw"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || len(payload.Raw) == 0 {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(payload.Raw, &raw); err != nil {
			continue
		}
		switch kind {
		case "codex":
			if raw["type"] == "thread.started" {
				if sessionID := stringMetadataValue(raw["thread_id"]); sessionID != "" {
					return sessionID
				}
			}
		case "claude":
			if raw["type"] == "system" && raw["subtype"] == "init" {
				if sessionID := stringMetadataValue(raw["session_id"]); sessionID != "" {
					return sessionID
				}
			}
		}
	}
	return ""
}

func workerExecutionInfo(snapshot core.Snapshot, workerID string) map[string]any {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.WorkerID != workerID || event.Type != core.EventExecutionPlanned {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err == nil {
			return payload
		}
	}
	return nil
}

func latestWorkerQuestion(snapshot core.Snapshot, taskID string) (string, string) {
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.TaskID != taskID {
			continue
		}
		switch event.Type {
		case core.EventWorkerCompleted:
			var payload struct {
				NeedsInput bool   `json:"needsInput"`
				Summary    string `json:"summary"`
				Error      string `json:"error"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.NeedsInput {
				return event.WorkerID, nonEmpty(payload.Summary, payload.Error, "worker requested orchestrator input")
			}
		case core.EventWorkerOutput:
			var payload struct {
				Kind string `json:"kind"`
				Text string `json:"text"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.Kind == string(worker.EventNeedsInput) {
				return event.WorkerID, nonEmpty(payload.Text, "worker requested orchestrator input")
			}
		}
	}
	return "", "worker requested orchestrator input"
}

func (s *Service) describeWorkspaceChanges(ctx context.Context, workspace PreparedWorkspace) WorkspaceChanges {
	changes, err := s.workspaces.DescribeChanges(ctx, workspace)
	if err != nil && changes.Error == "" {
		changes.Error = err.Error()
	}
	return changes
}

func (s *Service) describeWorkspaceChangesForCompletion(ctx context.Context, workspace PreparedWorkspace) WorkspaceChanges {
	changes := s.describeWorkspaceChanges(ctx, workspace)
	if changes.Error != "" {
		return changes
	}
	diff, err := s.describeWorkspaceDiff(ctx, workspace)
	if err != nil {
		changes.Error = err.Error()
		return changes
	}
	changes.Diff = strings.TrimSpace(diff)
	return changes
}

func (s *Service) describeWorkspaceDiff(ctx context.Context, workspace PreparedWorkspace) (string, error) {
	type workspaceDiffer interface {
		DescribeDiff(context.Context, PreparedWorkspace) (string, error)
	}
	differ, ok := s.workspaces.(workspaceDiffer)
	if !ok {
		return "", nil
	}
	return differ.DescribeDiff(ctx, workspace)
}

func (s *Service) retryWorkspace(ctx context.Context, taskID string, newWorkerID string, previousWorkerID string) (PreparedWorkspace, bool, error) {
	previousWorkerID = strings.TrimSpace(previousWorkerID)
	if previousWorkerID == "" {
		return PreparedWorkspace{}, false, nil
	}
	workspace, err := s.workspaceForWorker(ctx, previousWorkerID)
	if err != nil {
		return PreparedWorkspace{}, false, fmt.Errorf("load retry workspace for %s: %w", previousWorkerID, err)
	}
	cwd := strings.TrimSpace(workspace.CWD)
	if cwd == "" {
		return PreparedWorkspace{}, false, fmt.Errorf("retry workspace for %s has no cwd", previousWorkerID)
	}
	info, err := os.Stat(cwd)
	if err != nil {
		return PreparedWorkspace{}, false, fmt.Errorf("retry workspace %s is not available: %w", cwd, err)
	}
	if !info.IsDir() {
		return PreparedWorkspace{}, false, fmt.Errorf("retry workspace %s is not a directory", cwd)
	}
	workspace.TaskID = taskID
	workspace.WorkerID = newWorkerID
	return workspace, true, nil
}

func (s *Service) baseWorkspaceSpec(ctx context.Context, spec WorkspaceSpec, baseWorkerID string) (WorkspaceSpec, error) {
	base, err := s.workspaceForWorker(ctx, baseWorkerID)
	if err != nil {
		return spec, fmt.Errorf("load base workspace for %s: %w", baseWorkerID, err)
	}
	if strings.TrimSpace(base.CWD) == "" {
		return spec, fmt.Errorf("base workspace for %s has no cwd", baseWorkerID)
	}
	if base.VCSType == "ssh" {
		return spec, nil
	}
	spec.BaseWorkDir = base.CWD
	switch base.VCSType {
	case "jj":
		if strings.TrimSpace(base.WorkspaceName) != "" {
			spec.BaseRevision = base.WorkspaceName + "@"
		}
	case "git":
		spec.BaseRevision = ""
	}
	return spec, nil
}

func (s *Service) workerRanOnTarget(ctx context.Context, workerID string, targetID string) (bool, error) {
	workerID = strings.TrimSpace(workerID)
	targetID = strings.TrimSpace(targetID)
	if workerID == "" || targetID == "" {
		return false, nil
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventExecutionPlanned || event.WorkerID != workerID {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return false, err
		}
		return stringMetadataValue(payload["targetId"]) == targetID || stringMetadataValue(payload["targetID"]) == targetID, nil
	}
	return false, nil
}

func (s *Service) workerHandoffPatch(ctx context.Context, workerID string) (string, WorkspaceChanges, error) {
	changes, err := s.completedWorkspaceChanges(ctx, workerID)
	if err != nil {
		return "", WorkspaceChanges{}, err
	}
	if strings.TrimSpace(changes.Diff) != "" {
		return changes.Diff, changes, nil
	}
	if len(changes.ChangedFiles) == 0 && !changes.Dirty {
		return "", changes, nil
	}
	workspace, err := s.workspaceForWorker(ctx, workerID)
	if err != nil {
		return "", changes, fmt.Errorf("base worker %s has changes but no persisted patch: %w", workerID, err)
	}
	if workspace.VCSType == "ssh" {
		return "", changes, fmt.Errorf("base worker %s has remote changes but no persisted diff.patch", workerID)
	}
	diff, err := s.describeWorkspaceDiff(ctx, workspace)
	if err != nil {
		return "", changes, fmt.Errorf("read base worker patch for %s: %w", workerID, err)
	}
	changes.Diff = strings.TrimSpace(diff)
	if changes.Diff == "" {
		return "", changes, fmt.Errorf("base worker %s has changes but generated an empty patch", workerID)
	}
	return changes.Diff, changes, nil
}

func (s *Service) remoteRetryWorkDir(ctx context.Context, target TargetConfig, previousWorkerID string) (string, error) {
	workspace, err := s.workspaceForWorker(ctx, previousWorkerID)
	if err != nil {
		return "", fmt.Errorf("load retry workspace for %s: %w", previousWorkerID, err)
	}
	cwd := strings.TrimSpace(workspace.CWD)
	if cwd == "" {
		return "", fmt.Errorf("retry workspace for %s has no cwd", previousWorkerID)
	}
	ok, err := s.sshRunner.DirectoryExists(ctx, target, cwd)
	if err != nil {
		return "", fmt.Errorf("check retry workspace %s: %w", cwd, err)
	}
	if !ok {
		return "", fmt.Errorf("retry workspace %s is not available", cwd)
	}
	return cwd, nil
}

func (s *Service) workspaceForWorker(ctx context.Context, workerID string) (PreparedWorkspace, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return PreparedWorkspace{}, err
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventWorkerWorkspace || event.WorkerID != workerID {
			continue
		}
		var workspace PreparedWorkspace
		if err := json.Unmarshal(event.Payload, &workspace); err != nil {
			return PreparedWorkspace{}, fmt.Errorf("decode worker workspace: %w", err)
		}
		return workspace, nil
	}
	return PreparedWorkspace{}, eventstore.ErrNotFound
}

func (s *Service) completedWorkspaceChanges(ctx context.Context, workerID string) (WorkspaceChanges, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return WorkspaceChanges{}, err
	}
	for i := len(snapshot.Events) - 1; i >= 0; i-- {
		event := snapshot.Events[i]
		if event.Type != core.EventWorkerCompleted || event.WorkerID != workerID {
			continue
		}
		var payload struct {
			WorkspaceChanges WorkspaceChanges `json:"workspaceChanges"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return WorkspaceChanges{}, fmt.Errorf("decode worker completion: %w", err)
		}
		if payload.WorkspaceChanges.Root == "" && payload.WorkspaceChanges.CWD == "" {
			return WorkspaceChanges{}, eventstore.ErrNotFound
		}
		return payload.WorkspaceChanges, nil
	}
	return WorkspaceChanges{}, eventstore.ErrNotFound
}

func (s *Service) projectForTaskID(ctx context.Context, taskID string) (core.Project, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return core.Project{}, err
	}
	task, ok := findTask(snapshot, taskID)
	if !ok {
		return core.Project{}, eventstore.ErrNotFound
	}
	return s.projectForTask(task), nil
}

func applyRemotePatch(ctx context.Context, project core.Project, workspace PreparedWorkspace, changes WorkspaceChanges) (WorkerApplyResult, error) {
	result := baseWorkerApplyResult(workspace, "remote_patch_apply")
	result.SourceRoot = project.LocalPath
	result.AppliedFiles = changes.ChangedFiles
	if strings.TrimSpace(changes.Diff) == "" {
		if len(changes.ChangedFiles) == 0 && !changes.Dirty {
			return result, nil
		}
		return result, errors.New("remote worker changes did not include diff.patch")
	}
	patchFile := filepath.Join(os.TempDir(), "aged-remote-"+shortID(workspace.WorkerID)+".patch")
	if err := os.WriteFile(patchFile, []byte(changes.Diff), 0o600); err != nil {
		return result, err
	}
	defer os.Remove(patchFile)
	if _, err := runCommand(ctx, project.LocalPath, "git", "apply", "--check", "--whitespace=nowarn", patchFile); err != nil {
		if _, threeWayErr := runCommand(ctx, project.LocalPath, "git", "apply", "--3way", "--whitespace=nowarn", patchFile); threeWayErr == nil {
			result.Method = "remote_patch_apply_3way"
			return result, nil
		} else {
			return result, fmt.Errorf("remote patch has conflicts or no longer applies cleanly; check failed: %w; 3-way apply failed: %v", err, threeWayErr)
		}
	}
	if _, err := runCommand(ctx, project.LocalPath, "git", "apply", "--whitespace=nowarn", patchFile); err != nil {
		return result, fmt.Errorf("apply checked remote patch: %w", err)
	}
	return result, nil
}

func (s *Service) cleanupWorkspace(ctx context.Context, taskID string, workerID string, workspace PreparedWorkspace, result WorkspaceResult) error {
	cleanup, err := s.workspaces.Cleanup(ctx, workspace, result)
	if err != nil && cleanup.Error == "" {
		cleanup.Error = err.Error()
	}
	_, appendErr := s.append(ctx, core.Event{
		Type:     core.EventWorkerCleanup,
		TaskID:   taskID,
		WorkerID: workerID,
		Payload:  core.MustJSON(cleanup),
	})
	if appendErr != nil {
		return appendErr
	}
	return err
}

func (s *Service) setTaskStatus(ctx context.Context, taskID string, status core.TaskStatus) error {
	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": status,
		}),
	})
	return err
}

func (s *Service) updateTaskObjective(ctx context.Context, taskID string, status core.ObjectiveStatus, phase string, summary string) error {
	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskObjective,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status":  status,
			"phase":   phase,
			"summary": summary,
		}),
	})
	return err
}

func (s *Service) recordTaskMilestone(ctx context.Context, taskID string, name string, phase string, summary string, metadata map[string]any) error {
	if metadata == nil {
		metadata = map[string]any{}
	}
	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskMilestone,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"name":     name,
			"phase":    phase,
			"summary":  summary,
			"metadata": metadata,
		}),
	})
	return err
}

func (s *Service) recordTaskArtifact(ctx context.Context, taskID string, id string, kind string, name string, url string, ref string, metadata map[string]any) error {
	if metadata == nil {
		metadata = map[string]any{}
	}
	_, err := s.append(ctx, core.Event{
		Type:   core.EventTaskArtifact,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"id":       id,
			"kind":     kind,
			"name":     name,
			"url":      url,
			"ref":      ref,
			"metadata": metadata,
		}),
	})
	return err
}

func (s *Service) recordWorkerArtifacts(ctx context.Context, taskID string, workerID string, workerKind string, state *workerRunState, changes WorkspaceChanges) error {
	for _, artifact := range changes.Artifacts {
		metadata := artifact.Metadata
		if metadata == nil {
			metadata = map[string]any{}
		}
		metadata["workerId"] = workerID
		if artifact.Path != "" {
			metadata["path"] = artifact.Path
		}
		if artifact.Content != "" {
			metadata["content"] = artifact.Content
		}
		if err := s.recordTaskArtifact(ctx, taskID, nonEmpty(artifact.ID, workerID+"-"+artifact.Kind), artifact.Kind, artifact.Name, "", artifact.Path, metadata); err != nil {
			return err
		}
	}
	summary := state.summaryText()
	if strings.TrimSpace(summary) == "" {
		return nil
	}
	for _, artifact := range resultArtifacts(workerID, workerKind, summary) {
		if err := s.recordTaskArtifact(ctx, taskID, artifact.ID, artifact.Kind, artifact.Name, "", "", artifact.Metadata); err != nil {
			return err
		}
	}
	return nil
}

type resultArtifact struct {
	ID       string
	Kind     string
	Name     string
	Metadata map[string]any
}

func resultArtifacts(workerID string, workerKind string, summary string) []resultArtifact {
	lower := strings.ToLower(summary)
	artifacts := []resultArtifact{}
	if workerKind == "benchmark_compare" || strings.Contains(lower, "## benchmark results") {
		metadata := parseMarkdownKeyValues(summary)
		metadata["workerId"] = workerID
		metadata["content"] = truncateArtifactContent(summary)
		artifacts = append(artifacts, resultArtifact{
			ID:       workerID + "-benchmark",
			Kind:     "benchmark_report",
			Name:     "Benchmark report",
			Metadata: metadata,
		})
	}
	if strings.Contains(lower, "flamegraph") || strings.Contains(lower, "profiler") || strings.Contains(lower, "profile report") {
		artifacts = append(artifacts, resultArtifact{
			ID:   workerID + "-profile",
			Kind: "profiler_report",
			Name: "Profiler report",
			Metadata: map[string]any{
				"workerId": workerID,
				"content":  truncateArtifactContent(summary),
			},
		})
	}
	for _, spec := range []struct {
		marker string
		kind   string
		name   string
	}{
		{"## test report", "test_report", "Test report"},
		{"## ci", "ci_run", "CI run"},
		{"## review comments", "review_comments", "Review comments"},
		{"## deployment", "deployment", "Deployment"},
		{"## package", "package", "Package"},
	} {
		if strings.Contains(lower, spec.marker) {
			artifacts = append(artifacts, resultArtifact{
				ID:   workerID + "-" + spec.kind,
				Kind: spec.kind,
				Name: spec.name,
				Metadata: map[string]any{
					"workerId": workerID,
					"content":  truncateArtifactContent(summary),
				},
			})
		}
	}
	return artifacts
}

func parseMarkdownKeyValues(text string) map[string]any {
	values := map[string]any{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(line, "-"))
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		values[camelArtifactKey(key)] = value
	}
	return values
}

func camelArtifactKey(key string) string {
	parts := strings.FieldsFunc(strings.ToLower(key), func(r rune) bool {
		return r == '_' || r == '-' || r == ' '
	})
	if len(parts) == 0 {
		return key
	}
	out := parts[0]
	for _, part := range parts[1:] {
		if part == "" {
			continue
		}
		out += strings.ToUpper(part[:1]) + part[1:]
	}
	return out
}

func (s *Service) setExecutionNodeStatus(ctx context.Context, taskID string, nodeID string, status core.WorkerStatus) error {
	_, err := s.append(ctx, core.Event{
		Type:   core.EventExecutionStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"nodeId": nodeID,
			"status": status,
		}),
	})
	return err
}

func (s *Service) failTask(ctx context.Context, taskID string, err error) error {
	_, appendErr := s.append(ctx, core.Event{
		Type:   core.EventTaskStatus,
		TaskID: taskID,
		Payload: core.MustJSON(map[string]any{
			"status": core.TaskFailed,
			"error":  err.Error(),
		}),
	})
	return appendErr
}

func planMetadata(plan Plan) map[string]any {
	metadata := map[string]any{}
	for _, key := range []string{
		"brain",
		"scheduler",
		"model",
		"fallbackReason",
		"parentRationale",
		"dependsOn",
		"baseWorkerID",
		"baseWorkspaceCWD",
		"baseWorkspaceReuseError",
		"baseRevision",
		"baseHandoff",
		"basePatchApplied",
		"baseChangedFiles",
		"nodeID",
		"parentNodeID",
		"planID",
		"targetID",
		"targetKind",
		"targetLabels",
		"ignoredTargetLabels",
		"targetSelectionPolicy",
		"targetSelectionSource",
		"remoteSession",
		"remoteRunDir",
		"remoteWorkDir",
		"spawnID",
		"spawnReason",
		"spawnRole",
		"turn",
		"dynamicReplanTurn",
		"reasoningEffort",
		"retryFromWorkerID",
		"retryTargetID",
		"retryTargetKind",
		"retryRemoteSession",
		"retryRemoteRunDir",
		"retryRemoteWorkDir",
		"retryResumeSessionID",
		"retryWorkspaceReused",
		"retryWorkspaceCWD",
		"retryWorkspaceError",
	} {
		if value, ok := plan.Metadata[key]; ok && value != nil && value != "" {
			metadata[key] = value
		}
	}
	if plan.Rationale != "" {
		metadata["rationale"] = plan.Rationale
	}
	if len(plan.Steps) > 0 {
		metadata["steps"] = plan.Steps
	}
	if len(plan.RequiredApprovals) > 0 {
		metadata["requiredApprovals"] = plan.RequiredApprovals
	}
	if len(plan.Spawns) > 0 {
		metadata["spawns"] = plan.Spawns
	}
	return metadata
}

func normalizeReasoningEffort(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "default", "":
		return ""
	case "low", "medium", "high", "xhigh", "max":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}

func normalizePlanReasoning(plan *Plan) {
	if plan == nil {
		return
	}
	if plan.Metadata == nil {
		plan.Metadata = map[string]any{}
	}
	plan.ReasoningEffort = normalizeReasoningEffort(nonEmpty(plan.ReasoningEffort, stringMetadata(plan.Metadata, "reasoningEffort"), stringMetadata(plan.Metadata, "thinkingLevel"), stringMetadata(plan.Metadata, "effort")))
	if plan.ReasoningEffort != "" {
		plan.Metadata["reasoningEffort"] = plan.ReasoningEffort
	}
}

func stringMetadata(metadata map[string]any, key string) string {
	if metadata == nil {
		return ""
	}
	switch value := metadata[key].(type) {
	case string:
		return strings.TrimSpace(value)
	default:
		return ""
	}
}

func boolMetadata(metadata map[string]any, key string) bool {
	if metadata == nil {
		return false
	}
	value, _ := metadata[key].(bool)
	return value
}

func intMetadata(metadata map[string]any, key string) int {
	if metadata == nil {
		return 0
	}
	switch value := metadata[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case json.Number:
		number, _ := value.Int64()
		return int(number)
	default:
		return 0
	}
}

func stringSliceMetadata(metadata map[string]any, key string) []string {
	if metadata == nil {
		return nil
	}
	switch value := metadata[key].(type) {
	case []string:
		return value
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
				out = append(out, strings.TrimSpace(text))
			}
		}
		return out
	default:
		return nil
	}
}

func runnerSupportsSteering(runner worker.Runner) bool {
	support, ok := runner.(worker.SteeringSupport)
	return ok && support.SupportsSteering()
}

func (s *Service) append(ctx context.Context, event core.Event) (core.Event, error) {
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	stored, err := s.store.Append(ctx, event)
	if err != nil {
		return core.Event{}, err
	}
	s.broker.Publish(stored)
	return stored, nil
}

type eventSink struct {
	service  *Service
	taskID   string
	workerID string
	state    *workerRunState
}

func (s eventSink) Event(ctx context.Context, event worker.Event) error {
	if s.state != nil {
		s.state.observe(event)
	}
	_, err := s.service.append(ctx, core.Event{
		Type:     core.EventWorkerOutput,
		TaskID:   s.taskID,
		WorkerID: s.workerID,
		Payload:  core.MustJSON(event),
	})
	return err
}

type workerRunState struct {
	mu         sync.Mutex
	logCount   int
	summary    string
	lastError  string
	needsInput bool
	rawResult  []byte
}

func (s *workerRunState) observe(event worker.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch event.Kind {
	case worker.EventResult:
		s.summary = event.Text
		if len(event.Raw) > 0 {
			s.rawResult = append(s.rawResult[:0], event.Raw...)
		}
	case worker.EventError:
		s.lastError = event.Text
	case worker.EventNeedsInput:
		s.needsInput = true
		if s.summary == "" {
			s.summary = event.Text
		}
	default:
		s.logCount++
	}
}

func (s *workerRunState) isWaitingForInput() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.needsInput
}

func (s *workerRunState) summaryText() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.summary
}

func (s *workerRunState) completionPayload(status core.WorkerStatus, runErr error, changes WorkspaceChanges) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload := map[string]any{
		"status":           status,
		"logCount":         s.logCount,
		"needsInput":       s.needsInput,
		"workspaceChanges": changes,
	}
	if len(changes.ChangedFiles) > 0 {
		payload["changedFiles"] = changes.ChangedFiles
	}
	if s.summary != "" {
		payload["summary"] = s.summary
	}
	if s.lastError != "" {
		payload["error"] = s.lastError
	} else if runErr != nil {
		payload["error"] = runErr.Error()
	}
	if len(s.rawResult) > 0 {
		payload["rawResult"] = core.MustJSON(jsonRawMessage(s.rawResult))
	}
	return payload
}

func (s *workerRunState) turnResult(workerID string, plan Plan, status core.WorkerStatus, runErr error, changes WorkspaceChanges) WorkerTurnResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := WorkerTurnResult{
		WorkerID: workerID,
		Status:   status,
		Kind:     plan.WorkerKind,
		Summary:  s.summary,
		Changes:  changes,
	}
	if plan.Metadata != nil {
		if nodeID, ok := plan.Metadata["nodeID"].(string); ok {
			result.NodeID = nodeID
		}
		if role, ok := plan.Metadata["spawnRole"].(string); ok {
			result.Role = role
		}
		if spawnID, ok := plan.Metadata["spawnID"].(string); ok {
			result.SpawnID = spawnID
		}
		if baseWorkerID, ok := plan.Metadata["baseWorkerID"].(string); ok {
			result.BaseWorkerID = baseWorkerID
		}
	}
	if s.lastError != "" {
		result.Error = s.lastError
	} else if runErr != nil {
		result.Error = runErr.Error()
	}
	return result
}

type jsonRawMessage []byte

func (m jsonRawMessage) MarshalJSON() ([]byte, error) {
	if len(m) == 0 {
		return []byte("null"), nil
	}
	return m, nil
}
