package orchestrator

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/worker"
)

func TestTargetRegistrySelectsMatchingLeastLoadedTarget(t *testing.T) {
	registry := NewTargetRegistry([]TargetConfig{
		{ID: "small", Kind: TargetKindSSH, Host: "small", Labels: map[string]string{"role": "general"}, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 1}},
		{ID: "perf", Kind: TargetKindSSH, Host: "perf", Labels: map[string]string{"role": "benchmark"}, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 8, MemoryGB: 64}},
	})
	plan := Plan{
		Prompt: "run benchmark",
		Metadata: map[string]any{
			"targetLabels": map[string]any{"role": "benchmark"},
			"workerSize":   "large",
		},
	}
	target, err := registry.Select(plan)
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "perf" {
		t.Fatalf("target = %q", target.ID)
	}
}

func TestTargetRegistryAvoidsUnhealthySSHTargets(t *testing.T) {
	registry := NewTargetRegistry([]TargetConfig{
		{ID: "bad", Kind: TargetKindSSH, Host: "bad", Labels: map[string]string{"role": "benchmark"}, Capacity: TargetCapacity{MaxWorkers: 2, CPUWeight: 20, MemoryGB: 128}},
		{ID: "good", Kind: TargetKindSSH, Host: "good", Labels: map[string]string{"role": "benchmark"}, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1, MemoryGB: 16}},
	})
	registry.UpdateHealth("bad", core.TargetHealth{Status: "unhealthy", Reachable: true, Tmux: false, RepoPresent: true}, core.TargetResources{})
	registry.UpdateHealth("good", core.TargetHealth{Status: "ok", Reachable: true, Tmux: true, RepoPresent: true}, core.TargetResources{CPUCount: 4, Load1: 0.2, MemoryAvailableMB: 8192})

	target, err := registry.Select(Plan{
		Prompt:   "run benchmark",
		Metadata: map[string]any{"targetLabels": map[string]any{"role": "benchmark"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "good" {
		t.Fatalf("target = %q, want good", target.ID)
	}
	snapshot := registry.Snapshot()
	for _, state := range snapshot {
		if state.ID == "bad" && state.Available {
			t.Fatalf("bad target should not be available: %+v", state)
		}
	}
}

func TestTargetRegistrySkipsSSHWorkerWhenToolProbeIsMissing(t *testing.T) {
	registry := NewTargetRegistry([]TargetConfig{
		{ID: "local", Kind: TargetKindLocal, Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 1}},
		{ID: "vm", Kind: TargetKindSSH, Host: "vm", Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100}},
	})
	registry.UpdateHealth("vm", core.TargetHealth{
		Status: "ok",
		Tools:  map[string]bool{"codex": false},
	}, core.TargetResources{})

	target, err := registry.Select(Plan{WorkerKind: "codex", Prompt: "run codex"})
	if err != nil {
		t.Fatal(err)
	}
	if target.ID != "local" {
		t.Fatalf("target = %s, want local", target.ID)
	}
}

func TestSSHRunnerProbeReportsToolAvailability(t *testing.T) {
	executor := &fakeRemoteExecutor{probeOutput: strings.Join([]string{
		"tmux=true",
		"repoPresent=true",
		"tool.codex=false",
		"tool.claude=true",
		"cpuCount=4",
	}, "\n")}
	runner := SSHRunner{Executor: executor}

	health, _ := runner.Probe(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm", WorkDir: "/repo"})
	if health.Tools["codex"] {
		t.Fatalf("codex should be unavailable: %+v", health.Tools)
	}
	if !health.Tools["claude"] {
		t.Fatalf("claude should be available: %+v", health.Tools)
	}
}

func TestSSHRunnerStartsTmuxAndPollsStatus(t *testing.T) {
	executor := &fakeRemoteExecutor{}
	runner := SSHRunner{Executor: executor}
	target := TargetConfig{ID: "vm-1", Kind: TargetKindSSH, Host: "vm", WorkDir: "/repo", WorkRoot: "/runs"}
	spec := worker.Spec{ID: "worker-1234567890", WorkDir: "/repo"}
	run := NewRemoteRun(target, spec)
	if err := runner.Start(context.Background(), run, []string{"sh", "-lc", "echo ok"}, ""); err != nil {
		t.Fatal(err)
	}
	if len(executor.commands) < 2 || !strings.Contains(strings.Join(executor.commands[1], " "), "tmux new-session") {
		t.Fatalf("start command = %+v", executor.commands)
	}
	sink := &recordingWorkerSink{}
	stdoutOffset := 0
	stderrOffset := 0
	status, err := runner.PollOnce(context.Background(), run, worker.ParserForKind("mock"), sink, &stdoutOffset, &stderrOffset)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "succeeded" {
		t.Fatalf("status = %+v", status)
	}
	if !sink.has(worker.EventLog, "stdout", "remote output") {
		t.Fatalf("missing remote output: %+v", sink.events)
	}
	changes := runner.DescribeChanges(context.Background(), run)
	if changes.VCSType != "git" || !changes.Dirty || len(changes.ChangedFiles) != 1 || changes.ChangedFiles[0].Path != "main.go" || !strings.Contains(changes.Diff, "diff --git") {
		t.Fatalf("changes = %+v", changes)
	}
	if !strings.HasSuffix(changes.Diff, "\n") {
		t.Fatalf("diff should be normalized with trailing newline: %q", changes.Diff)
	}
	if len(changes.Artifacts) != 1 || changes.Artifacts[0].Kind != "worker_log" || !strings.Contains(changes.Artifacts[0].Content, "remote output") {
		t.Fatalf("artifacts = %+v", changes.Artifacts)
	}
}

func TestSSHRunnerApplyPatchNormalizesMissingTrailingNewline(t *testing.T) {
	executor := &fakeRemoteExecutor{}
	runner := SSHRunner{Executor: executor}
	target := TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}

	if err := runner.ApplyPatch(context.Background(), target, "/repo", "/tmp/run", "diff --git a/main.go b/main.go"); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(executor.input, "\n") {
		t.Fatalf("uploaded patch should end with newline: %q", executor.input)
	}
}

func TestRemoteChangeScriptIncludesUntrackedFilesInPatch(t *testing.T) {
	script := remoteChangeScript(NewRemoteRun(TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, worker.Spec{ID: "worker", WorkDir: "/repo"}))
	if !strings.Contains(script, "git ls-files --others --exclude-standard") || !strings.Contains(script, "git diff --no-index --binary") {
		t.Fatalf("remote change script does not append untracked files:\n%s", script)
	}
}

func TestSSHRunnerStartUploadsPromptForStdinCommand(t *testing.T) {
	executor := &fakeRemoteExecutor{}
	runner := SSHRunner{Executor: executor}
	run := NewRemoteRun(TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, worker.Spec{ID: "worker-stdin", WorkDir: "/repo"})

	if err := runner.Start(context.Background(), run, []string{"codex", "exec", "--json", "-"}, "large prompt"); err != nil {
		t.Fatal(err)
	}
	if executor.input != "large prompt" {
		t.Fatalf("input = %q", executor.input)
	}
	var sawPromptUpload bool
	var sawPromptRedirect bool
	var sawPathBootstrap bool
	for _, argv := range executor.commands {
		joined := strings.Join(argv, " ")
		if strings.Contains(joined, "cat >") && strings.Contains(joined, "prompt.txt") {
			sawPromptUpload = true
		}
		if strings.Contains(joined, "<") && strings.Contains(joined, "prompt.txt") {
			sawPromptRedirect = true
		}
		if strings.Contains(joined, ".local/share/mise/shims") {
			sawPathBootstrap = true
		}
	}
	if !sawPromptUpload || !sawPromptRedirect || !sawPathBootstrap {
		t.Fatalf("commands did not upload prompt, redirect stdin, and bootstrap PATH: %+v", executor.commands)
	}
}

func TestSSHRunnerProbeParsesTargetHealth(t *testing.T) {
	executor := &fakeRemoteExecutor{probeOutput: strings.Join([]string{
		"tmux=true",
		"repoPresent=true",
		"diskAvailableKB=10485760",
		"diskUsedPercent=42%",
		"memoryTotalKB=33554432",
		"memoryAvailableKB=16777216",
		"load1=1.25",
		"cpuCount=8",
	}, "\n")}
	runner := SSHRunner{Executor: executor}
	health, resources := runner.Probe(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm", WorkDir: "/repo"})
	if health.Status != "ok" || !health.Reachable || !health.Tmux || !health.RepoPresent {
		t.Fatalf("health = %+v", health)
	}
	if resources.CPUCount != 8 || resources.MemoryAvailableMB != 16384 || resources.DiskAvailableMB != 10240 || resources.DiskUsedPercent != 42 {
		t.Fatalf("resources = %+v", resources)
	}
}

func TestSSHRunnerProbeAllowsMissingRepoForPreparation(t *testing.T) {
	executor := &fakeRemoteExecutor{probeOutput: strings.Join([]string{
		"tmux=true",
		"repoPresent=false",
		"cpuCount=4",
		"load1=0.1",
	}, "\n")}
	runner := SSHRunner{Executor: executor}
	health, _ := runner.Probe(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm", WorkDir: "/repo"})
	if health.Status != "ok" || !strings.Contains(health.Error, "prepared") {
		t.Fatalf("health = %+v", health)
	}
}

func TestSSHRunnerPrepareCheckoutClonesAndChecksOutBase(t *testing.T) {
	executor := &fakeRemoteExecutor{}
	runner := SSHRunner{Executor: executor}
	if _, err := runner.PrepareCheckout(context.Background(), TargetConfig{ID: "vm", Kind: TargetKindSSH, Host: "vm"}, RemoteCheckoutSpec{
		RepoURL:     "https://github.com/nathanwhit/aged.git",
		WorkDir:     "/srv/aged/repos/aged",
		DefaultBase: "main",
	}); err != nil {
		t.Fatal(err)
	}
	if len(executor.commands) == 0 {
		t.Fatal("missing prepare command")
	}
	joined := strings.Join(executor.commands[0], " ")
	for _, want := range []string{"git clone", "git fetch origin --prune", "git checkout --detach", "origin/$base"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("prepare command missing %q:\n%s", want, joined)
		}
	}
}

func TestNewRemoteRunUsesSpecWorkDirWhenTargetOmitsWorkDir(t *testing.T) {
	run := NewRemoteRun(TargetConfig{ID: "vm-1", Kind: TargetKindSSH, Host: "vm"}, worker.Spec{
		ID:      "worker-1234567890",
		WorkDir: "/repo",
	})
	if run.WorkDir != "/repo" {
		t.Fatalf("remote workDir = %q, want /repo", run.WorkDir)
	}
}

func TestServiceRunsWorkerOnRealSSHTarget(t *testing.T) {
	host := os.Getenv("AGED_SSH_SMOKE_HOST")
	if host == "" {
		t.Skip("set AGED_SSH_SMOKE_HOST to run real SSH target smoke")
	}
	port, _ := strconv.Atoi(os.Getenv("AGED_SSH_SMOKE_PORT"))
	user := os.Getenv("AGED_SSH_SMOKE_USER")
	identityFile := os.Getenv("AGED_SSH_SMOKE_IDENTITY_FILE")
	workDir := os.Getenv("AGED_SSH_SMOKE_WORKDIR")
	if workDir == "" {
		workDir = "/repo"
	}
	workRoot := os.Getenv("AGED_SSH_SMOKE_WORKROOT")
	if workRoot == "" {
		workRoot = "/runs"
	}

	ctx := context.Background()
	store, err := eventstore.OpenSQLite(ctx, t.TempDir()+"/aged.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{{
		ID:                    "ssh-smoke",
		Kind:                  TargetKindSSH,
		Host:                  host,
		Port:                  port,
		User:                  user,
		IdentityFile:          identityFile,
		InsecureIgnoreHostKey: true,
		WorkDir:               workDir,
		WorkRoot:              workRoot,
		Labels:                map[string]string{"role": "remote"},
		Capacity:              TargetCapacity{MaxWorkers: 1, CPUWeight: 4},
	}})
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "remote",
		Prompt:     "run remote ssh smoke",
		Metadata: map[string]any{
			"targetLabels": map[string]any{"role": "remote"},
		},
	}}, map[string]worker.Runner{
		"remote": buildOnlyRunner{kind: "remote", command: []string{"sh", "-lc", "printf 'remote smoke\\n'"}},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{PollInterval: 100 * time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "SSH smoke", Prompt: "Run on container over SSH."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 || snapshot.ExecutionNodes[0].TargetID != "ssh-smoke" {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	if !eventContains(snapshot.Events, core.EventWorkerOutput, "remote smoke") {
		t.Fatalf("missing remote smoke output")
	}
}

func TestServiceFallsBackToLocalWhenRemoteCheckoutIsDirty(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	defer store.Close()

	targets := NewTargetRegistry([]TargetConfig{{
		ID:       "vm-dirty",
		Kind:     TargetKindSSH,
		Host:     "vm-dirty",
		WorkDir:  "/home/exedev/deno",
		Capacity: TargetCapacity{MaxWorkers: 1, CPUWeight: 100},
	}})
	executor := &fakeRemoteExecutor{
		prepareOutput: "remote checkout is dirty: /home/exedev/deno",
		prepareErr:    errors.New("exit status 20"),
	}
	service := NewServiceWithWorkspaceManagerAndTargets(store, fixedBrain{plan: Plan{
		WorkerKind: "mock",
		Prompt:     "run work",
		Metadata: map[string]any{
			"workerSize": "large",
		},
	}}, map[string]worker.Runner{
		"mock": eventRunner{kind: "mock"},
	}, t.TempDir(), fakeWorkspaceManager{cwd: t.TempDir()}, targets, SSHRunner{Executor: executor, PollInterval: time.Millisecond})

	task, err := service.CreateTask(ctx, core.CreateTaskRequest{Title: "Fallback", Prompt: "Run with remote fallback."})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitForTaskStatus(t, store, task.ID, core.TaskSucceeded)
	if len(snapshot.ExecutionNodes) != 1 {
		t.Fatalf("nodes = %+v", snapshot.ExecutionNodes)
	}
	node := snapshot.ExecutionNodes[0]
	if node.TargetID != "local" || node.TargetKind != "local" {
		t.Fatalf("node = %+v, want local fallback", node)
	}
	if !hasEventPayloadValue(snapshot.Events, core.EventWorkerCreated, task.ID, "fallbackFromTargetID", "vm-dirty") {
		t.Fatalf("missing fallback metadata")
	}
}

type fakeRemoteExecutor struct {
	commands      [][]string
	probeOutput   string
	prepareOutput string
	prepareErr    error
	input         string
}

func (e *fakeRemoteExecutor) Run(_ context.Context, argv []string) (string, error) {
	e.commands = append(e.commands, append([]string(nil), argv...))
	joined := strings.Join(argv, " ")
	switch {
	case e.prepareErr != nil && strings.Contains(joined, "git clone"):
		return e.prepareOutput, e.prepareErr
	case strings.Contains(joined, "repoPresent="):
		if e.probeOutput != "" {
			return e.probeOutput, nil
		}
		return "tmux=true\nrepoPresent=true\ncpuCount=4\nload1=0.1\n", nil
	case strings.Contains(joined, "stdout.log"):
		return "remote output\n", nil
	case strings.Contains(joined, "stderr.log"):
		return "", nil
	case strings.Contains(joined, "status.json"):
		return `{"status":"succeeded","exit":0}`, nil
	case strings.Contains(joined, "vcs.txt"):
		return "git\n", nil
	case strings.Contains(joined, "root.txt"):
		return "/repo\n", nil
	case strings.Contains(joined, "changes.txt"):
		return " M main.go\n", nil
	case strings.Contains(joined, "diffstat.txt"):
		return " main.go | 2 +-\n", nil
	case strings.Contains(joined, "diff.patch"):
		return "diff --git a/main.go b/main.go\n", nil
	default:
		return "", nil
	}
}

func (e *fakeRemoteExecutor) RunInput(_ context.Context, argv []string, input string) (string, error) {
	e.commands = append(e.commands, append([]string(nil), argv...))
	e.input = input
	return "", nil
}

type recordingWorkerSink struct {
	events []worker.Event
}

func (s *recordingWorkerSink) Event(_ context.Context, event worker.Event) error {
	s.events = append(s.events, event)
	return nil
}

func (s *recordingWorkerSink) has(kind worker.EventKind, stream string, text string) bool {
	for _, event := range s.events {
		if event.Kind == kind && event.Stream == stream && event.Text == text {
			return true
		}
	}
	return false
}

func eventContains(events []core.Event, eventType core.EventType, text string) bool {
	for _, event := range events {
		if event.Type == eventType && strings.Contains(string(event.Payload), text) {
			return true
		}
	}
	return false
}
