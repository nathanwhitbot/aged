package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"aged/internal/core"
)

type TargetKind string

const (
	TargetKindLocal TargetKind = "local"
	TargetKindSSH   TargetKind = "ssh"
)

type TargetCapacity struct {
	MaxWorkers int     `json:"maxWorkers"`
	CPUWeight  float64 `json:"cpuWeight,omitempty"`
	MemoryGB   float64 `json:"memoryGB,omitempty"`
}

type TargetConfig struct {
	ID                    string            `json:"id"`
	Kind                  TargetKind        `json:"kind"`
	Host                  string            `json:"host,omitempty"`
	User                  string            `json:"user,omitempty"`
	Port                  int               `json:"port,omitempty"`
	IdentityFile          string            `json:"identityFile,omitempty"`
	InsecureIgnoreHostKey bool              `json:"insecureIgnoreHostKey,omitempty"`
	WorkDir               string            `json:"workDir,omitempty"`
	WorkRoot              string            `json:"workRoot,omitempty"`
	Labels                map[string]string `json:"labels,omitempty"`
	Capacity              TargetCapacity    `json:"capacity,omitempty"`
}

type TargetRegistry struct {
	mu       sync.Mutex
	targets  map[string]TargetConfig
	running  map[string]int
	health   map[string]core.TargetHealth
	resource map[string]core.TargetResources
}

func NewLocalTargetRegistry() *TargetRegistry {
	return NewTargetRegistry([]TargetConfig{{
		ID:   "local",
		Kind: TargetKindLocal,
		Labels: map[string]string{
			"location": "local",
		},
		Capacity: TargetCapacity{MaxWorkers: 16, CPUWeight: 1},
	}})
}

func LoadTargetRegistry(path string) (*TargetRegistry, error) {
	if strings.TrimSpace(path) == "" {
		return NewLocalTargetRegistry(), nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var payload struct {
		Targets []TargetConfig `json:"targets"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	if len(payload.Targets) == 0 {
		return nil, errors.New("target config must include at least one target")
	}
	return NewTargetRegistry(payload.Targets), nil
}

func NewTargetRegistry(configs []TargetConfig) *TargetRegistry {
	registry := &TargetRegistry{
		targets:  map[string]TargetConfig{},
		running:  map[string]int{},
		health:   map[string]core.TargetHealth{},
		resource: map[string]core.TargetResources{},
	}
	for _, config := range configs {
		normalized, err := normalizeTargetConfig(config)
		if err != nil {
			continue
		}
		registry.targets[normalized.ID] = normalized
	}
	if len(registry.targets) == 0 {
		registry.targets["local"] = NewLocalTargetRegistry().targets["local"]
	}
	return registry
}

func normalizeTargetConfig(config TargetConfig) (TargetConfig, error) {
	config.ID = strings.TrimSpace(config.ID)
	if config.ID == "" {
		return TargetConfig{}, errors.New("target id is required")
	}
	if config.Kind == "" {
		config.Kind = TargetKindLocal
	}
	config.Host = strings.TrimSpace(config.Host)
	config.User = strings.TrimSpace(config.User)
	config.IdentityFile = strings.TrimSpace(config.IdentityFile)
	config.WorkDir = strings.TrimSpace(config.WorkDir)
	config.WorkRoot = strings.TrimSpace(config.WorkRoot)
	if config.Kind == TargetKindSSH && config.Host == "" {
		return TargetConfig{}, errors.New("ssh target host is required")
	}
	if config.Capacity.MaxWorkers <= 0 {
		config.Capacity.MaxWorkers = 1
	}
	if config.Capacity.CPUWeight <= 0 {
		config.Capacity.CPUWeight = 1
	}
	if config.Labels == nil {
		config.Labels = map[string]string{}
	}
	return config, nil
}

func targetConfigFromCore(config core.TargetConfig) TargetConfig {
	return TargetConfig{
		ID:                    config.ID,
		Kind:                  TargetKind(config.Kind),
		Host:                  config.Host,
		User:                  config.User,
		Port:                  config.Port,
		IdentityFile:          config.IdentityFile,
		InsecureIgnoreHostKey: config.InsecureIgnoreHostKey,
		WorkDir:               config.WorkDir,
		WorkRoot:              config.WorkRoot,
		Labels:                config.Labels,
		Capacity:              TargetCapacity(config.Capacity),
	}
}

func coreTargetConfig(config TargetConfig) core.TargetConfig {
	return core.TargetConfig{
		ID:                    config.ID,
		Kind:                  string(config.Kind),
		Host:                  config.Host,
		User:                  config.User,
		Port:                  config.Port,
		IdentityFile:          config.IdentityFile,
		InsecureIgnoreHostKey: config.InsecureIgnoreHostKey,
		WorkDir:               config.WorkDir,
		WorkRoot:              config.WorkRoot,
		Labels:                config.Labels,
		Capacity:              core.TargetCapacity(config.Capacity),
	}
}

func (r *TargetRegistry) Register(config TargetConfig) (TargetConfig, error) {
	if r == nil {
		return TargetConfig{}, errors.New("target registry is not configured")
	}
	normalized, err := normalizeTargetConfig(config)
	if err != nil {
		return TargetConfig{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets[normalized.ID] = normalized
	return normalized, nil
}

func (r *TargetRegistry) Delete(id string) error {
	if r == nil {
		return errors.New("target registry is not configured")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("target id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.targets[id]; !ok {
		return errors.New("target not found")
	}
	if r.running[id] > 0 {
		return errors.New("target has running workers")
	}
	if len(r.targets) <= 1 {
		return errors.New("cannot delete the only execution target")
	}
	delete(r.targets, id)
	delete(r.running, id)
	delete(r.health, id)
	delete(r.resource, id)
	return nil
}

func (r *TargetRegistry) Select(plan Plan) (TargetConfig, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	required := targetLabels(plan.Metadata)
	size := workerSize(plan.Metadata, plan.Prompt)
	candidates := make([]TargetConfig, 0, len(r.targets))
	for _, target := range r.targets {
		if labelsMatch(target.Labels, required) && r.isAvailableLocked(target) && r.supportsWorkerLocked(target, plan.WorkerKind) {
			candidates = append(candidates, target)
		}
	}
	if len(candidates) == 0 {
		return TargetConfig{}, fmt.Errorf("no execution target matches labels %v and worker kind %q", required, plan.WorkerKind)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return r.scoreLocked(candidates[i], size) > r.scoreLocked(candidates[j], size)
	})
	return candidates[0], nil
}

func (r *TargetRegistry) supportsWorkerLocked(target TargetConfig, workerKind string) bool {
	if target.Kind != TargetKindSSH {
		return true
	}
	workerKind = strings.TrimSpace(workerKind)
	if workerKind == "" || workerKind == "mock" || workerKind == "shell" || workerKind == "benchmark_compare" {
		return true
	}
	health := r.health[target.ID]
	if len(health.Tools) == 0 {
		return true
	}
	available, known := health.Tools[workerKind]
	return !known || available
}

func (r *TargetRegistry) SelectID(id string) (TargetConfig, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	target, ok := r.targets[id]
	if !ok {
		return TargetConfig{}, fmt.Errorf("execution target %q is not configured", id)
	}
	if !r.isAvailableLocked(target) {
		return TargetConfig{}, fmt.Errorf("execution target %q is at capacity", id)
	}
	return target, nil
}

func (r *TargetRegistry) SelectLocalFallback() (TargetConfig, error) {
	if r == nil {
		return TargetConfig{}, errors.New("target registry is not configured")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, target := range r.targets {
		if target.Kind == TargetKindLocal && r.isAvailableLocked(target) {
			return target, nil
		}
	}
	if _, ok := r.targets["local"]; ok {
		return TargetConfig{}, errors.New("local execution target is at capacity")
	}
	target := TargetConfig{
		ID:   "local",
		Kind: TargetKindLocal,
		Labels: map[string]string{
			"location": "local",
		},
		Capacity: TargetCapacity{MaxWorkers: 16, CPUWeight: 1},
	}
	r.targets[target.ID] = target
	return target, nil
}

func (r *TargetRegistry) Get(id string) (TargetConfig, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	target, ok := r.targets[id]
	return target, ok
}

func (r *TargetRegistry) Configs() []TargetConfig {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]TargetConfig, 0, len(r.targets))
	for _, target := range r.targets {
		out = append(out, target)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *TargetRegistry) Begin(targetID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.running[targetID]++
}

func (r *TargetRegistry) Finish(targetID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running[targetID] > 0 {
		r.running[targetID]--
	}
}

func (r *TargetRegistry) UpdateHealth(targetID string, health core.TargetHealth, resources core.TargetResources) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.targets[targetID]; !ok {
		return
	}
	r.health[targetID] = health
	r.resource[targetID] = resources
}

func (r *TargetRegistry) Snapshot() []core.TargetState {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]core.TargetState, 0, len(r.targets))
	for _, target := range r.targets {
		out = append(out, core.TargetState{
			TargetConfig: coreTargetConfig(target),
			Running:      r.running[target.ID],
			Available:    r.isAvailableLocked(target),
			Health:       r.health[target.ID],
			Resources:    r.resource[target.ID],
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *TargetRegistry) scoreLocked(target TargetConfig, size string) float64 {
	running := float64(r.running[target.ID])
	score := target.Capacity.CPUWeight - running*2
	if target.Kind == TargetKindLocal {
		score += 0.5
	}
	resources := r.resource[target.ID]
	if resources.CPUCount > 0 && resources.Load1 > 0 {
		loadRatio := resources.Load1 / float64(resources.CPUCount)
		score -= loadRatio * 2
	}
	if resources.MemoryAvailableMB > 0 {
		score += float64(resources.MemoryAvailableMB) / 32768
	}
	if resources.DiskAvailableMB > 0 {
		score += float64(resources.DiskAvailableMB) / 262144
	}
	if resources.DiskUsedPercent >= 90 {
		score -= 4
	}
	switch size {
	case "large":
		score += target.Capacity.MemoryGB / 16
		if resources.MemoryAvailableMB > 0 {
			score += float64(resources.MemoryAvailableMB) / 16384
		}
		score -= running * 2
	case "medium":
		score += target.Capacity.MemoryGB / 32
		score -= running
	}
	return score
}

func (r *TargetRegistry) isAvailableLocked(target TargetConfig) bool {
	if r.running[target.ID] >= target.Capacity.MaxWorkers {
		return false
	}
	health, hasHealth := r.health[target.ID]
	if !hasHealth || target.Kind == TargetKindLocal {
		return true
	}
	if strings.EqualFold(health.Status, "error") || strings.EqualFold(health.Status, "unhealthy") {
		return false
	}
	if health.Reachable && health.Tmux {
		return true
	}
	return false
}

func labelsMatch(labels map[string]string, required map[string]string) bool {
	for key, want := range required {
		if labels[key] != want {
			return false
		}
	}
	return true
}

func targetLabels(metadata map[string]any) map[string]string {
	out := map[string]string{}
	if metadata == nil {
		return out
	}
	switch value := metadata["targetLabels"].(type) {
	case map[string]string:
		return value
	case map[string]any:
		for key, item := range value {
			if text, ok := item.(string); ok {
				out[key] = text
			}
		}
	}
	return out
}

func workerSize(metadata map[string]any, prompt string) string {
	if metadata != nil {
		if size, ok := metadata["workerSize"].(string); ok && size != "" {
			return size
		}
	}
	lower := strings.ToLower(prompt)
	if strings.Contains(lower, "benchmark") || strings.Contains(lower, "profile") || strings.Contains(lower, "large") {
		return "large"
	}
	return "medium"
}

type RemoteExecutor interface {
	Run(ctx context.Context, argv []string) (string, error)
}

type RemoteInputExecutor interface {
	RunInput(ctx context.Context, argv []string, input string) (string, error)
}
