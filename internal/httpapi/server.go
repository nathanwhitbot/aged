package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"aged/internal/core"
	"aged/internal/eventstore"
	"aged/internal/orchestrator"
)

type Server struct {
	service *orchestrator.Service
	static  http.Handler
	auth    *GoogleAuth
}

func New(service *orchestrator.Service, static http.Handler) *Server {
	return &Server{service: service, static: static}
}

func NewWithAuth(service *orchestrator.Service, static http.Handler, auth *GoogleAuth) *Server {
	return &Server{service: service, static: static, auth: auth}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.health)
	if s.auth != nil {
		s.auth.RegisterRoutes(mux)
	}
	mux.HandleFunc("POST /mcp", s.mcp)
	mux.HandleFunc("GET /mcp", s.mcpInfo)
	mux.HandleFunc("GET /api/snapshot", s.snapshot)
	mux.HandleFunc("GET /api/projects", s.projects)
	mux.HandleFunc("POST /api/projects", s.createProject)
	mux.HandleFunc("PUT /api/projects/{id}", s.updateProject)
	mux.HandleFunc("DELETE /api/projects/{id}", s.deleteProject)
	mux.HandleFunc("GET /api/projects/{id}/health", s.projectHealth)
	mux.HandleFunc("GET /api/targets", s.targets)
	mux.HandleFunc("POST /api/targets", s.registerTarget)
	mux.HandleFunc("PUT /api/targets/{id}", s.updateTarget)
	mux.HandleFunc("POST /api/targets/{id}/health", s.refreshTargetHealth)
	mux.HandleFunc("DELETE /api/targets/{id}", s.deleteTarget)
	mux.HandleFunc("GET /api/plugins", s.plugins)
	mux.HandleFunc("POST /api/plugins", s.registerPlugin)
	mux.HandleFunc("PUT /api/plugins/{id}", s.updatePlugin)
	mux.HandleFunc("DELETE /api/plugins/{id}", s.deletePlugin)
	mux.HandleFunc("GET /api/events", s.events)
	mux.HandleFunc("GET /api/events/stream", s.eventStream)
	mux.HandleFunc("POST /api/assistant", s.assistant)
	mux.HandleFunc("GET /api/tasks/lookup", s.lookupTask)
	mux.HandleFunc("POST /api/tasks", s.createTask)
	mux.HandleFunc("GET /api/tasks/{id}/events", s.taskEvents)
	mux.HandleFunc("POST /api/tasks/clear-terminal", s.clearTerminalTasks)
	mux.HandleFunc("POST /api/tasks/{id}/clear", s.clearTask)
	mux.HandleFunc("PUT /api/tasks/{id}/loop-config", s.updateTaskLoopConfig)
	mux.HandleFunc("POST /api/tasks/{id}/steer", s.steerTask)
	mux.HandleFunc("POST /api/tasks/{id}/retry", s.retryTask)
	mux.HandleFunc("POST /api/tasks/{id}/cancel", s.cancelTask)
	mux.HandleFunc("POST /api/tasks/{id}/apply", s.applyTaskResult)
	mux.HandleFunc("POST /api/tasks/{id}/apply-policy", s.recommendApplyPolicy)
	mux.HandleFunc("POST /api/tasks/{id}/pull-request", s.publishTaskPullRequest)
	mux.HandleFunc("POST /api/tasks/{id}/watch-pull-requests", s.watchTaskPullRequests)
	mux.HandleFunc("POST /api/pull-requests/{id}/refresh", s.refreshPullRequest)
	mux.HandleFunc("POST /api/pull-requests/{id}/babysit", s.startPullRequestBabysitter)
	mux.HandleFunc("GET /api/workers/{id}/changes", s.reviewWorkerChanges)
	mux.HandleFunc("POST /api/workers/{id}/apply", s.applyWorkerChanges)
	mux.HandleFunc("POST /api/workers/{id}/cancel", s.cancelWorker)
	if s.static != nil {
		mux.Handle("/", s.static)
	}
	var handler http.Handler = mux
	if s.auth != nil {
		handler = s.auth.Middleware(handler)
	}
	return withCORS(handler)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	var (
		snapshot core.Snapshot
		err      error
	)
	if r.URL.Query().Get("events") == "none" {
		snapshot, err = s.service.SnapshotSummary(r.Context())
	} else {
		snapshot, err = s.service.Snapshot(r.Context())
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) projects(w http.ResponseWriter, r *http.Request) {
	s.writeSnapshotValue(w, r, func(snapshot core.Snapshot) any { return snapshot.Projects })
}

func (s *Server) createProject(w http.ResponseWriter, r *http.Request) {
	project, ok := decodeRequest[core.Project](w, r)
	if !ok {
		return
	}
	created, err := s.service.CreateProject(r.Context(), project)
	writeResult(w, http.StatusCreated, created, err)
}

func (s *Server) updateProject(w http.ResponseWriter, r *http.Request) {
	project, ok := decodeRequest[core.Project](w, r)
	if !ok {
		return
	}
	updated, err := s.service.UpdateProject(r.Context(), r.PathValue("id"), project)
	writeResult(w, http.StatusOK, updated, err)
}

func (s *Server) deleteProject(w http.ResponseWriter, r *http.Request) {
	writeNoContent(w, s.service.DeleteProject(r.Context(), r.PathValue("id")))
}

func (s *Server) projectHealth(w http.ResponseWriter, r *http.Request) {
	health, err := s.service.ProjectHealth(r.Context(), r.PathValue("id"))
	writeResult(w, http.StatusOK, health, err)
}

func (s *Server) targets(w http.ResponseWriter, r *http.Request) {
	s.writeSnapshotValue(w, r, func(snapshot core.Snapshot) any { return snapshot.Targets })
}

func (s *Server) registerTarget(w http.ResponseWriter, r *http.Request) {
	target, ok := decodeRequest[core.TargetConfig](w, r)
	if !ok {
		return
	}
	registered, err := s.service.RegisterTarget(r.Context(), target)
	writeResult(w, http.StatusCreated, registered, err)
}

func (s *Server) updateTarget(w http.ResponseWriter, r *http.Request) {
	target, ok := decodeRequest[core.TargetConfig](w, r)
	if !ok {
		return
	}
	if target.ID == "" {
		target.ID = r.PathValue("id")
	}
	if target.ID != r.PathValue("id") {
		writeError(w, fmt.Errorf("target id mismatch"))
		return
	}
	registered, err := s.service.RegisterTarget(r.Context(), target)
	writeResult(w, http.StatusOK, registered, err)
}

func (s *Server) refreshTargetHealth(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.service.RefreshTargetHealthFor(r.Context(), id)
	snapshot, err := s.service.Snapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	for _, target := range snapshot.Targets {
		if target.ID == id {
			writeJSON(w, http.StatusOK, target)
			return
		}
	}
	writeError(w, eventstore.ErrNotFound)
}

func (s *Server) deleteTarget(w http.ResponseWriter, r *http.Request) {
	writeNoContent(w, s.service.DeleteTarget(r.Context(), r.PathValue("id")))
}

func (s *Server) plugins(w http.ResponseWriter, r *http.Request) {
	s.writeSnapshotValue(w, r, func(snapshot core.Snapshot) any { return snapshot.Plugins })
}

func (s *Server) registerPlugin(w http.ResponseWriter, r *http.Request) {
	plugin, ok := decodeRequest[core.Plugin](w, r)
	if !ok {
		return
	}
	registered, err := s.service.RegisterPlugin(r.Context(), plugin)
	writeResult(w, http.StatusCreated, registered, err)
}

func (s *Server) updatePlugin(w http.ResponseWriter, r *http.Request) {
	plugin, ok := decodeRequest[core.Plugin](w, r)
	if !ok {
		return
	}
	if plugin.ID == "" {
		plugin.ID = r.PathValue("id")
	}
	if plugin.ID != r.PathValue("id") {
		writeError(w, fmt.Errorf("plugin id mismatch"))
		return
	}
	registered, err := s.service.RegisterPlugin(r.Context(), plugin)
	writeResult(w, http.StatusOK, registered, err)
}

func (s *Server) deletePlugin(w http.ResponseWriter, r *http.Request) {
	writeNoContent(w, s.service.DeletePlugin(r.Context(), r.PathValue("id")))
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	afterID := parseInt64(r.URL.Query().Get("after"))
	limit := int(parseInt64(r.URL.Query().Get("limit")))
	events, err := s.service.Events(r.Context(), afterID, limit)
	writeResult(w, http.StatusOK, events, err)
}

func (s *Server) taskEvents(w http.ResponseWriter, r *http.Request) {
	limit := int(parseInt64(r.URL.Query().Get("limit")))
	events, err := s.service.TaskEvents(r.Context(), r.PathValue("id"), limit)
	writeResult(w, http.StatusOK, events, err)
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeRequest[core.CreateTaskRequest](w, r)
	if !ok {
		return
	}
	req, err := orchestrator.NormalizeCreateTaskRequest(req)
	if err != nil {
		writeError(w, err)
		return
	}
	task, err := s.service.CreateTask(r.Context(), req)
	writeResult(w, http.StatusAccepted, task, err)
}

func (s *Server) updateTaskLoopConfig(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeRequest[core.UpdateLoopConfigRequest](w, r)
	if !ok {
		return
	}
	task, err := s.service.UpdateTaskLoopConfig(r.Context(), r.PathValue("id"), req)
	writeResult(w, http.StatusOK, task, err)
}

func (s *Server) assistant(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeRequest[core.AssistantRequest](w, r)
	if !ok {
		return
	}
	response, err := s.service.Ask(r.Context(), req)
	writeResult(w, http.StatusOK, response, err)
}

func (s *Server) lookupTask(w http.ResponseWriter, r *http.Request) {
	task, ok, err := s.service.FindTaskByExternalID(r.Context(), r.URL.Query().Get("source"), r.URL.Query().Get("externalId"))
	if err != nil {
		writeError(w, err)
		return
	}
	if !ok {
		writeError(w, eventstore.ErrNotFound)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) steerTask(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeRequest[core.SteeringRequest](w, r)
	if !ok {
		return
	}
	writeNoContent(w, s.service.SteerTask(r.Context(), r.PathValue("id"), req))
}

func (s *Server) retryTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.service.RetryTask(r.Context(), r.PathValue("id"))
	writeResult(w, http.StatusAccepted, task, err)
}

func (s *Server) cancelTask(w http.ResponseWriter, r *http.Request) {
	writeNoContent(w, s.service.CancelTask(r.Context(), r.PathValue("id")))
}

func (s *Server) clearTask(w http.ResponseWriter, r *http.Request) {
	writeNoContent(w, s.service.ClearTask(r.Context(), r.PathValue("id")))
}

func (s *Server) clearTerminalTasks(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.ClearTerminalTasks(r.Context())
	writeResult(w, http.StatusAccepted, result, err)
}

func (s *Server) cancelWorker(w http.ResponseWriter, r *http.Request) {
	writeNoContent(w, s.service.CancelWorker(r.Context(), r.PathValue("id")))
}

func (s *Server) reviewWorkerChanges(w http.ResponseWriter, r *http.Request) {
	review, err := s.service.ReviewWorkerChanges(r.Context(), r.PathValue("id"))
	writeResult(w, http.StatusOK, review, err)
}

func (s *Server) applyWorkerChanges(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.ApplyWorkerChanges(r.Context(), r.PathValue("id"))
	writeResult(w, http.StatusAccepted, result, err)
}

func (s *Server) applyTaskResult(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.ApplyTaskResult(r.Context(), r.PathValue("id"))
	writeResult(w, http.StatusAccepted, result, err)
}

func (s *Server) recommendApplyPolicy(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.RecommendApplyPolicy(r.Context(), r.PathValue("id"))
	writeResult(w, http.StatusAccepted, result, err)
}

func (s *Server) publishTaskPullRequest(w http.ResponseWriter, r *http.Request) {
	var req core.PublishPullRequestRequest
	if r.Body != nil && r.ContentLength != 0 {
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, err)
			return
		}
	}
	result, err := s.service.PublishTaskPullRequest(r.Context(), r.PathValue("id"), req)
	writeResult(w, http.StatusAccepted, result, err)
}

func (s *Server) watchTaskPullRequests(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeRequest[core.WatchPullRequestsRequest](w, r)
	if !ok {
		return
	}
	result, err := s.service.WatchPullRequests(r.Context(), r.PathValue("id"), req)
	writeResult(w, http.StatusAccepted, result, err)
}

func (s *Server) refreshPullRequest(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.RefreshPullRequest(r.Context(), r.PathValue("id"))
	writeResult(w, http.StatusAccepted, result, err)
}

func (s *Server) startPullRequestBabysitter(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.StartPullRequestBabysitter(r.Context(), r.PathValue("id"))
	writeResult(w, http.StatusAccepted, result, err)
}

func (s *Server) eventStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, errors.New("streaming is not supported"))
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	subID, events := s.service.Subscribe()
	defer s.service.Unsubscribe(subID)

	afterID := parseInt64(r.URL.Query().Get("after"))
	initial, err := s.service.Events(r.Context(), afterID, 1000)
	if err != nil {
		writeSSE(w, "error", map[string]string{"error": err.Error()})
		flusher.Flush()
		return
	}
	for _, event := range initial {
		writeSSE(w, "event", event)
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-events:
			writeSSE(w, "event", event)
			flusher.Flush()
		}
	}
}

func decodeJSON(r *http.Request, out any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(out)
}

func (s *Server) writeSnapshotValue(w http.ResponseWriter, r *http.Request, selectValue func(core.Snapshot) any) {
	snapshot, err := s.service.Snapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, selectValue(snapshot))
}

func decodeRequest[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var req T
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, err)
		return req, false
	}
	return req, true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeResult(w http.ResponseWriter, status int, value any, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, status, value)
}

func writeNoContent(w http.ResponseWriter, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, eventstore.ErrNotFound) {
		status = http.StatusNotFound
	} else if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "constraint failed") {
		status = http.StatusConflict
	} else if strings.Contains(err.Error(), "not allowed") {
		status = http.StatusForbidden
	} else if strings.Contains(err.Error(), "oauth") || strings.Contains(err.Error(), "id token") || strings.Contains(err.Error(), "email is not verified") {
		status = http.StatusUnauthorized
	} else if strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "unknown field") || strings.Contains(err.Error(), "unknown projectId") || strings.Contains(err.Error(), "terminal") || strings.Contains(err.Error(), "multiple unapplied") || strings.Contains(err.Error(), "failed task") || strings.Contains(err.Error(), "failed tasks") {
		status = http.StatusBadRequest
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeSSE(w http.ResponseWriter, eventName string, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		data = []byte(fmt.Sprintf(`{"error":%q}`, err.Error()))
	}
	_, _ = fmt.Fprintf(w, "event: %s\n", eventName)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
}

func parseInt64(value string) int64 {
	if value == "" {
		return 0
	}
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "content-type, mcp-session-id, mcp-method, mcp-name, mcp-protocol-version")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
