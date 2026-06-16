package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"github.com/tidwall/gjson"
)

type fusionFakeExecutor struct {
	provider   string
	failModels map[string]bool

	mu    sync.Mutex
	calls []fusionFakeCall
}

type fusionFakeCall struct {
	Model   string
	Payload []byte
	Stream  bool
	Judge   bool
}

func (e *fusionFakeExecutor) Identifier() string { return e.provider }

func (e *fusionFakeExecutor) Execute(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (coreexecutor.Response, error) {
	isJudge := strings.Contains(string(req.Payload), "BryanFusion judge and synthesizer")
	e.record(req.Model, req.Payload, false, isJudge)
	if e.failModels[strings.ToLower(req.Model)] && !isJudge {
		return coreexecutor.Response{}, errors.New("panel failed")
	}
	content := "panel answer from " + req.Model
	if isJudge {
		content = "final synthesized answer"
	}
	payload := fusionChatCompletion(req.Model, content)
	if strings.EqualFold(opts.SourceFormat.String(), "openai-response") {
		payload = fusionResponsesPayload(req.Model, content)
	}
	return coreexecutor.Response{Payload: payload, Headers: http.Header{"X-Test": []string{"fusion"}}}, nil
}

func (e *fusionFakeExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	isJudge := strings.Contains(string(req.Payload), "BryanFusion judge and synthesizer")
	e.record(req.Model, req.Payload, true, isJudge)
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: []byte(fmt.Sprintf(`{"id":"chunk","object":"chat.completion.chunk","model":%q,"choices":[{"index":0,"delta":{"content":"final stream"},"finish_reason":null}]}`, req.Model))}
	close(chunks)
	return &coreexecutor.StreamResult{Headers: http.Header{"X-Test": []string{"fusion-stream"}}, Chunks: chunks}, nil
}

func (e *fusionFakeExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *fusionFakeExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{Payload: []byte(`{"total_tokens":0}`)}, nil
}

func (e *fusionFakeExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
}

func (e *fusionFakeExecutor) record(model string, payload []byte, stream bool, judge bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, fusionFakeCall{Model: model, Payload: append([]byte(nil), payload...), Stream: stream, Judge: judge})
}

func (e *fusionFakeExecutor) snapshotCalls() []fusionFakeCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]fusionFakeCall, len(e.calls))
	copy(out, e.calls)
	return out
}

func fusionChatCompletion(model, content string) []byte {
	raw, _ := json.Marshal(map[string]any{
		"id":      "chatcmpl-test",
		"object":  "chat.completion",
		"created": int64(1),
		"model":   model,
		"choices": []any{map[string]any{
			"index":         0,
			"finish_reason": "stop",
			"message": map[string]any{
				"role":    "assistant",
				"content": content,
			},
		}},
	})
	return raw
}

func fusionResponsesPayload(model, content string) []byte {
	raw, _ := json.Marshal(map[string]any{
		"id":         "resp-test",
		"object":     "response",
		"created_at": int64(1),
		"model":      model,
		"status":     "completed",
		"output": []any{map[string]any{
			"type":   "message",
			"role":   "assistant",
			"status": "completed",
			"content": []any{map[string]any{
				"type": "output_text",
				"text": content,
			}},
		}},
	})
	return raw
}

func newFusionTestHandler(t *testing.T, cfg sdkconfig.FusionConfig, exec *fusionFakeExecutor, models ...string) *BaseAPIHandler {
	t.Helper()
	manager := coreauth.NewManager(nil, &coreauth.RoundRobinSelector{}, nil)
	manager.RegisterExecutor(exec)
	auth := &coreauth.Auth{ID: "fusion-auth-" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")), Provider: exec.provider}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	infos := make([]*registry.ModelInfo, 0, len(models))
	for _, model := range models {
		infos = append(infos, &registry.ModelInfo{ID: model, Object: "model", Created: 1, OwnedBy: exec.provider})
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, exec.provider, infos)
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	return NewBaseAPIHandlers(&sdkconfig.SDKConfig{Fusion: cfg}, manager)
}

func TestFusionNonStreamFansOutAndJudges(t *testing.T) {
	panel := []string{"fusion-p1", "fusion-p2", "fusion-p3", "fusion-p4"}
	judge := "fusion-judge"
	cfg := sdkconfig.FusionConfig{Enabled: true, Model: "BryanFusion", Panel: panel, Judge: judge, MinSuccesses: 3, SimulatedStreaming: true}
	exec := &fusionFakeExecutor{provider: "fusion-test-provider"}
	h := newFusionTestHandler(t, cfg, exec, append(panel, judge)...)

	raw := []byte(`{"model":"BryanFusion","messages":[{"role":"system","content":"Droid custom system prompt"},{"role":"user","content":"answer"}],"tools":[{"type":"function","function":{"name":"noop"}}],"tool_choice":"auto"}`)
	resp, _, errMsg := h.ExecuteWithAuthManager(context.Background(), "openai", "BryanFusion", raw, "")
	if errMsg != nil {
		t.Fatalf("ExecuteWithAuthManager error: %v", errMsg.Error)
	}
	if got := gjson.GetBytes(resp, "model").String(); got != "BryanFusion" {
		t.Fatalf("response model = %q, want BryanFusion: %s", got, string(resp))
	}
	if got := gjson.GetBytes(resp, "choices.0.message.content").String(); got != "final synthesized answer" {
		t.Fatalf("response content = %q", got)
	}

	calls := exec.snapshotCalls()
	if len(calls) != 5 {
		t.Fatalf("calls = %d, want 5", len(calls))
	}
	panelCalls := 0
	judgeCalls := 0
	var judgePayload []byte
	for _, call := range calls {
		if call.Judge {
			judgeCalls++
			judgePayload = call.Payload
			continue
		}
		panelCalls++
		if got := gjson.GetBytes(call.Payload, "messages.0.role").String(); got != "system" {
			t.Fatalf("panel first role = %q, payload=%s", got, string(call.Payload))
		}
		if !strings.Contains(gjson.GetBytes(call.Payload, "messages.0.content").String(), "independent BryanFusion panel analyst") {
			t.Fatalf("panel system prompt missing, payload=%s", string(call.Payload))
		}
		if gjson.GetBytes(call.Payload, "tools").Exists() || gjson.GetBytes(call.Payload, "tool_choice").Exists() {
			t.Fatalf("panel tool fields not stripped, payload=%s", string(call.Payload))
		}
		if got := gjson.GetBytes(call.Payload, "messages.1.content").String(); !strings.Contains(got, "Droid custom system prompt") {
			t.Fatalf("panel request system prompt missing, payload=%s", string(call.Payload))
		}
	}
	if panelCalls != 4 || judgeCalls != 1 {
		t.Fatalf("panelCalls=%d judgeCalls=%d, want 4/1", panelCalls, judgeCalls)
	}
	if got := gjson.GetBytes(judgePayload, "messages.0.content").String(); !strings.Contains(got, "Droid custom system prompt") {
		t.Fatalf("judge request system prompt missing, payload=%s", string(judgePayload))
	}
}

func TestFusionResponsesPreservesRequestInstructions(t *testing.T) {
	panel := []string{"fusion-resp-p1", "fusion-resp-p2"}
	judge := "fusion-resp-judge"
	cfg := sdkconfig.FusionConfig{Enabled: true, Model: "BryanFusion", Panel: panel, Judge: judge, MinSuccesses: 2, SimulatedStreaming: true}
	exec := &fusionFakeExecutor{provider: "fusion-resp-provider"}
	h := newFusionTestHandler(t, cfg, exec, append(panel, judge)...)

	raw := []byte(`{"model":"BryanFusion","instructions":"Droid responses system prompt","input":"answer"}`)
	resp, _, errMsg := h.ExecuteWithAuthManager(context.Background(), "openai-response", "BryanFusion", raw, "")
	if errMsg != nil {
		t.Fatalf("ExecuteWithAuthManager error: %v", errMsg.Error)
	}
	if got := gjson.GetBytes(resp, "model").String(); got != "BryanFusion" {
		t.Fatalf("response model = %q, want BryanFusion: %s", got, string(resp))
	}

	calls := exec.snapshotCalls()
	if len(calls) != 3 {
		t.Fatalf("calls = %d, want 3", len(calls))
	}
	for _, call := range calls {
		instructions := gjson.GetBytes(call.Payload, "instructions").String()
		if !strings.Contains(instructions, "Droid responses system prompt") {
			t.Fatalf("request instructions missing, payload=%s", string(call.Payload))
		}
		if call.Judge && !strings.Contains(instructions, "BryanFusion judge and synthesizer") {
			t.Fatalf("judge prompt missing, payload=%s", string(call.Payload))
		}
		if !call.Judge && !strings.Contains(instructions, "independent BryanFusion panel analyst") {
			t.Fatalf("panel prompt missing, payload=%s", string(call.Payload))
		}
	}
}

func TestFusionPartialPanelFailureThreshold(t *testing.T) {
	panel := []string{"fusion-fail-p1", "fusion-fail-p2", "fusion-fail-p3"}
	judge := "fusion-fail-judge"
	cfg := sdkconfig.FusionConfig{Enabled: true, Model: "BryanFusion", Panel: panel, Judge: judge, MinSuccesses: 2, SimulatedStreaming: true}
	exec := &fusionFakeExecutor{provider: "fusion-fail-provider", failModels: map[string]bool{"fusion-fail-p1": true, "fusion-fail-p2": true}}
	h := newFusionTestHandler(t, cfg, exec, append(panel, judge)...)

	_, _, errMsg := h.ExecuteWithAuthManager(context.Background(), "openai", "BryanFusion", []byte(`{"model":"BryanFusion","messages":[{"role":"user","content":"answer"}]}`), "")
	if errMsg == nil {
		t.Fatal("expected fusion threshold error")
	}
	if errMsg.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", errMsg.StatusCode)
	}
}

func TestFusionRecursionGuard(t *testing.T) {
	cfg := sdkconfig.FusionConfig{Enabled: true, Model: "BryanFusion", Panel: []string{"BryanFusion"}, Judge: "fusion-rec-judge", MinSuccesses: 1}
	exec := &fusionFakeExecutor{provider: "fusion-rec-provider"}
	h := newFusionTestHandler(t, cfg, exec, "fusion-rec-judge")

	_, _, errMsg := h.ExecuteWithAuthManager(context.Background(), "openai", "BryanFusion", []byte(`{"model":"BryanFusion","messages":[{"role":"user","content":"answer"}]}`), "")
	if errMsg == nil {
		t.Fatal("expected recursion guard error")
	}
}

func TestFusionSimulatedStreamingUsesJudgeStream(t *testing.T) {
	panel := []string{"fusion-stream-p1", "fusion-stream-p2"}
	judge := "fusion-stream-judge"
	cfg := sdkconfig.FusionConfig{Enabled: true, Model: "BryanFusion", Panel: panel, Judge: judge, MinSuccesses: 2, SimulatedStreaming: true}
	exec := &fusionFakeExecutor{provider: "fusion-stream-provider"}
	h := newFusionTestHandler(t, cfg, exec, append(panel, judge)...)

	data, _, errs := h.ExecuteStreamWithAuthManager(context.Background(), "openai", "BryanFusion", []byte(`{"model":"BryanFusion","messages":[{"role":"user","content":"answer"}],"stream":true}`), "")
	select {
	case errMsg := <-errs:
		if errMsg != nil {
			t.Fatalf("stream error: %v", errMsg.Error)
		}
	default:
	}
	chunk, ok := <-data
	if !ok {
		t.Fatal("expected stream chunk")
	}
	if got := gjson.GetBytes(chunk, "model").String(); got != "BryanFusion" {
		t.Fatalf("stream chunk model = %q, want BryanFusion: %s", got, string(chunk))
	}
}

func TestFusionModelForListingRequiresTargets(t *testing.T) {
	panel := []string{"fusion-list-p1", "fusion-list-p2"}
	judge := "fusion-list-judge"
	cfg := sdkconfig.FusionConfig{Enabled: true, Model: "BryanFusion", Panel: panel, Judge: judge, MinSuccesses: 2}
	exec := &fusionFakeExecutor{provider: "fusion-list-provider"}
	h := newFusionTestHandler(t, cfg, exec, append(panel, judge)...)

	model := h.FusionModelForListing()
	if model == nil {
		t.Fatal("expected fusion model listing")
	}
	if got := model["id"]; got != "BryanFusion" {
		t.Fatalf("model id = %#v, want BryanFusion", got)
	}
}
