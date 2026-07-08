package middleware

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
)

func TestExtractRequestBodyPrefersOverride(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	wrapper := &ResponseWriterWrapper{
		requestInfo: &RequestInfo{Body: []byte("original-body")},
	}

	body := wrapper.extractRequestBody(c)
	if string(body) != "original-body" {
		t.Fatalf("request body = %q, want %q", string(body), "original-body")
	}

	c.Set(requestBodyOverrideContextKey, []byte("override-body"))
	body = wrapper.extractRequestBody(c)
	if string(body) != "override-body" {
		t.Fatalf("request body = %q, want %q", string(body), "override-body")
	}
}

func TestExtractRequestBodySupportsStringOverride(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	wrapper := &ResponseWriterWrapper{body: &bytes.Buffer{}}
	c.Set(requestBodyOverrideContextKey, "override-as-string")

	body := wrapper.extractRequestBody(c)
	if string(body) != "override-as-string" {
		t.Fatalf("request body = %q, want %q", string(body), "override-as-string")
	}
}

func TestExtractResponseBodyPrefersOverride(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	wrapper := &ResponseWriterWrapper{body: &bytes.Buffer{}}
	wrapper.body.WriteString("original-response")

	body := wrapper.extractResponseBody(c)
	if string(body) != "original-response" {
		t.Fatalf("response body = %q, want %q", string(body), "original-response")
	}

	c.Set(responseBodyOverrideContextKey, []byte("override-response"))
	body = wrapper.extractResponseBody(c)
	if string(body) != "override-response" {
		t.Fatalf("response body = %q, want %q", string(body), "override-response")
	}

	body[0] = 'X'
	if got := wrapper.extractResponseBody(c); string(got) != "override-response" {
		t.Fatalf("response override should be cloned, got %q", string(got))
	}
}

func TestExtractResponseBodySupportsStringOverride(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	wrapper := &ResponseWriterWrapper{}
	c.Set(responseBodyOverrideContextKey, "override-response-as-string")

	body := wrapper.extractResponseBody(c)
	if string(body) != "override-response-as-string" {
		t.Fatalf("response body = %q, want %q", string(body), "override-response-as-string")
	}
}

func TestExtractBodyOverrideClonesBytes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	override := []byte("body-override")
	c.Set(requestBodyOverrideContextKey, override)

	body := extractBodyOverride(c, requestBodyOverrideContextKey)
	if !bytes.Equal(body, override) {
		t.Fatalf("body override = %q, want %q", string(body), string(override))
	}

	body[0] = 'X'
	if !bytes.Equal(override, []byte("body-override")) {
		t.Fatalf("override mutated: %q", string(override))
	}
}

func TestExtractWebsocketTimelineUsesOverride(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	wrapper := &ResponseWriterWrapper{}
	if got := wrapper.extractWebsocketTimeline(c); got != nil {
		t.Fatalf("expected nil websocket timeline, got %q", string(got))
	}

	c.Set(websocketTimelineOverrideContextKey, []byte("timeline"))
	body := wrapper.extractWebsocketTimeline(c)
	if string(body) != "timeline" {
		t.Fatalf("websocket timeline = %q, want %q", string(body), "timeline")
	}
}

func TestFinalizeStreamingWritesAPIWebsocketTimeline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	streamWriter := &testStreamingLogWriter{}
	wrapper := &ResponseWriterWrapper{
		ResponseWriter: c.Writer,
		logger:         &testRequestLogger{enabled: true},
		requestInfo: &RequestInfo{
			URL:       "/v1/responses",
			Method:    "POST",
			Headers:   map[string][]string{"Content-Type": {"application/json"}},
			RequestID: "req-1",
			Timestamp: time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC),
		},
		isStreaming:  true,
		streamWriter: streamWriter,
	}

	c.Set("API_WEBSOCKET_TIMELINE", []byte("Timestamp: 2026-04-01T12:00:00Z\nEvent: api.websocket.request\n{}"))

	if err := wrapper.Finalize(c); err != nil {
		t.Fatalf("Finalize error: %v", err)
	}
	if string(streamWriter.apiWebsocketTimeline) != "Timestamp: 2026-04-01T12:00:00Z\nEvent: api.websocket.request\n{}" {
		t.Fatalf("stream writer websocket timeline = %q", string(streamWriter.apiWebsocketTimeline))
	}
	if !streamWriter.closed {
		t.Fatal("expected stream writer to be closed")
	}
}

type testRequestLogger struct {
	enabled bool
}

func (l *testRequestLogger) LogRequest(string, string, map[string][]string, []byte, int, map[string][]string, []byte, []byte, []byte, []byte, []byte, []*interfaces.ErrorMessage, string, time.Time, time.Time) error {
	return nil
}

func (l *testRequestLogger) LogStreamingRequest(string, string, map[string][]string, []byte, string) (logging.StreamingLogWriter, error) {
	return &testStreamingLogWriter{}, nil
}

func (l *testRequestLogger) IsEnabled() bool {
	return l.enabled
}

type testStreamingLogWriter struct {
	apiWebsocketTimeline []byte
	closed               bool
}

func (w *testStreamingLogWriter) WriteChunkAsync([]byte) {}

func (w *testStreamingLogWriter) WriteStatus(int, map[string][]string) error {
	return nil
}

func (w *testStreamingLogWriter) WriteAPIRequest([]byte) error {
	return nil
}

func (w *testStreamingLogWriter) WriteAPIResponse([]byte) error {
	return nil
}

func (w *testStreamingLogWriter) WriteAPIWebsocketTimeline(apiWebsocketTimeline []byte) error {
	w.apiWebsocketTimeline = bytes.Clone(apiWebsocketTimeline)
	return nil
}

func (w *testStreamingLogWriter) SetFirstChunkTimestamp(time.Time) {}

func (w *testStreamingLogWriter) Close() error {
	w.closed = true
	return nil
}

type blockingStreamingLogWriter struct {
	started          chan struct{}
	release          chan struct{}
	apiRequestSource *logging.FileBodySource
	closed           atomic.Bool
}

func (w *blockingStreamingLogWriter) WriteChunkAsync([]byte)                     {}
func (w *blockingStreamingLogWriter) WriteStatus(int, map[string][]string) error { return nil }
func (w *blockingStreamingLogWriter) WriteAPIRequest([]byte) error               { return nil }
func (w *blockingStreamingLogWriter) WriteAPIResponse([]byte) error              { return nil }
func (w *blockingStreamingLogWriter) WriteAPIWebsocketTimeline([]byte) error     { return nil }
func (w *blockingStreamingLogWriter) WriteAPIRequestSource(source *logging.FileBodySource) error {
	w.apiRequestSource = source
	return nil
}
func (w *blockingStreamingLogWriter) WriteAPIResponseSource(*logging.FileBodySource) error {
	return nil
}
func (w *blockingStreamingLogWriter) WriteAPIWebsocketTimelineSource(*logging.FileBodySource) error {
	return nil
}
func (w *blockingStreamingLogWriter) SetFirstChunkTimestamp(time.Time) {}
func (w *blockingStreamingLogWriter) RetainsLogSources() bool          { return true }
func (w *blockingStreamingLogWriter) Close() error {
	if w.started != nil {
		select {
		case w.started <- struct{}{}:
		default:
		}
	}
	if w.release != nil {
		<-w.release
	}
	w.closed.Store(true)
	return nil
}

type asyncStreamingRequestLogger struct {
	enabled bool
	writer  logging.StreamingLogWriter
}

func (l *asyncStreamingRequestLogger) LogRequest(string, string, map[string][]string, []byte, int, map[string][]string, []byte, []byte, []byte, []byte, []byte, []*interfaces.ErrorMessage, string, time.Time, time.Time) error {
	return nil
}

func (l *asyncStreamingRequestLogger) LogStreamingRequest(string, string, map[string][]string, []byte, string) (logging.StreamingLogWriter, error) {
	return l.writer, nil
}

func (l *asyncStreamingRequestLogger) IsEnabled() bool {
	return l.enabled
}

func TestFinalizeStreamingReturnsWithoutWaitingForAsyncClose(t *testing.T) {
	gin.SetMode(gin.TestMode)

	blockingWriter := &blockingStreamingLogWriter{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	asyncLogger := logging.NewAsyncRequestLogger(&asyncStreamingRequestLogger{
		enabled: true,
		writer:  blockingWriter,
	}, 4)
	t.Cleanup(func() {
		close(blockingWriter.release)
		_ = asyncLogger.Close()
	})

	streamWriter, errLog := asyncLogger.LogStreamingRequest("/v1/chat/completions", http.MethodPost, nil, []byte(`{"stream":true}`), "stream-async")
	if errLog != nil {
		t.Fatalf("LogStreamingRequest: %v", errLog)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	wrapper := &ResponseWriterWrapper{
		ResponseWriter: c.Writer,
		logger:         asyncLogger,
		requestInfo: &RequestInfo{
			URL:       "/v1/chat/completions",
			Method:    http.MethodPost,
			Headers:   map[string][]string{"Content-Type": {"application/json"}},
			Body:      []byte(`{"stream":true}`),
			RequestID: "stream-async",
			Timestamp: time.Now(),
		},
		isStreaming:  true,
		streamWriter: streamWriter,
	}

	done := make(chan error, 1)
	go func() {
		done <- wrapper.Finalize(c)
	}()

	select {
	case errFinalize := <-done:
		if errFinalize != nil {
			t.Fatalf("Finalize: %v", errFinalize)
		}
	case <-time.After(time.Second):
		t.Fatal("Finalize blocked waiting for async stream close")
	}

	select {
	case <-blockingWriter.started:
	case <-time.After(time.Second):
		t.Fatal("async stream close job did not start")
	}
}

func TestFinalizeStreamingRetainsSourcesForAsyncClose(t *testing.T) {
	gin.SetMode(gin.TestMode)

	logsDir := t.TempDir()
	source, errSource := logging.NewFileBodySourceInDir(logsDir, "middleware-async-source")
	if errSource != nil {
		t.Fatalf("NewFileBodySourceInDir: %v", errSource)
	}
	if errAppend := source.AppendPart([]byte("api-request")); errAppend != nil {
		t.Fatalf("AppendPart: %v", errAppend)
	}
	partPaths := source.Paths()

	blockingWriter := &blockingStreamingLogWriter{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	asyncLogger := logging.NewAsyncRequestLogger(&asyncStreamingRequestLogger{
		enabled: true,
		writer:  blockingWriter,
	}, 4)

	streamWriter, errLog := asyncLogger.LogStreamingRequest("/v1/chat/completions", http.MethodPost, nil, []byte(`{"stream":true}`), "stream-source")
	if errLog != nil {
		t.Fatalf("LogStreamingRequest: %v", errLog)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Set(logging.APIRequestSourceContextKey, source)

	wrapper := &ResponseWriterWrapper{
		ResponseWriter: c.Writer,
		logger:         asyncLogger,
		requestInfo: &RequestInfo{
			URL:       "/v1/chat/completions",
			Method:    http.MethodPost,
			Headers:   map[string][]string{"Content-Type": {"application/json"}},
			Body:      []byte(`{"stream":true}`),
			RequestID: "stream-source",
			Timestamp: time.Now(),
		},
		isStreaming:  true,
		streamWriter: streamWriter,
	}

	if errFinalize := wrapper.Finalize(c); errFinalize != nil {
		t.Fatalf("Finalize: %v", errFinalize)
	}

	// Source must remain until the async close job finishes.
	for _, path := range partPaths {
		if _, errStat := os.Stat(path); errStat != nil {
			t.Fatalf("source cleaned too early: %v", errStat)
		}
	}
	if blockingWriter.apiRequestSource != source {
		t.Fatalf("api request source not transferred to stream writer")
	}

	select {
	case <-blockingWriter.started:
	case <-time.After(time.Second):
		t.Fatal("async stream close job did not start")
	}
	close(blockingWriter.release)
	if errClose := asyncLogger.Close(); errClose != nil {
		t.Fatalf("async logger Close: %v", errClose)
	}
	for _, path := range partPaths {
		if _, errStat := os.Stat(path); !os.IsNotExist(errStat) {
			t.Fatalf("source still present after async close: %v", errStat)
		}
	}
}
