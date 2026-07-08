package logging

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
)

type slowRequestLogger struct {
	mu        sync.Mutex
	started   chan struct{}
	release   chan struct{}
	calls     atomic.Int32
	bodies    [][]byte
	enabled   bool
	streaming StreamingLogWriter
}

func (l *slowRequestLogger) LogRequest(url, method string, requestHeaders map[string][]string, body []byte, statusCode int, responseHeaders map[string][]string, response, websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline []byte, apiResponseErrors []*interfaces.ErrorMessage, requestID string, requestTimestamp, apiResponseTimestamp time.Time) error {
	return l.LogRequestWithOptionsAndAllSources(url, method, requestHeaders, body, statusCode, responseHeaders, response, websocketTimeline, nil, apiRequest, nil, apiResponse, nil, apiWebsocketTimeline, nil, apiResponseErrors, false, requestID, requestTimestamp, apiResponseTimestamp)
}

func (l *slowRequestLogger) LogRequestWithOptionsAndAllSources(url, method string, requestHeaders map[string][]string, body []byte, statusCode int, responseHeaders map[string][]string, response, websocketTimeline []byte, websocketTimelineSource *FileBodySource, apiRequest []byte, apiRequestSource *FileBodySource, apiResponse []byte, apiResponseSource *FileBodySource, apiWebsocketTimeline []byte, apiWebsocketTimelineSource *FileBodySource, apiResponseErrors []*interfaces.ErrorMessage, force bool, requestID string, requestTimestamp, apiResponseTimestamp time.Time) error {
	l.calls.Add(1)
	if l.started != nil {
		select {
		case l.started <- struct{}{}:
		default:
		}
	}
	if l.release != nil {
		<-l.release
	}
	l.mu.Lock()
	l.bodies = append(l.bodies, append([]byte(nil), body...))
	l.mu.Unlock()
	cleanupFileBodySources(websocketTimelineSource, apiRequestSource, apiResponseSource, apiWebsocketTimelineSource)
	return nil
}

func (l *slowRequestLogger) LogStreamingRequest(string, string, map[string][]string, []byte, string) (StreamingLogWriter, error) {
	if l.streaming != nil {
		return l.streaming, nil
	}
	return &NoOpStreamingLogWriter{}, nil
}

func (l *slowRequestLogger) IsEnabled() bool {
	return l.enabled
}

func TestAsyncRequestLogger_LogRequestReturnsBeforeInnerFinishes(t *testing.T) {
	inner := &slowRequestLogger{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		enabled: true,
	}
	logger := NewAsyncRequestLogger(inner, 4)
	t.Cleanup(func() {
		close(inner.release)
		_ = logger.Close()
	})

	done := make(chan error, 1)
	go func() {
		done <- logger.LogRequest(
			"/v1/chat/completions",
			"POST",
			map[string][]string{"Content-Type": {"application/json"}},
			[]byte(`{"prompt":"hello"}`),
			200,
			map[string][]string{"Content-Type": {"application/json"}},
			[]byte(`{"ok":true}`),
			nil,
			nil,
			nil,
			nil,
			nil,
			"req-1",
			time.Now(),
			time.Now(),
		)
	}()

	select {
	case errLog := <-done:
		if errLog != nil {
			t.Fatalf("LogRequest returned error: %v", errLog)
		}
	case <-time.After(time.Second):
		t.Fatal("LogRequest blocked on slow inner logger")
	}

	select {
	case <-inner.started:
	case <-time.After(time.Second):
		t.Fatal("inner logger was not started")
	}
}

func TestAsyncRequestLogger_CloseDrainsQueuedJobs(t *testing.T) {
	inner := &slowRequestLogger{enabled: true}
	logger := NewAsyncRequestLogger(inner, 8)

	for range 5 {
		if errLog := logger.LogRequest(
			"/v1/chat/completions",
			"POST",
			nil,
			[]byte(`{"n":1}`),
			200,
			nil,
			[]byte(`{}`),
			nil,
			nil,
			nil,
			nil,
			nil,
			"req",
			time.Now(),
			time.Now(),
		); errLog != nil {
			t.Fatalf("LogRequest: %v", errLog)
		}
	}

	if errClose := logger.Close(); errClose != nil {
		t.Fatalf("Close: %v", errClose)
	}
	if got := inner.calls.Load(); got != 5 {
		t.Fatalf("inner calls = %d, want 5", got)
	}
}

func TestAsyncRequestLogger_QueuedPayloadsAreCopied(t *testing.T) {
	inner := &slowRequestLogger{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		enabled: true,
	}
	logger := NewAsyncRequestLogger(inner, 4)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(inner.release) }) }
	t.Cleanup(func() {
		release()
		_ = logger.Close()
	})
	body := []byte(`{"prompt":"original"}`)
	if errLog := logger.LogRequest(
		"/v1/chat/completions",
		"POST",
		nil,
		body,
		200,
		nil,
		[]byte(`{}`),
		nil,
		nil,
		nil,
		nil,
		nil,
		"req",
		time.Now(),
		time.Now(),
	); errLog != nil {
		t.Fatalf("LogRequest: %v", errLog)
	}

	select {
	case <-inner.started:
	case <-time.After(time.Second):
		t.Fatal("inner logger was not started")
	}

	copy(body, []byte(`{"prompt":"mutated!"}`))
	release()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if inner.calls.Load() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if errClose := logger.Close(); errClose != nil {
		t.Fatalf("Close: %v", errClose)
	}

	inner.mu.Lock()
	defer inner.mu.Unlock()
	if len(inner.bodies) != 1 {
		t.Fatalf("bodies = %d, want 1", len(inner.bodies))
	}
	if string(inner.bodies[0]) != `{"prompt":"original"}` {
		t.Fatalf("body = %q, want original snapshot", inner.bodies[0])
	}
}

func fillAsyncLoggerQueue(t *testing.T, logger *AsyncRequestLogger, inner *slowRequestLogger) {
	t.Helper()
	if errLog := logger.LogRequest("/v1/x", "POST", nil, []byte("body"), 200, nil, []byte("resp"), nil, nil, nil, nil, nil, "req", time.Now(), time.Now()); errLog != nil {
		t.Fatalf("LogRequest fill worker: %v", errLog)
	}
	select {
	case <-inner.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	if errLog := logger.LogRequest("/v1/x", "POST", nil, []byte("body"), 200, nil, []byte("resp"), nil, nil, nil, nil, nil, "req", time.Now(), time.Now()); errLog != nil {
		t.Fatalf("LogRequest fill queue: %v", errLog)
	}
}

func TestAsyncRequestLogger_QueueFullDropsAndCleansSources(t *testing.T) {
	logsDir := t.TempDir()
	source, errSource := NewFileBodySourceInDir(logsDir, "async-drop")
	if errSource != nil {
		t.Fatalf("NewFileBodySourceInDir: %v", errSource)
	}
	if errAppend := source.AppendPart([]byte("payload")); errAppend != nil {
		t.Fatalf("AppendPart: %v", errAppend)
	}
	partPaths := source.Paths()
	if len(partPaths) == 0 {
		t.Fatal("expected part paths")
	}

	inner := &slowRequestLogger{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		enabled: true,
	}
	logger := NewAsyncRequestLogger(inner, 1)
	t.Cleanup(func() {
		close(inner.release)
		_ = logger.Close()
	})

	fillAsyncLoggerQueue(t, logger, inner)

	if errLog := logger.LogRequestWithOptionsAndAllSources(
		"/v1/x",
		"POST",
		nil,
		[]byte("body"),
		200,
		nil,
		[]byte("resp"),
		nil,
		source,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		"req-drop",
		time.Now(),
		time.Now(),
	); errLog != nil {
		t.Fatalf("LogRequest drop path: %v", errLog)
	}

	if logger.Dropped() == 0 {
		t.Fatal("expected dropped counter to increase")
	}
	assertFileBodySourceCleaned(t, partPaths)
}

type slowStreamingLogWriter struct {
	started chan struct{}
	release chan struct{}
	closed  atomic.Bool
}

func (w *slowStreamingLogWriter) WriteChunkAsync([]byte)                     {}
func (w *slowStreamingLogWriter) WriteStatus(int, map[string][]string) error { return nil }
func (w *slowStreamingLogWriter) WriteAPIRequest([]byte) error               { return nil }
func (w *slowStreamingLogWriter) WriteAPIResponse([]byte) error              { return nil }
func (w *slowStreamingLogWriter) WriteAPIWebsocketTimeline([]byte) error     { return nil }
func (w *slowStreamingLogWriter) SetFirstChunkTimestamp(time.Time)           {}
func (w *slowStreamingLogWriter) Close() error {
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

func TestAsyncStreamingLogWriter_CloseReturnsImmediately(t *testing.T) {
	stream := &slowStreamingLogWriter{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	inner := &slowRequestLogger{enabled: true, streaming: stream}
	logger := NewAsyncRequestLogger(inner, 4)
	t.Cleanup(func() {
		close(stream.release)
		_ = logger.Close()
	})

	writer, errLog := logger.LogStreamingRequest("/v1/chat/completions", "POST", nil, []byte(`{}`), "req")
	if errLog != nil {
		t.Fatalf("LogStreamingRequest: %v", errLog)
	}

	done := make(chan error, 1)
	go func() {
		done <- writer.Close()
	}()

	select {
	case errClose := <-done:
		if errClose != nil {
			t.Fatalf("Close: %v", errClose)
		}
	case <-time.After(time.Second):
		t.Fatal("streaming Close blocked on slow inner writer")
	}

	select {
	case <-stream.started:
	case <-time.After(time.Second):
		t.Fatal("inner streaming close was not started")
	}
}

func TestAsyncStreamingLogWriter_RetainsSourcesUntilAsyncClose(t *testing.T) {
	logsDir := t.TempDir()
	source, errSource := NewFileBodySourceInDir(logsDir, "async-stream-source")
	if errSource != nil {
		t.Fatalf("NewFileBodySourceInDir: %v", errSource)
	}
	if errAppend := source.AppendPart([]byte("api-request-part")); errAppend != nil {
		t.Fatalf("AppendPart: %v", errAppend)
	}
	partPaths := source.Paths()

	stream := &slowStreamingLogWriter{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	inner := &slowRequestLogger{enabled: true, streaming: stream}
	logger := NewAsyncRequestLogger(inner, 4)

	writer, errLog := logger.LogStreamingRequest("/v1/chat/completions", "POST", nil, []byte(`{}`), "req")
	if errLog != nil {
		t.Fatalf("LogStreamingRequest: %v", errLog)
	}
	sourceWriter, ok := writer.(interface {
		WriteAPIRequestSource(*FileBodySource) error
		RetainsLogSources() bool
	})
	if !ok {
		t.Fatalf("writer type %T missing source APIs", writer)
	}
	if errWrite := sourceWriter.WriteAPIRequestSource(source); errWrite != nil {
		t.Fatalf("WriteAPIRequestSource: %v", errWrite)
	}
	if !sourceWriter.RetainsLogSources() {
		t.Fatal("expected retained sources")
	}

	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("Close: %v", errClose)
	}

	for _, path := range partPaths {
		if _, errStat := os.Stat(path); errStat != nil {
			t.Fatalf("source cleaned too early: %v", errStat)
		}
	}

	select {
	case <-stream.started:
	case <-time.After(time.Second):
		t.Fatal("inner streaming close was not started")
	}
	close(stream.release)
	if errClose := logger.Close(); errClose != nil {
		t.Fatalf("logger Close: %v", errClose)
	}
	assertFileBodySourceCleaned(t, partPaths)
}

func TestAsyncRequestLogger_QueueFullCleansSourceDirectory(t *testing.T) {
	logsDir := t.TempDir()
	source, errSource := NewFileBodySourceInDir(logsDir, "async-drop-dir")
	if errSource != nil {
		t.Fatalf("NewFileBodySourceInDir: %v", errSource)
	}
	if errAppend := source.AppendPart([]byte("x")); errAppend != nil {
		t.Fatalf("AppendPart: %v", errAppend)
	}
	dir := filepath.Dir(source.Paths()[0])

	inner := &slowRequestLogger{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		enabled: true,
	}
	logger := NewAsyncRequestLogger(inner, 1)
	t.Cleanup(func() {
		close(inner.release)
		_ = logger.Close()
	})

	fillAsyncLoggerQueue(t, logger, inner)
	_ = logger.LogRequestWithOptionsAndAllSources("/v1/x", "POST", nil, []byte("b"), 200, nil, []byte("r"), nil, source, nil, nil, nil, nil, nil, nil, nil, false, "drop", time.Now(), time.Now())
	if _, errStat := os.Stat(dir); !os.IsNotExist(errStat) {
		t.Fatalf("source dir still present after drop: %v", errStat)
	}
}
