package logging

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
)

// DefaultRequestLogQueueSize is the bounded async audit-log queue capacity.
// Full queues drop jobs instead of blocking the request path.
const DefaultRequestLogQueueSize = 1024

const requestLogDropWarnInterval = time.Minute

// AsyncRequestLogger wraps a RequestLogger and performs final log assembly off the request path.
type AsyncRequestLogger struct {
	inner RequestLogger

	queue chan func()
	stop  chan struct{}
	wg    sync.WaitGroup

	closed       atomic.Bool
	closeOnce    sync.Once
	dropped      atomic.Uint64
	lastDropWarn atomic.Int64
}

// NewAsyncRequestLogger wraps inner with a bounded async worker.
// queueSize <= 0 uses DefaultRequestLogQueueSize.
func NewAsyncRequestLogger(inner RequestLogger, queueSize int) *AsyncRequestLogger {
	if queueSize <= 0 {
		queueSize = DefaultRequestLogQueueSize
	}
	logger := &AsyncRequestLogger{
		inner: inner,
		queue: make(chan func(), queueSize),
		stop:  make(chan struct{}),
	}
	logger.wg.Add(1)
	go logger.run()
	return logger
}

// IsEnabled reports whether the wrapped logger is enabled.
func (l *AsyncRequestLogger) IsEnabled() bool {
	if l == nil || l.inner == nil {
		return false
	}
	return l.inner.IsEnabled()
}

// SetEnabled forwards enablement to the wrapped logger when supported.
func (l *AsyncRequestLogger) SetEnabled(enabled bool) {
	if l == nil || l.inner == nil {
		return
	}
	if setter, ok := l.inner.(interface{ SetEnabled(bool) }); ok {
		setter.SetEnabled(enabled)
	}
}

// SetHomeEnabled forwards home forwarding to the wrapped logger when supported.
func (l *AsyncRequestLogger) SetHomeEnabled(enabled bool) {
	if l == nil || l.inner == nil {
		return
	}
	if setter, ok := l.inner.(interface{ SetHomeEnabled(bool) }); ok {
		setter.SetHomeEnabled(enabled)
	}
}

// SetErrorLogsMaxFiles forwards error-log retention to the wrapped logger when supported.
func (l *AsyncRequestLogger) SetErrorLogsMaxFiles(maxFiles int) {
	if l == nil || l.inner == nil {
		return
	}
	if setter, ok := l.inner.(interface{ SetErrorLogsMaxFiles(int) }); ok {
		setter.SetErrorLogsMaxFiles(maxFiles)
	}
}

// NewFileBodySource creates a temp-backed source via the wrapped logger when supported.
func (l *AsyncRequestLogger) NewFileBodySource(prefix string) (*FileBodySource, error) {
	if l == nil || l.inner == nil {
		return nil, nil
	}
	if factory, ok := l.inner.(interface {
		NewFileBodySource(string) (*FileBodySource, error)
	}); ok {
		return factory.NewFileBodySource(prefix)
	}
	return nil, nil
}

// LogRequest enqueues a non-streaming request log and returns immediately.
func (l *AsyncRequestLogger) LogRequest(url, method string, requestHeaders map[string][]string, body []byte, statusCode int, responseHeaders map[string][]string, response, websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline []byte, apiResponseErrors []*interfaces.ErrorMessage, requestID string, requestTimestamp, apiResponseTimestamp time.Time) error {
	return l.LogRequestWithOptionsAndAllSources(
		url,
		method,
		requestHeaders,
		body,
		statusCode,
		responseHeaders,
		response,
		websocketTimeline,
		nil,
		apiRequest,
		nil,
		apiResponse,
		nil,
		apiWebsocketTimeline,
		nil,
		apiResponseErrors,
		false,
		requestID,
		requestTimestamp,
		apiResponseTimestamp,
	)
}

// LogRequestWithOptions enqueues a non-streaming request log with force support.
func (l *AsyncRequestLogger) LogRequestWithOptions(url, method string, requestHeaders map[string][]string, body []byte, statusCode int, responseHeaders map[string][]string, response, websocketTimeline, apiRequest, apiResponse, apiWebsocketTimeline []byte, apiResponseErrors []*interfaces.ErrorMessage, force bool, requestID string, requestTimestamp, apiResponseTimestamp time.Time) error {
	return l.LogRequestWithOptionsAndAllSources(
		url,
		method,
		requestHeaders,
		body,
		statusCode,
		responseHeaders,
		response,
		websocketTimeline,
		nil,
		apiRequest,
		nil,
		apiResponse,
		nil,
		apiWebsocketTimeline,
		nil,
		apiResponseErrors,
		force,
		requestID,
		requestTimestamp,
		apiResponseTimestamp,
	)
}

// LogRequestWithOptionsAndSources enqueues a request log with selected file-backed sections.
func (l *AsyncRequestLogger) LogRequestWithOptionsAndSources(url, method string, requestHeaders map[string][]string, body []byte, statusCode int, responseHeaders map[string][]string, response, websocketTimeline []byte, websocketTimelineSource *FileBodySource, apiRequest, apiResponse, apiWebsocketTimeline []byte, apiWebsocketTimelineSource *FileBodySource, apiResponseErrors []*interfaces.ErrorMessage, force bool, requestID string, requestTimestamp, apiResponseTimestamp time.Time) error {
	return l.LogRequestWithOptionsAndAllSources(
		url,
		method,
		requestHeaders,
		body,
		statusCode,
		responseHeaders,
		response,
		websocketTimeline,
		websocketTimelineSource,
		apiRequest,
		nil,
		apiResponse,
		nil,
		apiWebsocketTimeline,
		apiWebsocketTimelineSource,
		apiResponseErrors,
		force,
		requestID,
		requestTimestamp,
		apiResponseTimestamp,
	)
}

// LogRequestWithOptionsAndAllSources deep-copies in-memory payloads, transfers source ownership, and returns immediately.
func (l *AsyncRequestLogger) LogRequestWithOptionsAndAllSources(url, method string, requestHeaders map[string][]string, body []byte, statusCode int, responseHeaders map[string][]string, response, websocketTimeline []byte, websocketTimelineSource *FileBodySource, apiRequest []byte, apiRequestSource *FileBodySource, apiResponse []byte, apiResponseSource *FileBodySource, apiWebsocketTimeline []byte, apiWebsocketTimelineSource *FileBodySource, apiResponseErrors []*interfaces.ErrorMessage, force bool, requestID string, requestTimestamp, apiResponseTimestamp time.Time) error {
	if l == nil || l.inner == nil {
		cleanupFileBodySources(websocketTimelineSource, apiRequestSource, apiResponseSource, apiWebsocketTimelineSource)
		return nil
	}

	urlCopy := url
	methodCopy := method
	requestIDCopy := requestID
	requestHeadersCopy := cloneHeaders(requestHeaders)
	responseHeadersCopy := cloneHeaders(responseHeaders)
	bodyCopy := cloneBytes(body)
	responseCopy := cloneBytes(response)
	websocketTimelineCopy := cloneBytes(websocketTimeline)
	apiRequestCopy := cloneBytes(apiRequest)
	apiResponseCopy := cloneBytes(apiResponse)
	apiWebsocketTimelineCopy := cloneBytes(apiWebsocketTimeline)
	apiResponseErrorsCopy := cloneErrorMessages(apiResponseErrors)

	sources := []*FileBodySource{websocketTimelineSource, apiRequestSource, apiResponseSource, apiWebsocketTimelineSource}
	job := func() {
		if loggerWithAllSources, ok := l.inner.(interface {
			LogRequestWithOptionsAndAllSources(string, string, map[string][]string, []byte, int, map[string][]string, []byte, []byte, *FileBodySource, []byte, *FileBodySource, []byte, *FileBodySource, []byte, *FileBodySource, []*interfaces.ErrorMessage, bool, string, time.Time, time.Time) error
		}); ok {
			if errLog := loggerWithAllSources.LogRequestWithOptionsAndAllSources(
				urlCopy,
				methodCopy,
				requestHeadersCopy,
				bodyCopy,
				statusCode,
				responseHeadersCopy,
				responseCopy,
				websocketTimelineCopy,
				websocketTimelineSource,
				apiRequestCopy,
				apiRequestSource,
				apiResponseCopy,
				apiResponseSource,
				apiWebsocketTimelineCopy,
				apiWebsocketTimelineSource,
				apiResponseErrorsCopy,
				force,
				requestIDCopy,
				requestTimestamp,
				apiResponseTimestamp,
			); errLog != nil {
				log.WithError(errLog).Warn("async request log failed")
			}
			return
		}

		if loggerWithSources, ok := l.inner.(interface {
			LogRequestWithOptionsAndSources(string, string, map[string][]string, []byte, int, map[string][]string, []byte, []byte, *FileBodySource, []byte, []byte, []byte, *FileBodySource, []*interfaces.ErrorMessage, bool, string, time.Time, time.Time) error
		}); ok {
			mergedAPIRequest, errMerge := mergeSourceBytes(apiRequestCopy, apiRequestSource)
			if errMerge != nil {
				cleanupFileBodySources(sources...)
				log.WithError(errMerge).Warn("async request log merge failed")
				return
			}
			mergedAPIResponse, errMerge := mergeSourceBytes(apiResponseCopy, apiResponseSource)
			if errMerge != nil {
				cleanupFileBodySources(websocketTimelineSource, apiWebsocketTimelineSource)
				log.WithError(errMerge).Warn("async request log merge failed")
				return
			}
			if errLog := loggerWithSources.LogRequestWithOptionsAndSources(
				urlCopy,
				methodCopy,
				requestHeadersCopy,
				bodyCopy,
				statusCode,
				responseHeadersCopy,
				responseCopy,
				websocketTimelineCopy,
				websocketTimelineSource,
				mergedAPIRequest,
				mergedAPIResponse,
				apiWebsocketTimelineCopy,
				apiWebsocketTimelineSource,
				apiResponseErrorsCopy,
				force,
				requestIDCopy,
				requestTimestamp,
				apiResponseTimestamp,
			); errLog != nil {
				log.WithError(errLog).Warn("async request log failed")
			}
			return
		}

		if loggerWithOptions, ok := l.inner.(interface {
			LogRequestWithOptions(string, string, map[string][]string, []byte, int, map[string][]string, []byte, []byte, []byte, []byte, []byte, []*interfaces.ErrorMessage, bool, string, time.Time, time.Time) error
		}); ok {
			mergedWebsocketTimeline, errMerge := mergeSourceBytes(websocketTimelineCopy, websocketTimelineSource)
			if errMerge != nil {
				cleanupFileBodySources(apiRequestSource, apiResponseSource, apiWebsocketTimelineSource)
				log.WithError(errMerge).Warn("async request log merge failed")
				return
			}
			mergedAPIRequest, errMerge := mergeSourceBytes(apiRequestCopy, apiRequestSource)
			if errMerge != nil {
				cleanupFileBodySources(apiResponseSource, apiWebsocketTimelineSource)
				log.WithError(errMerge).Warn("async request log merge failed")
				return
			}
			mergedAPIResponse, errMerge := mergeSourceBytes(apiResponseCopy, apiResponseSource)
			if errMerge != nil {
				cleanupFileBodySources(apiWebsocketTimelineSource)
				log.WithError(errMerge).Warn("async request log merge failed")
				return
			}
			mergedAPIWebsocketTimeline, errMerge := mergeSourceBytes(apiWebsocketTimelineCopy, apiWebsocketTimelineSource)
			if errMerge != nil {
				log.WithError(errMerge).Warn("async request log merge failed")
				return
			}
			if errLog := loggerWithOptions.LogRequestWithOptions(
				urlCopy,
				methodCopy,
				requestHeadersCopy,
				bodyCopy,
				statusCode,
				responseHeadersCopy,
				responseCopy,
				mergedWebsocketTimeline,
				mergedAPIRequest,
				mergedAPIResponse,
				mergedAPIWebsocketTimeline,
				apiResponseErrorsCopy,
				force,
				requestIDCopy,
				requestTimestamp,
				apiResponseTimestamp,
			); errLog != nil {
				log.WithError(errLog).Warn("async request log failed")
			}
			return
		}

		mergedWebsocketTimeline, errMerge := mergeSourceBytes(websocketTimelineCopy, websocketTimelineSource)
		if errMerge != nil {
			cleanupFileBodySources(apiRequestSource, apiResponseSource, apiWebsocketTimelineSource)
			log.WithError(errMerge).Warn("async request log merge failed")
			return
		}
		mergedAPIRequest, errMerge := mergeSourceBytes(apiRequestCopy, apiRequestSource)
		if errMerge != nil {
			cleanupFileBodySources(apiResponseSource, apiWebsocketTimelineSource)
			log.WithError(errMerge).Warn("async request log merge failed")
			return
		}
		mergedAPIResponse, errMerge := mergeSourceBytes(apiResponseCopy, apiResponseSource)
		if errMerge != nil {
			cleanupFileBodySources(apiWebsocketTimelineSource)
			log.WithError(errMerge).Warn("async request log merge failed")
			return
		}
		mergedAPIWebsocketTimeline, errMerge := mergeSourceBytes(apiWebsocketTimelineCopy, apiWebsocketTimelineSource)
		if errMerge != nil {
			log.WithError(errMerge).Warn("async request log merge failed")
			return
		}
		if errLog := l.inner.LogRequest(
			urlCopy,
			methodCopy,
			requestHeadersCopy,
			bodyCopy,
			statusCode,
			responseHeadersCopy,
			responseCopy,
			mergedWebsocketTimeline,
			mergedAPIRequest,
			mergedAPIResponse,
			mergedAPIWebsocketTimeline,
			apiResponseErrorsCopy,
			requestIDCopy,
			requestTimestamp,
			apiResponseTimestamp,
		); errLog != nil {
			log.WithError(errLog).Warn("async request log failed")
		}
	}

	l.enqueue(job, func() {
		cleanupFileBodySources(sources...)
	})
	return nil
}

// LogStreamingRequest returns an async wrapper around the inner streaming writer.
func (l *AsyncRequestLogger) LogStreamingRequest(url, method string, headers map[string][]string, body []byte, requestID string) (StreamingLogWriter, error) {
	if l == nil || l.inner == nil {
		return &NoOpStreamingLogWriter{}, nil
	}
	// Headers/body are copied by the inner logger implementations.
	writer, err := l.inner.LogStreamingRequest(url, method, headers, body, requestID)
	if err != nil {
		return nil, err
	}
	return newAsyncStreamingLogWriter(l, writer), nil
}

// Close drains queued jobs and stops the worker.
func (l *AsyncRequestLogger) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		l.closed.Store(true)
		close(l.stop)
		l.wg.Wait()
	})
	if closer, ok := l.inner.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// Dropped returns how many audit logs were dropped due to a full queue.
func (l *AsyncRequestLogger) Dropped() uint64 {
	if l == nil {
		return 0
	}
	return l.dropped.Load()
}

func (l *AsyncRequestLogger) run() {
	defer l.wg.Done()
	for {
		select {
		case job := <-l.queue:
			if job != nil {
				job()
			}
		case <-l.stop:
			for {
				select {
				case job := <-l.queue:
					if job != nil {
						job()
					}
				default:
					return
				}
			}
		}
	}
}

func (l *AsyncRequestLogger) enqueue(job func(), onDrop func()) {
	if l == nil || job == nil {
		if onDrop != nil {
			onDrop()
		}
		return
	}
	if l.closed.Load() {
		if onDrop != nil {
			onDrop()
		}
		return
	}
	select {
	case l.queue <- job:
	case <-l.stop:
		if onDrop != nil {
			onDrop()
		}
	default:
		if onDrop != nil {
			onDrop()
		}
		l.noteDropped()
	}
}

func (l *AsyncRequestLogger) noteDropped() {
	if l == nil {
		return
	}
	dropped := l.dropped.Add(1)
	now := time.Now().UnixNano()
	last := l.lastDropWarn.Load()
	if last != 0 && time.Duration(now-last) < requestLogDropWarnInterval {
		return
	}
	if !l.lastDropWarn.CompareAndSwap(last, now) {
		return
	}
	log.WithField("dropped", dropped).Warn("request log queue full; dropped audit log")
}

type asyncStreamingLogWriter struct {
	logger *AsyncRequestLogger
	inner  StreamingLogWriter

	mu              sync.Mutex
	retainedSources []*FileBodySource
	retainsSources  bool
}

func newAsyncStreamingLogWriter(logger *AsyncRequestLogger, inner StreamingLogWriter) *asyncStreamingLogWriter {
	return &asyncStreamingLogWriter{
		logger: logger,
		inner:  inner,
	}
}

func (w *asyncStreamingLogWriter) WriteChunkAsync(chunk []byte) {
	if w == nil || w.inner == nil {
		return
	}
	w.inner.WriteChunkAsync(chunk)
}

func (w *asyncStreamingLogWriter) WriteStatus(status int, headers map[string][]string) error {
	if w == nil || w.inner == nil {
		return nil
	}
	return w.inner.WriteStatus(status, headers)
}

func (w *asyncStreamingLogWriter) WriteAPIRequest(apiRequest []byte) error {
	if w == nil || w.inner == nil {
		return nil
	}
	return w.inner.WriteAPIRequest(apiRequest)
}

func (w *asyncStreamingLogWriter) WriteAPIResponse(apiResponse []byte) error {
	if w == nil || w.inner == nil {
		return nil
	}
	return w.inner.WriteAPIResponse(apiResponse)
}

func (w *asyncStreamingLogWriter) WriteAPIWebsocketTimeline(apiWebsocketTimeline []byte) error {
	if w == nil || w.inner == nil {
		return nil
	}
	return w.inner.WriteAPIWebsocketTimeline(apiWebsocketTimeline)
}

func (w *asyncStreamingLogWriter) WriteAPIRequestSource(apiRequestSource *FileBodySource) error {
	if w == nil {
		return nil
	}
	w.retainSource(apiRequestSource)
	if sourceWriter, ok := w.inner.(interface {
		WriteAPIRequestSource(*FileBodySource) error
	}); ok {
		return sourceWriter.WriteAPIRequestSource(apiRequestSource)
	}
	if apiRequestSource == nil || !apiRequestSource.HasPayload() {
		return nil
	}
	payload, errBytes := apiRequestSource.Bytes()
	if errBytes != nil {
		return errBytes
	}
	return w.inner.WriteAPIRequest(payload)
}

func (w *asyncStreamingLogWriter) WriteAPIResponseSource(apiResponseSource *FileBodySource) error {
	if w == nil {
		return nil
	}
	w.retainSource(apiResponseSource)
	if sourceWriter, ok := w.inner.(interface {
		WriteAPIResponseSource(*FileBodySource) error
	}); ok {
		return sourceWriter.WriteAPIResponseSource(apiResponseSource)
	}
	if apiResponseSource == nil || !apiResponseSource.HasPayload() {
		return nil
	}
	payload, errBytes := apiResponseSource.Bytes()
	if errBytes != nil {
		return errBytes
	}
	return w.inner.WriteAPIResponse(payload)
}

func (w *asyncStreamingLogWriter) WriteAPIWebsocketTimelineSource(apiWebsocketTimelineSource *FileBodySource) error {
	if w == nil {
		return nil
	}
	w.retainSource(apiWebsocketTimelineSource)
	if sourceWriter, ok := w.inner.(interface {
		WriteAPIWebsocketTimelineSource(*FileBodySource) error
	}); ok {
		return sourceWriter.WriteAPIWebsocketTimelineSource(apiWebsocketTimelineSource)
	}
	if apiWebsocketTimelineSource == nil || !apiWebsocketTimelineSource.HasPayload() {
		return nil
	}
	payload, errBytes := apiWebsocketTimelineSource.Bytes()
	if errBytes != nil {
		return errBytes
	}
	return w.inner.WriteAPIWebsocketTimeline(payload)
}

func (w *asyncStreamingLogWriter) SetFirstChunkTimestamp(timestamp time.Time) {
	if w == nil || w.inner == nil {
		return
	}
	w.inner.SetFirstChunkTimestamp(timestamp)
}

// RetainsLogSources reports whether Finalize must leave transferred FileBodySources alone.
func (w *asyncStreamingLogWriter) RetainsLogSources() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.retainsSources
}

// Close enqueues final log assembly and returns immediately.
func (w *asyncStreamingLogWriter) Close() error {
	if w == nil || w.inner == nil {
		return nil
	}

	w.mu.Lock()
	sources := append([]*FileBodySource(nil), w.retainedSources...)
	inner := w.inner
	w.mu.Unlock()

	job := func() {
		if errClose := inner.Close(); errClose != nil {
			log.WithError(errClose).Warn("async streaming request log failed")
		}
		cleanupFileBodySources(sources...)
	}
	if w.logger == nil {
		job()
		return nil
	}
	w.logger.enqueue(job, func() {
		// Drop final assembly; still free temps without blocking the client path.
		abandonStreamingLogWriter(inner)
		cleanupFileBodySources(sources...)
	})
	return nil
}

func (w *asyncStreamingLogWriter) retainSource(source *FileBodySource) {
	if w == nil || source == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.retainedSources = append(w.retainedSources, source)
	w.retainsSources = true
}

func abandonStreamingLogWriter(writer StreamingLogWriter) {
	switch w := writer.(type) {
	case *FileStreamingLogWriter:
		if w.chunkChan != nil {
			close(w.chunkChan)
			if w.closeChan != nil {
				<-w.closeChan
			}
			w.chunkChan = nil
		}
		w.cleanupTempFiles()
		cleanupFileBodySources(w.apiRequestSource, w.apiResponseSource, w.apiWebsocketTimelineSource)
	case *homeStreamingLogWriter:
		if w.chunkChan != nil {
			close(w.chunkChan)
			if w.doneChan != nil {
				<-w.doneChan
			}
			w.chunkChan = nil
		}
	default:
		if writer != nil {
			_ = writer.Close()
		}
	}
}

func cloneBytes(payload []byte) []byte {
	if payload == nil {
		return nil
	}
	return bytes.Clone(payload)
}

func cloneErrorMessages(messages []*interfaces.ErrorMessage) []*interfaces.ErrorMessage {
	if messages == nil {
		return nil
	}
	out := make([]*interfaces.ErrorMessage, len(messages))
	copy(out, messages)
	return out
}

func mergeSourceBytes(payload []byte, source *FileBodySource) ([]byte, error) {
	if source == nil {
		return payload, nil
	}
	defer cleanupFileBodySources(source)
	if !source.HasPayload() {
		return payload, nil
	}
	var buf bytes.Buffer
	if len(payload) > 0 {
		buf.Write(payload)
		if !bytes.HasSuffix(payload, []byte("\n")) {
			buf.WriteByte('\n')
		}
		buf.WriteByte('\n')
	}
	if errWrite := source.WriteTo(&buf); errWrite != nil {
		return nil, errWrite
	}
	return buf.Bytes(), nil
}
