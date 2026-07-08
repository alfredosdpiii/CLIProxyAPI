// Package logging re-exports request logging primitives for SDK consumers.
package logging

import internallogging "github.com/router-for-me/CLIProxyAPI/v7/internal/logging"

const defaultErrorLogsMaxFiles = 10

// RequestLogger defines the interface for logging HTTP requests and responses.
type RequestLogger = internallogging.RequestLogger

// StreamingLogWriter handles real-time logging of streaming response chunks.
type StreamingLogWriter = internallogging.StreamingLogWriter

// FileRequestLogger implements RequestLogger using file-based storage.
type FileRequestLogger = internallogging.FileRequestLogger

// AsyncRequestLogger wraps a RequestLogger and performs final log assembly off the request path.
type AsyncRequestLogger = internallogging.AsyncRequestLogger

// DefaultRequestLogQueueSize is the bounded async audit-log queue capacity.
const DefaultRequestLogQueueSize = internallogging.DefaultRequestLogQueueSize

// NewFileRequestLogger creates a new file-based request logger with default error log retention (10 files).
func NewFileRequestLogger(enabled bool, logsDir string, configDir string) *FileRequestLogger {
	return internallogging.NewFileRequestLogger(enabled, logsDir, configDir, defaultErrorLogsMaxFiles)
}

// NewFileRequestLoggerWithOptions creates a new file-based request logger with configurable error log retention.
func NewFileRequestLoggerWithOptions(enabled bool, logsDir string, configDir string, errorLogsMaxFiles int) *FileRequestLogger {
	return internallogging.NewFileRequestLogger(enabled, logsDir, configDir, errorLogsMaxFiles)
}

// NewAsyncRequestLogger wraps a RequestLogger with a bounded async worker.
func NewAsyncRequestLogger(inner RequestLogger, queueSize int) *AsyncRequestLogger {
	return internallogging.NewAsyncRequestLogger(inner, queueSize)
}
