package test

import (
	"github.com/pennsieve/rehydration-service/shared/logging"
	"log/slog"
	"testing"
)

// SetLogLevel sets the log level for a test and restores the original level once the test is complete.
// For example, if you want to avoid a lot of Info logging in a test do
// SetLogLevel(t, slog.LevelError)
func SetLogLevel(t *testing.T, level slog.Level) {
	originalLogLevel := logging.Level.Level()
	logging.Level.Set(level)
	t.Cleanup(func() {
		logging.Level.Set(originalLogLevel)
	})
}
