// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/processortest"
)

const (
	profileDirectoryEnv           = "PERFLOOP_BATCH_LOGS_PROFILE_DIR"
	attributionProfileIterations  = 256
	attributionProfileRecordCount = 4096
)

func TestBatchLogsMaxSizeAttributionProfiles(t *testing.T) {
	directory := profileDirectory(t)
	const maxBatchSize = benchmarkBatchLogsMaxSize
	expectedPages := attributionProfileRecordCount / maxBatchSize
	ctx := context.Background()
	sink := &batchLogsProfileSink{pages: make(chan struct{}, expectedPages)}
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
		SendBatchMaxSize: maxBatchSize,
	}, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, logsProcessor.Shutdown(ctx)) })

	cpuInputs := newBatchLogsProfileInputs()
	cpuProfile, err := os.Create(filepath.Join(directory, "cpu.pprof"))
	require.NoError(t, err)
	require.NoError(t, pprof.StartCPUProfile(cpuProfile))
	consumeBatchLogsProfileInputs(t, ctx, logsProcessor.(*logsBatchProcessor), sink, expectedPages, cpuInputs)
	pprof.StopCPUProfile()
	require.NoError(t, cpuProfile.Close())

	cpuInputs = nil
	runtime.GC()
	allocationInputs := newBatchLogsProfileInputs()
	previousMemProfileRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	t.Cleanup(func() { runtime.MemProfileRate = previousMemProfileRate })
	runtime.GC()
	writeProfile(t, filepath.Join(directory, "alloc-before.pprof"), "allocs")
	consumeBatchLogsProfileInputs(t, ctx, logsProcessor.(*logsBatchProcessor), sink, expectedPages, allocationInputs)
	runtime.GC()
	writeProfile(t, filepath.Join(directory, "alloc-after.pprof"), "allocs")
}

func profileDirectory(t *testing.T) string {
	t.Helper()
	directory := os.Getenv(profileDirectoryEnv)
	if directory == "" {
		t.Skip("set PERFLOOP_BATCH_LOGS_PROFILE_DIR to capture attribution profiles")
	}
	require.NoError(t, os.MkdirAll(directory, 0o755))
	return directory
}

func newBatchLogsProfileInputs() []plog.Logs {
	inputs := make([]plog.Logs, attributionProfileIterations)
	for index := range inputs {
		inputs[index] = newCappedTestLogs(1, 1, attributionProfileRecordCount)
	}
	return inputs
}

func consumeBatchLogsProfileInputs(t *testing.T, ctx context.Context, logsProcessor *logsBatchProcessor, sink *batchLogsProfileSink, expectedPages int, inputs []plog.Logs) {
	t.Helper()
	for _, input := range inputs {
		require.NoError(t, logsProcessor.ConsumeLogs(ctx, input))
		for range expectedPages {
			<-sink.pages
		}
	}
}

type batchLogsProfileSink struct {
	pages chan struct{}
}

func (*batchLogsProfileSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func (s *batchLogsProfileSink) ConsumeLogs(_ context.Context, _ plog.Logs) error {
	s.pages <- struct{}{}
	return nil
}

func writeProfile(t *testing.T, path, name string) {
	t.Helper()

	profile := pprof.Lookup(name)
	require.NotNil(t, profile)
	output, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, profile.WriteTo(output, 0))
	require.NoError(t, output.Close())
}
