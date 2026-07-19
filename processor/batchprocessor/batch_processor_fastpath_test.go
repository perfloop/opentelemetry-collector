// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/xpdata/pref"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestBatchLogsRetainsPooledLogsOnSingleResourceScopeSplit(t *testing.T) {
	previousPooling := pref.UseProtoPooling.IsEnabled()
	require.NoError(t, featuregate.GlobalRegistry().Set(pref.UseProtoPooling.ID(), true))
	t.Cleanup(func() {
		require.NoError(t, featuregate.GlobalRegistry().Set(pref.UseProtoPooling.ID(), previousPooling))
	})

	const sendBatchMax = 5
	ctx := context.Background()
	sink := new(capturingLogsSink)
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
		SendBatchMaxSize: sendBatchMax,
	}, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))

	// This input takes the one-resource/one-scope partial-record path for five pages.
	ld, expected := newMaxSizeTestLogs("ownership", 1, 1, 28)
	require.NoError(t, logsProcessor.ConsumeLogs(ctx, ld))
	pref.UnrefLogs(ld)
	require.NoError(t, logsProcessor.Shutdown(ctx))

	var actual []string
	for _, received := range sink.all() {
		require.Positive(t, received.logs.LogRecordCount())
		require.LessOrEqual(t, received.logs.LogRecordCount(), sendBatchMax)
		actual = append(actual, logIDs(t, "ownership", received.logs)...)
	}
	require.Equal(t, expected, actual)
	// The caller released its reference immediately after enqueueing. The batcher must
	// release its retained reference after every page has been exported.
	require.Panics(t, func() { pref.UnrefLogs(ld) })
}
