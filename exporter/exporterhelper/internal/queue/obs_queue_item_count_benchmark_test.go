// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/requesttest"
	"go.opentelemetry.io/collector/pipeline"
)

// itemCountProbeRequest makes ItemsCount calls observable while otherwise using
// the request behavior exercised by obsQueue's unit tests.
type itemCountProbeRequest struct {
	requesttest.FakeRequest
	itemsCountCalls atomic.Int64
}

func (r *itemCountProbeRequest) ItemsCount() int {
	r.itemsCountCalls.Add(1)
	return r.FakeRequest.ItemsCount()
}

// TestObsQueueItemsCountAfterOfferProbe distinguishes successful admission
// from rejected admission across a runtime-varied number of otherwise identical
// calls. The successful queue is the path that must not inspect item count,
// while the failing queue must still inspect it once to report the
// enqueue-failure metric.
func TestObsQueueItemsCountAfterOfferProbe(t *testing.T) {
	telemetry := componenttest.NewTelemetry()
	t.Cleanup(func() {
		if err := telemetry.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})

	successful, err := newObsQueue[request.Request](Settings[request.Request]{
		Signal:    pipeline.SignalMetrics,
		ID:        exporterID,
		Telemetry: telemetry.NewTelemetrySettings(),
	}, newFakeQueue[request.Request](nil, 0, 1))
	if err != nil {
		t.Fatal(err)
	}
	failed, err := newObsQueue[request.Request](Settings[request.Request]{
		Signal:    pipeline.SignalMetrics,
		ID:        exporterID,
		Telemetry: telemetry.NewTelemetrySettings(),
	}, newFakeQueue[request.Request](errors.New("queue full"), 1, 1))
	if err != nil {
		t.Fatal(err)
	}

	successfulRequest := &itemCountProbeRequest{FakeRequest: requesttest.FakeRequest{Items: 7}}
	failedRequest := &itemCountProbeRequest{FakeRequest: requesttest.FakeRequest{Items: 7}}
	operations := itemCountProbeOperations()
	ctx := context.Background()
	for range operations {
		if err := successful.Offer(ctx, successfulRequest); err != nil {
			t.Fatal(err)
		}
		if err := failed.Offer(ctx, failedRequest); err == nil {
			t.Fatal("failed queue accepted request")
		}
	}

	successfulCalls := successfulRequest.itemsCountCalls.Load()
	failedCalls := failedRequest.itemsCountCalls.Load()
	if failedCalls != int64(operations) {
		t.Fatalf("a rejected Offer must obtain exactly one item count: got %d, want %d", failedCalls, operations)
	}
	failedError := failedCalls - int64(operations)
	if failedError < 0 {
		failedError = -failedError
	}
	t.Logf("PERFLOOP_ITEM_COUNT_PROBE_OPERATIONS=%d", operations)
	t.Logf("PERFLOOP_SUCCESSFUL_ITEM_COUNT_CALLS=%d", successfulCalls)
	t.Logf("PERFLOOP_FAILED_ITEM_COUNT_CALLS=%d", failedCalls)
	t.Logf("PERFLOOP_FAILED_ITEM_COUNT_ABSOLUTE_ERROR=%d", failedError)
}

func itemCountProbeOperations() int {
	const minimumOperations = 10_240
	return minimumOperations + int(time.Now().UnixNano()%17)*32
}
