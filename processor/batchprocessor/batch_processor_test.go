// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/testdata"
	"go.opentelemetry.io/collector/pdata/xpdata/pref"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadatatest"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestProcessorShutdown(t *testing.T) {
	factory := NewFactory()

	ctx := context.Background()
	processorCreationSet := processortest.NewNopSettings(metadata.Type)

	for range 5 {
		require.NotPanics(t, func() {
			tProc, err := factory.CreateTraces(ctx, processorCreationSet, factory.CreateDefaultConfig(), consumertest.NewNop())
			require.NoError(t, err)
			_ = tProc.Shutdown(ctx)
		})

		require.NotPanics(t, func() {
			mProc, err := factory.CreateMetrics(ctx, processorCreationSet, factory.CreateDefaultConfig(), consumertest.NewNop())
			require.NoError(t, err)
			_ = mProc.Shutdown(ctx)
		})

		require.NotPanics(t, func() {
			lProc, err := factory.CreateLogs(ctx, processorCreationSet, factory.CreateDefaultConfig(), consumertest.NewNop())
			require.NoError(t, err)
			_ = lProc.Shutdown(ctx)
		})
	}
}

func TestProcessorLifecycle(t *testing.T) {
	factory := NewFactory()

	ctx := context.Background()
	processorCreationSet := processortest.NewNopSettings(metadata.Type)

	for range 5 {
		tProc, err := factory.CreateTraces(ctx, processorCreationSet, factory.CreateDefaultConfig(), consumertest.NewNop())
		require.NoError(t, err)
		require.NoError(t, tProc.Start(ctx, componenttest.NewNopHost()))
		require.NoError(t, tProc.Shutdown(ctx))

		mProc, err := factory.CreateMetrics(ctx, processorCreationSet, factory.CreateDefaultConfig(), consumertest.NewNop())
		require.NoError(t, err)
		require.NoError(t, mProc.Start(ctx, componenttest.NewNopHost()))
		require.NoError(t, mProc.Shutdown(ctx))

		lProc, err := factory.CreateLogs(ctx, processorCreationSet, factory.CreateDefaultConfig(), consumertest.NewNop())
		require.NoError(t, err)
		require.NoError(t, lProc.Start(ctx, componenttest.NewNopHost()))
		require.NoError(t, lProc.Shutdown(ctx))
	}
}

func TestBatchProcessorSpansDelivered(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := createDefaultConfig().(*Config)
	cfg.SendBatchSize = 128
	traces, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, traces.Start(context.Background(), componenttest.NewNopHost()))

	requestCount := 1000
	spansPerRequest := 100
	sentResourceSpans := ptrace.NewTraces().ResourceSpans()
	for requestNum := range requestCount {
		td := testdata.GenerateTraces(spansPerRequest)
		spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
		for spanIndex := range spansPerRequest {
			spans.At(spanIndex).SetName(getTestSpanName(requestNum, spanIndex))
		}
		td.ResourceSpans().At(0).CopyTo(sentResourceSpans.AppendEmpty())
		require.NoError(t, traces.ConsumeTraces(context.Background(), td))
	}

	// Added to test logic that check for empty resources.
	td := ptrace.NewTraces()
	assert.NoError(t, traces.ConsumeTraces(context.Background(), td))

	require.NoError(t, traces.Shutdown(context.Background()))

	require.Equal(t, requestCount*spansPerRequest, sink.SpanCount())
	receivedTraces := sink.AllTraces()
	spansReceivedByName := spansReceivedByName(receivedTraces)
	for requestNum := range requestCount {
		spans := sentResourceSpans.At(requestNum).ScopeSpans().At(0).Spans()
		for spanIndex := range spansPerRequest {
			require.Equal(t,
				spans.At(spanIndex),
				spansReceivedByName[getTestSpanName(requestNum, spanIndex)])
		}
	}
}

func TestBatchProcessorSpansDeliveredEnforceBatchSize(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := createDefaultConfig().(*Config)
	cfg.SendBatchSize = 128
	cfg.SendBatchMaxSize = 130
	traces, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, traces.Start(context.Background(), componenttest.NewNopHost()))

	requestCount := 1000
	spansPerRequest := 150
	for requestNum := range requestCount {
		td := testdata.GenerateTraces(spansPerRequest)
		spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
		for spanIndex := range spansPerRequest {
			spans.At(spanIndex).SetName(getTestSpanName(requestNum, spanIndex))
		}
		require.NoError(t, traces.ConsumeTraces(context.Background(), td))
	}

	// Added to test logic that check for empty resources.
	td := ptrace.NewTraces()
	require.NoError(t, traces.ConsumeTraces(context.Background(), td))

	// wait for all spans to be reported
	for sink.SpanCount() != requestCount*spansPerRequest {
		<-time.After(cfg.Timeout)
	}

	require.NoError(t, traces.Shutdown(context.Background()))

	require.Equal(t, requestCount*spansPerRequest, sink.SpanCount())
	for i := 0; i < len(sink.AllTraces())-1; i++ {
		assert.Equal(t, int(cfg.SendBatchMaxSize), sink.AllTraces()[i].SpanCount())
	}
	// the last batch has the remaining size
	assert.Equal(t, (requestCount*spansPerRequest)%int(cfg.SendBatchMaxSize), sink.AllTraces()[len(sink.AllTraces())-1].SpanCount())
}

func TestBatchProcessorSentBySize(t *testing.T) {
	const (
		sendBatchSize          = 20
		requestCount           = 100
		spansPerRequest        = 5
		expectedBatchesNum     = requestCount * spansPerRequest / sendBatchSize
		expectedBatchingFactor = sendBatchSize / spansPerRequest
	)

	tel := componenttest.NewTelemetry()
	sizer := &ptrace.ProtoMarshaler{}
	sink := new(consumertest.TracesSink)
	cfg := createDefaultConfig().(*Config)
	cfg.SendBatchSize = sendBatchSize
	cfg.Timeout = 500 * time.Millisecond

	traces, err := NewFactory().CreateTraces(context.Background(), metadatatest.NewSettings(tel), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, traces.Start(context.Background(), componenttest.NewNopHost()))

	start := time.Now()
	sizeSum := 0
	for range requestCount {
		td := testdata.GenerateTraces(spansPerRequest)

		require.NoError(t, traces.ConsumeTraces(context.Background(), td))
	}

	require.NoError(t, traces.Shutdown(context.Background()))

	elapsed := time.Since(start)
	require.LessOrEqual(t, elapsed.Nanoseconds(), cfg.Timeout.Nanoseconds())

	require.Equal(t, requestCount*spansPerRequest, sink.SpanCount())
	receivedTraces := sink.AllTraces()
	require.Len(t, receivedTraces, expectedBatchesNum)
	for _, td := range receivedTraces {
		sizeSum += sizer.TracesSize(td)
		rss := td.ResourceSpans()
		require.Equal(t, expectedBatchingFactor, rss.Len())
		for i := range expectedBatchingFactor {
			require.Equal(t, spansPerRequest, rss.At(i).ScopeSpans().At(0).Spans().Len())
		}
	}

	metadatatest.AssertEqualProcessorBatchBatchSendSizeBytes(t, tel,
		[]metricdata.HistogramDataPoint[int64]{
			{
				Attributes:   attribute.NewSet(attribute.String("processor", "batch")),
				Count:        uint64(expectedBatchesNum),
				Bounds:       []float64{128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216},
				BucketCounts: []uint64{0, 0, 0, 0, 0, uint64(expectedBatchesNum), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Sum:          int64(sizeSum),
				Min:          metricdata.NewExtrema(int64(sizeSum / expectedBatchesNum)),
				Max:          metricdata.NewExtrema(int64(sizeSum / expectedBatchesNum)),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchBatchSendSize(t, tel,
		[]metricdata.HistogramDataPoint[int64]{
			{
				Attributes:   attribute.NewSet(attribute.String("processor", "batch")),
				Count:        uint64(expectedBatchesNum),
				Bounds:       []float64{10, 25, 50, 75, 100, 250, 500, 750, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 50000, 100000},
				BucketCounts: []uint64{0, uint64(expectedBatchesNum), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Sum:          int64(sink.SpanCount()),
				Min:          metricdata.NewExtrema(int64(sendBatchSize)),
				Max:          metricdata.NewExtrema(int64(sendBatchSize)),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchBatchSizeTriggerSend(t, tel,
		[]metricdata.DataPoint[int64]{
			{
				Value:      int64(expectedBatchesNum),
				Attributes: attribute.NewSet(attribute.String("processor", "batch")),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchMetadataCardinality(t, tel,
		[]metricdata.DataPoint[int64]{
			{
				Value:      1,
				Attributes: attribute.NewSet(attribute.String("processor", "batch")),
			},
		}, metricdatatest.IgnoreTimestamp())

	require.NoError(t, tel.Shutdown(context.Background()))
}

func TestBatchProcessorSentBySizeWithMaxSize(t *testing.T) {
	const (
		sendBatchSize    = 20
		sendBatchMaxSize = 37
		requestCount     = 1
		spansPerRequest  = 500
		totalSpans       = requestCount * spansPerRequest
	)

	tel := componenttest.NewTelemetry()
	sizer := &ptrace.ProtoMarshaler{}
	sink := new(consumertest.TracesSink)
	cfg := createDefaultConfig().(*Config)
	cfg.SendBatchSize = uint32(sendBatchSize)
	cfg.SendBatchMaxSize = uint32(sendBatchMaxSize)
	cfg.Timeout = 500 * time.Millisecond

	traces, err := NewFactory().CreateTraces(context.Background(), metadatatest.NewSettings(tel), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, traces.Start(context.Background(), componenttest.NewNopHost()))

	start := time.Now()

	sizeSum := 0
	for range requestCount {
		td := testdata.GenerateTraces(spansPerRequest)
		require.NoError(t, traces.ConsumeTraces(context.Background(), td))
	}

	require.NoError(t, traces.Shutdown(context.Background()))

	elapsed := time.Since(start)
	require.LessOrEqual(t, elapsed.Nanoseconds(), cfg.Timeout.Nanoseconds())

	// The max batch size is not a divisor of the total number of spans
	expectedBatchesNum := math.Ceil(float64(totalSpans) / float64(sendBatchMaxSize))

	require.Equal(t, totalSpans, sink.SpanCount())
	receivedTraces := sink.AllTraces()
	require.Len(t, receivedTraces, int(expectedBatchesNum))
	// we have to count the size after it was processed since splitTraces will cause some
	// repeated ResourceSpan data to be sent through the processor
	minSize := math.MaxInt
	maxSize := math.MinInt
	for _, td := range receivedTraces {
		minSize = min(minSize, sizer.TracesSize(td))
		maxSize = max(maxSize, sizer.TracesSize(td))
		sizeSum += sizer.TracesSize(td)
	}

	metadatatest.AssertEqualProcessorBatchBatchSendSizeBytes(t, tel,
		[]metricdata.HistogramDataPoint[int64]{
			{
				Attributes:   attribute.NewSet(attribute.String("processor", "batch")),
				Count:        uint64(expectedBatchesNum),
				Bounds:       []float64{128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216},
				BucketCounts: []uint64{0, 0, 0, 0, 0, 1, uint64(expectedBatchesNum - 1), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Sum:          int64(sizeSum),
				Min:          metricdata.NewExtrema(int64(minSize)),
				Max:          metricdata.NewExtrema(int64(maxSize)),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchBatchSendSize(t, tel,
		[]metricdata.HistogramDataPoint[int64]{
			{
				Attributes:   attribute.NewSet(attribute.String("processor", "batch")),
				Count:        uint64(expectedBatchesNum),
				Bounds:       []float64{10, 25, 50, 75, 100, 250, 500, 750, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 50000, 100000},
				BucketCounts: []uint64{0, 1, uint64(expectedBatchesNum - 1), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Sum:          int64(sink.SpanCount()),
				Min:          metricdata.NewExtrema(int64(sendBatchSize - 1)),
				Max:          metricdata.NewExtrema(int64(cfg.SendBatchMaxSize)),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchBatchSizeTriggerSend(t, tel,
		[]metricdata.DataPoint[int64]{
			{
				Value:      int64(expectedBatchesNum - 1),
				Attributes: attribute.NewSet(attribute.String("processor", "batch")),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchMetadataCardinality(t, tel,
		[]metricdata.DataPoint[int64]{
			{
				Value:      1,
				Attributes: attribute.NewSet(attribute.String("processor", "batch")),
			},
		}, metricdatatest.IgnoreTimestamp())

	require.NoError(t, tel.Shutdown(context.Background()))
}

func TestBatchProcessorSentByTimeout(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := createDefaultConfig().(*Config)
	sendBatchSize := 100
	cfg.SendBatchSize = uint32(sendBatchSize)
	cfg.Timeout = 100 * time.Millisecond

	requestCount := 5
	spansPerRequest := 10
	start := time.Now()

	traces, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, traces.Start(context.Background(), componenttest.NewNopHost()))

	for range requestCount {
		td := testdata.GenerateTraces(spansPerRequest)
		require.NoError(t, traces.ConsumeTraces(context.Background(), td))
	}

	// Wait for at least one batch to be sent.
	for sink.SpanCount() == 0 {
		<-time.After(cfg.Timeout)
	}

	elapsed := time.Since(start)
	require.LessOrEqual(t, cfg.Timeout.Nanoseconds(), elapsed.Nanoseconds())

	// This should not change the results in the sink, verified by the expectedBatchesNum
	require.NoError(t, traces.Shutdown(context.Background()))

	expectedBatchesNum := 1
	expectedBatchingFactor := 5

	require.Equal(t, requestCount*spansPerRequest, sink.SpanCount())
	receivedTraces := sink.AllTraces()
	require.Len(t, receivedTraces, expectedBatchesNum)
	for _, td := range receivedTraces {
		rss := td.ResourceSpans()
		require.Equal(t, expectedBatchingFactor, rss.Len())
		for i := range expectedBatchingFactor {
			require.Equal(t, spansPerRequest, rss.At(i).ScopeSpans().At(0).Spans().Len())
		}
	}
}

func TestBatchProcessorTraceSendWhenClosing(t *testing.T) {
	cfg := &Config{
		Timeout:       3 * time.Second,
		SendBatchSize: 1000,
	}
	sink := new(consumertest.TracesSink)

	traces, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, traces.Start(context.Background(), componenttest.NewNopHost()))

	requestCount := 10
	spansPerRequest := 10
	for range requestCount {
		td := testdata.GenerateTraces(spansPerRequest)
		require.NoError(t, traces.ConsumeTraces(context.Background(), td))
	}

	require.NoError(t, traces.Shutdown(context.Background()))

	require.Equal(t, requestCount*spansPerRequest, sink.SpanCount())
	require.Len(t, sink.AllTraces(), 1)
}

func TestBatchMetricProcessor_ReceivingData(t *testing.T) {
	// Instantiate the batch processor with low config values to test data
	// gets sent through the processor.
	cfg := &Config{
		Timeout:       200 * time.Millisecond,
		SendBatchSize: 50,
	}

	requestCount := 100
	metricsPerRequest := 5
	sink := new(consumertest.MetricsSink)

	metrics, err := NewFactory().CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, metrics.Start(context.Background(), componenttest.NewNopHost()))

	sentResourceMetrics := pmetric.NewMetrics().ResourceMetrics()

	for requestNum := range requestCount {
		md := testdata.GenerateMetrics(metricsPerRequest)
		ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
		for metricIndex := range metricsPerRequest {
			ms.At(metricIndex).SetName(getTestMetricName(requestNum, metricIndex))
		}
		md.ResourceMetrics().At(0).CopyTo(sentResourceMetrics.AppendEmpty())
		require.NoError(t, metrics.ConsumeMetrics(context.Background(), md))
	}

	// Added to test case with empty resources sent.
	md := pmetric.NewMetrics()
	assert.NoError(t, metrics.ConsumeMetrics(context.Background(), md))

	require.NoError(t, metrics.Shutdown(context.Background()))

	require.Equal(t, requestCount*2*metricsPerRequest, sink.DataPointCount())
	receivedMds := sink.AllMetrics()
	metricsReceivedByName := metricsReceivedByName(receivedMds)
	for requestNum := range requestCount {
		ms := sentResourceMetrics.At(requestNum).ScopeMetrics().At(0).Metrics()
		for metricIndex := range metricsPerRequest {
			require.Equal(t,
				ms.At(metricIndex),
				metricsReceivedByName[getTestMetricName(requestNum, metricIndex)])
		}
	}
}

func TestBatchMetricProcessorBatchSize(t *testing.T) {
	tel := componenttest.NewTelemetry()
	sizer := &pmetric.ProtoMarshaler{}

	// Instantiate the batch processor with low config values to test data
	// gets sent through the processor.
	cfg := &Config{
		Timeout:       100 * time.Millisecond,
		SendBatchSize: 50,
	}

	const (
		requestCount         = 100
		metricsPerRequest    = 5
		dataPointsPerMetric  = 2 // Since the int counter uses two datapoints.
		dataPointsPerRequest = metricsPerRequest * dataPointsPerMetric
	)
	sink := new(consumertest.MetricsSink)

	metrics, err := NewFactory().CreateMetrics(context.Background(), metadatatest.NewSettings(tel), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, metrics.Start(context.Background(), componenttest.NewNopHost()))

	start := time.Now()
	size := 0
	for range requestCount {
		md := testdata.GenerateMetrics(metricsPerRequest)
		size += sizer.MetricsSize(md)
		require.NoError(t, metrics.ConsumeMetrics(context.Background(), md))
	}
	require.NoError(t, metrics.Shutdown(context.Background()))

	elapsed := time.Since(start)
	require.LessOrEqual(t, elapsed.Nanoseconds(), cfg.Timeout.Nanoseconds())

	expectedBatchesNum := requestCount * dataPointsPerRequest / cfg.SendBatchSize
	expectedBatchingFactor := int(cfg.SendBatchSize) / dataPointsPerRequest

	require.Equal(t, requestCount*2*metricsPerRequest, sink.DataPointCount())
	receivedMds := sink.AllMetrics()
	require.Len(t, receivedMds, int(expectedBatchesNum))
	for _, md := range receivedMds {
		require.Equal(t, expectedBatchingFactor, md.ResourceMetrics().Len())
		for i := range expectedBatchingFactor {
			require.Equal(t, metricsPerRequest, md.ResourceMetrics().At(i).ScopeMetrics().At(0).Metrics().Len())
		}
	}

	metadatatest.AssertEqualProcessorBatchBatchSendSizeBytes(t, tel,
		[]metricdata.HistogramDataPoint[int64]{
			{
				Attributes:   attribute.NewSet(attribute.String("processor", "batch")),
				Count:        uint64(expectedBatchesNum),
				Bounds:       []float64{128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216},
				BucketCounts: []uint64{0, 0, 0, 0, 0, 0, uint64(expectedBatchesNum), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Sum:          int64(size),
				Min:          metricdata.NewExtrema(int64(size / int(expectedBatchesNum))),
				Max:          metricdata.NewExtrema(int64(size / int(expectedBatchesNum))),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchBatchSendSize(t, tel,
		[]metricdata.HistogramDataPoint[int64]{
			{
				Attributes:   attribute.NewSet(attribute.String("processor", "batch")),
				Count:        uint64(expectedBatchesNum),
				Bounds:       []float64{10, 25, 50, 75, 100, 250, 500, 750, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 50000, 100000},
				BucketCounts: []uint64{0, 0, uint64(expectedBatchesNum), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Sum:          int64(sink.DataPointCount()),
				Min:          metricdata.NewExtrema(int64(cfg.SendBatchSize)),
				Max:          metricdata.NewExtrema(int64(cfg.SendBatchSize)),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchBatchSizeTriggerSend(t, tel,
		[]metricdata.DataPoint[int64]{
			{
				Value:      int64(expectedBatchesNum),
				Attributes: attribute.NewSet(attribute.String("processor", "batch")),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchMetadataCardinality(t, tel,
		[]metricdata.DataPoint[int64]{
			{
				Value:      1,
				Attributes: attribute.NewSet(attribute.String("processor", "batch")),
			},
		}, metricdatatest.IgnoreTimestamp())

	require.NoError(t, tel.Shutdown(context.Background()))
}

func TestBatchMetrics_UnevenBatchMaxSize(t *testing.T) {
	ctx := context.Background()
	sink := new(metricsSink)
	metricsCount := 50
	dataPointsPerMetric := 2
	sendBatchMaxSize := 99

	batchMetrics := newMetricsBatch(sink)
	md := testdata.GenerateMetrics(metricsCount)

	batchMetrics.add(md)
	require.Equal(t, dataPointsPerMetric*metricsCount, batchMetrics.dataPointCount)
	sent, req := batchMetrics.split(sendBatchMaxSize)
	sendErr := batchMetrics.export(ctx, req)
	require.NoError(t, sendErr)
	require.Equal(t, sendBatchMaxSize, sent)
	remainingDataPointCount := metricsCount*dataPointsPerMetric - sendBatchMaxSize
	require.Equal(t, remainingDataPointCount, batchMetrics.dataPointCount)
}

func TestBatchMetricsProcessor_Timeout(t *testing.T) {
	cfg := &Config{
		Timeout:       100 * time.Millisecond,
		SendBatchSize: 101,
	}
	requestCount := 5
	metricsPerRequest := 10
	sink := new(consumertest.MetricsSink)

	metrics, err := NewFactory().CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, metrics.Start(context.Background(), componenttest.NewNopHost()))

	start := time.Now()
	for range requestCount {
		md := testdata.GenerateMetrics(metricsPerRequest)
		require.NoError(t, metrics.ConsumeMetrics(context.Background(), md))
	}

	// Wait for at least one batch to be sent.
	for sink.DataPointCount() == 0 {
		<-time.After(cfg.Timeout)
	}

	elapsed := time.Since(start)
	require.LessOrEqual(t, cfg.Timeout.Nanoseconds(), elapsed.Nanoseconds())

	// This should not change the results in the sink, verified by the expectedBatchesNum
	require.NoError(t, metrics.Shutdown(context.Background()))

	expectedBatchesNum := 1
	expectedBatchingFactor := 5

	require.Equal(t, requestCount*2*metricsPerRequest, sink.DataPointCount())
	receivedMds := sink.AllMetrics()
	require.Len(t, receivedMds, expectedBatchesNum)
	for _, md := range receivedMds {
		require.Equal(t, expectedBatchingFactor, md.ResourceMetrics().Len())
		for i := range expectedBatchingFactor {
			require.Equal(t, metricsPerRequest, md.ResourceMetrics().At(i).ScopeMetrics().At(0).Metrics().Len())
		}
	}
}

func TestBatchMetricProcessor_Shutdown(t *testing.T) {
	cfg := &Config{
		Timeout:       3 * time.Second,
		SendBatchSize: 1000,
	}
	requestCount := 5
	metricsPerRequest := 10
	sink := new(consumertest.MetricsSink)

	metrics, err := NewFactory().CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, metrics.Start(context.Background(), componenttest.NewNopHost()))

	for range requestCount {
		md := testdata.GenerateMetrics(metricsPerRequest)
		require.NoError(t, metrics.ConsumeMetrics(context.Background(), md))
	}

	require.NoError(t, metrics.Shutdown(context.Background()))

	require.Equal(t, requestCount*2*metricsPerRequest, sink.DataPointCount())
	require.Len(t, sink.AllMetrics(), 1)
}

func getTestSpanName(requestNum, index int) string {
	return fmt.Sprintf("test-span-%d-%d", requestNum, index)
}

func spansReceivedByName(tds []ptrace.Traces) map[string]ptrace.Span {
	spansReceivedByName := map[string]ptrace.Span{}
	for i := range tds {
		rss := tds[i].ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			ilss := rss.At(i).ScopeSpans()
			for j := 0; j < ilss.Len(); j++ {
				spans := ilss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					span := spans.At(k)
					spansReceivedByName[spans.At(k).Name()] = span
				}
			}
		}
	}
	return spansReceivedByName
}

func metricsReceivedByName(mds []pmetric.Metrics) map[string]pmetric.Metric {
	metricsReceivedByName := map[string]pmetric.Metric{}
	for _, md := range mds {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			ilms := rms.At(i).ScopeMetrics()
			for j := 0; j < ilms.Len(); j++ {
				metrics := ilms.At(j).Metrics()
				for k := 0; k < metrics.Len(); k++ {
					metric := metrics.At(k)
					metricsReceivedByName[metric.Name()] = metric
				}
			}
		}
	}
	return metricsReceivedByName
}

func getTestMetricName(requestNum, index int) string {
	return fmt.Sprintf("test-metric-int-%d-%d", requestNum, index)
}

func BenchmarkTraceSizeBytes(b *testing.B) {
	sizer := &ptrace.ProtoMarshaler{}
	td := testdata.GenerateTraces(8192)
	for b.Loop() {
		fmt.Println(sizer.TracesSize(td))
	}
}

func BenchmarkTraceSizeSpanCount(b *testing.B) {
	td := testdata.GenerateTraces(8192)
	for b.Loop() {
		td.SpanCount()
	}
}

func BenchmarkBatchMetricProcessor2k(b *testing.B) {
	b.StopTimer()
	cfg := &Config{
		Timeout:       100 * time.Millisecond,
		SendBatchSize: 2000,
	}
	runMetricsProcessorBenchmark(b, cfg)
}

func BenchmarkMultiBatchMetricProcessor2k(b *testing.B) {
	b.StopTimer()
	cfg := &Config{
		Timeout:       100 * time.Millisecond,
		SendBatchSize: 2000,
		MetadataKeys:  []string{"test", "test2"},
	}
	runMetricsProcessorBenchmark(b, cfg)
}

func runMetricsProcessorBenchmark(b *testing.B, cfg *Config) {
	ctx := context.Background()
	sink := new(metricsSink)
	metrics, err := NewFactory().CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(b, err)
	require.NoError(b, metrics.Start(ctx, componenttest.NewNopHost()))

	const metricsPerRequest = 150_000
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			require.NoError(b, metrics.ConsumeMetrics(ctx, testdata.GenerateMetrics(metricsPerRequest)))
		}
	})
	b.StopTimer()
	require.NoError(b, metrics.Shutdown(ctx))
	require.Equal(b, b.N*metricsPerRequest, sink.metricsCount)
}

type metricsSink struct {
	mu           sync.Mutex
	metricsCount int
}

func (sme *metricsSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: false,
	}
}

func (sme *metricsSink) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	sme.mu.Lock()
	defer sme.mu.Unlock()
	sme.metricsCount += md.MetricCount()
	return nil
}

func TestBatchLogProcessor_ReceivingData(t *testing.T) {
	// Instantiate the batch processor with low config values to test data
	// gets sent through the processor.
	cfg := &Config{
		Timeout:       200 * time.Millisecond,
		SendBatchSize: 50,
	}

	requestCount := 100
	logsPerRequest := 5
	sink := new(consumertest.LogsSink)

	logs, err := NewFactory().CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, logs.Start(context.Background(), componenttest.NewNopHost()))

	sentResourceLogs := plog.NewLogs().ResourceLogs()

	for requestNum := range requestCount {
		ld := testdata.GenerateLogs(logsPerRequest)
		lrs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
		for logIndex := range logsPerRequest {
			lrs.At(logIndex).SetSeverityText(getTestLogSeverityText(requestNum, logIndex))
		}
		ld.ResourceLogs().At(0).CopyTo(sentResourceLogs.AppendEmpty())
		require.NoError(t, logs.ConsumeLogs(context.Background(), ld))
	}

	// Added to test case with empty resources sent.
	ld := plog.NewLogs()
	assert.NoError(t, logs.ConsumeLogs(context.Background(), ld))

	require.NoError(t, logs.Shutdown(context.Background()))

	require.Equal(t, requestCount*logsPerRequest, sink.LogRecordCount())
	receivedMds := sink.AllLogs()
	logsReceivedBySeverityText := logsReceivedBySeverityText(receivedMds)
	for requestNum := range requestCount {
		lrs := sentResourceLogs.At(requestNum).ScopeLogs().At(0).LogRecords()
		for logIndex := range logsPerRequest {
			require.Equal(t,
				lrs.At(logIndex),
				logsReceivedBySeverityText[getTestLogSeverityText(requestNum, logIndex)])
		}
	}
}

func TestBatchLogProcessor_BatchSize(t *testing.T) {
	tel := componenttest.NewTelemetry()
	sizer := &plog.ProtoMarshaler{}

	// Instantiate the batch processor with low config values to test data
	// gets sent through the processor.
	cfg := &Config{
		Timeout:       100 * time.Millisecond,
		SendBatchSize: 50,
	}

	const (
		requestCount   = 100
		logsPerRequest = 5
	)
	sink := new(consumertest.LogsSink)

	logs, err := NewFactory().CreateLogs(context.Background(), metadatatest.NewSettings(tel), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, logs.Start(context.Background(), componenttest.NewNopHost()))

	start := time.Now()
	size := 0
	for range requestCount {
		ld := testdata.GenerateLogs(logsPerRequest)
		size += sizer.LogsSize(ld)
		require.NoError(t, logs.ConsumeLogs(context.Background(), ld))
	}
	require.NoError(t, logs.Shutdown(context.Background()))

	elapsed := time.Since(start)
	require.LessOrEqual(t, elapsed.Nanoseconds(), cfg.Timeout.Nanoseconds())

	expectedBatchesNum := requestCount * logsPerRequest / cfg.SendBatchSize
	expectedBatchingFactor := int(cfg.SendBatchSize) / logsPerRequest

	require.Equal(t, requestCount*logsPerRequest, sink.LogRecordCount())
	receivedMds := sink.AllLogs()
	require.Len(t, receivedMds, int(expectedBatchesNum))
	for _, ld := range receivedMds {
		require.Equal(t, expectedBatchingFactor, ld.ResourceLogs().Len())
		for i := range expectedBatchingFactor {
			require.Equal(t, logsPerRequest, ld.ResourceLogs().At(i).ScopeLogs().At(0).LogRecords().Len())
		}
	}

	metadatatest.AssertEqualProcessorBatchBatchSendSizeBytes(t, tel,
		[]metricdata.HistogramDataPoint[int64]{
			{
				Attributes:   attribute.NewSet(attribute.String("processor", "batch")),
				Count:        uint64(expectedBatchesNum),
				Bounds:       []float64{128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216},
				BucketCounts: []uint64{0, 0, 0, 0, 0, 0, uint64(expectedBatchesNum), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Sum:          int64(size),
				Min:          metricdata.NewExtrema(int64(size / int(expectedBatchesNum))),
				Max:          metricdata.NewExtrema(int64(size / int(expectedBatchesNum))),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchBatchSendSize(t, tel,
		[]metricdata.HistogramDataPoint[int64]{
			{
				Attributes:   attribute.NewSet(attribute.String("processor", "batch")),
				Count:        uint64(expectedBatchesNum),
				Bounds:       []float64{10, 25, 50, 75, 100, 250, 500, 750, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 50000, 100000},
				BucketCounts: []uint64{0, 0, uint64(expectedBatchesNum), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Sum:          int64(sink.LogRecordCount()),
				Min:          metricdata.NewExtrema(int64(cfg.SendBatchSize)),
				Max:          metricdata.NewExtrema(int64(cfg.SendBatchSize)),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchBatchSizeTriggerSend(t, tel,
		[]metricdata.DataPoint[int64]{
			{
				Value:      int64(expectedBatchesNum),
				Attributes: attribute.NewSet(attribute.String("processor", "batch")),
			},
		}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualProcessorBatchMetadataCardinality(t, tel,
		[]metricdata.DataPoint[int64]{
			{
				Value:      1,
				Attributes: attribute.NewSet(attribute.String("processor", "batch")),
			},
		}, metricdatatest.IgnoreTimestamp())

	require.NoError(t, tel.Shutdown(context.Background()))
}

func TestBatchLogsProcessor_Timeout(t *testing.T) {
	cfg := &Config{
		Timeout:       100 * time.Millisecond,
		SendBatchSize: 100,
	}
	requestCount := 5
	logsPerRequest := 10
	sink := new(consumertest.LogsSink)

	logs, err := NewFactory().CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, logs.Start(context.Background(), componenttest.NewNopHost()))

	start := time.Now()
	for range requestCount {
		ld := testdata.GenerateLogs(logsPerRequest)
		require.NoError(t, logs.ConsumeLogs(context.Background(), ld))
	}

	// Wait for at least one batch to be sent.
	for sink.LogRecordCount() == 0 {
		<-time.After(cfg.Timeout)
	}

	elapsed := time.Since(start)
	require.LessOrEqual(t, cfg.Timeout.Nanoseconds(), elapsed.Nanoseconds())

	// This should not change the results in the sink, verified by the expectedBatchesNum
	require.NoError(t, logs.Shutdown(context.Background()))

	expectedBatchesNum := 1
	expectedBatchingFactor := 5

	require.Equal(t, requestCount*logsPerRequest, sink.LogRecordCount())
	receivedMds := sink.AllLogs()
	require.Len(t, receivedMds, expectedBatchesNum)
	for _, ld := range receivedMds {
		require.Equal(t, expectedBatchingFactor, ld.ResourceLogs().Len())
		for i := range expectedBatchingFactor {
			require.Equal(t, logsPerRequest, ld.ResourceLogs().At(i).ScopeLogs().At(0).LogRecords().Len())
		}
	}
}

func TestBatchLogsProcessorCappedPageTimeoutRemainder(t *testing.T) {
	const maxBatchSize = 5
	input := testdata.GenerateLogs(7)
	resourceLogs := input.ResourceLogs().At(0)
	resourceLogs.Resource().Attributes().PutStr("resource.attr", "resource-value")
	resourceLogs.SetSchemaUrl("https://example.com/resource")
	scopeLogs := resourceLogs.ScopeLogs().At(0)
	scopeLogs.Scope().SetName("scope-name")
	scopeLogs.Scope().Attributes().PutStr("scope.attr", "scope-value")
	scopeLogs.SetSchemaUrl("https://example.com/scope")
	for index := range scopeLogs.LogRecords().Len() {
		scopeLogs.LogRecords().At(index).SetSeverityText(fmt.Sprintf("record-%d", index))
	}

	ctx := context.Background()
	sink := new(consumertest.LogsSink)
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
		Timeout:          5 * time.Millisecond,
		SendBatchSize:    maxBatchSize,
		SendBatchMaxSize: maxBatchSize,
	}, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))
	require.NoError(t, logsProcessor.ConsumeLogs(ctx, input))
	// The capped first page exercises the specialization; timeout flushes its remainder.
	require.Eventually(t, func() bool { return sink.LogRecordCount() == 7 }, time.Second, time.Millisecond)
	require.NoError(t, logsProcessor.Shutdown(ctx))

	pages := sink.AllLogs()
	require.Len(t, pages, 2)
	for pageIndex, recordCount := range []int{maxBatchSize, 2} {
		pageResourceLogs := pages[pageIndex].ResourceLogs()
		require.Equal(t, 1, pageResourceLogs.Len())
		pageResource := pageResourceLogs.At(0)
		resourceAttribute, ok := pageResource.Resource().Attributes().Get("resource.attr")
		require.True(t, ok)
		require.Equal(t, "resource-value", resourceAttribute.Str())
		require.Equal(t, "https://example.com/resource", pageResource.SchemaUrl())
		pageScopeLogs := pageResource.ScopeLogs()
		require.Equal(t, 1, pageScopeLogs.Len())
		pageScope := pageScopeLogs.At(0)
		require.Equal(t, "scope-name", pageScope.Scope().Name())
		scopeAttribute, ok := pageScope.Scope().Attributes().Get("scope.attr")
		require.True(t, ok)
		require.Equal(t, "scope-value", scopeAttribute.Str())
		require.Equal(t, "https://example.com/scope", pageScope.SchemaUrl())
		pageRecords := pageScope.LogRecords()
		require.Equal(t, recordCount, pageRecords.Len())
		for recordIndex := range recordCount {
			require.Equal(t, fmt.Sprintf("record-%d", pageIndex*maxBatchSize+recordIndex), pageRecords.At(recordIndex).SeverityText())
		}
	}
}

func TestBatchLogsProcessorCappedPages(t *testing.T) {
	testCases := []struct {
		name         string
		resources    int
		scopes       int
		records      int
		maxBatchSize int
	}{
		{name: "one_resource_one_scope", resources: 1, scopes: 1, records: 17, maxBatchSize: 5},
		{name: "many_resources", resources: 3, scopes: 1, records: 4, maxBatchSize: 5},
		{name: "one_resource_many_scopes", resources: 1, scopes: 3, records: 4, maxBatchSize: 5},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			expected := expectedCappedBoundaryPages(testCase.resources, testCase.scopes, testCase.records, testCase.maxBatchSize)
			actual := consumeCappedLogsAtBoundary(t, newCappedBoundaryLogs(testCase.resources, testCase.scopes, testCase.records), &Config{
				SendBatchMaxSize: uint32(testCase.maxBatchSize),
			})

			assertCappedBoundaryPages(t, expected, actual, testCase.maxBatchSize)
		})
	}
}

func TestBatchLogsProcessorCappedPagesPooledOwnership(t *testing.T) {
	previousPooling := pref.UseProtoPooling.IsEnabled()
	require.NoError(t, featuregate.GlobalRegistry().Set(pref.UseProtoPooling.ID(), true))
	t.Cleanup(func() {
		require.NoError(t, featuregate.GlobalRegistry().Set(pref.UseProtoPooling.ID(), previousPooling))
	})

	const maxBatchSize = 5
	input := newCappedBoundaryLogs(1, 1, 27)
	expected := expectedCappedBoundaryPages(1, 1, 27, maxBatchSize)

	sink := new(consumertest.LogsSink)
	ctx := context.Background()
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
		SendBatchMaxSize: maxBatchSize,
	}, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))
	require.NoError(t, logsProcessor.ConsumeLogs(ctx, input))
	pref.UnrefLogs(input)
	require.NoError(t, logsProcessor.Shutdown(ctx))

	assertCappedBoundaryPages(t, expected, sink.AllLogs(), maxBatchSize)
	require.Panics(t, func() { pref.UnrefLogs(input) })
}

func TestBatchLogsProcessorCappedPagesMetadataShards(t *testing.T) {
	const maxBatchSize = 5
	sink := new(consumertest.LogsSink)
	ctx := context.Background()
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
		SendBatchMaxSize: maxBatchSize,
		MetadataKeys:     []string{"tenant"},
	}, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))

	expected := map[string][]plog.Logs{}
	for _, tenant := range []string{"a", "b"} {
		expected[tenant] = expectedCappedBoundaryPages(1, 1, 12, maxBatchSize)
		tenantCtx := client.NewContext(ctx, client.Info{
			Metadata: client.NewMetadata(map[string][]string{"tenant": {tenant}}),
		})
		require.NoError(t, logsProcessor.ConsumeLogs(tenantCtx, newCappedBoundaryLogs(1, 1, 12)))
	}
	require.NoError(t, logsProcessor.Shutdown(ctx))

	actual := map[string][]plog.Logs{}
	for index, logs := range sink.AllLogs() {
		tenants := client.FromContext(sink.Contexts()[index]).Metadata.Get("tenant")
		require.Len(t, tenants, 1)
		actual[tenants[0]] = append(actual[tenants[0]], logs)
	}
	for tenant, want := range expected {
		assertCappedBoundaryPages(t, want, actual[tenant], maxBatchSize)
	}
}

func TestBatchLogsProcessorCappedPagesTriggers(t *testing.T) {
	t.Run("batch_size", func(t *testing.T) {
		const maxBatchSize = 5
		expected := expectedCappedBoundaryPages(1, 1, 7, maxBatchSize)
		actual := consumeCappedLogsAtBoundary(t, newCappedBoundaryLogs(1, 1, 7), &Config{
			Timeout:          time.Hour,
			SendBatchSize:    maxBatchSize,
			SendBatchMaxSize: maxBatchSize,
		})
		assertCappedBoundaryPages(t, expected, actual, maxBatchSize)
	})
}

func consumeCappedLogsAtBoundary(t *testing.T, input plog.Logs, cfg *Config) []plog.Logs {
	t.Helper()

	sink := new(consumertest.LogsSink)
	ctx := context.Background()
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))
	require.NoError(t, logsProcessor.ConsumeLogs(ctx, input))
	require.NoError(t, logsProcessor.Shutdown(ctx))

	return sink.AllLogs()
}

// expectedCappedBoundaryPages constructs the FIFO page sequence independently of splitLogs.
func expectedCappedBoundaryPages(resources, scopes, records, maxBatchSize int) []plog.Logs {
	pages := []plog.Logs{}
	pageRecords := maxBatchSize
	var page cappedBoundaryPage
	for resourceIndex := range resources {
		for scopeIndex := range scopes {
			for recordIndex := range records {
				if pageRecords == maxBatchSize {
					page = cappedBoundaryPage{logs: plog.NewLogs(), resourceIndex: -1, scopeIndex: -1}
					pages = append(pages, page.logs)
					pageRecords = 0
				}
				page.append(resourceIndex, scopeIndex, recordIndex)
				pageRecords++
			}
		}
	}
	return pages
}

type cappedBoundaryPage struct {
	logs          plog.Logs
	resourceLogs  plog.ResourceLogs
	scopeLogs     plog.ScopeLogs
	resourceIndex int
	scopeIndex    int
}

func (page *cappedBoundaryPage) append(resourceIndex, scopeIndex, recordIndex int) {
	if page.resourceIndex != resourceIndex {
		page.resourceLogs = page.logs.ResourceLogs().AppendEmpty()
		page.resourceLogs.Resource().Attributes().PutStr("resource.name", fmt.Sprintf("resource-%d", resourceIndex))
		page.resourceLogs.Resource().Attributes().PutInt("resource.index", int64(resourceIndex))
		page.resourceLogs.SetSchemaUrl(fmt.Sprintf("https://example.com/resource/%d", resourceIndex))
		page.resourceIndex = resourceIndex
		page.scopeIndex = -1
	}
	if page.scopeIndex != scopeIndex {
		page.scopeLogs = page.resourceLogs.ScopeLogs().AppendEmpty()
		page.scopeLogs.Scope().SetName(fmt.Sprintf("scope-%d", scopeIndex))
		page.scopeLogs.Scope().SetVersion(fmt.Sprintf("v%d", scopeIndex))
		page.scopeLogs.Scope().Attributes().PutStr("scope.name", fmt.Sprintf("scope-%d", scopeIndex))
		page.scopeLogs.SetSchemaUrl(fmt.Sprintf("https://example.com/scope/%d", scopeIndex))
		page.scopeIndex = scopeIndex
	}
	logRecord := page.scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetSeverityText(fmt.Sprintf("resource-%d/scope-%d/record-%d", resourceIndex, scopeIndex, recordIndex))
	logRecord.Body().SetStr(fmt.Sprintf("body-%d", recordIndex))
	logRecord.Attributes().PutInt("record.index", int64(recordIndex))
}

func assertCappedBoundaryPages(t *testing.T, expected, actual []plog.Logs, maxBatchSize int) {
	t.Helper()

	require.Len(t, actual, len(expected))
	for page := range expected {
		require.LessOrEqual(t, actual[page].LogRecordCount(), maxBatchSize)
		require.Truef(t, pref.EqualLogs(expected[page], actual[page]), "page %d differs", page)
	}
}

func newCappedBoundaryLogs(resources, scopes, records int) plog.Logs {
	logs := plog.NewLogs()
	for resourceIndex := range resources {
		resourceLogs := logs.ResourceLogs().AppendEmpty()
		resourceLogs.Resource().Attributes().PutStr("resource.name", fmt.Sprintf("resource-%d", resourceIndex))
		resourceLogs.Resource().Attributes().PutInt("resource.index", int64(resourceIndex))
		resourceLogs.SetSchemaUrl(fmt.Sprintf("https://example.com/resource/%d", resourceIndex))

		for scopeIndex := range scopes {
			scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
			scopeLogs.Scope().SetName(fmt.Sprintf("scope-%d", scopeIndex))
			scopeLogs.Scope().SetVersion(fmt.Sprintf("v%d", scopeIndex))
			scopeLogs.Scope().Attributes().PutStr("scope.name", fmt.Sprintf("scope-%d", scopeIndex))
			scopeLogs.SetSchemaUrl(fmt.Sprintf("https://example.com/scope/%d", scopeIndex))

			for recordIndex := range records {
				logRecord := scopeLogs.LogRecords().AppendEmpty()
				logRecord.SetSeverityText(fmt.Sprintf("resource-%d/scope-%d/record-%d", resourceIndex, scopeIndex, recordIndex))
				logRecord.Body().SetStr(fmt.Sprintf("body-%d", recordIndex))
				logRecord.Attributes().PutInt("record.index", int64(recordIndex))
			}
		}
	}
	return logs
}

func TestBatchLogProcessor_Shutdown(t *testing.T) {
	cfg := &Config{
		Timeout:       3 * time.Second,
		SendBatchSize: 1000,
	}
	requestCount := 5
	logsPerRequest := 10
	sink := new(consumertest.LogsSink)

	logs, err := NewFactory().CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, logs.Start(context.Background(), componenttest.NewNopHost()))

	for range requestCount {
		ld := testdata.GenerateLogs(logsPerRequest)
		require.NoError(t, logs.ConsumeLogs(context.Background(), ld))
	}

	require.NoError(t, logs.Shutdown(context.Background()))

	require.Equal(t, requestCount*logsPerRequest, sink.LogRecordCount())
	require.Len(t, sink.AllLogs(), 1)
}

func getTestLogSeverityText(requestNum, index int) string {
	return fmt.Sprintf("test-log-int-%d-%d", requestNum, index)
}

func logsReceivedBySeverityText(lds []plog.Logs) map[string]plog.LogRecord {
	logsReceivedBySeverityText := map[string]plog.LogRecord{}
	for i := range lds {
		ld := lds[i]
		rms := ld.ResourceLogs()
		for i := 0; i < rms.Len(); i++ {
			ilms := rms.At(i).ScopeLogs()
			for j := 0; j < ilms.Len(); j++ {
				logs := ilms.At(j).LogRecords()
				for k := 0; k < logs.Len(); k++ {
					log := logs.At(k)
					logsReceivedBySeverityText[log.SeverityText()] = log
				}
			}
		}
	}
	return logsReceivedBySeverityText
}

func TestShutdown(t *testing.T) {
	factory := NewFactory()
	processortest.VerifyShutdown(t, factory, factory.CreateDefaultConfig())
}

type metadataTracesSink struct {
	*consumertest.TracesSink

	lock               sync.Mutex
	spanCountByToken12 map[string]int
}

func formatTwo(first, second []string) string {
	return fmt.Sprintf("%s;%s", first, second)
}

func (mts *metadataTracesSink) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	info := client.FromContext(ctx)
	token1 := info.Metadata.Get("token1")
	token2 := info.Metadata.Get("token2")
	mts.lock.Lock()
	defer mts.lock.Unlock()

	mts.spanCountByToken12[formatTwo(
		token1,
		token2,
	)] += td.SpanCount()
	return mts.TracesSink.ConsumeTraces(ctx, td)
}

func TestBatchProcessorSpansBatchedByMetadata(t *testing.T) {
	sink := &metadataTracesSink{
		TracesSink:         &consumertest.TracesSink{},
		spanCountByToken12: map[string]int{},
	}
	cfg := createDefaultConfig().(*Config)
	cfg.SendBatchSize = 1000
	cfg.Timeout = 10 * time.Minute
	cfg.MetadataKeys = []string{"token1", "token2"}
	traces, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, traces.Start(context.Background(), componenttest.NewNopHost()))

	bg := context.Background()
	callCtxs := []context.Context{
		client.NewContext(bg, client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"token1": {"single"},
				"token3": {"n/a"},
			}),
		}),
		client.NewContext(bg, client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"token1": {"single"},
				"token2": {"one", "two"},
				"token4": {"n/a"},
			}),
		}),
		client.NewContext(bg, client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"token1": nil,
				"token2": {"single"},
			}),
		}),
		client.NewContext(bg, client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"token1": {"one", "two", "three"},
				"token2": {"single"},
				"token3": {"n/a"},
				"token4": {"n/a", "d/c"},
			}),
		}),
	}
	expectByContext := make([]int, len(callCtxs))

	requestCount := 1000
	spansPerRequest := 33
	sentResourceSpans := ptrace.NewTraces().ResourceSpans()
	for requestNum := range requestCount {
		td := testdata.GenerateTraces(spansPerRequest)
		spans := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
		for spanIndex := range spansPerRequest {
			spans.At(spanIndex).SetName(getTestSpanName(requestNum, spanIndex))
		}
		td.ResourceSpans().At(0).CopyTo(sentResourceSpans.AppendEmpty())
		// use round-robin to assign context.
		num := requestNum % len(callCtxs)
		expectByContext[num] += spansPerRequest
		require.NoError(t, traces.ConsumeTraces(callCtxs[num], td))
	}

	require.NoError(t, traces.Shutdown(context.Background()))

	// The following tests are the same as TestBatchProcessorSpansDelivered().
	require.Equal(t, requestCount*spansPerRequest, sink.SpanCount())
	receivedTraces := sink.AllTraces()
	spansReceivedByName := spansReceivedByName(receivedTraces)
	for requestNum := range requestCount {
		spans := sentResourceSpans.At(requestNum).ScopeSpans().At(0).Spans()
		for spanIndex := range spansPerRequest {
			require.Equal(t,
				spans.At(spanIndex),
				spansReceivedByName[getTestSpanName(requestNum, spanIndex)])
		}
	}

	// This test ensures each context had the expected number of spans.
	require.Len(t, sink.spanCountByToken12, len(callCtxs))
	for idx, ctx := range callCtxs {
		md := client.FromContext(ctx).Metadata
		exp := formatTwo(md.Get("token1"), md.Get("token2"))
		require.Equal(t, expectByContext[idx], sink.spanCountByToken12[exp])
	}
}

func TestBatchProcessorDuplicateMetadataKeys(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.MetadataKeys = []string{"myTOKEN", "mytoken"}
	err := cfg.Validate()
	require.ErrorContains(t, err, "duplicate")
	require.ErrorContains(t, err, "mytoken")
}

func TestBatchProcessorMetadataCardinalityLimit(t *testing.T) {
	const cardLimit = 10

	sink := new(consumertest.TracesSink)
	cfg := createDefaultConfig().(*Config)
	cfg.MetadataKeys = []string{"token"}
	cfg.MetadataCardinalityLimit = cardLimit
	traces, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, traces.Start(context.Background(), componenttest.NewNopHost()))

	bg := context.Background()
	for requestNum := range cardLimit {
		td := testdata.GenerateTraces(1)
		ctx := client.NewContext(bg, client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"token": {strconv.Itoa(requestNum)},
			}),
		})

		require.NoError(t, traces.ConsumeTraces(ctx, td))
	}

	td := testdata.GenerateTraces(1)
	ctx := client.NewContext(bg, client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"token": {"limit_exceeded"},
		}),
	})
	err = traces.ConsumeTraces(ctx, td)

	require.Error(t, err)
	assert.True(t, consumererror.IsPermanent(err))
	require.ErrorContains(t, err, "too many")

	require.NoError(t, traces.Shutdown(context.Background()))
}

func TestBatchZeroConfig(t *testing.T) {
	// This is a no-op configuration. No need for a timer, no
	// minimum, no maximum, just a pass through.
	cfg := &Config{}

	require.NoError(t, cfg.Validate())

	const requestCount = 5
	const logsPerRequest = 10
	sink := new(consumertest.LogsSink)
	logs, err := NewFactory().CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, logs.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, logs.Shutdown(context.Background())) }()

	expect := 0
	for requestNum := range requestCount {
		cnt := logsPerRequest + requestNum
		expect += cnt
		ld := testdata.GenerateLogs(cnt)
		require.NoError(t, logs.ConsumeLogs(context.Background(), ld))
	}

	// Wait for all batches.
	require.Eventually(t, func() bool {
		return sink.LogRecordCount() == expect
	}, time.Second, 5*time.Millisecond)

	// Expect them to be the original sizes.
	receivedMds := sink.AllLogs()
	require.Len(t, receivedMds, requestCount)
	for i, ld := range receivedMds {
		require.Equal(t, 1, ld.ResourceLogs().Len())
		require.Equal(t, logsPerRequest+i, ld.LogRecordCount())
	}
}

func TestBatchSplitOnly(t *testing.T) {
	const maxBatch = 10
	const requestCount = 5
	const logsPerRequest = 100

	cfg := &Config{
		SendBatchMaxSize: maxBatch,
	}

	require.NoError(t, cfg.Validate())

	sink := new(consumertest.LogsSink)
	logs, err := NewFactory().CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, logs.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, logs.Shutdown(context.Background())) }()

	for range requestCount {
		ld := testdata.GenerateLogs(logsPerRequest)
		require.NoError(t, logs.ConsumeLogs(context.Background(), ld))
	}

	// Wait for all batches.
	require.Eventually(t, func() bool {
		return sink.LogRecordCount() == logsPerRequest*requestCount
	}, time.Second, 5*time.Millisecond)

	// Expect them to be the limited by maxBatch.
	receivedMds := sink.AllLogs()
	require.Len(t, receivedMds, requestCount*logsPerRequest/maxBatch)
	for _, ld := range receivedMds {
		require.Equal(t, maxBatch, ld.LogRecordCount())
	}
}
