// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/pipelines"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
)

const (
	benchmarkMetricResourceCount = 8
	benchmarkMetricScopeCount    = 8
	benchmarkMetricCount         = 8
	benchmarkDataPointCount      = 4
	profileBatchSize             = 32
	profileBatches               = 1536
	counterProbeBatches          = 48
)

var queueBatchBenchmarkExporterType = component.MustNewType("queuebatchbenchmark")

// BenchmarkQueueBatchTelemetryNormalLargeMetrics measures one exporter
// admission call under the service's normal telemetry configuration. The
// service constructs the SDK from its own default views before it constructs
// the queued exporter, so this follows the same telemetry configuration path
// that determines whether the batch-size histograms are dropped in a running
// Collector. Input construction is deliberately outside the timed Offer path;
// every iteration owns a distinct request while the live queue worker consumes
// previous requests asynchronously.
func BenchmarkQueueBatchTelemetryNormalLargeMetrics(b *testing.B) {
	benchmarkQueueBatchTelemetryOffer(b, configtelemetry.LevelNormal)
}

// BenchmarkQueueBatchTelemetryDetailedLargeMetrics is the paired control for
// the normal benchmark. It keeps the same exporter, queue, request shape, and
// asynchronous worker while enabling the service's detailed telemetry views.
func BenchmarkQueueBatchTelemetryDetailedLargeMetrics(b *testing.B) {
	benchmarkQueueBatchTelemetryOffer(b, configtelemetry.LevelDetailed)
}

// BenchmarkQueueBatchTelemetryDetailedFinalBatchObservation compares detailed
// histogram totals with the actual encoded post-merge/split exports. It samples
// two legitimate batch shapes at runtime: three one-point requests become one
// final batch, while a 1+1+4-point group splits into two final batches. The
// varied final-batch count makes an eager per-Offer observation distinguishable
// from a final-batch observer without relying on a constant synthetic shape.
func BenchmarkQueueBatchTelemetryDetailedFinalBatchObservation(b *testing.B) {
	port := freeBenchmarkPrometheusPort(b)
	received := make(chan pmetric.Metrics, 2)
	exporter, exported := newQueueBatchTelemetryBenchmarkExporterWithOptions(b, configtelemetry.LevelDetailed, 3, port, queueBatchTelemetryBenchmarkOptions{
		batchMaxSize: 3,
		capture:      received,
	})

	var (
		expectedBytes        float64
		expectedItems        float64
		expectedFinalBatches int64
		expectedExports      int64
	)
	ctx := context.Background()
	marshaler := pmetric.ProtoMarshaler{}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for b.Loop() {
		b.StopTimer()
		pointCounts := [3]int{1, 1, 1}
		finalBatches := int64(1)
		if rng.Intn(2) != 0 {
			pointCounts[2] = 4
			finalBatches = 2
		}
		inputs := make([]pmetric.Metrics, len(pointCounts))
		for index, points := range pointCounts {
			inputs[index] = benchmarkMetricsWithPoints(int(expectedExports*10)+index, points)
		}
		expectedFinalBatches += finalBatches
		expectedExports += finalBatches
		b.StartTimer()

		for _, input := range inputs {
			if err := exporter.ConsumeMetrics(ctx, input); err != nil {
				b.Fatal(err)
			}
		}
		waitForBenchmarkExports(b, exported, expectedExports)
		b.StopTimer()
		for range finalBatches {
			output := <-received
			expectedBytes += float64(marshaler.MetricsSize(output))
			expectedItems += float64(output.DataPointCount())
		}
		b.StartTimer()
	}

	b.StopTimer()
	itemsCount, itemsSum := scrapeBenchmarkHistogram(b, port, "otelcol_exporter_queue_batch_send_size")
	bytesCount, bytesSum := scrapeBenchmarkHistogram(b, port, "otelcol_exporter_queue_batch_send_size_bytes")
	require.NotZero(b, itemsCount)
	require.NotZero(b, bytesCount)
	require.NotZero(b, expectedBytes)

	operations := float64(b.N)
	finalBatches := float64(expectedFinalBatches)
	itemsCountError := math.Abs(itemsCount-finalBatches) / finalBatches
	bytesCountError := math.Abs(bytesCount-finalBatches) / finalBatches
	observationError := math.Max(itemsCountError, bytesCountError)
	bytesError := math.Abs(bytesSum-expectedBytes) / expectedBytes
	itemsError := math.Abs(itemsSum-expectedItems) / expectedItems
	b.ReportMetric(itemsCount/operations, "FinalBatch_observations/op")
	b.ReportMetric(bytesCount/operations, "FinalBatch_byte_observations/op")
	b.ReportMetric(observationError, "FinalBatch_observation_coverage_relative_error/op")
	b.ReportMetric(itemsSum/itemsCount, "FinalBatch_items_per_observation")
	b.ReportMetric(bytesError, "FinalBatch_bytes_relative_error/op")
	b.ReportMetric(itemsError, "FinalBatch_items_relative_error/op")
	b.ReportMetric(math.Max(observationError, math.Max(bytesError, itemsError)), "FinalBatch_accounting_relative_error/op")
}

// BenchmarkQueueBatchTelemetryDetailedSplitFinalBatchObservation verifies the
// same detailed histograms after one oversized request is split by the batcher.
// The expected values come from the actual encoded split outputs, not the
// pre-split input. This distinguishes final-batch observation from the eager
// admission observations used by the baseline.
func BenchmarkQueueBatchTelemetryDetailedSplitFinalBatchObservation(b *testing.B) {
	port := freeBenchmarkPrometheusPort(b)
	received := make(chan pmetric.Metrics, 4)
	exporter, exported := newQueueBatchTelemetryBenchmarkExporterWithOptions(b, configtelemetry.LevelDetailed, 1, port, queueBatchTelemetryBenchmarkOptions{
		batchMaxSize: 1,
		capture:      received,
	})

	var (
		expectedBytes        float64
		expectedItems        float64
		expectedObservations int64
	)
	ctx := context.Background()
	marshaler := pmetric.ProtoMarshaler{}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for b.Loop() {
		b.StopTimer()
		input := benchmarkSplitMetrics(int(expectedObservations), 3+rng.Intn(2))
		outputCount := int64(input.DataPointCount())
		expectedObservations += outputCount
		b.StartTimer()

		if err := exporter.ConsumeMetrics(ctx, input); err != nil {
			b.Fatal(err)
		}
		waitForBenchmarkExports(b, exported, expectedObservations)
		b.StopTimer()
		for range outputCount {
			output := <-received
			expectedBytes += float64(marshaler.MetricsSize(output))
			expectedItems += float64(output.DataPointCount())
		}
		b.StartTimer()
	}

	b.StopTimer()
	itemsCount, itemsSum := scrapeBenchmarkHistogram(b, port, "otelcol_exporter_queue_batch_send_size")
	bytesCount, bytesSum := scrapeBenchmarkHistogram(b, port, "otelcol_exporter_queue_batch_send_size_bytes")
	require.NotZero(b, expectedObservations)
	require.NotZero(b, expectedBytes)

	observations := float64(expectedObservations)
	itemsCountError := math.Abs(itemsCount-observations) / observations
	bytesCountError := math.Abs(bytesCount-observations) / observations
	bytesError := math.Abs(bytesSum-expectedBytes) / expectedBytes
	itemsError := math.Abs(itemsSum-expectedItems) / expectedItems
	observationError := math.Max(itemsCountError, bytesCountError)
	b.ReportMetric(observationError, "SplitFinalBatch_observation_coverage_relative_error/op")
	b.ReportMetric(bytesError, "SplitFinalBatch_bytes_relative_error/op")
	b.ReportMetric(itemsError, "SplitFinalBatch_items_relative_error/op")
	b.ReportMetric(math.Max(observationError, math.Max(bytesError, itemsError)), "SplitFinalBatch_accounting_relative_error/op")
}

// TestQueueBatchTelemetryNormalOfferCPUProfile captures only the repeated
// Normal asynchronous Offer batches. Its request ring is built before profiling
// and a ring slot is reused only after the queue worker has exported that whole
// batch, so the profile neither races request ownership nor attributes payload
// construction to the admission path.
func TestQueueBatchTelemetryNormalOfferCPUProfile(t *testing.T) {
	testQueueBatchTelemetryOfferCPUProfile(t, configtelemetry.LevelNormal, false)
}

// TestQueueBatchTelemetryBasicOfferCPUProfile is the Basic-view control for the
// Normal profile probe.
func TestQueueBatchTelemetryBasicOfferCPUProfile(t *testing.T) {
	testQueueBatchTelemetryOfferCPUProfile(t, configtelemetry.LevelBasic, false)
}

// TestQueueBatchTelemetryNoneOfferCPUProfile verifies the disabled telemetry
// view has no producer-side byte sizing work.
func TestQueueBatchTelemetryNoneOfferCPUProfile(t *testing.T) {
	testQueueBatchTelemetryOfferCPUProfile(t, configtelemetry.LevelNone, false)
}

// TestQueueBatchTelemetryExplicitByteSizerOfferCPUProfile keeps the queue's
// configured byte sizer while measuring the removal of the extra telemetry
// sizing call.
func TestQueueBatchTelemetryExplicitByteSizerOfferCPUProfile(t *testing.T) {
	testQueueBatchTelemetryOfferCPUProfile(t, configtelemetry.LevelNormal, true)
}

func testQueueBatchTelemetryOfferCPUProfile(t *testing.T, level configtelemetry.Level, queueSizerBytes bool) {
	t.Helper()
	profilePath := os.Getenv("PERFLOOP_CPU_PROFILE")
	if profilePath == "" {
		t.Skip("PERFLOOP_CPU_PROFILE is required for the profile probe")
	}

	exporter, exported := newQueueBatchTelemetryBenchmarkExporter(t, level, 1, 0, queueSizerBytes)
	requests := queueBatchTelemetryProbeRequests()

	// Keep construction and a collection cycle outside the profile interval.
	runtime.GC()
	profileFile, err := os.Create(profilePath)
	require.NoError(t, err)
	defer func() { require.NoError(t, profileFile.Close()) }()
	require.NoError(t, pprof.StartCPUProfile(profileFile))
	profiling := true
	defer func() {
		if profiling {
			pprof.StopCPUProfile()
		}
	}()

	runQueueBatchTelemetryOfferProbe(t, exporter, exported, requests, profileBatches, true)
	pprof.StopCPUProfile()
	profiling = false
}

// TestQueueBatchTelemetryNormalOfferCounter exposes the non-profiled Normal
// Offer path to the coverage-based SizeProto counter.
func TestQueueBatchTelemetryNormalOfferCounter(t *testing.T) {
	exporter, exported := newQueueBatchTelemetryBenchmarkExporter(t, configtelemetry.LevelNormal, 1, 0, false)
	runQueueBatchTelemetryOfferProbe(t, exporter, exported, queueBatchTelemetryProbeRequests(), counterProbeBatchCount(), false)
}

// TestQueueBatchTelemetryBasicOfferCounter verifies that the service's Basic
// telemetry view takes the same no-byte-observer admission path as Normal.
func TestQueueBatchTelemetryBasicOfferCounter(t *testing.T) {
	exporter, exported := newQueueBatchTelemetryBenchmarkExporter(t, configtelemetry.LevelBasic, 1, 0, false)
	runQueueBatchTelemetryOfferProbe(t, exporter, exported, queueBatchTelemetryProbeRequests(), counterProbeBatchCount(), false)
}

// TestQueueBatchTelemetryNoneOfferCounter verifies the disabled telemetry view
// also takes the no-byte-observer Offer path.
func TestQueueBatchTelemetryNoneOfferCounter(t *testing.T) {
	exporter, exported := newQueueBatchTelemetryBenchmarkExporter(t, configtelemetry.LevelNone, 1, 0, false)
	runQueueBatchTelemetryOfferProbe(t, exporter, exported, queueBatchTelemetryProbeRequests(), counterProbeBatchCount(), false)
}

// TestQueueBatchTelemetryExplicitByteSizerOfferCounter is the demand-driven
// control: a queue explicitly configured with SizerTypeBytes must still call
// the request byte sizer even when Normal telemetry drops the histograms.
func TestQueueBatchTelemetryExplicitByteSizerOfferCounter(t *testing.T) {
	exporter, exported := newQueueBatchTelemetryBenchmarkExporter(t, configtelemetry.LevelNormal, 1, 0, true)
	runQueueBatchTelemetryOfferProbe(t, exporter, exported, queueBatchTelemetryProbeRequests(), counterProbeBatchCount(), false)
}

// TestQueueBatchTelemetryExplicitByteSizerBatchAccounting verifies the byte
// value used at the configured byte-size boundary. The batch threshold is the
// exact sum of two owned requests, and the captured batch must contain both
// requests with the exact post-merge protobuf size.
func TestQueueBatchTelemetryExplicitByteSizerBatchAccounting(t *testing.T) {
	first := benchmarkSmallMetrics(1, 1)
	second := benchmarkSmallMetrics(2, 2)
	marshaler := pmetric.ProtoMarshaler{}
	expected := mergeBenchmarkMetrics(first, second)
	expectedBatchBytes := marshaler.MetricsSize(expected)
	byteThreshold := int64(marshaler.MetricsSize(first) + marshaler.MetricsSize(second))
	received := make(chan pmetric.Metrics, 2)

	exporter, exported := newQueueBatchTelemetryBenchmarkExporterWithOptions(t, configtelemetry.LevelNormal, byteThreshold, 0, queueBatchTelemetryBenchmarkOptions{
		queueSizerBytes: true,
		batchSizerBytes: true,
		capture:         received,
	})
	ctx := context.Background()
	require.NoError(t, exporter.ConsumeMetrics(ctx, first))
	require.NoError(t, exporter.ConsumeMetrics(ctx, second))
	waitForBenchmarkExports(t, exported, 1)

	observed := <-received
	require.Equal(t, expected.DataPointCount(), observed.DataPointCount())
	require.Equal(t, expectedBatchBytes, marshaler.MetricsSize(observed))
}

// TestQueueBatchTelemetryExplicitByteSizerQueueCapacityAccounting verifies the
// queue-side byte boundary. While the first exported request is deliberately
// held before it is marked done, two requests whose protobuf sizes exactly fill
// the configured byte capacity must be admitted and a third must be rejected.
func TestQueueBatchTelemetryExplicitByteSizerQueueCapacityAccounting(t *testing.T) {
	first := benchmarkSmallMetrics(1, 1)
	second := benchmarkSmallMetrics(2, 2)
	third := benchmarkSmallMetrics(3, 3)
	marshaler := pmetric.ProtoMarshaler{}
	queueCapacity := int64(marshaler.MetricsSize(first) + marshaler.MetricsSize(second))
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	exporter, exported := newQueueBatchTelemetryBenchmarkExporterWithOptions(t, configtelemetry.LevelNormal, 1, 0, queueBatchTelemetryBenchmarkOptions{
		queueSizerBytes: true,
		queueSize:       queueCapacity,
		onExport: func(pmetric.Metrics) {
			select {
			case started <- struct{}{}:
				<-release
			default:
			}
		},
	})
	ctx := context.Background()
	require.NoError(t, exporter.ConsumeMetrics(ctx, first))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first byte-sized request did not reach the held exporter")
	}
	require.NoError(t, exporter.ConsumeMetrics(ctx, second))
	require.ErrorIs(t, exporter.ConsumeMetrics(ctx, third), exporterhelper.ErrQueueIsFull)
	close(release)
	waitForBenchmarkExports(t, exported, 2)
}

// counterProbeBatchCount selects a nearby valid repeat count for each process.
// The coverage probes report absolute calls as well as calls/op, so their
// sample-level counter remains observable while every request still follows the
// same service-built queue path.
func counterProbeBatchCount() int {
	return counterProbeBatches + int(time.Now().UnixNano()%17)
}

func queueBatchTelemetryProbeRequests() []pmetric.Metrics {
	requests := make([]pmetric.Metrics, profileBatchSize)
	for index := range requests {
		requests[index] = benchmarkLargeMetrics(index)
	}
	return requests
}

func runQueueBatchTelemetryOfferProbe(t *testing.T, exporter exporter.Metrics, exported *atomic.Int64, requests []pmetric.Metrics, batches int, labelOfferCPU bool) {
	t.Helper()
	ctx := context.Background()
	var want int64
	offerRequests := func(ctx context.Context) {
		for _, metrics := range requests {
			require.NoError(t, exporter.ConsumeMetrics(ctx, metrics))
			want++
		}
	}
	for range batches {
		if labelOfferCPU {
			// CPU profiling is process-wide. Label only the producer goroutine's
			// ConsumeMetrics calls so the reporting command can exclude queue-worker,
			// exporter, wait, and scheduler samples from the Offer-path denominator.
			pprof.Do(ctx, pprof.Labels("perfloop_phase", "offer"), offerRequests)
		} else {
			offerRequests(ctx)
		}
		waitForBenchmarkExports(t, exported, want)
	}
	t.Logf("PERFLOOP_PROFILE_OPERATIONS=%d", profileBatchSize*batches)
}

func benchmarkQueueBatchTelemetryOffer(b *testing.B, level configtelemetry.Level) {
	exporter, exported := newQueueBatchTelemetryBenchmarkExporter(b, level, 1, 0, false)

	// b.Loop chooses its final iteration count after setup. This reserve avoids
	// growing the latency slice in the measured path for the intended bench time.
	latencies := make([]int64, 0, 1_000_000)
	ctx := context.Background()
	requestIndex := 0
	for b.Loop() {
		// The queue batcher is allowed to move data when it batches. Give each
		// asynchronous Offer a distinct request and exclude upstream request
		// construction from the producer-admission metric.
		b.StopTimer()
		metrics := benchmarkLargeMetrics(requestIndex)
		requestIndex++
		b.StartTimer()

		started := time.Now()
		if err := exporter.ConsumeMetrics(ctx, metrics); err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, time.Since(started).Nanoseconds())
	}

	b.StopTimer()
	waitForBenchmarkExports(b, exported, int64(len(latencies)))
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	b.ReportMetric(float64(latencies[(len(latencies)*95+99)/100-1]), "p95_ns/op")
	b.ReportMetric(float64(latencies[(len(latencies)*99+99)/100-1]), "p99_ns/op")
}

// newQueueBatchTelemetryBenchmarkExporter builds the exporter through Service
// rather than constructing SDK views in the benchmark. In particular,
// otelconftelemetry.NewFactory calls the service's metric-view compiler and
// applies the resulting normal or detailed views to the live MeterProvider.
func newQueueBatchTelemetryBenchmarkExporter(tb testing.TB, level configtelemetry.Level, batchMinSize int64, prometheusPort int, queueSizerBytes bool) (exporter.Metrics, *atomic.Int64) {
	return newQueueBatchTelemetryBenchmarkExporterWithOptions(tb, level, batchMinSize, prometheusPort, queueBatchTelemetryBenchmarkOptions{
		queueSizerBytes: queueSizerBytes,
	})
}

type queueBatchTelemetryBenchmarkOptions struct {
	queueSizerBytes bool
	batchSizerBytes bool
	batchMaxSize    int64
	queueSize       int64
	capture         chan<- pmetric.Metrics
	onExport        func(pmetric.Metrics)
}

func newQueueBatchTelemetryBenchmarkExporterWithOptions(tb testing.TB, level configtelemetry.Level, batchMinSize int64, prometheusPort int, options queueBatchTelemetryBenchmarkOptions) (exporter.Metrics, *atomic.Int64) {
	var built exporter.Metrics
	exported := &atomic.Int64{}

	queueConfig := exporterhelper.NewDefaultQueueConfig()
	queueConfig.QueueSize = 1_000_000_000
	if options.queueSize > 0 {
		queueConfig.QueueSize = options.queueSize
	}
	queueConfig.NumConsumers = 1
	if options.queueSizerBytes {
		require.NoError(tb, queueConfig.Sizer.UnmarshalText([]byte("bytes")))
	}
	batchConfig := queueConfig.Batch.GetOrInsertDefault()
	batchConfig.FlushTimeout = time.Hour
	batchConfig.MinSize = batchMinSize
	if options.batchMaxSize > 0 {
		batchConfig.MaxSize = options.batchMaxSize
	}
	if options.batchSizerBytes {
		require.NoError(tb, batchConfig.Sizer.UnmarshalText([]byte("bytes")))
	}

	exporterFactory := exporter.NewFactory(
		queueBatchBenchmarkExporterType,
		func() component.Config { return &queueBatchBenchmarkConfig{} },
		exporter.WithMetrics(func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
			exporter, err := exporterhelper.NewMetrics(
				ctx,
				set,
				cfg,
				func(_ context.Context, metrics pmetric.Metrics) error {
					if options.capture != nil {
						options.capture <- metrics
					}
					if options.onExport != nil {
						options.onExport(metrics)
					}
					exported.Add(1)
					return nil
				},
				exporterhelper.WithQueue(configoptional.Some(queueConfig)),
			)
			if err == nil {
				built = exporter
			}
			return exporter, err
		}, component.StabilityLevelStable),
	)

	receiverFactory := receivertest.NewNopFactory()
	receiverID := component.NewID(receiverFactory.Type())
	exporterID := component.NewID(queueBatchBenchmarkExporterType)
	telemetryConfig := otelconftelemetry.NewFactory().CreateDefaultConfig().(*otelconftelemetry.Config)
	telemetryConfig.Metrics.Level = level
	// The real telemetry factory creates the Prometheus reader. Normal and
	// detailed timing probes use port zero; the final-batch probe supplies a
	// short-lived dynamically selected port so it can scrape its histogram.
	*telemetryConfig.Metrics.Readers[0].Pull.Exporter.Prometheus.Host = "127.0.0.1"
	*telemetryConfig.Metrics.Readers[0].Pull.Exporter.Prometheus.Port = prometheusPort

	srv, err := service.New(context.Background(), service.Settings{
		BuildInfo: component.NewDefaultBuildInfo(),
		ReceiversConfigs: map[component.ID]component.Config{
			receiverID: receiverFactory.CreateDefaultConfig(),
		},
		ReceiversFactories: map[component.Type]receiver.Factory{
			receiverFactory.Type(): receiverFactory,
		},
		ExportersConfigs: map[component.ID]component.Config{
			exporterID: &queueBatchBenchmarkConfig{},
		},
		ExportersFactories: map[component.Type]exporter.Factory{
			queueBatchBenchmarkExporterType: exporterFactory,
		},
		AsyncErrorChannel: make(chan error, 1),
		TelemetryFactory:  otelconftelemetry.NewFactory(),
	}, service.Config{
		Telemetry: telemetryConfig,
		Pipelines: pipelines.Config{
			pipeline.NewID(pipeline.SignalMetrics): {
				Receivers: []component.ID{receiverID},
				Exporters: []component.ID{exporterID},
			},
		},
	})
	require.NoError(tb, err)
	assertQueueBatchTelemetryViews(tb, telemetryConfig, level)
	require.NotNil(tb, built)
	require.NoError(tb, srv.Start(context.Background()))
	tb.Cleanup(func() { require.NoError(tb, srv.Shutdown(context.Background())) })

	return built, exported
}

type queueBatchBenchmarkConfig struct{}

const queueBatchTelemetryMeter = "go.opentelemetry.io/collector/exporter/exporterhelper"

func assertQueueBatchTelemetryViews(tb testing.TB, cfg *otelconftelemetry.Config, level configtelemetry.Level) {
	tb.Helper()
	if level == configtelemetry.LevelNone {
		require.Empty(tb, cfg.Metrics.Views)
		return
	}
	dropped := map[string]bool{}
	for _, view := range cfg.Metrics.Views {
		if view.Selector == nil || view.Selector.MeterName == nil || view.Selector.InstrumentName == nil {
			continue
		}
		if *view.Selector.MeterName == queueBatchTelemetryMeter {
			dropped[*view.Selector.InstrumentName] = true
		}
	}
	for _, instrument := range []string{
		"otelcol_exporter_queue_batch_send_size",
		"otelcol_exporter_queue_batch_send_size_bytes",
	} {
		if level < configtelemetry.LevelDetailed {
			require.Truef(tb, dropped[instrument], "service normal views must drop %s", instrument)
		} else {
			require.Falsef(tb, dropped[instrument], "service detailed views must retain %s", instrument)
		}
	}
}

func benchmarkLargeMetrics(seed int) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	for resourceIndex := range benchmarkMetricResourceCount {
		resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
		resourceMetrics.Resource().Attributes().PutStr("resource", strconv.Itoa(seed*benchmarkMetricResourceCount+resourceIndex))
		for scopeIndex := range benchmarkMetricScopeCount {
			scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
			scopeMetrics.Scope().SetName("scope-" + strconv.Itoa(scopeIndex))
			for metricIndex := range benchmarkMetricCount {
				metric := scopeMetrics.Metrics().AppendEmpty()
				metric.SetName("metric-" + strconv.Itoa(metricIndex))
				dataPoints := metric.SetEmptyGauge().DataPoints()
				for pointIndex := range benchmarkDataPointCount {
					dataPoint := dataPoints.AppendEmpty()
					dataPoint.SetIntValue(int64(seed + resourceIndex + scopeIndex + metricIndex + pointIndex))
					dataPoint.Attributes().PutStr("point", strconv.Itoa(pointIndex))
				}
			}
		}
	}
	return metrics
}

func waitForBenchmarkExports(tb testing.TB, exported *atomic.Int64, want int64) {
	deadline := time.Now().Add(10 * time.Second)
	for exported.Load() != want {
		if time.Now().After(deadline) {
			tb.Fatalf("queue exported %d requests, want %d", exported.Load(), want)
		}
		runtime.Gosched()
	}
}

func benchmarkSplitMetrics(seed, points int) pmetric.Metrics {
	return benchmarkMetricsWithPoints(seed, points)
}

func benchmarkMetricsWithPoints(seed, points int) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	resourceMetrics.Resource().Attributes().PutStr("resource", strconv.Itoa(seed))
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	scopeMetrics.Scope().SetName("scope")
	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName("metric")
	dataPoints := metric.SetEmptyGauge().DataPoints()
	for point := range points {
		dataPoints.AppendEmpty().SetIntValue(int64(seed + point))
	}
	return metrics
}

func benchmarkSmallMetrics(seed, value int) pmetric.Metrics {
	metrics := benchmarkMetricsWithPoints(seed, 1)
	metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).SetIntValue(int64(value))
	return metrics
}

func mergeBenchmarkMetrics(metrics ...pmetric.Metrics) pmetric.Metrics {
	merged := pmetric.NewMetrics()
	for _, input := range metrics {
		resourceMetrics := input.ResourceMetrics()
		for index := range resourceMetrics.Len() {
			resourceMetrics.At(index).CopyTo(merged.ResourceMetrics().AppendEmpty())
		}
	}
	return merged
}

func freeBenchmarkPrometheusPort(tb testing.TB) int {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(tb, listener.Close())
	return port
}

func scrapeBenchmarkHistogram(tb testing.TB, port int, name string) (float64, float64) {
	tb.Helper()
	url := "http://127.0.0.1:" + strconv.Itoa(port) + "/metrics"
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, err := (&http.Client{Timeout: time.Second}).Get(url)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr == nil && closeErr == nil {
				count, sum, found := histogramValues(string(body), name)
				if found {
					return count, sum
				}
			}
		}
		if time.Now().After(deadline) {
			tb.Fatalf("histogram %s was not exported by %s", name, url)
		}
		runtime.Gosched()
	}
}

func histogramValues(body, name string) (float64, float64, bool) {
	var (
		count      float64
		sum        float64
		foundCount bool
		foundSum   bool
	)
	for _, line := range strings.Split(body, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		switch {
		case strings.HasPrefix(fields[0], name+"_count"):
			count += value
			foundCount = true
		case strings.HasPrefix(fields[0], name+"_sum"):
			sum += value
			foundSum = true
		}
	}
	return count, sum, foundCount && foundSum
}
