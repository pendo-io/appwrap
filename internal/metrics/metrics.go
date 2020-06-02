package metrics

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	ConnectionPoolUsageResultHit     = "hit"
	ConnectionPoolUsageResultMiss    = "miss"
	ConnectionPoolUsageResultTimeout = "timeout"

	ConnectionPoolConnectionStateIdle   = "idle"
	ConnectionPoolConnectionStateActive = "active"

	EnvMetricsRecordingIntervalSeconds = "metrics_recording_interval_seconds"
)

var (
	// KeyConnectionShard is used to differentiate between each connection shard, as our client shards the
	// usage across multiple Redis servers based on hashing keys.
	KeyConnectionShard = tag.MustNewKey("connection_shard")

	// MMemoryStoreConnectionPoolUsage is used to track the connection pool's usage (hits, misses, timeouts).
	MMemoryStoreConnectionPoolUsage = stats.Int64("memorystore/redis_connection_pool_usage",
		"Redis client connection pool usage statistics", "")

	// KeyPoolUsageResult is used to group pool accesses by the result (hit, miss, or timeout).
	KeyPoolUsageResult = tag.MustNewKey("usage_result")

	// MMemoryStoreConnectionPoolConnections is used to track the connection pool's number of connections
	// to the Redis server, and the state of that connection (idle, active).
	MMemoryStoreConnectionPoolConnections = stats.Int64("memorystore/redis_connection_pool",
		"Redis client connection pool connection statistics", "")

	// KeyPoolConnState is used to group metrics by state (e.g. idle, stale, etc.)
	KeyPoolConnState = tag.MustNewKey("state")
)

// RecordWithTagName is a convenience method for recording a metric with a given tag and name.
func RecordWithTagName(ctx context.Context, measurement stats.Measurement, key tag.Key, name string) {
	mctx, err := tag.New(ctx, tag.Insert(key, name))
	if err != nil {
		fmt.Fprintln(os.Stderr, "RecordWithTagName failed: "+err.Error())
	}
	stats.Record(mctx, measurement)
}

func registerMetricView(measure stats.Measure, description string, agg *view.Aggregation, tagKeys []tag.Key) {
	if err := view.Register(
		&view.View{Measure: measure, Description: description, Aggregation: agg, TagKeys: tagKeys},
	); err != nil {
		fmt.Fprintf(os.Stderr, "failed to register view for metric: %s\n", err)
	}
}

var metricsRecordingInterval time.Duration

// GetMetricsRecordingInterval returns the intended time between recording intervals for gathering
// metrics. The actual reporting interval is controlled by the exporter (if implemented elsewhere).
func GetMetricsRecordingInterval() time.Duration {
	return metricsRecordingInterval
}

// ParseRecordingIntervalFromEnvironment will attempt to read the environment variable and
// update the metricsRecordingInterval exported by GetMetricsRecordingInterval. Should be
// called by Init() and test functions only.
func ParseRecordingIntervalFromEnvironment() {
	metricsRecordingIntervalStr := os.Getenv(EnvMetricsRecordingIntervalSeconds)
	if metricsRecordingIntervalStr != "" {
		intervalSeconds, err := strconv.ParseInt(metricsRecordingIntervalStr, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse '%s' value: '%s': %s\n",
				EnvMetricsRecordingIntervalSeconds, metricsRecordingIntervalStr, err)
		} else {
			metricsRecordingInterval = time.Duration(intervalSeconds) * time.Second
		}
	}
}

func init() {

	// Get the recording interval from the environment.
	ParseRecordingIntervalFromEnvironment()

	// Register each stat in OpenCensus. If your application doesn't have an OpenCensus exporter, this
	// has no effect at all.

	// The MMemoryStoreConnectionPoolUsage records cumulative values for hits, misses, and timeouts. The only thing
	// that kind of works here is LastValue(). Doing this requires that the ultimate receiver of the metric use
	// aggregations within the display tool (i.e. a "delta" aggregation) to show the chart correctly.
	registerMetricView(MMemoryStoreConnectionPoolUsage, MMemoryStoreConnectionPoolUsage.Description(), view.LastValue(),
		[]tag.Key{KeyConnectionShard, KeyPoolUsageResult})

	// The MMemoryStoreConnectionPoolConnections requires a LastValue() type aggregation, as it's gauge metric.
	registerMetricView(MMemoryStoreConnectionPoolConnections, MMemoryStoreConnectionPoolConnections.Description(), view.LastValue(),
		[]tag.Key{KeyConnectionShard, KeyPoolConnState})

}
