package fabric

import (
	"strings"
	"testing"
)

func TestPrometheusMetricsExporter(t *testing.T) {
	exporter := NewPrometheusMetricsExporter()

	exporter.RecordHit()
	exporter.RecordHit()
	exporter.RecordMiss()

	hitRate := exporter.CalculateHitRate()
	if hitRate < 0.66 || hitRate > 0.67 {
		t.Fatalf("expected hit rate ~0.666, got %f", hitRate)
	}

	exporter.RecordEffectiveGain(6090.28)
	exporter.SetHbmUsageBytes(1024 * 1024 * 1024)
	exporter.IncQuotaBackpressure()

	metricsOutput := exporter.ExportPrometheusMetrics()
	if !strings.Contains(metricsOutput, "nexuskv_cache_hit_rate 0.6667") {
		t.Fatalf("expected hit rate in metrics output, got:\n%s", metricsOutput)
	}
	if !strings.Contains(metricsOutput, "nexuskv_effective_gain_ms_total 6090.28") {
		t.Fatalf("expected effective gain in metrics output, got:\n%s", metricsOutput)
	}
	if !strings.Contains(metricsOutput, "nexuskv_hbm_usage_bytes 1073741824") {
		t.Fatalf("expected HBM usage in metrics output, got:\n%s", metricsOutput)
	}
	if !strings.Contains(metricsOutput, "nexuskv_quota_backpressure_events_total 1") {
		t.Fatalf("expected backpressure counter in metrics output, got:\n%s", metricsOutput)
	}
}
