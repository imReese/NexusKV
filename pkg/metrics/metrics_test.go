// pkg/metrics/metrics_test.go
package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMetricsInitializationAndHandler(t *testing.T) {
	Init()

	CacheLookupsTotal.WithLabelValues("tenant_a", "llama_70b", "hit").Inc()
	PrefillSavedTokensTotal.WithLabelValues("tenant_a", "llama_70b").Add(1024)
	ActivePinnedMemoryBytes.Set(1073741824)
	FailOpenFallbacksTotal.WithLabelValues("timeout").Inc()

	handler := Handler()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status OK, got %d", rec.Code)
	}

	body := rec.Body.String()
	if body == "" {
		t.Fatal("expected non-empty metrics output")
	}
}
