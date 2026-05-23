package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	nodeID string
	cache  *LRUCache

	hitsTotal     prometheus.Counter
	missesTotal   prometheus.Counter
	evictTotal    prometheus.Counter
	sizeGauge     prometheus.Gauge
	capacityGauge prometheus.Gauge

	prevEvictions int64
)

// ── request / response shapes (match Python models exactly) ────────────────

type setRequest struct {
	Value string `json:"value"`
	TTL   *int   `json:"ttl"`
}

type setResponse struct {
	Key    string `json:"key"`
	Node   string `json:"node"`
	Status string `json:"status"`
}

type getResponse struct {
	Key          string `json:"key"`
	Value        string `json:"value"`
	TTLRemaining *int   `json:"ttl_remaining"`
}

type deleteResponse struct {
	Key    string `json:"key"`
	Status string `json:"status"`
}

type errorResponse struct {
	Error string  `json:"error"`
	Key   string  `json:"key"`
	Node  *string `json:"node"`
}

// ── helpers ─────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func syncMetrics() {
	s := cache.Stats()
	sizeGauge.Set(float64(s.Size))
	delta := s.Evictions - prevEvictions
	if delta > 0 {
		evictTotal.Add(float64(delta))
		prevEvictions = s.Evictions
	}
}

// ── handlers ─────────────────────────────────────────────────────────────────

func handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var req setRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	cache.Set(key, req.Value, req.TTL)
	syncMetrics()
	writeJSON(w, http.StatusOK, setResponse{Key: key, Node: nodeID, Status: "ok"})
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value, ttlRemaining, err := cache.Get(key)
	syncMetrics()

	switch err {
	case nil:
		hitsTotal.Inc()
		writeJSON(w, http.StatusOK, getResponse{Key: key, Value: value, TTLRemaining: ttlRemaining})
	case ErrExpired:
		missesTotal.Inc()
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "expired", Key: key})
	case ErrMiss:
		missesTotal.Inc()
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "miss", Key: key})
	}
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	cache.Delete(key)
	syncMetrics()
	writeJSON(w, http.StatusOK, deleteResponse{Key: key, Status: "deleted"})
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, cache.Stats())
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "node": nodeID})
}

// ── main ─────────────────────────────────────────────────────────────────────

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	nodeID = getEnv("NODE_ID", "node1")
	capacity, _ := strconv.Atoi(getEnv("CAPACITY", "100"))
	port := getEnv("NODE_PORT", "8001")

	cache = NewLRUCache(capacity)

	hitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "cache_hits_total",
		Help:        "Cache hits",
		ConstLabels: prometheus.Labels{"node": nodeID},
	})
	missesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "cache_misses_total",
		Help:        "Cache misses",
		ConstLabels: prometheus.Labels{"node": nodeID},
	})
	evictTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "cache_evictions_total",
		Help:        "Cache evictions",
		ConstLabels: prometheus.Labels{"node": nodeID},
	})
	sizeGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name:        "cache_size",
		Help:        "Current number of keys",
		ConstLabels: prometheus.Labels{"node": nodeID},
	})
	capacityGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name:        "cache_capacity",
		Help:        "Max keys (capacity)",
		ConstLabels: prometheus.Labels{"node": nodeID},
	})
	capacityGauge.Set(float64(capacity))

	mux := http.NewServeMux()
	mux.HandleFunc("POST /cache/{key}", handleSet)
	mux.HandleFunc("GET /cache/{key}", handleGet)
	mux.HandleFunc("DELETE /cache/{key}", handleDelete)
	mux.HandleFunc("GET /stats", handleStats)
	mux.HandleFunc("GET /health", handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())

	log.Printf("Go cache node %s starting on :%s (capacity=%d)", nodeID, port, capacity)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
