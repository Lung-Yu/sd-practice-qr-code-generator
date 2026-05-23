package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ── globals ──────────────────────────────────────────────────────────────────

var (
	nodeURLs map[string]string
	ring     *hashRing

	requestsTotal *prometheus.CounterVec
	routeDuration prometheus.Histogram

	// Single shared client — connection-pooled, goroutine-safe.
	// MaxIdleConnsPerHost prevents exhaustion under high concurrency.
	proxyClient = &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   200,
			IdleConnTimeout:       90 * time.Second,
			DisableCompression:    true, // cache values are already plaintext
		},
	}
)

// ── response shapes ───────────────────────────────────────────────────────────

type ringResp struct {
	Key          string `json:"key"`
	Node         string `json:"node"`
	VirtualNodes int    `json:"virtual_nodes"`
}

type statsResp struct {
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
	Size      int64 `json:"size"`
	Capacity  int64 `json:"capacity"`
}

type errResp struct {
	Error string  `json:"error"`
	Key   string  `json:"key"`
	Node  *string `json:"node"`
}

// ── helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func nodeFor(key string) (nodeID, base string) {
	nodeID = ring.nodeForKey(key)
	return nodeID, nodeURLs[nodeID]
}

// proxy forwards a request to a cache node and streams the response back.
// It records Prometheus counters and the route duration histogram.
func proxy(w http.ResponseWriter, r *http.Request, targetURL, handler string) {
	start := time.Now()

	req, _ := http.NewRequestWithContext(r.Context(), r.Method, targetURL, r.Body)
	req.Header = r.Header.Clone()
	req.ContentLength = r.ContentLength

	resp, err := proxyClient.Do(req)
	routeDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		requestsTotal.WithLabelValues(handler, "503").Inc()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "node_unreachable"})
		return
	}
	defer resp.Body.Close()

	requestsTotal.WithLabelValues(handler, strconv.Itoa(resp.StatusCode)).Inc()

	// Forward headers + body verbatim — router is transparent.
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// ── route handlers ────────────────────────────────────────────────────────────

func handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	_, base := nodeFor(key)
	proxy(w, r, base+"/cache/"+key, "set")
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	_, base := nodeFor(key)
	proxy(w, r, base+"/cache/"+key, "get")
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	_, base := nodeFor(key)
	proxy(w, r, base+"/cache/"+key, "delete")
}

func handleRing(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	nodeID := ring.nodeForKey(key)
	writeJSON(w, http.StatusOK, ringResp{Key: key, Node: nodeID, VirtualNodes: ring.virtualCount()})
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	var mu sync.Mutex
	var totals statsResp
	var wg sync.WaitGroup

	for _, base := range nodeURLs {
		wg.Add(1)
		go func(base string) {
			defer wg.Done()
			resp, err := proxyClient.Get(base + "/stats")
			if err != nil || resp.StatusCode != http.StatusOK {
				return
			}
			defer resp.Body.Close()
			var s statsResp
			if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
				return
			}
			mu.Lock()
			totals.Hits += s.Hits
			totals.Misses += s.Misses
			totals.Evictions += s.Evictions
			totals.Size += s.Size
			totals.Capacity += s.Capacity
			mu.Unlock()
		}(base)
	}
	wg.Wait()
	writeJSON(w, http.StatusOK, totals)
}

// ── main ──────────────────────────────────────────────────────────────────────

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	rawURLs := getEnv("NODE_URLS", "http://dc-node1:8001,http://dc-node2:8002,http://dc-node3:8003")
	stratEnv := getEnv("HASH_STRATEGY", "ring")
	virtualNodes, _ := strconv.Atoi(getEnv("VIRTUAL_NODES", "150"))
	port := getEnv("ROUTER_PORT", "8000")

	strat := strategyRing
	if stratEnv == "rendezvous" {
		strat = strategyRendezvous
	}

	nodeURLs = make(map[string]string)
	for _, url := range strings.Split(rawURLs, ",") {
		url = strings.TrimSpace(url)
		if url == "" {
			continue
		}
		parts := strings.SplitN(strings.TrimPrefix(url, "http://"), ":", 2)
		host := parts[0]
		nodeID := strings.TrimPrefix(host, "dc-")
		nodeURLs[nodeID] = url
	}

	ring = newHashRing(virtualNodes, strat)
	for nodeID := range nodeURLs {
		ring.addNode(nodeID)
	}

	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests",
	}, []string{"handler", "status"})

	routeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "cache_route_duration_seconds",
		Help:    "Time to route + proxy a request",
		Buckets: prometheus.DefBuckets,
	})

	mux := http.NewServeMux()
	mux.HandleFunc("POST /cache/{key}", handleSet)
	mux.HandleFunc("GET /cache/{key}", handleGet)
	mux.HandleFunc("DELETE /cache/{key}", handleDelete)
	mux.HandleFunc("GET /ring/{key}", handleRing)
	mux.HandleFunc("GET /stats", handleStats)
	mux.Handle("GET /metrics", promhttp.Handler())

	log.Printf("Go router starting on :%s (nodes=%d, strategy=%s, vnodes=%d)",
		port, len(nodeURLs), stratEnv, virtualNodes)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
