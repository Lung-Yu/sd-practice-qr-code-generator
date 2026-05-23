package main

import (
	"bytes"
	"context"
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

// ── constants ────────────────────────────────────────────────────────────────

const (
	healthInterval = 2 * time.Second
	failThreshold  = 2 // consecutive health-poll failures before marking DOWN

	cbFailThreshold = 3               // request failures before circuit OPENS
	cbOpenTimeout   = 15 * time.Second // how long circuit stays OPEN before HALF_OPEN trial
)

// ── globals ──────────────────────────────────────────────────────────────────

var (
	nodeURLs        map[string]string
	ring            *hashRing
	circuitBreakers map[string]*CircuitBreaker // per-node CB, read-only after init

	healthMu    sync.RWMutex
	healthState map[string]*nodeHealthState

	requestsTotal    *prometheus.CounterVec
	routeDuration    prometheus.Histogram
	circuitOpenTotal *prometheus.CounterVec

	proxyClient = &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 200,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}
)

var (
	storeURL         string // empty → write-through disabled
	writeThroughMode string // "parallel" | "store_first" | "cache_first"
)

type nodeHealthState struct {
	Alive    bool `json:"alive"`
	Failures int  `json:"failures"`
}

// ── response shapes ───────────────────────────────────────────────────────────

type ringResp struct {
	Key          string `json:"key"`
	Node         string `json:"node"`    // primary (kept for backward compat)
	Replica      string `json:"replica"` // replica node; empty if only one node in ring
	VirtualNodes int    `json:"virtual_nodes"`
}

type statsResp struct {
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
	Size      int64 `json:"size"`
	Capacity  int64 `json:"capacity"`
}

// nodeResult holds the outcome of a single proxied call to a cache node.
// errMsg is non-empty when the request never reached the node (circuit_open
// or node_unreachable); status/body/headers are populated on a real HTTP response.
type nodeResult struct {
	status  int
	body    []byte
	headers http.Header
	errMsg  string // "circuit_open" | "node_unreachable" | ""
}

// ── helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// callNode executes one proxied HTTP request to a node and returns the result.
// It manages the circuit breaker: records failure on TCP error or 5xx, success otherwise.
// 404 (cache miss) is a success — it is normal cache behaviour.
func callNode(ctx context.Context, nodeID, method, targetURL string, body []byte, header http.Header) nodeResult {
	cb := circuitBreakers[nodeID]
	if !cb.Allow() {
		circuitOpenTotal.WithLabelValues(nodeID).Inc()
		return nodeResult{errMsg: "circuit_open"}
	}
	start := time.Now()
	req, _ := http.NewRequestWithContext(ctx, method, targetURL, bytes.NewReader(body))
	req.Header = header.Clone()
	req.ContentLength = int64(len(body))
	resp, err := proxyClient.Do(req)
	routeDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		cb.RecordFailure()
		return nodeResult{errMsg: "node_unreachable"}
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 500 {
		cb.RecordFailure()
	} else {
		cb.RecordSuccess()
	}
	return nodeResult{status: resp.StatusCode, body: respBody, headers: resp.Header.Clone()}
}

// callStore sends one request to the backing store and returns the result.
// No circuit breaker — store failure surfaces directly as node_unreachable.
func callStore(ctx context.Context, method, key string, body []byte) nodeResult {
	req, _ := http.NewRequestWithContext(ctx, method, storeURL+"/store/"+key, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	resp, err := proxyClient.Do(req)
	if err != nil {
		return nodeResult{errMsg: "node_unreachable"}
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return nodeResult{status: resp.StatusCode, body: respBody, headers: resp.Header.Clone()}
}

// writeResult writes a nodeResult to w, copying status, headers, and body.
func writeResult(w http.ResponseWriter, res nodeResult) {
	for k, vs := range res.headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(res.status)
	w.Write(res.body) //nolint:errcheck
}

// proxy forwards a single request to one node, writing the response to w.
// Used for handlers that do not need replication (e.g. /stats, /ring).
func proxy(w http.ResponseWriter, r *http.Request, nodeID, targetURL, handler string) {
	body, _ := io.ReadAll(r.Body)
	res := callNode(r.Context(), nodeID, r.Method, targetURL, body, r.Header)
	if res.errMsg == "circuit_open" {
		requestsTotal.WithLabelValues(handler, "503").Inc()
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "circuit_open",
			"node":  nodeID,
		})
		return
	}
	if res.errMsg == "node_unreachable" {
		requestsTotal.WithLabelValues(handler, "503").Inc()
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "node_unreachable", "node": nodeID})
		return
	}
	requestsTotal.WithLabelValues(handler, strconv.Itoa(res.status)).Inc()
	writeResult(w, res)
}

// ── route handlers ────────────────────────────────────────────────────────────

func handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	nodes := ring.nodesForKey(key, 2)
	if len(nodes) == 0 {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "no_nodes_available"})
		return
	}
	body, _ := io.ReadAll(r.Body)

	if len(nodes) == 1 {
		// Degraded ring (only one node alive) — single write, no replication
		res := callNode(r.Context(), nodes[0], "POST",
			nodeURLs[nodes[0]]+"/cache/"+key, body, r.Header)
		if res.errMsg != "" {
			requestsTotal.WithLabelValues("set", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable,
				map[string]string{"error": res.errMsg, "node": nodes[0]})
			return
		}
		requestsTotal.WithLabelValues("set", strconv.Itoa(res.status)).Inc()
		writeResult(w, res)
		return
	}

	// Sync replication: parallel write to primary (nodes[0]) + replica (nodes[1])
	results := make([]nodeResult, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i, nodeID := range nodes[:2] {
		go func(i int, nodeID string) {
			defer wg.Done()
			results[i] = callNode(r.Context(), nodeID, "POST",
				nodeURLs[nodeID]+"/cache/"+key, body, r.Header)
		}(i, nodeID)
	}
	wg.Wait()

	for i, nodeID := range nodes[:2] {
		res := results[i]
		if res.errMsg != "" || res.status >= 500 {
			requestsTotal.WithLabelValues("set", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": "replication_failed",
				"node":  nodeID,
			})
			return
		}
	}
	requestsTotal.WithLabelValues("set", strconv.Itoa(results[0].status)).Inc()
	writeResult(w, results[0])
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	nodes := ring.nodesForKey(key, 2)
	if len(nodes) == 0 {
		requestsTotal.WithLabelValues("get", "503").Inc()
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "no_nodes_available"})
		return
	}
	for _, nodeID := range nodes {
		res := callNode(r.Context(), nodeID, "GET",
			nodeURLs[nodeID]+"/cache/"+key, nil, r.Header)
		if res.errMsg != "" {
			// Transport failure (circuit_open or node_unreachable) → try replica.
			// HTTP 5xx is not retried (errMsg is empty then); CB records it so the
			// circuit opens and the next request will fall back automatically.
			continue
		}
		requestsTotal.WithLabelValues("get", strconv.Itoa(res.status)).Inc()
		writeResult(w, res)
		return
	}
	// All nodes in the replication set are unreachable
	requestsTotal.WithLabelValues("get", "503").Inc()
	writeJSON(w, http.StatusServiceUnavailable,
		map[string]string{"error": "no_nodes_available"})
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	nodes := ring.nodesForKey(key, 2)
	if len(nodes) == 0 {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "no_nodes_available"})
		return
	}
	body, _ := io.ReadAll(r.Body)

	if len(nodes) == 1 {
		res := callNode(r.Context(), nodes[0], "DELETE",
			nodeURLs[nodes[0]]+"/cache/"+key, body, r.Header)
		if res.errMsg != "" {
			requestsTotal.WithLabelValues("delete", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable,
				map[string]string{"error": res.errMsg, "node": nodes[0]})
			return
		}
		requestsTotal.WithLabelValues("delete", strconv.Itoa(res.status)).Inc()
		writeResult(w, res)
		return
	}

	results := make([]nodeResult, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i, nodeID := range nodes[:2] {
		go func(i int, nodeID string) {
			defer wg.Done()
			results[i] = callNode(r.Context(), nodeID, "DELETE",
				nodeURLs[nodeID]+"/cache/"+key, body, r.Header)
		}(i, nodeID)
	}
	wg.Wait()

	for i, nodeID := range nodes[:2] {
		res := results[i]
		if res.errMsg != "" || res.status >= 500 {
			requestsTotal.WithLabelValues("delete", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": "replication_failed",
				"node":  nodeID,
			})
			return
		}
	}
	requestsTotal.WithLabelValues("delete", strconv.Itoa(results[0].status)).Inc()
	writeResult(w, results[0])
}

func handleRing(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	nodes := ring.nodesForKey(key, 2)
	if len(nodes) == 0 {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "no_nodes_available"})
		return
	}
	replica := ""
	if len(nodes) > 1 {
		replica = nodes[1]
	}
	writeJSON(w, http.StatusOK, ringResp{
		Key:          key,
		Node:         nodes[0],
		Replica:      replica,
		VirtualNodes: ring.virtualCount(),
	})
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

func handleHealth(w http.ResponseWriter, r *http.Request) {
	healthMu.RLock()
	type nodeInfo struct {
		Alive    bool   `json:"alive"`
		Failures int    `json:"failures"`
		Circuit  string `json:"circuit"`
	}
	nodes := make(map[string]nodeInfo, len(healthState))
	degraded := false
	for id, s := range healthState {
		nodes[id] = nodeInfo{
			Alive:    s.Alive,
			Failures: s.Failures,
			Circuit:  circuitBreakers[id].State(),
		}
		if !s.Alive {
			degraded = true
		}
	}
	healthMu.RUnlock()

	status := "ok"
	if degraded {
		status = "degraded"
	}
	out := map[string]any{"status": status, "nodes": nodes}
	if storeURL != "" {
		storeAlive := false
		resp, err := proxyClient.Get(storeURL + "/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			storeAlive = true
		}
		if resp != nil {
			resp.Body.Close()
		}
		out["store"] = map[string]any{
			"alive":              storeAlive,
			"write_through_mode": writeThroughMode,
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// ── health checker ────────────────────────────────────────────────────────────

func checkNode(nodeID, base string) {
	resp, err := proxyClient.Get(base + "/health")
	alive := err == nil && resp.StatusCode == http.StatusOK
	if resp != nil {
		resp.Body.Close()
	}

	healthMu.Lock()
	s := healthState[nodeID]
	wasAlive := s.Alive

	if alive {
		if !wasAlive {
			s.Alive = true
			s.Failures = 0
			healthMu.Unlock()
			ring.addNode(nodeID)
			circuitBreakers[nodeID].Reset() // node is back, close the circuit
			log.Printf("[health] node %s recovered → ring restored, circuit closed", nodeID)
			return
		}
		s.Failures = 0
	} else {
		s.Failures++
		if wasAlive && s.Failures >= failThreshold {
			s.Alive = false
			healthMu.Unlock()
			ring.removeNode(nodeID)
			log.Printf("[health] node %s unreachable (%d failures) → removed from ring", nodeID, s.Failures)
			return
		}
	}
	healthMu.Unlock()
}

func startHealthChecker() {
	healthMu.Lock()
	for nodeID := range nodeURLs {
		healthState[nodeID] = &nodeHealthState{Alive: true}
	}
	healthMu.Unlock()

	go func() {
		ticker := time.NewTicker(healthInterval)
		defer ticker.Stop()
		for range ticker.C {
			for nodeID, base := range nodeURLs {
				go checkNode(nodeID, base)
			}
		}
	}()
	log.Printf("[health] checker started (interval=%s, health_fail=%d, cb_fail=%d, cb_timeout=%s)",
		healthInterval, failThreshold, cbFailThreshold, cbOpenTimeout)
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
	storeURL = getEnv("STORE_URL", "")
	writeThroughMode = getEnv("WRITE_THROUGH_MODE", "parallel")

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
	circuitBreakers = make(map[string]*CircuitBreaker, len(nodeURLs))
	for nodeID := range nodeURLs {
		ring.addNode(nodeID)
		circuitBreakers[nodeID] = NewCB(cbFailThreshold, cbOpenTimeout)
	}

	healthState = make(map[string]*nodeHealthState)

	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests",
	}, []string{"handler", "status"})

	routeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "cache_route_duration_seconds",
		Help:    "Time to route + proxy a request",
		Buckets: prometheus.DefBuckets,
	})

	circuitOpenTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_circuit_open_total",
		Help: "Requests rejected because the node's circuit breaker is open",
	}, []string{"node"})

	startHealthChecker()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /cache/{key}", handleSet)
	mux.HandleFunc("GET /cache/{key}", handleGet)
	mux.HandleFunc("DELETE /cache/{key}", handleDelete)
	mux.HandleFunc("GET /ring/{key}", handleRing)
	mux.HandleFunc("GET /stats", handleStats)
	mux.HandleFunc("GET /health", handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())

	log.Printf("Go router starting on :%s (nodes=%d, strategy=%s, vnodes=%d, store=%q, wt_mode=%s)",
		port, len(nodeURLs), stratEnv, virtualNodes, storeURL, writeThroughMode)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
