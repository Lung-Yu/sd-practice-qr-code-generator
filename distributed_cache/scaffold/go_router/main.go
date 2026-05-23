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

// ── constants ────────────────────────────────────────────────────────────────

const (
	healthInterval = 2 * time.Second
	failThreshold  = 2 // consecutive failures before marking DOWN
)

// ── globals ──────────────────────────────────────────────────────────────────

var (
	nodeURLs map[string]string
	ring     *hashRing

	// health state — separate lock from the ring so they don't block each other
	healthMu    sync.RWMutex
	healthState map[string]*nodeHealthState // nodeID → state

	requestsTotal *prometheus.CounterVec
	routeDuration prometheus.Histogram

	proxyClient = &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 200,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}
)

type nodeHealthState struct {
	Alive    bool `json:"alive"`
	Failures int  `json:"failures"`
}

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

// nodeFor returns (nodeID, base, true) or fails with 503 if ring is empty.
func nodeFor(w http.ResponseWriter, key string) (nodeID, base string, ok bool) {
	id, found := ring.nodeForKey(key)
	if !found {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "no_nodes_available"})
		return "", "", false
	}
	return id, nodeURLs[id], true
}

// proxy forwards a request to a cache node and streams the response back.
func proxy(w http.ResponseWriter, r *http.Request, targetURL, handler string) {
	start := time.Now()

	req, _ := http.NewRequestWithContext(r.Context(), r.Method, targetURL, r.Body)
	req.Header = r.Header.Clone()
	req.ContentLength = r.ContentLength

	resp, err := proxyClient.Do(req)
	routeDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		requestsTotal.WithLabelValues(handler, "503").Inc()
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "node_unreachable"})
		return
	}
	defer resp.Body.Close()

	requestsTotal.WithLabelValues(handler, strconv.Itoa(resp.StatusCode)).Inc()
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
	_, base, ok := nodeFor(w, key)
	if !ok {
		return
	}
	proxy(w, r, base+"/cache/"+key, "set")
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	_, base, ok := nodeFor(w, key)
	if !ok {
		return
	}
	proxy(w, r, base+"/cache/"+key, "get")
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	_, base, ok := nodeFor(w, key)
	if !ok {
		return
	}
	proxy(w, r, base+"/cache/"+key, "delete")
}

func handleRing(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	nodeID, _, ok := nodeFor(w, key)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, ringResp{
		Key:          key,
		Node:         nodeID,
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
	snapshot := make(map[string]*nodeHealthState, len(healthState))
	for id, s := range healthState {
		snapshot[id] = &nodeHealthState{Alive: s.Alive, Failures: s.Failures}
	}
	healthMu.RUnlock()

	status := "ok"
	for _, s := range snapshot {
		if !s.Alive {
			status = "degraded"
			break
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": status,
		"nodes":  snapshot,
	})
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
			log.Printf("[health] node %s recovered → added back to ring", nodeID)
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
	// initialise state — all nodes start as alive (they were just started)
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
	log.Printf("[health] checker started (interval=%s, fail_threshold=%d)", healthInterval, failThreshold)
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

	startHealthChecker()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /cache/{key}", handleSet)
	mux.HandleFunc("GET /cache/{key}", handleGet)
	mux.HandleFunc("DELETE /cache/{key}", handleDelete)
	mux.HandleFunc("GET /ring/{key}", handleRing)
	mux.HandleFunc("GET /stats", handleStats)
	mux.HandleFunc("GET /health", handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())

	log.Printf("Go router starting on :%s (nodes=%d, strategy=%s, vnodes=%d)",
		port, len(nodeURLs), stratEnv, virtualNodes)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
