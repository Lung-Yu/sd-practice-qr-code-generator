package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

var (
	mu    sync.RWMutex
	store = map[string]json.RawMessage{}
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON encode error: %v", err)
	}
}

func main() {
	port := os.Getenv("STORE_PORT")
	if port == "" {
		port = "8004"
	}

	mux := http.NewServeMux()

	mux.HandleFunc("POST /store/{key}", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		key := r.PathValue("key")
		var body json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
			return
		}
		mu.Lock()
		store[key] = body
		mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{"key": key, "status": "stored"})
	})

	mux.HandleFunc("GET /store/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		mu.RLock()
		v, ok := store[key]
		mu.RUnlock()
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "miss", "key": key})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(v); err != nil {
			log.Printf("write error: %v", err)
		}
	})

	mux.HandleFunc("DELETE /store/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		mu.Lock()
		delete(store, key)
		mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{"key": key, "status": "deleted"})
	})

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
	log.Printf("go_store listening on :%s", port)
	log.Fatal(srv.ListenAndServe())
}
