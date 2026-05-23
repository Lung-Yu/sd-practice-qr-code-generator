package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
)

var (
	mu    sync.RWMutex
	store = map[string]json.RawMessage{}
)

func main() {
	port := os.Getenv("STORE_PORT")
	if port == "" {
		port = "8004"
	}

	mux := http.NewServeMux()

	mux.HandleFunc("POST /store/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		var body json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "bad_request"})
			return
		}
		mu.Lock()
		store[key] = body
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"key": key, "status": "stored"})
	})

	mux.HandleFunc("GET /store/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		mu.RLock()
		v, ok := store[key]
		mu.RUnlock()
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "miss", "key": key})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(v)
	})

	mux.HandleFunc("DELETE /store/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		mu.Lock()
		delete(store, key)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"key": key, "status": "deleted"})
	})

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	log.Printf("go_store listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
