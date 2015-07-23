package taurus

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

const APIVERSION = "v1"

type handler func(c *Context, w http.ResponseWriter, r *http.Request)

type routes map[string]map[string]map[string]handler

func Routes() routes {
	return routes{
		"v1": {
			"GET": {
				"/job/all":     getAllJobs,
				"/job/{id:.+}": getJob,
			},
			"POST": {
				"/job/new": startJob,
			},
			"DELETE": {
				"/job/all":     delAllJobs,
				"/job/{id:.+}": delJob,
			},
		},
	}
}

func getAllJobs(c *Context, w http.ResponseWriter, r *http.Request) {
	var mu sync.RWMutex
	mu.Lock()
	defer mu.Unlock()

	jobs, err := c.store.GetAllJobs()
	if err != nil {
		log.Printf("Error: %s", err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	if err := json.NewEncoder(w).Encode(jobs); err != nil {
		panic(err)
	}
	w.WriteHeader(http.StatusOK)
}

func getJob(c *Context, w http.ResponseWriter, r *http.Request) {
	var mu sync.RWMutex
	mu.Lock()
	defer mu.Unlock()

	vars := mux.Vars(r)
	jobId := vars["id"]

	job, err := c.store.GetJob(jobId)
	if err != nil {
		if serr, ok := err.(*StoreError); ok {
			if serr.Code == ErrNotFound {
				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}
		log.Printf("Error: %s", err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	if err := json.NewEncoder(w).Encode(job); err != nil {
		log.Printf("Error: %s", err)
		panic(err)
	}
	w.WriteHeader(http.StatusOK)
}

func startJob(c *Context, w http.ResponseWriter, r *http.Request) {
	var mu sync.RWMutex
	mu.Lock()
	defer mu.Unlock()

	var job Job
	err := json.NewDecoder(r.Body).Decode(&job)
	if err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		log.Printf("Error: %s", err)
		return
	}

	job.State = PENDING
	log.Printf("Submitting Job %s", job.Id)
	err = c.store.AddJob(&job)
	if err != nil {
		if serr, ok := err.(*StoreError); ok {
			if serr.Code == ErrExists {
				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		log.Printf("Could not store job %s: %s", job.Id, err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
}

func delJob(c *Context, w http.ResponseWriter, r *http.Request) {
	var mu sync.RWMutex
	mu.Lock()
	defer mu.Unlock()

	vars := mux.Vars(r)
	jobId := vars["id"]

	log.Printf("Stopping Job %s", jobId)
	job, err := c.store.GetJob(jobId)
	if err != nil {
		if serr, ok := err.(*StoreError); ok {
			if serr.Code == ErrNotFound {
				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}
		log.Printf("Could not retrieve %s job: %s", jobId, err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	job.State = STOPPED
	log.Printf("Killing Job %s", job.Id)
	if err := c.store.UpdateJob(job); err != nil {
		log.Printf("Could not update job %s: %s", jobId, err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
}

func delAllJobs(c *Context, w http.ResponseWriter, r *http.Request) {
	jobs, err := c.store.GetAllJobs()
	if err != nil {
		log.Printf("Error: %s", err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	for _, job := range jobs {
		job.State = STOPPED
		log.Printf("Killing Job %s", job.Id)
		if err := c.store.UpdateJob(job); err != nil {
			log.Printf("Could not update job %s: %s", job.Id, err)
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
}
