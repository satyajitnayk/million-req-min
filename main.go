package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

type Payload struct {
	URL string `json:"url"`
}

type PayloadCollection struct {
	WindowsVersion string    `json:"version"`
	Token          string    `json:"token"`
	Payloads       []Payload `json:"data"`
}

// UploadToS3 uploads the payload to S3
func (p *Payload) UploadToS3() error {
	time.Sleep(2 * time.Second) // Simulate S3 upload delay
	// Simulate an error 1 in 1000 times
	if rand.Intn(1000) == 0 {
		return fmt.Errorf("Simulated error during S3 upload")
	}
	return nil
}

const (
	MaxWorker = 10
	MaxQueue  = 40
	MaxLength = 500
)

type Job struct {
	Payload Payload
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
	}
}

// Start method starts the run loop for the worker, listening for a quit channel
// in case we need to stop it
func (w Worker) Start() {
	go func() {
		fmt.Println("Worker started")
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel
			fmt.Println("Worker added to pool")

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				fmt.Println("Worker processing job")
				if err := job.Payload.UploadToS3(); err != nil {
					log.Printf("Error uploading to S3: %s", err.Error())
				}
				fmt.Println("Worker finished job")

			case <-w.quit:
				// we have received a signal to stop
				fmt.Println("Worker quitting")
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

func PayloadHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received request to /payload")
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Read the body into a string for json decoding
	var content PayloadCollection
	err := json.NewDecoder(r.Body).Decode(&content)
	fmt.Println(err)
	if err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	fmt.Println("content", content)

	// Go through each payload and queue items individually to be posted to S3
	for _, payload := range content.Payloads {
		// let's create a job with the payload
		work := Job{Payload: payload}

		// Push the work onto the queue.
		JobQueue <- work
		fmt.Println("Job queued")
	}

	w.WriteHeader(http.StatusOK)
}

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
}

func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{WorkerPool: pool}
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	fmt.Println("workerpool capacity", cap(d.WorkerPool))
	for i := 0; i < cap(d.WorkerPool); i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}

	// Initiates the dispatch method in a new goroutine.
	// This allows the dispatcher to continuously listen for incoming jobs and
	// dispatch them to available workers concurrently.
	go d.dispatch()
}

// Listens for incoming jobs from the JobQueue.
// Dispatches each job to an available worker.
func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-JobQueue:
			// a job request has been received
			fmt.Println("Dispatcher received job", job)
			go func(job Job) {
				fmt.Println("workerpool", d.WorkerPool)
				fmt.Println("job", job)
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				// dispatch the job to the worker job channel
				// effectively dispatches the job to an available worker.
				jobChannel <- job
				fmt.Println("Job dispatched to worker")
			}(job)
		}
	}
}

func main() {
	// Initialize the JobQueue
	JobQueue = make(chan Job, MaxQueue)

	// Create a dispatcher and run it
	dispatcher := NewDispatcher(MaxWorker)
	go dispatcher.Run()

	// Set up the HTTP server
	http.HandleFunc("/payload", PayloadHandler)
	fmt.Println("HTTP server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
