package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// NewEventSummaryCounter creates a new event counter that will
// log a summary at every interval.
func NewEventSummaryCounter(interval time.Duration) *EventSummaryCounter {
	return &EventSummaryCounter{
		Interval: interval,
		Counts:   make(map[string]int),
	}
}

// EventSummaryCounter keeps a set of counters, one for each event name string it receives
// Periodically, it logs all the counts and then clears its timers.
// This is to reduce log volume while allowing some visibility into the shape of the OPC-UA event traffic
// being received by the exporter
type EventSummaryCounter struct {
	Interval    time.Duration
	Counts      map[string]int
	Total       int
	mutex       sync.Mutex
	promCounter *prometheus.CounterVec
}

// Inc adds one to the counter for the given node
func (esc *EventSummaryCounter) Inc(nodeID string) {
	// Internal counter for logging
	esc.mutex.Lock()
	if _, ok := esc.Counts[nodeID]; ok {
		esc.Counts[nodeID]++
	} else {
		esc.Counts[nodeID] = 1
	}
	esc.Total++
	esc.mutex.Unlock()

	// Also update prometheus counter
	if esc.promCounter != nil {
		esc.promCounter.WithLabelValues(nodeID).Inc()
	}
}

// Reset all the counters to zero
func (esc *EventSummaryCounter) Reset() {
	esc.mutex.Lock()
	esc.Counts = make(map[string]int)
	esc.Total = 0
	esc.mutex.Unlock()
}

// set up the prometheus counter
func (esc *EventSummaryCounter) initPrometheusCounter() {
	esc.promCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: exporterNamespace,
			Name:      "event_count",
			Help:      "Number of updates received from the OPC-UA endpoint",
		},
		[]string{"nodeID"},
	)
	prometheus.MustRegister(esc.promCounter)
}

// Start the goroutine that periodically logs the counter summary,
// then resets the counters.
func (esc *EventSummaryCounter) Start(ctx context.Context) {
	esc.initPrometheusCounter()
	go func() {
		log.Printf("Starting %v summary timer", esc.Interval.String())
		esc.Reset()
		ticker := time.NewTicker(esc.Interval)
		for {
			select {
			case <-ticker.C:
				esc.logSummary()
				esc.Reset()
			case <-ctx.Done():
				log.Println("Exiting summary printing loop")
				break
			}
		}
	}()
}

func (esc *EventSummaryCounter) logSummary() {
	log.Printf("Received %d events on %d channels in the last %v", esc.Total, len(esc.Counts), esc.Interval.String())
	for channel, count := range esc.Counts {
		rate := float64(count) / esc.Interval.Seconds()
		log.Printf("CHANNEL: %v\tEVENTS: %v (%.3f per second)", channel, count, rate)
	}
}
