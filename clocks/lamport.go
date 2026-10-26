// Lab 7: Chrono-Solver -- LamportClock and MessageQueue starter.
package clocks

import (
	"sort"
	"sync"
)

// LamportClock is a single node's logical clock.
type LamportClock struct {
	mu    sync.Mutex
	value int
}

// Tick increments the clock (a local event) and returns the new value.
// Must be safe for concurrent use.
func (c *LamportClock) Tick() int {

	c.mu.Lock()
	defer c.mu.Unlock()
	c.value++
	return c.value

}

// Observe handles receiving a message stamped with `received`: sets
// the clock to max(current, received) + 1 and returns the new value.
// Must be safe for concurrent use.
func (c *LamportClock) Observe(received int) int {

	c.mu.Lock()
	defer c.mu.Unlock()
	if received > c.value {
		c.value = received
	}
	c.value++
	return c.value

}

// LogEvent is a single timestamped event arriving from some node.
type LogEvent struct {
	NodeID    string
	Timestamp int
	Payload   string
}

// MessageQueue buffers LogEvents (which may arrive out of order) and
// can drain them in deterministic Lamport timestamp order. Timestamp order
// alone does not establish that two events are causally related.
type MessageQueue struct {
	mu     sync.Mutex
	events []LogEvent
}

// Enqueue adds an event to the internal buffer. Safe for concurrent use.
func (q *MessageQueue) Enqueue(event LogEvent) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.events = append(q.events, event)
}

// Drain returns all buffered events sorted by Timestamp ascending,
// with ties broken by NodeID ascending (a deterministic tiebreak --
// NOT a claim of real causal precedence between equal-timestamp
// events from different nodes; Lamport timestamps alone cannot
// distinguish concurrent events from causally-ordered ones, see Part
// A, Question 3).
func (q *MessageQueue) Drain() []LogEvent {

	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]LogEvent, len(q.events))
	copy(out, q.events)
	q.events = nil
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Timestamp != out[j].Timestamp {
			return out[i].Timestamp < out[j].Timestamp
		}
		return out[i].NodeID < out[j].NodeID
	})
	return out

}
