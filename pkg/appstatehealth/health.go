// Package appstatehealth is a tiny process-wide registry of the sync health of each WhatsApp
// app-state collection (regular_low = pin/archive, regular_high = mute, regular = labels), keyed by
// (instanceId, collection). It is shared between two sides that must agree on that health:
//
//   - the auto-heal event handler (whatsmeow service), which WRITES it: it flags a collection
//     poisoned when a full resync can't fix it, or clears it when a sync completes cleanly, and
//     rate-limits its own resync attempts;
//   - the outgoing send path (chat service), which READS it: it refuses to publish an app-state
//     patch onto a collection that is known poisoned (that would only pile more bad patches onto
//     a collection the server can't verify — the toggle stays local in the CRM until a reset+QR).
//
// State lives only in memory: a poisoned flag is a hint, not durable truth. On reconnect the
// collection either syncs cleanly (Clear) or errors again (re-flagged), so losing it is safe.
package appstatehealth

import (
	"sync"
	"time"
)

const (
	healDebounce = 60 * time.Second // ignore repeated sync errors within this window
	healCap      = 3                // max resync attempts per collection within healCapWindow
	healCapWindow = time.Hour
	// A recovery request (whatsmeow issue #858 step 2) asks the primary device for an uncrypted
	// snapshot that bypasses the diverged patch chain. It's the rung between "a full resync failed"
	// and "give up / mark poisoned". Cap it so a primary that never answers can't spin peer messages.
	recoveryCap       = 2
	recoveryCapWindow = time.Hour
)

type entry struct {
	mu          sync.Mutex // guards the fields below; held only for short, non-blocking sections
	lastAttempt time.Time
	attempts    int
	recoveryAt  time.Time // last recovery request sent
	recoveries  int       // recovery requests sent in the current window
	poisoned    bool      // server-side poison: a full resync can't fix it, only reset-appstate + re-scan
}

var registry sync.Map // key: instanceId + "|" + collection -> *entry

func get(instanceID, collection string) *entry {
	v, _ := registry.LoadOrStore(instanceID+"|"+collection, &entry{})
	return v.(*entry)
}

// ShouldAttemptHeal reports whether a full resync should run NOW for this collection, recording the
// attempt if so. It returns false when the collection is already flagged poisoned, when the last
// attempt was within the debounce window, or when the per-hour cap is reached. The short critical
// section serializes concurrent callers, so at most one resync runs per collection at a time (the
// resync itself must NOT hold this lock — it makes a network call).
func ShouldAttemptHeal(instanceID, collection string) bool {
	e := get(instanceID, collection)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.poisoned {
		return false
	}
	now := time.Now()
	if now.Sub(e.lastAttempt) < healDebounce {
		return false
	}
	if e.attempts >= healCap && now.Sub(e.lastAttempt) < healCapWindow {
		return false
	}
	e.attempts++
	e.lastAttempt = now
	return true
}

// ShouldAttemptRecovery reports whether an app-state RECOVERY REQUEST (BuildAppStateRecoveryRequest →
// SendPeerMessage, whatsmeow issue #858 step 2) should be sent NOW for this collection, recording the
// attempt if so. This is the rung between "a full resync failed with a MAC/LTHash mismatch" and "give
// up / mark poisoned": the primary is asked for an uncrypted snapshot that bypasses the diverged patch
// chain (ProcessRecovery resets version+hash from it → AppStateSyncComplete → Clear), healing WITHOUT a
// re-scan. Capped (recoveryCap per recoveryCapWindow) so a primary that never answers can't spin peer
// messages — after the cap the caller falls through to MarkPoisoned (manual reset-appstate + QR).
func ShouldAttemptRecovery(instanceID, collection string) bool {
	e := get(instanceID, collection)
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	if now.Sub(e.recoveryAt) >= recoveryCapWindow {
		e.recoveries = 0 // window elapsed — fresh budget
	}
	if e.recoveries >= recoveryCap {
		return false
	}
	e.recoveries++
	e.recoveryAt = now
	return true
}

// MarkHealed clears the backoff, recovery budget and poison flag after a successful resync or a clean
// sync complete (including a recovery snapshot, which arrives as AppStateSyncComplete).
func MarkHealed(instanceID, collection string) {
	e := get(instanceID, collection)
	e.mu.Lock()
	e.attempts = 0
	e.recoveries = 0
	e.poisoned = false
	e.mu.Unlock()
}

// Clear is an alias for MarkHealed, for the AppStateSyncComplete path (reads better there).
func Clear(instanceID, collection string) { MarkHealed(instanceID, collection) }

// MarkPoisoned flags the collection as server-side poisoned (a full resync returned a MAC/LTHash
// mismatch, so it can't be auto-fixed — only reset-appstate + re-scan recovers it).
func MarkPoisoned(instanceID, collection string) {
	e := get(instanceID, collection)
	e.mu.Lock()
	e.poisoned = true
	e.mu.Unlock()
}

// IsPoisoned reports whether the collection is currently flagged poisoned. The send path checks this
// before publishing a patch, to avoid piling more bad data onto a collection the server can't verify.
func IsPoisoned(instanceID, collection string) bool {
	e := get(instanceID, collection)
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.poisoned
}
