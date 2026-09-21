// Package client_registry holds the synchronization primitives for the three
// process-wide runtime maps (clientPointer, myClientPointer, killChannel).
//
// Those maps are created ONCE in cmd/evolution-go/main.go and handed by
// reference to every service (whatsmeow, instance, sendMessage, chat, group,
// user, call, community, label, newsletter) and to every MyClient. They are
// plain Go maps, so any two goroutines writing at the same time (e.g. the N
// `go StartClient` spawned by ConnectOnStartup, or a ReconnectClient racing an
// API call) crash the whole process with `fatal error: concurrent map writes`.
//
// A single package-level RWMutex is the honest model for a single process-wide
// set of maps: the service structs use VALUE receivers (`func (w
// whatsmeowService)`), so a mutex stored as a struct field would be copied on
// every call and protect nothing.
//
// Locking rules (keep them, they are what prevents deadlocks):
//   - Hold Mu ONLY around the map access itself (index, assign, delete).
//   - Copy the value out under the lock, release, THEN use it. Never call
//     client.Connect()/Disconnect(), send/close a channel, hit the network or
//     call back into StartClient/ReconnectClient while holding Mu.
//   - Writers take Mu.Lock(); readers take Mu.RLock().
package client_registry

import "sync"

// Mu guards clientPointer, myClientPointer and killChannel.
var Mu sync.RWMutex

// startLocks holds one *sync.Mutex per instance id (see StartLock).
var startLocks sync.Map

// StartLock returns the per-instance mutex that serializes the setup phase of
// StartClient (existence check -> device store -> client registration ->
// Connect) so two StartClient calls for the SAME instance cannot interleave and
// overwrite each other's client. It is independent from Mu and is never held
// while Mu is held for longer than a map access.
func StartLock(instanceId string) *sync.Mutex {
	l, _ := startLocks.LoadOrStore(instanceId, &sync.Mutex{})
	return l.(*sync.Mutex)
}
