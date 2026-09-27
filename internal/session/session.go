// Package session handles sticky-session bindings between clients and peers.
//
// The cookie value is an opaque random token; the token -> peer mapping lives
// only server side. A session never silently moves to another peer: the proxy
// only re-binds when the current peer is down (an explicit, logged migration),
// and cookie-less responses are explained by the proxy logs.
package session

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"
	"time"
)

type binding struct {
	peer      string
	expiresAt int64 // unix millis; 0 = session-only (no expiry while stored)
}

// Store maps route+sticky-token to a chosen peer name with TTL expiry.
type Store struct {
	mu       sync.Mutex
	bindings map[string]binding
}

// NewStore creates a sticky session store.
func NewStore() *Store {
	return &Store{bindings: map[string]binding{}}
}

func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand should not fail; fall back to time-based uniqueness.
		return hex.EncodeToString([]byte(time.Now().String()))
	}
	return hex.EncodeToString(b[:])
}

// CookiePeer returns the peer bound to the request's cookie, or "" if the
// cookie is absent, unknown (e.g. after gateway restart), or expired.
func (s *Store) CookiePeer(cookieName string, r *http.Request, nowMillis int64) string {
	c, err := r.Cookie(cookieName)
	if err != nil || c.Value == "" {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bindings[cookieName+"|"+c.Value]
	if !ok {
		return ""
	}
	if b.expiresAt != 0 && nowMillis > b.expiresAt {
		delete(s.bindings, cookieName+"|"+c.Value)
		return ""
	}
	return b.peer
}

// Bind ensures the client has a binding for cookieName.
//
//   - Existing valid binding: it is returned and nothing is changed (no
//     re-bind, no new Set-Cookie), so a healthy session stays pinned.
//   - Missing/expired/unknown token: a fresh token is created for peerName
//     and Set-Cookie is written to w.
//
// Returns the bound peer name and whether a new cookie was issued.
func (s *Store) Bind(cookieName, peerName string, ttlSeconds int, w http.ResponseWriter, r *http.Request, nowMillis int64) (string, bool) {
	c, err := r.Cookie(cookieName)
	key := ""
	if err == nil && c.Value != "" {
		key = cookieName + "|" + c.Value
	}
	s.mu.Lock()
	if key != "" {
		if b, ok := s.bindings[key]; ok && (b.expiresAt == 0 || nowMillis <= b.expiresAt) {
			peer := b.peer
			s.mu.Unlock()
			return peer, false
		}
	}
	token := newToken()
	key = cookieName + "|" + token
	var exp int64
	if ttlSeconds > 0 {
		exp = nowMillis + int64(ttlSeconds)*1000
	}
	s.bindings[key] = binding{peer: peerName, expiresAt: exp}
	s.mu.Unlock()

	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   ttlSeconds, // 0 => session cookie
	})
	return peerName, true
}

// MigrateTo moves an existing client to newPeer and issues a new cookie,
// deleting the previous token binding first. Used when the bound peer has
// been ejected: migration is explicit and logged by the caller, so a session
// never straddles two peers.
func (s *Store) MigrateTo(cookieName, newPeer string, ttlSeconds int, w http.ResponseWriter, r *http.Request, nowMillis int64) string {
	if c, err := r.Cookie(cookieName); err == nil && c.Value != "" {
		s.mu.Lock()
		delete(s.bindings, cookieName+"|"+c.Value)
		s.mu.Unlock()
	}
	// Remove the cookie from the request so Bind cannot reuse it.
	r.Header.Set("Cookie", stripCookie(r.Header.Get("Cookie"), cookieName))
	peer, _ := s.Bind(cookieName, newPeer, ttlSeconds, w, r, nowMillis)
	return peer
}

// stripCookie removes name=... pairs from a Cookie header value.
func stripCookie(header, name string) string {
	var keep []string
	for _, part := range strings.Split(header, ";") {
		k, _, ok := strings.Cut(part, "=")
		if ok && strings.TrimSpace(k) == name {
			continue
		}
		if strings.TrimSpace(part) != "" {
			keep = append(keep, strings.TrimSpace(part))
		}
	}
	return strings.Join(keep, "; ")
}

// ReapExpired deletes expired bindings.
func (s *Store) ReapExpired(nowMillis int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, b := range s.bindings {
		if b.expiresAt != 0 && nowMillis > b.expiresAt {
			delete(s.bindings, k)
		}
	}
}
