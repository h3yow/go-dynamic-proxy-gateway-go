package session

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func reqWithCookies(cookie string) *http.Request {
	r := httptest.NewRequest("GET", "/", nil)
	if cookie != "" {
		r.Header.Set("Cookie", cookie)
	}
	return r
}

func extractCookie(w *httptest.ResponseRecorder) string {
	sc := w.Result().Header.Get("Set-Cookie")
	if i := strings.IndexByte(sc, ';'); i >= 0 {
		return sc[:i]
	}
	return sc
}

func TestBindPersistMigrate(t *testing.T) {
	s := NewStore()

	// first bind => cookie issued, peer a
	w := httptest.NewRecorder()
	r := reqWithCookies("")
	peer, issued := s.Bind("SID", "a", 60, w, r, 1000)
	if peer != "a" || !issued {
		t.Fatalf("first bind: peer=%s issued=%v", peer, issued)
	}
	cookie := extractCookie(w)
	t.Logf("判定: 首次绑定签发 %s", cookie)

	// returning with cookie resolves to a, no new cookie
	r2 := reqWithCookies(cookie)
	if got := s.CookiePeer("SID", r2, 2000); got != "a" {
		t.Fatalf("sticky resolution got %q", got)
	}
	w2 := httptest.NewRecorder()
	peer2, issued2 := s.Bind("SID", "a", 60, w2, r2, 2000)
	if peer2 != "a" || issued2 || w2.Result().Header.Get("Set-Cookie") != "" {
		t.Fatalf("existing binding must not re-issue cookie: %+v", w2.Header())
	}

	// TTL expiry
	if got := s.CookiePeer("SID", r2, 62_000); got != "" {
		t.Fatalf("expired binding must resolve empty, got %q", got)
	}
	t.Log("判定: TTL 过期后解析为空，客户端将被重新绑定")

	// migration: old token deleted, new cookie issued
	r3 := reqWithCookies(cookie)
	w3 := httptest.NewRecorder()
	migrated := s.MigrateTo("SID", "b", 60, w3, r3, 100_000)
	if migrated != "b" {
		t.Fatalf("migrated peer=%s", migrated)
	}
	newCookie := extractCookie(w3)
	if newCookie == cookie {
		t.Fatal("migration must issue a new token")
	}
	if got := s.CookiePeer("SID", reqWithCookies(cookie), 100_000); got != "" {
		t.Fatal("old token must be invalid after migration")
	}
	if got := s.CookiePeer("SID", reqWithCookies(newCookie), 100_000); got != "b" {
		t.Fatal("new token must bind to b")
	}
	t.Logf("判定: 迁移后旧 token 失效、新 token=%s 绑定 b，不会出现会话错乱", newCookie)
}

var _ = http.StatusOK
