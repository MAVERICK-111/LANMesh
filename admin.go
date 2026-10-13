package main

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// adminSecret is injected at build time — never commit a real value to source:
//
//	go build -ldflags="-X main.adminSecret=PASSWORD" -o lanmesh-admin .
//
// Empty string → regular node; all mutating /admin routes return 403.
var adminSecret string

var (
	nickMu             sync.RWMutex
	configuredNickname string
)

func setConfiguredNickname(nick string) {
	nickMu.Lock()
	defer nickMu.Unlock()
	configuredNickname = strings.TrimSpace(nick)
}

func getConfiguredNickname() string {
	nickMu.RLock()
	defer nickMu.RUnlock()
	return configuredNickname
}

func isAdminBinary() bool { return adminSecret != "" }

// ─── Nonce store (single-use, 2-min TTL) ─────────────────────────────────────

type nonceStore struct {
	mu     sync.Mutex
	nonces map[string]time.Time
}

var nonces = &nonceStore{nonces: make(map[string]time.Time)}

func (ns *nonceStore) issue() string {
	b := make([]byte, 16)
	rand.Read(b) //nolint:errcheck
	id := hex.EncodeToString(b)
	ns.mu.Lock()
	ns.nonces[id] = time.Now().Add(2 * time.Minute)
	ns.mu.Unlock()
	return id
}

// consume returns true if the nonce exists, is fresh, and deletes it (single-use).
func (ns *nonceStore) consume(id string) bool {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	exp, ok := ns.nonces[id]
	if !ok || time.Now().After(exp) {
		delete(ns.nonces, id)
		return false
	}
	delete(ns.nonces, id)
	return true
}

// ─── Session store (8-hour TTL) ──────────────────────────────────────────────

type sessionStore struct {
	mu       sync.Mutex
	sessions map[string]time.Time
}

var adminSessions = &sessionStore{sessions: make(map[string]time.Time)}

func (ss *sessionStore) create() string {
	b := make([]byte, 24)
	rand.Read(b) //nolint:errcheck
	tok := hex.EncodeToString(b)
	ss.mu.Lock()
	ss.sessions[tok] = time.Now().Add(8 * time.Hour)
	ss.mu.Unlock()
	return tok
}

func (ss *sessionStore) valid(tok string) bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	exp, ok := ss.sessions[tok]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(ss.sessions, tok)
		return false
	}
	return true
}

func (ss *sessionStore) revoke(tok string) {
	ss.mu.Lock()
	delete(ss.sessions, tok)
	ss.mu.Unlock()
}

func sessionToken(r *http.Request) (string, bool) {
	c, err := r.Cookie("lanmesh_admin")
	if err != nil {
		return "", false
	}
	if adminSessions.valid(c.Value) {
		return c.Value, true
	}
	return "", false
}

func hasAdminSession(r *http.Request) bool {
	_, ok := sessionToken(r)
	return ok
}

// requireAdmin gates a handler: must be admin binary + valid session cookie.
func requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !isAdminBinary() {
			http.Error(w, "not an admin node", http.StatusForbidden)
			return
		}
		if !hasAdminSession(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// computeHMAC returns HMAC-SHA256(nonce, adminSecret) as a hex string.
// The password never travels over the wire — only this digest does.
func computeHMAC(nonce string) string {
	mac := hmac.New(sha256.New, []byte(adminSecret))
	mac.Write([]byte(nonce))
	return hex.EncodeToString(mac.Sum(nil))
}

// ─── Admin log (append-only JSONL) ───────────────────────────────────────────

type adminStore struct {
	mu       sync.Mutex
	file     *os.File
	messages []ChatMessage
	seenIDs  map[string]struct{}
}

func newAdminStore(path string) (*adminStore, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	store := &adminStore{
		file:    f,
		seenIDs: make(map[string]struct{}),
	}
	// Replay prior sessions so the dashboard survives restarts.
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		var m ChatMessage
		if err := json.Unmarshal(scanner.Bytes(), &m); err == nil {
			if m.Type == "ping" || m.Type == "typing" || m.Type == "whoami" || m.Type == "" {
				continue
			}
			if m.MessageID != "" {
				store.seenIDs[m.MessageID] = struct{}{}
			}
			store.messages = append(store.messages, m)
		}
	}
	return store, nil
}

func (a *adminStore) record(msg ChatMessage) {
	// Heartbeats, typing indicators, whoami, and empty messages are ephemeral — never log them.
	if msg.Type == "ping" || msg.Type == "typing" || msg.Type == "whoami" || msg.Type == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	// Deduplicate by MessageID so synced or replayed messages aren't logged twice
	if msg.MessageID != "" {
		if _, exists := a.seenIDs[msg.MessageID]; exists {
			return
		}
		a.seenIDs[msg.MessageID] = struct{}{}
	}

	a.messages = append(a.messages, msg)
	if data, err := json.Marshal(msg); err == nil {
		a.file.Write(data)        //nolint:errcheck
		a.file.Write([]byte("\n")) //nolint:errcheck
	}
}

// backfillFromDB scans all groups in the node's local BoltDB (lanmesh.db)
// and ingests any historical or synced messages that aren't yet in the admin store.
func (a *adminStore) backfillFromDB() {
	if db == nil {
		return
	}
	groups := append(knownGroups(), "general")
	seenGroup := make(map[string]struct{})

	for _, g := range groups {
		if _, dup := seenGroup[g]; dup {
			continue
		}
		seenGroup[g] = struct{}{}

		_ = db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(g))
			if b == nil {
				return nil
			}
			return b.ForEach(func(k, v []byte) error {
				var msg ChatMessage
				if err := json.Unmarshal(v, &msg); err == nil {
					a.record(msg)
				}
				return nil
			})
		})
	}
}

func (a *adminStore) snapshot() []ChatMessage {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]ChatMessage, len(a.messages))
	copy(out, a.messages)
	return out
}

// ─── Runtime admin store state ────────────────────────────────────────────────

var (
	adminMu   sync.RWMutex
	adminStor *adminStore
	adminDir  string
)

func setAdminDataDir(dir string) { adminDir = dir }

func currentAdminStore() *adminStore {
	adminMu.RLock()
	defer adminMu.RUnlock()
	return adminStor
}

func enableAdminLogging() (*adminStore, error) {
	adminMu.Lock()
	defer adminMu.Unlock()
	if adminStor != nil {
		adminStor.backfillFromDB()
		return adminStor, nil
	}
	logPath := filepath.Join(adminDir, "lanmesh-admin-log.jsonl")
	s, err := newAdminStore(logPath)
	if err != nil {
		return nil, err
	}
	adminStor = s
	fmt.Println("Admin logging enabled — writing to", logPath)
	adminStor.backfillFromDB()
	return adminStor, nil
}

func isAdminLoggingEnabled() bool { return currentAdminStore() != nil }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// ─── Route registration ───────────────────────────────────────────────────────

// registerAdminRoutes mounts all /admin/* and /is-admin endpoints.
// broadcastGroup is called when admin creates a new group; it must persist
// the group_announce and relay it over mesh + local WebSocket hub.
func registerAdminRoutes(mux *http.ServeMux, broadcastGroup func(name string)) {

	// /is-admin — no auth needed; tells the frontend what powers this node has.
	mux.HandleFunc("/is-admin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
			"isAdmin":         isAdminBinary(),
			"sessionActive":   isAdminBinary() && hasAdminSession(r),
			"defaultNickname": getConfiguredNickname(),
		})
	})

	// /admin/challenge — GET; issue a fresh nonce for the HMAC handshake.
	mux.HandleFunc("/admin/challenge", func(w http.ResponseWriter, r *http.Request) {
		if !isAdminBinary() {
			http.Error(w, "not an admin node", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"nonce": nonces.issue()}) //nolint:errcheck
	})

	// /admin/auth — POST { nonce, token } → sets session cookie.
	// The client computes token = HMAC-SHA256(nonce, password) locally
	// via WebCrypto so the raw password never leaves the browser.
	mux.HandleFunc("/admin/auth", func(w http.ResponseWriter, r *http.Request) {
		if !isAdminBinary() {
			http.Error(w, "not an admin node", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Nonce string `json:"nonce"`
			Token string `json:"token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Nonce == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if !nonces.consume(body.Nonce) {
			http.Error(w, "invalid or expired nonce", http.StatusUnauthorized)
			return
		}
		expected := computeHMAC(body.Nonce)
		// Constant-time compare prevents timing attacks.
		if !hmac.Equal([]byte(body.Token), []byte(expected)) {
			http.Error(w, "wrong password", http.StatusUnauthorized)
			return
		}
		// Activate logging on first successful auth.
		enableAdminLogging() //nolint:errcheck
		tok := adminSessions.create()
		http.SetCookie(w, &http.Cookie{
			Name:     "lanmesh_admin",
			Value:    tok,
			Path:     "/",
			MaxAge:   8 * 3600,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
		})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"ok": true}) //nolint:errcheck
	})

	// /admin/logout — POST; revoke session cookie.
	mux.HandleFunc("/admin/logout", func(w http.ResponseWriter, r *http.Request) {
		if tok, ok := sessionToken(r); ok {
			adminSessions.revoke(tok)
		}
		http.SetCookie(w, &http.Cookie{Name: "lanmesh_admin", Value: "", MaxAge: -1, Path: "/"})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"ok": true}) //nolint:errcheck
	})

	// /admin/groups — POST { name }; create group (admin session required).
	// This is the ONLY path that creates a group — the WebSocket handler
	// explicitly drops any group_announce sent directly by a browser tab.
	mux.HandleFunc("/admin/groups", requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		name := strings.TrimSpace(strings.ToLower(body.Name))
		if name == "" {
			http.Error(w, "name required", http.StatusBadRequest)
			return
		}
		broadcastGroup(name)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"group": name}) //nolint:errcheck
	}))

	// /admin/status — machine-readable capability check.
	mux.HandleFunc("/admin/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"enabled":%v,"isAdminNode":%v}`, isAdminLoggingEnabled(), isAdminBinary())
	})

	// /admin/export.json — full log download (session required).
	mux.HandleFunc("/admin/export.json", requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		store := currentAdminStore()
		w.Header().Set("Content-Type", "application/json")
		if store == nil {
			w.Write([]byte("[]")) //nolint:errcheck
			return
		}
		json.NewEncoder(w).Encode(store.snapshot()) //nolint:errcheck
	}))

	// /admin — human-readable dashboard; shows login form if unauthenticated.
	mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		if !isAdminBinary() {
			http.Error(w, "not an admin node", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if !hasAdminSession(r) {
			serveAdminLogin(w)
			return
		}
		serveAdminDashboard(w, currentAdminStore())
	})
}

// ─── Admin login page ─────────────────────────────────────────────────────────

// ─── Admin login page ─────────────────────────────────────────────────────────

func serveAdminLogin(w http.ResponseWriter) {
	fmt.Fprint(w, `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LANMesh — Admin Login</title>
<link href="https://fonts.googleapis.com/css2?family=Sora:wght@700;800&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;}
body{background:#050506;color:#eee;font-family:'Inter',sans-serif;display:flex;
     align-items:center;justify-content:center;min-height:100vh;margin:0;
     background-image:radial-gradient(900px 500px at 20% 10%,rgba(242,194,48,0.06),transparent 60%);}
.card{background:#141414;border:1px solid #2a2a2a;border-radius:20px;padding:40px 36px;width:340px;
      box-shadow:0 30px 60px -20px rgba(0,0,0,.8),0 0 0 1px rgba(242,194,48,0.04);}
.brand{display:flex;align-items:center;gap:10px;margin-bottom:8px;}
.brand h1{font-family:'Sora',sans-serif;font-size:1.15rem;font-weight:800;letter-spacing:.05em;margin:0;}
.brand h1 span{color:#f2c230;}
.lock-icon{width:36px;height:36px;border-radius:10px;background:rgba(242,194,48,0.12);
           display:flex;align-items:center;justify-content:center;flex-shrink:0;}
p{font-size:.8rem;color:#8d8d87;margin:0 0 26px;line-height:1.55;}
label{font-size:.72rem;font-weight:600;color:#8d8d87;letter-spacing:.05em;
      text-transform:uppercase;display:block;margin-bottom:6px;}
input{width:100%;background:#1b1b1c;border:1px solid #2a2a2a;color:#eee;
      padding:12px 15px;border-radius:10px;font-size:.9rem;outline:none;
      margin-bottom:18px;transition:border-color .15s;}
input:focus{border-color:#7a6318;box-shadow:0 0 0 3px rgba(242,194,48,.08);}
button{width:100%;background:#f2c230;color:#0b0b0c;border:none;padding:12px;
       border-radius:10px;font-weight:700;font-size:.9rem;cursor:pointer;transition:opacity .15s,transform .1s;}
button:hover{opacity:.88;}
button:active{transform:scale(.97);}
.err{color:#e2483d;font-size:.78rem;margin-top:8px;display:none;padding:8px 12px;
     background:rgba(226,72,61,.08);border-radius:8px;border:1px solid rgba(226,72,61,.2);}
.admin-gif-wrap{margin:14px 0 16px;border-radius:12px;overflow:hidden;border:1px solid #2a2a2a;background:#0b0b0c;box-shadow:0 8px 24px rgba(0,0,0,0.5);}
.admin-gif{width:100%;height:auto;max-height:170px;object-fit:cover;display:block;}
</style></head><body>
<div class="card">
  <div class="brand">
    <div class="lock-icon">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none">
        <rect x="3" y="11" width="18" height="11" rx="2" stroke="#f2c230" stroke-width="1.8"/>
        <path d="M7 11V7a5 5 0 0110 0v4" stroke="#f2c230" stroke-width="1.8" stroke-linecap="round"/>
      </svg>
    </div>
    <h1>LAN<span>MESH</span> Admin</h1>
  </div>
  <div class="admin-gif-wrap">
    <img src="/maverickclassified.gif" alt="Classified" class="admin-gif">
  </div>
  <p>Enter the admin password to unlock the dashboard and group management.</p>
  <label for="pw">Password</label>
  <input type="password" id="pw" placeholder="Enter admin password"
         onkeydown="if(event.key===&apos;Enter&apos;)login()">
  <button onclick="login()">Unlock Dashboard</button>
  <a href="/" style="display:block;text-align:center;margin-top:16px;color:#8d8d87;font-size:.8rem;text-decoration:none;transition:color .15s;" onmouseover="this.style.color='#f2c230'" onmouseout="this.style.color='#8d8d87'">&#x2190; Return to Chat Screen</a>
  <div class="err" id="err"></div>
</div>
<script>
async function login(){
  const pw=document.getElementById('pw').value;
  const err=document.getElementById('err');
  err.style.display='none';
  if(!pw){err.textContent='Password required';err.style.display='block';return;}
  try{
    const{nonce}=await fetch('/admin/challenge').then(r=>r.json());
    const key=await crypto.subtle.importKey('raw',new TextEncoder().encode(pw),
      {name:'HMAC',hash:'SHA-256'},false,['sign']);
    const sig=await crypto.subtle.sign('HMAC',key,new TextEncoder().encode(nonce));
    const token=Array.from(new Uint8Array(sig)).map(b=>b.toString(16).padStart(2,'0')).join('');
    const res=await fetch('/admin/auth',{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({nonce,token})});
    if(res.ok){window.location.reload();}
    else{err.textContent='Wrong password';err.style.display='block';}
  }catch(e){err.textContent='Error: '+e.message;err.style.display='block';}
}
</script></body></html>`)
}

// ─── Admin dashboard ──────────────────────────────────────────────────────────

func serveAdminDashboard(w http.ResponseWriter, store *adminStore) {
	var rows string
	msgCount := 0
	if store != nil {
		msgs := store.snapshot()
		msgCount = len(msgs)
		sort.Slice(msgs, func(i, j int) bool { return msgs[i].Timestamp > msgs[j].Timestamp })
		for _, m := range msgs {
			preview := m.Payload
			if m.Type != "text" {
				preview = ""
			} else {
				preview = truncate(preview, 300)
			}
			ts := ""
			if m.Timestamp > 0 {
				ts = time.UnixMilli(m.Timestamp).Format("Jan 02 15:04:05")
			}
			rows += fmt.Sprintf(
				"<tr data-group=%q data-type=%q data-sender=%q>"+
					"<td class=ts>%s</td>"+
					"<td><span class=tag>#%s</span></td>"+
					"<td class=nick>%s</td>"+
					"<td class=pid title=%q>%s&hellip;<button class=copy-btn onclick=\"copyPID(this,%q)\">&#x2398;</button></td>"+
					"<td><span class='typebadge typebadge-%s'>%s</span></td>"+
					"<td class=msg-cell>%s</td></tr>",
				m.GroupID, m.Type, m.Sender,
				html.EscapeString(ts),
				html.EscapeString(m.GroupID),
				html.EscapeString(m.Nickname),
				m.Sender, html.EscapeString(truncate(m.Sender, 16)), m.Sender,
				html.EscapeString(m.Type), html.EscapeString(m.Type),
				html.EscapeString(preview),
			)
		}
	}

	page := `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LANMesh Admin</title>
<link href="https://fonts.googleapis.com/css2?family=Sora:wght@700;800&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;}
:root{--bg:#0b0b0c;--panel:#141414;--raised:#1b1b1c;--line:#2a2a2a;
      --yellow:#f2c230;--yellow-dim:#7a6318;--green:#3ddc84;--red:#e2483d;
      --text:#eeeeea;--muted:#8d8d87;}
body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;margin:0;min-height:100vh;}
.topbar{display:flex;align-items:center;gap:14px;padding:14px 28px;
        border-bottom:1px solid var(--line);background:var(--panel);
        position:sticky;top:0;z-index:10;flex-wrap:wrap;}
.brand h1{font-family:'Sora',sans-serif;font-size:1rem;font-weight:800;
          letter-spacing:.05em;margin:0;color:var(--text);}
.brand h1 span{color:var(--yellow);}
.live-pill{display:flex;align-items:center;gap:6px;padding:4px 10px;
           background:rgba(61,220,132,.08);border:1px solid rgba(61,220,132,.2);
           border-radius:999px;font-size:.7rem;font-weight:600;color:var(--green);}
.live-dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:blink 1.4s infinite;}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
.topbar-right{margin-left:auto;display:flex;align-items:center;gap:10px;}
.btn-ghost{background:none;border:1px solid var(--line);color:var(--muted);padding:6px 14px;
           border-radius:8px;cursor:pointer;font-size:.78rem;transition:border-color .15s,color .15s;}
.btn-ghost:hover{border-color:var(--red);color:var(--red);}
.btn-yellow{background:var(--yellow);color:#0b0b0c;border:none;padding:7px 16px;
            border-radius:8px;cursor:pointer;font-size:.78rem;font-weight:700;transition:opacity .15s;}
.btn-yellow:hover{opacity:.85;}
a.export-link{color:var(--yellow);font-size:.78rem;text-decoration:none;}
a.export-link:hover{text-decoration:underline;}
.btn-chat{display:inline-flex;align-items:center;gap:6px;background:rgba(242,194,48,.1);border:1px solid rgba(242,194,48,.3);color:var(--yellow);padding:6px 14px;border-radius:8px;text-decoration:none;font-size:.78rem;font-weight:600;transition:all .15s;}
.btn-chat:hover{background:var(--yellow);color:#0b0b0c;border-color:var(--yellow);}
.stats-bar{display:flex;gap:14px;padding:16px 28px;border-bottom:1px solid var(--line);flex-wrap:wrap;}
.stat-card{background:var(--panel);border:1px solid var(--line);border-radius:12px;
           padding:12px 20px;flex:1;min-width:120px;}
.stat-val{font-family:'Sora',sans-serif;font-size:1.5rem;font-weight:800;color:var(--yellow);line-height:1;}
.stat-label{font-size:.67rem;color:var(--muted);margin-top:4px;text-transform:uppercase;letter-spacing:.06em;}
.create-bar{display:flex;gap:10px;padding:12px 28px;border-bottom:1px solid var(--line);
            align-items:center;flex-wrap:wrap;}
.create-bar input{background:var(--raised);border:1px solid var(--line);color:var(--text);
                  padding:9px 15px;border-radius:999px;font-size:.85rem;outline:none;
                  flex:1;min-width:180px;transition:border-color .15s;}
.create-bar input:focus{border-color:var(--yellow-dim);}
.ok-msg{font-size:.78rem;display:none;}
.filter-bar{display:flex;gap:10px;padding:12px 28px;border-bottom:1px solid var(--line);
            flex-wrap:wrap;align-items:center;}
.filter-bar input,.filter-bar select{background:var(--raised);border:1px solid var(--line);
  color:var(--text);padding:7px 12px;border-radius:8px;font-size:.8rem;
  outline:none;transition:border-color .15s;}
.filter-bar input:focus,.filter-bar select:focus{border-color:var(--yellow-dim);}
.filter-bar select option{background:var(--raised);}
.filter-label{font-size:.72rem;color:var(--muted);font-weight:600;letter-spacing:.05em;text-transform:uppercase;}
.result-count{margin-left:auto;font-size:.75rem;color:var(--muted);}
.table-wrap{padding:0 28px 40px;overflow-x:auto;}
table{width:100%;border-collapse:collapse;font-size:.83rem;margin-top:16px;}
th,td{border-bottom:1px solid var(--line);padding:9px 10px;text-align:left;vertical-align:top;}
th{color:var(--muted);text-transform:uppercase;font-size:.67rem;letter-spacing:.06em;font-weight:600;}
tr:hover td{background:rgba(255,255,255,.02);}
.tag{background:var(--raised);border:1px solid var(--line);color:var(--yellow);
     padding:2px 8px;border-radius:999px;font-size:.72rem;}
.typebadge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:.7rem;font-weight:600;}
.typebadge-text{background:rgba(61,220,132,.1);color:var(--green);}
.typebadge-image,.typebadge-file,.typebadge-audio{background:rgba(242,194,48,.1);color:var(--yellow);}
.typebadge-reaction,.typebadge-edit,.typebadge-delete{background:rgba(255,255,255,.06);color:var(--muted);}
.typebadge-location{background:rgba(100,149,237,.1);color:#6495ed;}
.ts{font-size:.75rem;color:var(--muted);white-space:nowrap;}
.nick{font-weight:600;}
.pid{font-size:.72rem;color:var(--muted);white-space:nowrap;}
.msg-cell{max-width:300px;word-break:break-word;}
.copy-btn{background:none;border:none;color:var(--muted);cursor:pointer;font-size:.75rem;
          padding:0 0 0 4px;line-height:1;opacity:.6;}
.copy-btn:hover{opacity:1;color:var(--yellow);}
.empty{padding:48px;text-align:center;color:var(--muted);font-size:.88rem;}
</style></head><body>
<div class="topbar">
  <div class="brand">
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
      <circle cx="6" cy="6" r="2.5" stroke="#f2c230" stroke-width="1.8"/>
      <circle cx="18" cy="6" r="2.5" stroke="#f2c230" stroke-width="1.8"/>
      <circle cx="12" cy="18" r="2.5" stroke="#f2c230" stroke-width="1.8"/>
      <path d="M8 7.5L11 16M16 7.5L13 16M8.5 6H15.5" stroke="#f2c230" stroke-width="1.5"/>
    </svg>
    <h1>LAN<span>MESH</span> Admin</h1>
  </div>
  <div class="live-pill"><div class="live-dot"></div>LIVE</div>
  <div class="topbar-right">
    <a class="btn-chat" href="/" title="Open LANMesh Chat Screen">&#x1F4AC; Open Chat</a>
    <a class="export-link" href="/admin/export.json" download="lanmesh-log.json">&#x2B07; Export JSON</a>
    <button class="btn-ghost" onclick="logout()">Logout</button>
  </div>
</div>
<div class="stats-bar">
  <div class="stat-card"><div class="stat-val" id="statMsgs">__COUNT__</div><div class="stat-label">Messages logged</div></div>
  <div class="stat-card"><div class="stat-val" id="statPeers">&#8211;</div><div class="stat-label">Connected peers</div></div>
  <div class="stat-card"><div class="stat-val" id="statHops">&#8211;</div><div class="stat-label">Max hops observed</div></div>
  <div class="stat-card"><div class="stat-val" id="statActivity">&#8211;</div><div class="stat-label">Last activity</div></div>
</div>
<div class="create-bar">
  <input type="text" id="grpName" placeholder="New group name&#x2026;"
         onkeydown="if(event.key===&apos;Enter&apos;)createGroup()">
  <button class="btn-yellow" onclick="createGroup()">+ Create Group</button>
  <span class="ok-msg" id="grpMsg"></span>
</div>
<div class="filter-bar">
  <span class="filter-label">Filter</span>
  <input type="text" id="fSearch" placeholder="Search content or nickname&#x2026;" oninput="applyFilter()">
  <select id="fGroup" onchange="applyFilter()"><option value="">All groups</option></select>
  <select id="fType" onchange="applyFilter()">
    <option value="">All types</option>
    <option>text</option><option>image</option><option>audio</option><option>file</option>
    <option>location</option><option>reaction</option><option>edit</option><option>delete</option>
  </select>
  <span class="result-count" id="resultCount"></span>
</div>
<div class="table-wrap">
  <table id="logTable">
    <thead><tr><th>Time</th><th>Group</th><th>Nickname</th><th>Peer ID</th><th>Type</th><th>Content</th></tr></thead>
    <tbody id="logBody">__ROWS__</tbody>
  </table>
  <div class="empty" id="emptyMsg" style="display:none">No messages match your filter.</div>
</div>
<script>
async function refreshStats(){
  try{
    const h=await fetch('/health').then(r=>r.json());
    document.getElementById('statPeers').textContent=h.connectedPeers??'--';
    document.getElementById('statHops').textContent=h.maxHopsObserved??'--';
    const s=h.secondsSinceLastActivity;
    document.getElementById('statActivity').textContent=
      s<0?'--':s<60?Math.round(s)+'s ago':s<3600?Math.round(s/60)+'m ago':Math.round(s/3600)+'h ago';
  }catch(e){}
}
refreshStats();
setInterval(refreshStats,10000);
setInterval(function(){window.location.reload();},30000);
(function(){
  const groups=new Set();
  document.querySelectorAll('#logBody tr').forEach(function(r){groups.add(r.dataset.group);});
  const sel=document.getElementById('fGroup');
  Array.from(groups).filter(Boolean).sort().forEach(function(g){
    const o=document.createElement('option');o.value=g;o.textContent='#'+g;sel.appendChild(o);
  });
  updateCount();
})();
function applyFilter(){
  const search=document.getElementById('fSearch').value.toLowerCase();
  const group=document.getElementById('fGroup').value;
  const type=document.getElementById('fType').value;
  let visible=0;
  document.querySelectorAll('#logBody tr').forEach(function(row){
    const show=(!group||row.dataset.group===group)&&(!type||row.dataset.type===type)&&
               (!search||row.textContent.toLowerCase().includes(search)||row.dataset.sender.includes(search));
    row.style.display=show?'':'none';
    if(show)visible++;
  });
  updateCount(visible);
}
function updateCount(n){
  const total=document.querySelectorAll('#logBody tr').length;
  if(n===undefined)n=total;
  document.getElementById('resultCount').textContent=n===total?total+' messages':n+' of '+total+' shown';
  document.getElementById('emptyMsg').style.display=n===0?'block':'none';
  document.getElementById('logTable').style.display=n===0?'none':'table';
}
async function createGroup(){
  const name=document.getElementById('grpName').value.trim();
  if(!name)return;
  const msgEl=document.getElementById('grpMsg');msgEl.style.display='none';
  const res=await fetch('/admin/groups',{method:'POST',
    headers:{'Content-Type':'application/json'},body:JSON.stringify({name})});
  if(res.ok){
    const d=await res.json();
    document.getElementById('grpName').value='';
    msgEl.textContent='Group "'+d.group+'" created';
    msgEl.style.color='#3ddc84';msgEl.style.display='inline';
    setTimeout(function(){msgEl.style.display='none';},5000);
  }else{msgEl.textContent=await res.text();msgEl.style.color='#e2483d';msgEl.style.display='inline';}
}
async function logout(){await fetch('/admin/logout',{method:'POST'});window.location.reload();}
function copyPID(btn,pid){
  navigator.clipboard.writeText(pid).then(function(){
    btn.textContent='OK';setTimeout(function(){btn.textContent='Copy';},1500);
  });
}
</script></body></html>`

	out := strings.ReplaceAll(page, "__COUNT__", fmt.Sprintf("%d", msgCount))
	out = strings.ReplaceAll(out, "__ROWS__", rows)
	fmt.Fprint(w, out)
}
