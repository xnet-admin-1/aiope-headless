package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/XNet-NGO/AIOPE-Headless/internal/conversation"
	"github.com/XNet-NGO/AIOPE-Headless/internal/gateway"
	"github.com/XNet-NGO/AIOPE-Headless/internal/llm"
	"github.com/XNet-NGO/AIOPE-Headless/internal/mcp"
	"github.com/XNet-NGO/AIOPE-Headless/internal/message"
	"github.com/XNet-NGO/AIOPE-Headless/internal/provider"
	"github.com/XNet-NGO/AIOPE-Headless/internal/remote"
	"github.com/XNet-NGO/AIOPE-Headless/internal/settings"
	"github.com/XNet-NGO/AIOPE-Headless/internal/terminal"
	"github.com/XNet-NGO/AIOPE-Headless/internal/ws"
	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/pquerna/otp/totp"
)

const defaultSystemPrompt = `## Identity
You are AIOPE, a personal intelligent agent and system orchestrator running on the user's server infrastructure. You are not a distant cloud AI — you run on their hardware with direct access to the filesystem, shell, network, and connected remote servers via SSH.

## Identity
Competent, efficient, and quietly confident. You do not chat — you solve. You are warm but not saccharine, helpful but not deferential. Be direct: give the user exactly what they need, not conversational filler. Be proactive: if you see a better way, take the initiative.
Concise and professional. Use short sentences. Avoid hedging language. When presenting information, use tables, lists, or structured formats over prose. Match the user's energy — brief questions get brief answers, detailed questions get thorough responses.

## Values & Rules
Privacy first: you have access to deeply personal data — respect that. Never leak or log sensitive info unnecessarily.
Efficiency: minimize round-trips. Chain tools together to get answers in one go.
Autonomy: when the user gives you a goal, figure out the best path. You do not wait to be told every step.
If you are about to do something significant (deleting data, writing to important files, running destructive commands), confirm with the user first.
If you are uncertain, say so and propose a path forward rather than guessing.
Do not make up information — use tools to verify facts.

## Tool Guidance
You MUST use tools to accomplish tasks. Do NOT describe what you would do — actually call the tools. Never fabricate tool output or pretend you ran a command.
For multi-step tasks, chain tools together. Use parallel execution for independent read operations.
When a tool fails, explain what happened and try an alternative approach.
Use search_web for current events and facts you don't know.
Use fetch_url to retrieve web pages (mode: text for readable content, md for markdown with links/headings, raw for raw response).
Use task to delegate independent research to a subagent — it runs in parallel with read-only tools.
Use ssh_start/ssh_exec/ssh_exit for remote server operations. NEVER use run_sh with ssh/scp commands — always use the built-in SSH tools.
IMPORTANT: run_sh executes on THIS container only. To run commands on remote servers, you MUST use ssh_exec. Do not simulate or emulate remote execution locally.
Each tool call must include ALL required parameters with valid values. Never send empty arguments.

## Response Style
Use markdown for code blocks with language tags.
Use tables for structured data, bullet points for lists.
Keep responses focused — answer the question, then stop.
For code: always use fenced code blocks with the language specified.
For commands: show the command, then the expected output.
For errors: explain what went wrong and suggest a fix.
For multi-step tasks: number the steps and execute them sequentially.
For images: always use markdown image syntax ![alt](url) — never bare URLs. Local files render inline: ![desc](/path/to/file.png).`

type Server struct {
	Conversations *conversation.Service
	Messages      *message.Service
	Settings      *settings.Service
	Providers     *provider.Service
	Hub           *ws.Hub
	Provider      llm.Provider
	Model         string
	WebFS         fs.FS
	DB            *sql.DB
	MCP           *mcp.Manager
	Remote        *remote.Service
	Gateway       *gateway.Router
	Password      string
	BasePath      string
	sessionToken  string
	mu            sync.RWMutex
	cancels       sync.Map // conversationId -> context.CancelFunc
	autoRun       sync.Map // conversationId -> bool
	autoRunCount  sync.Map // conversationId -> int
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("GET /api/conversations", s.listConversations)
	mux.HandleFunc("POST /api/conversations", s.createConversation)
	mux.HandleFunc("GET /api/conversations/{id}", s.getConversation)
	mux.HandleFunc("PATCH /api/conversations/{id}", s.updateConversation)
	mux.HandleFunc("DELETE /api/conversations/{id}", s.deleteConversation)
	mux.HandleFunc("GET /api/conversations/{id}/export", s.exportConversation)
	mux.HandleFunc("GET /api/settings", s.getSettings)
	mux.HandleFunc("PUT /api/settings/{key}", s.setSetting)
	mux.HandleFunc("GET /api/settings/export", s.exportSettings)
	mux.HandleFunc("POST /api/settings/import", s.importSettings)

	// Provider routes
	mux.HandleFunc("GET /api/providers", s.listProviders)
	mux.HandleFunc("POST /api/providers", s.createProvider)
	mux.HandleFunc("PUT /api/providers/{id}", s.updateProvider)
	mux.HandleFunc("DELETE /api/providers/{id}", s.deleteProvider)
	mux.HandleFunc("POST /api/providers/{id}/activate", s.activateProvider)
	mux.HandleFunc("GET /api/providers/{id}/models", s.fetchModels)

	// Tool toggles
	mux.HandleFunc("GET /api/tools", s.listToolToggles)
	mux.HandleFunc("PUT /api/tools/{id}", s.setToolToggle)

	// Gateway routes
	mux.HandleFunc("GET /api/gateway/providers", s.gwListProviders)
	mux.HandleFunc("POST /api/gateway/providers", s.gwAddProvider)
	mux.HandleFunc("PUT /api/gateway/providers", s.gwUpdateProvider)
	mux.HandleFunc("DELETE /api/gateway/providers", s.gwDeleteProvider)
	mux.HandleFunc("POST /api/gateway/discover", s.gwDiscoverModels)
	mux.HandleFunc("GET /api/gateway/routes", s.gwListRoutes)
	mux.HandleFunc("POST /api/gateway/routes", s.gwAddRoute)
	mux.HandleFunc("PUT /api/gateway/routes", s.gwUpdateRoute)
	mux.HandleFunc("DELETE /api/gateway/routes", s.gwDeleteRoute)

	// Memories
	mux.HandleFunc("GET /api/memories", s.listMemories)
	mux.HandleFunc("POST /api/memories", s.createMemory)
	mux.HandleFunc("PUT /api/memories/{key}", s.updateMemory)
	mux.HandleFunc("DELETE /api/memories/{key}", s.deleteMemory)

	// MCP servers
	mux.HandleFunc("GET /api/mcp/servers", s.listMcpServers)
	mux.HandleFunc("POST /api/mcp/servers", s.addMcpServer)
	mux.HandleFunc("PUT /api/mcp/servers/{id}", s.updateMcpServer)
	mux.HandleFunc("DELETE /api/mcp/servers/{id}", s.deleteMcpServer)
	mux.HandleFunc("POST /api/mcp/servers/{id}/connect", s.connectMcpServer)

	// Remote servers
	mux.HandleFunc("GET /api/remote/servers", s.listRemoteServers)
	mux.HandleFunc("POST /api/remote/servers", s.createRemoteServer)
	mux.HandleFunc("PUT /api/remote/servers/{id}", s.updateRemoteServer)
	mux.HandleFunc("DELETE /api/remote/servers/{id}", s.deleteRemoteServer)
	mux.HandleFunc("POST /api/remote/servers/{id}/connect", s.connectRemoteServer)
	mux.HandleFunc("POST /api/remote/servers/{id}/disconnect", s.disconnectRemoteServer)
	mux.HandleFunc("POST /api/remote/servers/{id}/deploy", s.deployRemoteServer)
	mux.HandleFunc("GET /api/remote/servers/{id}/health", s.healthRemoteServer)

	// Image upload & serve
	mux.HandleFunc("POST /api/upload", s.uploadImage)
	mux.HandleFunc("GET /api/upload", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("path")
		if p == "" {
			http.Error(w, "missing path", 400)
			return
		}
		http.ServeFile(w, r, p)
	})

	// Terminal
	mux.HandleFunc("/ws/term", terminal.HandleTerm)

	// File browser
	mux.HandleFunc("GET /api/files", s.listFiles)
	mux.HandleFunc("GET /api/files/read", s.readFileContent)

	// Connectivity check
	mux.HandleFunc("GET /api/check", s.checkConnectivity)

	// WebSocket
	mux.HandleFunc("/ws", s.handleWS)

	// Web client
	mux.HandleFunc("GET /login", func(w http.ResponseWriter, r *http.Request) {
		f, _ := s.WebFS.Open("login.html")
		if f != nil {
			defer f.Close()
			w.Header().Set("Content-Type", "text/html")
			io.Copy(w, f)
		}
	})
	mux.Handle("/", noCacheHandler(http.FileServer(http.FS(s.WebFS))))

	// Auth: if password set, wrap with session auth
	if s.Password != "" {
		s.sessionToken = uuid.NewString()
		mux.HandleFunc("POST /api/login", s.handleLogin)
		mux.HandleFunc("GET /api/totp/setup", s.handleTOTPSetup)
		mux.HandleFunc("POST /api/totp/verify", s.handleTOTPVerify)
		mux.HandleFunc("POST /api/totp/disable", s.handleTOTPDisable)
		mux.HandleFunc("POST /api/password", s.handlePasswordChange)
		return s.authMiddleware(mux)
	}
	return mux
}

func (s *Server) totpSecret() string {
	var secret string
	s.DB.QueryRow("SELECT value FROM settings_kv WHERE key='totp_secret'").Scan(&secret)
	return secret
}

func (s *Server) totpEnabled() bool {
	return s.totpSecret() != ""
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Password string `json:"password"`
		Code     string `json:"code"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if req.Password != s.Password {
		http.Error(w, `{"error":"invalid password"}`, 401)
		return
	}

	// If TOTP is enabled, require code
	if s.totpEnabled() {
		if req.Code == "" {
			// Password OK but need TOTP — tell client
			w.WriteHeader(200)
			w.Write([]byte(`{"needTotp":true}`))
			return
		}
		valid := totp.Validate(req.Code, s.totpSecret())
		if !valid {
			http.Error(w, `{"error":"invalid TOTP code"}`, 401)
			return
		}
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "aiope_session",
		Value:    s.sessionToken,
		Path:     s.BasePath + "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   86400 * 30,
	})
	w.WriteHeader(200)
	w.Write([]byte(`{"ok":true}`))
}

func (s *Server) handleTOTPSetup(w http.ResponseWriter, r *http.Request) {
	// Only allow if already authenticated
	cookie, err := r.Cookie("aiope_session")
	if err != nil || cookie.Value != s.sessionToken {
		http.Error(w, `{"error":"unauthorized"}`, 401)
		return
	}

	if s.totpEnabled() {
		w.Write([]byte(`{"enabled":true}`))
		return
	}

	key, err := totp.Generate(totp.GenerateOpts{
		Issuer:      "AgentX",
		AccountName: "user-x@agentx.fxcb3.dev",
	})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Store secret temporarily — confirmed on verify
	s.DB.Exec("INSERT OR REPLACE INTO settings_kv(key,value) VALUES('totp_pending',?)", key.Secret())
	writeJSON(w, map[string]string{"secret": key.Secret(), "url": key.URL()})
}

func (s *Server) handleTOTPVerify(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("aiope_session")
	if err != nil || cookie.Value != s.sessionToken {
		http.Error(w, `{"error":"unauthorized"}`, 401)
		return
	}

	var req struct{ Code string }
	json.NewDecoder(r.Body).Decode(&req)

	var pending string
	s.DB.QueryRow("SELECT value FROM settings_kv WHERE key='totp_pending'").Scan(&pending)
	if pending == "" {
		http.Error(w, `{"error":"no pending TOTP setup"}`, 400)
		return
	}

	if !totp.Validate(req.Code, pending) {
		http.Error(w, `{"error":"invalid code"}`, 401)
		return
	}

	// Confirmed — save as active secret
	s.DB.Exec("INSERT OR REPLACE INTO settings_kv(key,value) VALUES('totp_secret',?)", pending)
	s.DB.Exec("DELETE FROM settings_kv WHERE key='totp_pending'")
	w.Write([]byte(`{"ok":true}`))
}

func (s *Server) handleTOTPDisable(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("aiope_session")
	if err != nil || cookie.Value != s.sessionToken {
		http.Error(w, `{"error":"unauthorized"}`, 401)
		return
	}
	s.DB.Exec("DELETE FROM settings_kv WHERE key='totp_secret'")
	s.DB.Exec("DELETE FROM settings_kv WHERE key='totp_pending'")
	w.Write([]byte(`{"ok":true}`))
}

func (s *Server) handlePasswordChange(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("aiope_session")
	if err != nil || cookie.Value != s.sessionToken {
		http.Error(w, `{"error":"unauthorized"}`, 401)
		return
	}
	var req struct {
		Current string `json:"current"`
		New     string `json:"new"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if req.Current != s.Password {
		http.Error(w, `{"error":"current password incorrect"}`, 401)
		return
	}
	if len(req.New) < 6 {
		http.Error(w, `{"error":"password must be at least 6 characters"}`, 400)
		return
	}
	s.Password = req.New
	// Persist to DB so it survives restarts
	s.DB.Exec("INSERT OR REPLACE INTO settings_kv(key,value) VALUES('password',?)", req.New)
	w.Write([]byte(`{"ok":true}`))
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow login endpoint and static login page
		if r.URL.Path == "/api/login" || r.URL.Path == "/login" || r.URL.Path == "/api/totp/setup" || r.URL.Path == "/api/totp/verify" || r.URL.Path == "/api/totp/disable" || r.URL.Path == "/api/password" {
			next.ServeHTTP(w, r)
			return
		}
		cookie, err := r.Cookie("aiope_session")
		if err != nil || cookie.Value != s.sessionToken {
			// For API/WS requests return 401, for browser redirect to login
			if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/ws") {
				http.Error(w, `{"error":"unauthorized"}`, 401)
			} else {
				http.Redirect(w, r, s.BasePath+"/login", http.StatusTemporaryRedirect)
			}
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) listConversations(w http.ResponseWriter, r *http.Request) {
	convs, err := s.Conversations.List()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if convs == nil {
		convs = []conversation.Conversation{}
	}
	writeJSON(w, convs)
}

func (s *Server) createConversation(w http.ResponseWriter, r *http.Request) {
	var req struct{ Title string }
	json.NewDecoder(r.Body).Decode(&req)
	if req.Title == "" {
		req.Title = "New Chat"
	}
	c, err := s.Conversations.Create(req.Title)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.Hub.BroadcastJSON(map[string]any{"type": "conversation.created", "conversation": c})
	w.WriteHeader(201)
	writeJSON(w, c)
}

func (s *Server) getConversation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	c, err := s.Conversations.Get(id)
	if err != nil {
		http.Error(w, "not found", 404)
		return
	}
	msgs, _ := s.Messages.List(id)
	if msgs == nil {
		msgs = []message.Message{}
	}
	writeJSON(w, map[string]any{"conversation": c, "messages": msgs})
}

func (s *Server) updateConversation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req struct{ Title string }
	json.NewDecoder(r.Body).Decode(&req)
	if err := s.Conversations.UpdateTitle(id, req.Title); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.Hub.BroadcastJSON(map[string]any{"type": "conversation.updated", "id": id, "title": req.Title, "updatedAt": time.Now().UnixMilli()})
	w.WriteHeader(204)
}

func (s *Server) deleteConversation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.Conversations.Delete(id)
	s.Hub.BroadcastJSON(map[string]any{"type": "conversation.deleted", "id": id})
	w.WriteHeader(204)
}

func (s *Server) exportConversation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	conv, err := s.Conversations.Get(id)
	if err != nil {
		http.Error(w, "not found", 404)
		return
	}
	msgs, _ := s.Messages.List(id)
	format := r.URL.Query().Get("format")
	if format == "text" || format == "md" {
		w.Header().Set("Content-Type", "text/markdown")
		var b strings.Builder
		fmt.Fprintf(&b, "# %s\n\n", conv.Title)
		for _, m := range msgs {
			switch m.Role {
			case "user":
				fmt.Fprintf(&b, "## User\n\n%s\n\n", m.Content)
			case "assistant":
				fmt.Fprintf(&b, "## Assistant\n\n%s\n\n", m.Content)
			default:
				fmt.Fprintf(&b, "> _%s: %s_\n\n", m.Role, m.Content)
			}
		}
		w.Write([]byte(b.String()))
		return
	}
	// Default: JSON
	writeJSON(w, map[string]any{
		"title":    conv.Title,
		"exported": time.Now().UnixMilli(),
		"messages": msgs,
	})
}

func (s *Server) getSettings(w http.ResponseWriter, r *http.Request) {
	all, _ := s.Settings.All()
	writeJSON(w, all)
}

func (s *Server) setSetting(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var req struct{ Value string }
	json.NewDecoder(r.Body).Decode(&req)
	s.Settings.Set(key, req.Value)
	w.WriteHeader(204)
}

func (s *Server) exportSettings(w http.ResponseWriter, r *http.Request) {
	export := map[string]any{}
	// Settings
	all, _ := s.Settings.All()
	export["settings"] = all
	// Providers
	ps, _ := s.Providers.List()
	export["providers"] = ps
	// Memories
	rows, err := s.DB.Query("SELECT key,content,category FROM memories")
	if err == nil {
		defer rows.Close()
		var mems []map[string]string
		for rows.Next() {
			var k, c, cat string
			rows.Scan(&k, &c, &cat)
			mems = append(mems, map[string]string{"key": k, "content": c, "category": cat})
		}
		export["memories"] = mems
	}
	// Tool toggles
	trows, err := s.DB.Query("SELECT toolId, enabled FROM tool_toggles")
	if err == nil {
		defer trows.Close()
		toggles := map[string]bool{}
		for trows.Next() {
			var id string
			var en int
			trows.Scan(&id, &en)
			toggles[id] = en == 1
		}
		export["toolToggles"] = toggles
	}
	w.Header().Set("Content-Disposition", "attachment; filename=aiope-settings.json")
	writeJSON(w, export)
}

func (s *Server) importSettings(w http.ResponseWriter, r *http.Request) {
	var data map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "invalid JSON", 400)
		return
	}
	// Import settings
	if raw, ok := data["settings"]; ok {
		var kv map[string]string
		if json.Unmarshal(raw, &kv) == nil {
			for k, v := range kv {
				s.Settings.Set(k, v)
			}
		}
	}
	// Import providers
	if raw, ok := data["providers"]; ok {
		var ps []provider.Profile
		if json.Unmarshal(raw, &ps) == nil {
			for i := range ps {
				s.Providers.Create(&ps[i])
			}
		}
	}
	// Import memories
	if raw, ok := data["memories"]; ok {
		var mems []struct {
			Key, Content, Category string
		}
		if json.Unmarshal(raw, &mems) == nil {
			now := time.Now().UnixMilli()
			for _, m := range mems {
				cat := m.Category
				if cat == "" {
					cat = "general"
				}
				s.DB.Exec("INSERT INTO memories(key,content,category,createdAt,updatedAt) VALUES(?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET content=?,category=?,updatedAt=?",
					m.Key, m.Content, cat, now, now, m.Content, cat, now)
			}
		}
	}
	// Import tool toggles
	if raw, ok := data["toolToggles"]; ok {
		var toggles map[string]bool
		if json.Unmarshal(raw, &toggles) == nil {
			for id, en := range toggles {
				v := 0
				if en {
					v = 1
				}
				s.DB.Exec("INSERT INTO tool_toggles(toolId,enabled) VALUES(?,?) ON CONFLICT(toolId) DO UPDATE SET enabled=?", id, v, v)
			}
		}
	}
	s.refreshProvider()
	w.WriteHeader(200)
	writeJSON(w, map[string]string{"status": "imported"})
}

// WebSocket handler
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	conn.SetReadLimit(10 * 1024 * 1024) // 10MB for image uploads
	defer conn.CloseNow()

	client := &ws.Client{Conn: conn, Send: make(chan []byte, 64)}
	s.Hub.Register(client)
	defer s.Hub.Unregister(client)

	// Writer goroutine
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case msg, ok := <-client.Send:
				if !ok {
					return
				}
				conn.Write(ctx, websocket.MessageText, msg)
			case <-ticker.C:
				conn.Ping(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Reader loop
	chunks := map[string]string{} // id -> accumulated base64
	chunkNames := map[string]string{}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var msg struct {
			Type           string   `json:"type"`
			ConversationID string   `json:"conversationId"`
			Content        string   `json:"content"`
			Mode           string   `json:"mode"`
			MessageID      string   `json:"messageId"`
			AtIndex        int      `json:"atIndex"`
			Language       string   `json:"language"`
			Text           string   `json:"text"`
			ImagePaths     []string `json:"imagePaths"`
			ImageData      []struct {
				Name string `json:"name"`
				Data string `json:"data"`
			} `json:"imageData"`
			// file.chunk fields
			ChunkID   string `json:"id"`
			ChunkData string `json:"data"`
			ChunkDone bool   `json:"done"`
			FileName  string `json:"name"`
		}
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		switch msg.Type {
		case "chat.send":
			// Save inline base64 images/docs to disk
			var docContents []string
			for _, img := range msg.ImageData {
				if b, err := base64.StdEncoding.DecodeString(img.Data); err == nil {
					dir := filepath.Join(os.Getenv("HOME"), ".aiope-headless", "uploads")
					os.MkdirAll(dir, 0755)
					dst := filepath.Join(dir, fmt.Sprintf("%d_%s", time.Now().UnixMilli(), img.Name))
					os.WriteFile(dst, b, 0644)
					ext := strings.ToLower(filepath.Ext(img.Name))
					if ext == ".txt" || ext == ".md" || ext == ".csv" || ext == ".json" || ext == ".xml" || ext == ".html" || ext == ".py" || ext == ".js" || ext == ".ts" || ext == ".go" || ext == ".rs" || ext == ".java" || ext == ".c" || ext == ".cpp" || ext == ".h" || ext == ".sh" || ext == ".yaml" || ext == ".yml" || ext == ".toml" {
						docContents = append(docContents, fmt.Sprintf("[File: %s]\n```\n%s\n```", img.Name, string(b)))
					} else if ext == ".pdf" || ext == ".doc" || ext == ".docx" {
						if text := extractDocText(dst); text != "" {
							docContents = append(docContents, fmt.Sprintf("[File: %s]\n```\n%s\n```", img.Name, text))
						} else {
							msg.ImagePaths = append(msg.ImagePaths, dst)
						}
					} else {
						msg.ImagePaths = append(msg.ImagePaths, dst)
					}
				}
			}
			if len(docContents) > 0 {
				msg.Content = msg.Content + "\n\n" + strings.Join(docContents, "\n\n")
			}
			go s.handleChatSend(ctx, client, msg.ConversationID, msg.Content, msg.Mode, msg.ImagePaths)
		case "chat.retry":
			go s.handleRetry(ctx, client, msg.ConversationID, msg.AtIndex, msg.Mode)
		case "chat.edit_resend":
			go s.handleEditResend(ctx, client, msg.ConversationID, msg.Text, msg.AtIndex, msg.Mode)
		case "chat.fork":
			go s.handleFork(client, msg.ConversationID, msg.AtIndex)
		case "chat.compact":
			log.Printf("chat.compact: conv=%s atIndex=%d", msg.ConversationID, msg.AtIndex)
			go s.handleCompact(client, msg.ConversationID, msg.AtIndex)
		case "chat.translate":
			go s.handleTranslate(client, msg.ConversationID, msg.MessageID, msg.Language)
		case "chat.cancel":
			if fn, ok := s.cancels.LoadAndDelete(msg.ConversationID); ok {
				fn.(context.CancelFunc)()
				s.Hub.BroadcastJSON(map[string]any{"type": "stream.end", "conversationId": msg.ConversationID, "finishReason": "cancelled"})
			}
		case "chat.auto_run":
			enabled := msg.Content == "true" || msg.Content == "1"
			s.autoRun.Store(msg.ConversationID, enabled)
			if !enabled {
				s.autoRunCount.Delete(msg.ConversationID)
			}
		case "file.chunk":
			chunks[msg.ChunkID] += msg.ChunkData
			if msg.FileName != "" {
				chunkNames[msg.ChunkID] = msg.FileName
			}
			if msg.ChunkDone {
				log.Printf("file.chunk: assembled %s (%d bytes b64)", chunkNames[msg.ChunkID], len(chunks[msg.ChunkID]))
				b, err := base64.StdEncoding.DecodeString(chunks[msg.ChunkID])
				if err == nil {
					dir := filepath.Join(os.Getenv("HOME"), ".aiope-headless", "uploads")
					os.MkdirAll(dir, 0755)
					dst := filepath.Join(dir, fmt.Sprintf("%d_%s", time.Now().UnixMilli(), chunkNames[msg.ChunkID]))
					os.WriteFile(dst, b, 0644)
					client.SendJSON(map[string]any{"type": "file.uploaded", "id": msg.ChunkID, "path": dst})
				}
				delete(chunks, msg.ChunkID)
				delete(chunkNames, msg.ChunkID)
			}
		}
	}
}

func extractDocText(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".pdf":
		out, err := exec.Command("pdftotext", "-layout", path, "-").Output()
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(out))
	case ".docx":
		// Extract text from docx (zip of XML)
		out, err := exec.Command("sh", "-c", fmt.Sprintf(`unzip -p %q word/document.xml | sed -e 's/<[^>]*>//g' -e 's/&amp;/\&/g' -e 's/&lt;/</g' -e 's/&gt;/>/g'`, path)).Output()
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(out))
	case ".doc":
		out, err := exec.Command("antiword", path).Output()
		if err != nil {
			// fallback: try catdoc
			out, err = exec.Command("catdoc", path).Output()
			if err != nil {
				return ""
			}
		}
		return strings.TrimSpace(string(out))
	}
	return ""
}

func (s *Server) handleChatSend(ctx context.Context, client *ws.Client, convID, content, mode string, imagePaths []string) {
	log.Printf("chat.send: conv=%s content=%q mode=%s images=%d", convID, content, mode, len(imagePaths))
	// Create conversation if needed
	if convID == "" {
		c, err := s.Conversations.Create("New Chat")
		if err != nil {
			return
		}
		convID = c.ID
		s.Hub.BroadcastJSON(map[string]any{"type": "conversation.created", "conversation": c})
	}

	// Save user message
	userMsg, err := s.Messages.Add(convID, "user", content, imagePaths...)
	if err != nil {
		return
	}
	s.Hub.BroadcastJSON(map[string]any{"type": "message.created", "message": userMsg})

	// Build message history
	msgs, _ := s.Messages.List(convID)
	isFirstExchange := len(msgs) <= 1
	log.Printf("chat.send: msgs=%d isFirstExchange=%v", len(msgs), isFirstExchange)
	var chatMsgs []llm.ChatMessage

	// System prompt from settings
	allSettings, _ := s.Settings.All()
	sysPrompt := defaultSystemPrompt
	if extra := llm.BuildSystemPrompt(allSettings, mode); extra != "" {
		sysPrompt += "\n\n" + extra
	}
	// Apply per-model system prompt override
	if p := s.Providers.GetActive(); p != nil {
		if mc, ok := p.ModelConfigs[p.SelectedModelID]; ok && mc.SystemPromptOverride != "" {
			sysPrompt = mc.SystemPromptOverride + "\n\n" + sysPrompt
		}
	}
	if s.Remote != nil {
		if rc := s.Remote.BuildSystemContext(); rc != "" {
			sysPrompt += "\n" + rc
		}
	}

	// If conversation was compacted, merge summary into system prompt
	var summaryText string
	for _, m := range msgs {
		if m.Role == "system" && m.Content != "" && m.Content != "⟳ Context compacted — earlier messages summarized" {
			summaryText = m.Content
			break
		}
	}
	if summaryText != "" {
		sysPrompt = sysPrompt + "\n\n--- CONVERSATION CONTEXT ---\n" + summaryText
		log.Printf("chat.send: injected summary (%d chars) into system prompt", len(summaryText))
	}

	chatMsgs = append(chatMsgs, llm.ChatMessage{Role: "system", Content: sysPrompt})

	// Check if active model supports vision (must be explicitly enabled)
	supportsVision := false
	if p := s.Providers.GetActive(); p != nil {
		if mc, ok := p.ModelConfigs[p.SelectedModelID]; ok && mc.VisionOverride != nil && *mc.VisionOverride {
			supportsVision = true
		}
	}

	for _, m := range msgs {
		if m.Role == "system" {
			continue
		}
		cm := llm.ChatMessage{Role: m.Role, Content: m.Content, Reasoning: m.Reasoning}
		if len(m.ImagePaths) > 0 {
			if supportsVision {
				parts := []any{map[string]any{"type": "text", "text": m.Content}}
				for _, img := range m.ImagePaths {
					if b64, err := encodeImageBase64(img); err == nil {
						parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/jpeg;base64," + b64}})
					}
				}
				cm.Content = parts
			}
			// Non-vision: historical images already described at send time, skip
		}
		chatMsgs = append(chatMsgs, cm)
	}

	// Attach images to last user message from current send
	if len(imagePaths) > 0 && len(chatMsgs) > 0 {
		last := &chatMsgs[len(chatMsgs)-1]
		if last.Role == "user" {
			if supportsVision {
				parts := []any{map[string]any{"type": "text", "text": last.Content}}
				for _, img := range imagePaths {
					b64, err := encodeImageBase64(img)
					if err != nil {
						log.Printf("image encode error: %v", err)
						continue
					}
					parts = append(parts, map[string]any{
						"type":      "image_url",
						"image_url": map[string]any{"url": "data:image/jpeg;base64," + b64},
					})
				}
				last.Content = parts
			} else {
				// Fallback: use analyze_image task model to describe images as text
				toolCtx := s.buildToolContext()
				var descriptions []string
				for _, img := range imagePaths {
					desc, err := llm.ExecuteTool("analyze_image", map[string]any{"url": img, "question": "Describe this image in detail."}, toolCtx)
					if err == nil && desc != "" {
						descriptions = append(descriptions, desc)
					}
				}
				if len(descriptions) > 0 {
					last.Content = fmt.Sprintf("%s\n\n[Attached image descriptions]\n%s", last.Content, strings.Join(descriptions, "\n---\n"))
				}
			}
		}
	}

	// Determine tools (PLAN mode = no write tools, respect toggles)
	tools := s.getEnabledTools(mode)

	// Stream response via orchestrator
	assistantID := uuid.NewString()
	s.Hub.BroadcastJSON(map[string]any{"type": "stream.start", "conversationId": convID, "messageId": assistantID})

	streamCtx, streamCancel := context.WithCancel(context.Background())
	s.cancels.Store(convID, streamCancel)
	defer func() {
		streamCancel()
		s.cancels.Delete(convID)
	}()

	s.mu.RLock()
	prov := s.Provider
	model := s.Model
	s.mu.RUnlock()
	if model == "" {
		model = "gpt-4o"
	}

	// Get gateway info for search/query tools
	toolCtx := s.buildToolContext()

	var hadToolCalls bool
	orch := &llm.Orchestrator{
		Provider: prov,
		Model:    model,
		Tools:    tools,
		Ctx:      streamCtx,
		ToolCtx:  toolCtx,
		OnEvent: func(ev llm.StreamEvent) {
			if ev.Delta != "" {
				s.Hub.BroadcastJSON(map[string]any{"type": "stream.delta", "conversationId": convID, "messageId": assistantID, "delta": ev.Delta})
			}
			if ev.Reasoning != "" {
				s.Hub.BroadcastJSON(map[string]any{"type": "stream.reasoning", "conversationId": convID, "messageId": assistantID, "delta": ev.Reasoning})
			}
			if len(ev.ToolCalls) > 0 {
				hadToolCalls = true
				for _, tc := range ev.ToolCalls {
					s.Hub.BroadcastJSON(map[string]any{"type": "stream.tool_call", "conversationId": convID, "messageId": assistantID, "toolCall": tc})
				}
			}
			if len(ev.ToolResults) > 0 {
				for _, tr := range ev.ToolResults {
					s.Hub.BroadcastJSON(map[string]any{"type": "stream.tool_result", "conversationId": convID, "toolCallId": tr.ID, "name": tr.Name, "result": tr.Result, "isError": tr.IsErr})
				}
			}
			if ev.Done {
				s.Hub.BroadcastJSON(map[string]any{"type": "stream.end", "conversationId": convID, "messageId": assistantID, "finishReason": ev.FinishReason})
			}
		},
	}

	fullContent, reasoning, err := orch.Run(chatMsgs)
	if err != nil {
		if streamCtx.Err() != nil {
			// Already sent stream.end from cancel handler
		} else {
			log.Printf("LLM error: %v", err)
			s.Hub.BroadcastJSON(map[string]any{"type": "stream.error", "conversationId": convID, "error": err.Error()})
		}
		// Save partial content if any
		if fullContent != "" {
			s.Messages.AddWithReasoning(convID, "assistant", fullContent, reasoning)
		}
		return
	}

	// Save assistant message
	if fullContent != "" {
		aMsg, _ := s.Messages.AddWithReasoning(convID, "assistant", fullContent, reasoning)
		if aMsg != nil {
			s.Hub.BroadcastJSON(map[string]any{"type": "message.created", "message": aMsg})
		}
	}
	s.Conversations.Touch(convID)

	// Auto-title after first exchange
	if isFirstExchange {
		go s.autoTitle(convID, content)
	}

	// Auto-run: continue after tool use
	if hadToolCalls {
		if on, ok := s.autoRun.Load(convID); ok && on.(bool) {
			cnt := 0
			if v, ok := s.autoRunCount.Load(convID); ok {
				cnt = v.(int)
			}
			if cnt < 20 {
				s.autoRunCount.Store(convID, cnt+1)
				go s.handleChatSend(ctx, client, convID, "continue", mode, nil)
				return
			}
			s.autoRunCount.Delete(convID)
		}
	} else {
		s.autoRunCount.Delete(convID)
	}

	// Auto-compact: check if context exceeds 95% of token limit
	s.maybeAutoCompact(client, convID)
}

func (s *Server) autoTitle(convID, userMsg string) {
	prompt := []llm.ChatMessage{
		{Role: "user", Content: fmt.Sprintf("Generate a short title (max 6 words) for a conversation that starts with: %q. Reply with ONLY the title, no quotes.", userMsg[:min(len(userMsg), 200)])},
	}
	var title strings.Builder
	s.mu.RLock()
	prov := s.Provider
	s.mu.RUnlock()
	if prov == nil {
		log.Println("autoTitle: no provider")
		return
	}

	// Use title task model if available
	toolCtx := s.newToolContext()
	if active := s.Providers.GetActive(); active != nil {
		toolCtx.APIBase = active.APIBase
		toolCtx.APIKey = active.APIKey
	}
	_, model := toolCtx.ResolveTaskModel("title")
	if model == "" {
		s.mu.RLock()
		model = s.Model
		s.mu.RUnlock()
	}
	if model == "" {
		model = "gpt-4o"
	}
	log.Printf("autoTitle: conv=%s model=%s", convID, model)
	err := prov.Stream(context.Background(), prompt, model, nil, func(ev llm.StreamEvent) {
		if ev.Delta != "" {
			title.WriteString(ev.Delta)
		}
	})
	if err != nil {
		log.Printf("autoTitle error: %v", err)
		return
	}
	t := strings.TrimSpace(title.String())
	log.Printf("autoTitle result: %q", t)
	if t != "" && len(t) < 100 {
		s.Conversations.UpdateTitle(convID, t)
		s.Hub.BroadcastJSON(map[string]any{"type": "conversation.updated", "id": convID, "title": t, "updatedAt": time.Now().UnixMilli()})
	}
}

func (s *Server) listProviders(w http.ResponseWriter, r *http.Request) {
	ps, _ := s.Providers.List()
	if ps == nil {
		ps = []provider.Profile{}
	}
	// Redact API keys in response
	for i := range ps {
		if len(ps[i].APIKey) > 8 {
			ps[i].APIKey = ps[i].APIKey[:4] + "****" + ps[i].APIKey[len(ps[i].APIKey)-4:]
		} else if ps[i].APIKey != "" {
			ps[i].APIKey = "****"
		}
	}
	writeJSON(w, ps)
}

func (s *Server) createProvider(w http.ResponseWriter, r *http.Request) {
	var p provider.Profile
	json.NewDecoder(r.Body).Decode(&p)
	if err := s.Providers.Create(&p); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(201)
	writeJSON(w, p)
}

func (s *Server) updateProvider(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	existing, _ := s.Providers.Get(id)
	if existing == nil {
		http.Error(w, "not found", 404)
		return
	}
	var patch provider.Profile
	json.NewDecoder(r.Body).Decode(&patch)
	// Merge: only overwrite non-zero fields
	if patch.Label != "" {
		existing.Label = patch.Label
	}
	if patch.APIKey != "" && !strings.Contains(patch.APIKey, "****") {
		existing.APIKey = patch.APIKey
	}
	if patch.APIBase != "" {
		existing.APIBase = patch.APIBase
	}
	if patch.SelectedModelID != "" {
		existing.SelectedModelID = patch.SelectedModelID
	}
	if patch.ModelConfigs != nil {
		existing.ModelConfigs = patch.ModelConfigs
	}
	if err := s.Providers.Update(existing); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.refreshProvider()
	w.WriteHeader(204)
}

func (s *Server) deleteProvider(w http.ResponseWriter, r *http.Request) {
	s.Providers.Delete(r.PathValue("id"))
	s.refreshProvider()
	w.WriteHeader(204)
}

func (s *Server) activateProvider(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.Providers.SetActive(id); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.refreshProvider()
	w.WriteHeader(204)
}

func (s *Server) fetchModels(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ps, _ := s.Providers.List()
	var p *provider.Profile
	for i := range ps {
		if ps[i].ID == id {
			p = &ps[i]
			break
		}
	}
	if p == nil {
		http.Error(w, "not found", 404)
		return
	}
	models, err := s.Providers.FetchModels(p)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	writeJSON(w, models)
}

// --- Gateway API handlers ---

func (s *Server) gwListProviders(w http.ResponseWriter, r *http.Request) {
	ps, _ := s.Gateway.Store().ListProviders()
	if ps == nil {
		ps = []gateway.ProviderConfig{}
	}
	for i := range ps {
		if len(ps[i].APIKey) > 8 {
			ps[i].APIKey = ps[i].APIKey[:4] + "****" + ps[i].APIKey[len(ps[i].APIKey)-4:]
		} else if ps[i].APIKey != "" {
			ps[i].APIKey = "****"
		}
	}
	writeJSON(w, ps)
}

func (s *Server) gwAddProvider(w http.ResponseWriter, r *http.Request) {
	var p gateway.ProviderConfig
	json.NewDecoder(r.Body).Decode(&p)
	if p.Name == "" {
		http.Error(w, "name required", 400)
		return
	}
	p.Enabled = true
	if err := s.Gateway.AddProvider(&p); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(201)
}

func (s *Server) gwUpdateProvider(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	var patch struct {
		APIKey  string `json:"apiKey"`
		APIBase string `json:"apiBase"`
		Enabled *bool  `json:"enabled"`
	}
	json.NewDecoder(r.Body).Decode(&patch)
	if err := s.Gateway.UpdateProvider(name, patch.APIKey, patch.APIBase, patch.Enabled); err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) gwDeleteProvider(w http.ResponseWriter, r *http.Request) {
	s.Gateway.RemoveProvider(r.URL.Query().Get("name"))
	w.WriteHeader(204)
}

func (s *Server) gwDiscoverModels(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	models, err := s.Gateway.DiscoverModels(name)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	writeJSON(w, models)
}

func (s *Server) gwListRoutes(w http.ResponseWriter, r *http.Request) {
	routes, _ := s.Gateway.Store().ListRoutes()
	if routes == nil {
		routes = []gateway.ModelRoute{}
	}
	writeJSON(w, routes)
}

func (s *Server) gwAddRoute(w http.ResponseWriter, r *http.Request) {
	var route gateway.ModelRoute
	json.NewDecoder(r.Body).Decode(&route)
	if route.DisplayID == "" || route.Provider == "" || route.Model == "" {
		http.Error(w, "displayId, provider, model required", 400)
		return
	}
	route.Enabled = true
	if err := s.Gateway.AddRoute(&route); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(201)
}

func (s *Server) gwUpdateRoute(w http.ResponseWriter, r *http.Request) {
	displayID := r.URL.Query().Get("id")
	var patch struct {
		Enabled *bool `json:"enabled"`
	}
	json.NewDecoder(r.Body).Decode(&patch)
	if patch.Enabled == nil {
		w.WriteHeader(204)
		return
	}
	if err := s.Gateway.UpdateRoute(displayID, *patch.Enabled); err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) gwDeleteRoute(w http.ResponseWriter, r *http.Request) {
	s.Gateway.RemoveRoute(r.URL.Query().Get("id"))
	w.WriteHeader(204)
}

func (s *Server) listToolToggles(w http.ResponseWriter, r *http.Request) {
	rows, err := s.DB.Query("SELECT toolId, enabled FROM tool_toggles")
	if err != nil {
		writeJSON(w, map[string]any{})
		return
	}
	defer rows.Close()
	toggles := map[string]bool{}
	for rows.Next() {
		var id string
		var enabled int
		rows.Scan(&id, &enabled)
		toggles[id] = enabled == 1
	}
	// Include all builtin tools with defaults
	result := make([]map[string]any, 0, len(llm.BuiltinTools))
	for _, t := range llm.BuiltinTools {
		enabled := true
		if v, ok := toggles[t.Name]; ok {
			enabled = v
		}
		result = append(result, map[string]any{"id": t.Name, "name": t.Name, "description": t.Description, "enabled": enabled})
	}
	writeJSON(w, result)
}

func (s *Server) setToolToggle(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req struct{ Enabled bool }
	json.NewDecoder(r.Body).Decode(&req)
	enabled := 0
	if req.Enabled {
		enabled = 1
	}
	s.DB.Exec("INSERT INTO tool_toggles(toolId,enabled) VALUES(?,?) ON CONFLICT(toolId) DO UPDATE SET enabled=?", id, enabled, enabled)
	w.WriteHeader(204)
}

func (s *Server) listMemories(w http.ResponseWriter, r *http.Request) {
	rows, err := s.DB.Query("SELECT key,content,category,createdAt,updatedAt FROM memories ORDER BY updatedAt DESC")
	if err != nil {
		writeJSON(w, []any{})
		return
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var key, content, cat string
		var created, updated int64
		rows.Scan(&key, &content, &cat, &created, &updated)
		out = append(out, map[string]any{"key": key, "content": content, "category": cat, "createdAt": created, "updatedAt": updated})
	}
	if out == nil {
		out = []map[string]any{}
	}
	writeJSON(w, out)
}

func (s *Server) createMemory(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key      string `json:"key"`
		Content  string `json:"content"`
		Category string `json:"category"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if req.Key == "" || req.Content == "" {
		http.Error(w, "key and content required", 400)
		return
	}
	if req.Category == "" {
		req.Category = "general"
	}
	now := time.Now().UnixMilli()
	s.DB.Exec("INSERT INTO memories(key,content,category,createdAt,updatedAt) VALUES(?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET content=?,category=?,updatedAt=?",
		req.Key, req.Content, req.Category, now, now, req.Content, req.Category, now)
	w.WriteHeader(201)
	writeJSON(w, map[string]any{"key": req.Key, "content": req.Content, "category": req.Category})
}

func (s *Server) updateMemory(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var req struct {
		Content  string `json:"content"`
		Category string `json:"category"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	now := time.Now().UnixMilli()
	if req.Category != "" {
		s.DB.Exec("UPDATE memories SET content=?,category=?,updatedAt=? WHERE key=?", req.Content, req.Category, now, key)
	} else {
		s.DB.Exec("UPDATE memories SET content=?,updatedAt=? WHERE key=?", req.Content, now, key)
	}
	w.WriteHeader(204)
}

func (s *Server) deleteMemory(w http.ResponseWriter, r *http.Request) {
	s.DB.Exec("DELETE FROM memories WHERE key=?", r.PathValue("key"))
	w.WriteHeader(204)
}

func (s *Server) listMcpServers(w http.ResponseWriter, r *http.Request) {
	servers := s.MCP.ListServers()
	if servers == nil {
		servers = []mcp.ServerConfig{}
	}
	writeJSON(w, servers)
}

func (s *Server) addMcpServer(w http.ResponseWriter, r *http.Request) {
	var srv mcp.ServerConfig
	json.NewDecoder(r.Body).Decode(&srv)
	if srv.ID == "" {
		srv.ID = uuid.NewString()[:8]
	}
	if srv.Status == "" {
		srv.Status = "idle"
	}
	s.MCP.SaveServer(&srv)
	w.WriteHeader(201)
	writeJSON(w, srv)
}

func (s *Server) updateMcpServer(w http.ResponseWriter, r *http.Request) {
	var srv mcp.ServerConfig
	json.NewDecoder(r.Body).Decode(&srv)
	srv.ID = r.PathValue("id")
	s.MCP.SaveServer(&srv)
	w.WriteHeader(204)
}

func (s *Server) deleteMcpServer(w http.ResponseWriter, r *http.Request) {
	s.MCP.DeleteServer(r.PathValue("id"))
	w.WriteHeader(204)
}

func (s *Server) connectMcpServer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	servers := s.MCP.ListServers()
	var srv *mcp.ServerConfig
	for i := range servers {
		if servers[i].ID == id {
			srv = &servers[i]
			break
		}
	}
	if srv == nil {
		http.Error(w, "not found", 404)
		return
	}
	tools, err := s.MCP.DiscoverTools(*srv)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	writeJSON(w, map[string]any{"tools": tools, "count": len(tools)})
}

// Remote servers

func (s *Server) listRemoteServers(w http.ResponseWriter, r *http.Request) {
	servers, _ := s.Remote.List()
	if servers == nil {
		servers = []remote.Server{}
	}
	// Annotate connected status
	type resp struct {
		remote.Server
		Connected bool `json:"connected"`
	}
	out := make([]resp, len(servers))
	for i, srv := range servers {
		out[i] = resp{srv, s.Remote.IsConnected(srv.ID)}
	}
	writeJSON(w, out)
}

func (s *Server) createRemoteServer(w http.ResponseWriter, r *http.Request) {
	var srv remote.Server
	json.NewDecoder(r.Body).Decode(&srv)
	if srv.ID == "" {
		srv.ID = uuid.NewString()
	}
	if srv.Port == 0 {
		srv.Port = 22
	}
	if srv.BootstrapPort == 0 {
		srv.BootstrapPort = srv.Port
	}
	if err := s.Remote.Upsert(&srv); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, srv)
}

func (s *Server) updateRemoteServer(w http.ResponseWriter, r *http.Request) {
	var srv remote.Server
	json.NewDecoder(r.Body).Decode(&srv)
	srv.ID = r.PathValue("id")
	if err := s.Remote.Upsert(&srv); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) deleteRemoteServer(w http.ResponseWriter, r *http.Request) {
	s.Remote.Delete(r.PathValue("id"))
	w.WriteHeader(204)
}

func (s *Server) connectRemoteServer(w http.ResponseWriter, r *http.Request) {
	srv := s.Remote.Get(r.PathValue("id"))
	if srv == nil {
		http.Error(w, "not found", 404)
		return
	}
	if err := s.Remote.Connect(srv); err != nil {
		s.Remote.UpdateStatus(srv.ID, "error")
		http.Error(w, err.Error(), 502)
		return
	}
	writeJSON(w, map[string]string{"status": "connected"})
}

func (s *Server) disconnectRemoteServer(w http.ResponseWriter, r *http.Request) {
	s.Remote.Disconnect(r.PathValue("id"))
	w.WriteHeader(204)
}

func (s *Server) healthRemoteServer(w http.ResponseWriter, r *http.Request) {
	srv := s.Remote.Get(r.PathValue("id"))
	if srv == nil {
		http.Error(w, "not found", 404)
		return
	}
	if !s.Remote.IsConnected(srv.ID) {
		if err := s.Remote.Connect(srv); err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
	}
	result, err := s.Remote.Exec(srv.ID, "__aiope_health__", 10)
	if err != nil || !json.Valid([]byte(result)) {
		// Daemon not deployed — fall back to basic shell health
		result, err = s.Remote.Exec(srv.ID, `printf '{"os":"%s","arch":"%s","hostname":"%s","uptime":"%s","daemon":false}' "$(uname -s)" "$(uname -m)" "$(hostname)" "$(uptime -p 2>/dev/null||uptime)"`, 10)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
	}
	// Parse and store health info
	var health map[string]any
	if json.Unmarshal([]byte(result), &health) == nil {
		osInfo := fmt.Sprintf("%v %v - %v", health["os"], health["arch"], health["hostname"])
		ver, _ := health["version"].(string)
		s.Remote.UpdateHealth(srv.ID, osInfo, ver)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(result))
}

func (s *Server) deployRemoteServer(w http.ResponseWriter, r *http.Request) {
	srv := s.Remote.Get(r.PathValue("id"))
	if srv == nil {
		http.Error(w, "not found", 404)
		return
	}
	// Deploy runs the installer via bootstrap SSH on the standard port
	s.Remote.UpdateStatus(srv.ID, "deploying")

	// Connect to bootstrap port
	bootstrap := *srv
	bootstrap.Port = srv.BootstrapPort
	if err := s.Remote.Connect(&bootstrap); err != nil {
		s.Remote.UpdateStatus(srv.ID, "error")
		http.Error(w, "bootstrap connect: "+err.Error(), 502)
		return
	}

	// Check if installer exists locally
	installerPath := os.ExpandEnv("$HOME/.aiope/aiope-remote-installer.sh")
	if _, err := os.Stat(installerPath); err != nil {
		s.Remote.UpdateStatus(srv.ID, "error")
		http.Error(w, "installer not found at "+installerPath+". Build it first with the daemon build script.", 400)
		return
	}

	// SCP installer and run it
	data, _ := os.ReadFile(installerPath)
	scpCmd := fmt.Sprintf("cat > /tmp/aiope-remote-installer.sh << 'INSTALLER_EOF'\n%s\nINSTALLER_EOF\nchmod +x /tmp/aiope-remote-installer.sh && /tmp/aiope-remote-installer.sh", string(data))
	result, err := s.Remote.Exec(srv.ID, scpCmd, 120)
	s.Remote.Disconnect(srv.ID)

	if err != nil {
		s.Remote.UpdateStatus(srv.ID, "error")
		http.Error(w, "deploy failed: "+err.Error(), 502)
		return
	}

	// Update to daemon port and try connecting
	srv.Port = 2222
	s.Remote.Upsert(srv)
	s.Remote.UpdateStatus(srv.ID, "online")

	writeJSON(w, map[string]string{"status": "deployed", "output": result})
}

func (s *Server) newToolContext() *llm.ToolContext {
	tc := &llm.ToolContext{DB: s.DB}
	tc.OnProgress = func(toolName, status string) {
		s.Hub.BroadcastJSON(map[string]any{"type": "tool.progress", "tool": toolName, "status": status})
	}
	if s.Remote != nil {
		tc.SSHStart = func(server string) (string, error) {
			srv := s.Remote.GetByName(server)
			if srv == nil {
				srv = s.Remote.Get(server)
			}
			if srv == nil {
				return "", fmt.Errorf("unknown server: %s", server)
			}
			if err := s.Remote.Connect(srv); err != nil {
				return "", err
			}
			return fmt.Sprintf(`{"status":"connected","server":"%s","host":"%s:%d"}`, srv.Name, srv.Host, srv.Port), nil
		}
		tc.SSHExec = func(server, command string, timeout int) (string, error) {
			srv := s.Remote.GetByName(server)
			if srv == nil {
				srv = s.Remote.Get(server)
			}
			if srv == nil {
				return "", fmt.Errorf("unknown server: %s", server)
			}
			return s.Remote.Exec(srv.ID, command, timeout)
		}
		tc.SSHExit = func(server string) error {
			srv := s.Remote.GetByName(server)
			if srv == nil {
				srv = s.Remote.Get(server)
			}
			if srv == nil {
				return nil
			}
			s.Remote.Disconnect(srv.ID)
			return nil
		}
	}
	return tc
}

func (s *Server) listFiles(w http.ResponseWriter, r *http.Request) {
	dir := r.URL.Query().Get("path")
	if dir == "" {
		dir = os.Getenv("HOME")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	type item struct {
		Name  string `json:"name"`
		IsDir bool   `json:"isDir"`
		Size  int64  `json:"size"`
	}
	items := make([]item, 0, len(entries))
	for _, e := range entries {
		info, _ := e.Info()
		sz := int64(0)
		if info != nil {
			sz = info.Size()
		}
		items = append(items, item{Name: e.Name(), IsDir: e.IsDir(), Size: sz})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"path": dir, "items": items})
}

func (s *Server) readFileContent(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Query().Get("path")
	if p == "" {
		http.Error(w, "path required", 400)
		return
	}
	info, err := os.Stat(p)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	if info.IsDir() {
		http.Error(w, "is a directory", 400)
		return
	}
	if info.Size() > 2*1024*1024 {
		http.Error(w, "file too large (>2MB)", 400)
		return
	}
	http.ServeFile(w, r, p)
}

func (s *Server) checkConnectivity(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	p := s.Providers.GetActive()
	s.mu.RUnlock()
	if p == nil {
		writeJSON(w, map[string]any{"ok": false, "error": "no active provider"})
		return
	}
	client := &http.Client{Timeout: 10 * time.Second}
	url := strings.TrimRight(p.APIBase, "/") + "/models"
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+p.APIKey)
	resp, err := client.Do(req)
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	resp.Body.Close()
	writeJSON(w, map[string]any{"ok": resp.StatusCode < 400, "status": resp.StatusCode, "provider": p.Label})
}

func (s *Server) uploadImage(w http.ResponseWriter, r *http.Request) {
	dir := filepath.Join(os.Getenv("HOME"), ".aiope-headless", "uploads")
	os.MkdirAll(dir, 0755)

	var imgBytes []byte
	var fname string

	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		var req struct {
			Name string `json:"name"`
			Data string `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		var err error
		imgBytes, err = base64.StdEncoding.DecodeString(req.Data)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		fname = req.Name
	} else {
		r.ParseMultipartForm(10 << 20)
		file, header, err := r.FormFile("file")
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		defer file.Close()
		imgBytes, _ = io.ReadAll(file)
		fname = header.Filename
	}

	dst := filepath.Join(dir, fmt.Sprintf("%d_%s", time.Now().UnixMilli(), fname))
	os.WriteFile(dst, imgBytes, 0644)
	writeJSON(w, map[string]string{"path": dst})
}

func (s *Server) refreshProvider() {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.Providers.GetActive()
	if p == nil {
		return
	}
	s.Model = p.SelectedModelID

	// Try gateway router first — resolves model to correct upstream
	if s.Gateway != nil {
		if oai, resolvedModel, err := s.Gateway.Resolve(p.SelectedModelID); err == nil {
			s.Model = resolvedModel
			if mc, ok := p.ModelConfigs[p.SelectedModelID]; ok {
				oai.Temperature = mc.Temperature
				oai.TopP = mc.TopP
				oai.MaxTokens = mc.MaxTokens
				oai.ReasoningEffort = mc.ReasoningEffort
				oai.EndpointOverride = mc.EndpointOverride
			}
			s.Provider = oai
			return
		}
	}

	// Fallback: use profile's API base/key directly
	oai := &llm.OpenAI{APIKey: p.APIKey, APIBase: p.APIBase}
	if mc, ok := p.ModelConfigs[p.SelectedModelID]; ok {
		oai.Temperature = mc.Temperature
		oai.TopP = mc.TopP
		oai.MaxTokens = mc.MaxTokens
		oai.ReasoningEffort = mc.ReasoningEffort
		oai.EndpointOverride = mc.EndpointOverride
	}
	s.Provider = oai
}

func encodeImageBase64(path string) (string, error) {
	var data []byte
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		resp, err := http.Get(path)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		data, _ = io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	} else {
		var err error
		data, err = os.ReadFile(path)
		if err != nil {
			return "", err
		}
	}
	// Preprocess: decode, scale to max 1024px, square-pad, re-encode as JPEG
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		// Not a decodable image (SVG, etc.) — send raw
		return base64.StdEncoding.EncodeToString(data), nil
	}
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	maxDim := 1024
	if w > maxDim || h > maxDim {
		scale := float64(maxDim) / float64(w)
		if float64(maxDim)/float64(h) < scale {
			scale = float64(maxDim) / float64(h)
		}
		nw, nh := int(float64(w)*scale), int(float64(h)*scale)
		scaled := image.NewRGBA(image.Rect(0, 0, nw, nh))
		for y := 0; y < nh; y++ {
			for x := 0; x < nw; x++ {
				sx := int(float64(x) / scale)
				sy := int(float64(y) / scale)
				scaled.Set(x, y, img.At(b.Min.X+sx, b.Min.Y+sy))
			}
		}
		img = scaled
		w, h = nw, nh
	}
	// Square pad
	side := w
	if h > side {
		side = h
	}
	if w != h {
		padded := image.NewRGBA(image.Rect(0, 0, side, side))
		// Fill with black
		ox, oy := (side-w)/2, (side-h)/2
		ib := img.Bounds()
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				padded.Set(ox+x, oy+y, img.At(ib.Min.X+x, ib.Min.Y+y))
			}
		}
		img = padded
	}
	var buf bytes.Buffer
	jpeg.Encode(&buf, img, &jpeg.Options{Quality: 85})
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func ListenAndServe(bind string, port int, handler http.Handler) error {
	host := bind
	if host == "" {
		host = "0.0.0.0"
	}
	addr := fmt.Sprintf("%s:%d", host, port)
	srv := &http.Server{Addr: addr, Handler: handler}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("AIOPE-Headless running on http://%s", addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-done
	log.Println("Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

// --- Chat action handlers ---

func (s *Server) streamToConv(convID, assistantID, mode string, chatMsgs []llm.ChatMessage) (string, error) {
	s.Hub.BroadcastJSON(map[string]any{"type": "stream.start", "conversationId": convID, "messageId": assistantID})

	ctx, cancel := context.WithCancel(context.Background())
	s.cancels.Store(convID, cancel)
	defer func() {
		cancel()
		s.cancels.Delete(convID)
	}()

	s.mu.RLock()
	prov := s.Provider
	model := s.Model
	s.mu.RUnlock()
	if model == "" {
		model = "gpt-4o"
	}

	tools := s.getEnabledTools(mode)

	toolCtx := s.buildToolContext()

	orch := &llm.Orchestrator{
		Provider: prov, Model: model, Tools: tools, Ctx: ctx,
		ToolCtx: toolCtx,
		OnEvent: func(ev llm.StreamEvent) {
			if ev.Delta != "" {
				s.Hub.BroadcastJSON(map[string]any{"type": "stream.delta", "conversationId": convID, "messageId": assistantID, "delta": ev.Delta})
			}
			if ev.Reasoning != "" {
				s.Hub.BroadcastJSON(map[string]any{"type": "stream.reasoning", "conversationId": convID, "messageId": assistantID, "delta": ev.Reasoning})
			}
			for _, tc := range ev.ToolCalls {
				s.Hub.BroadcastJSON(map[string]any{"type": "stream.tool_call", "conversationId": convID, "messageId": assistantID, "toolCall": tc})
			}
			for _, tr := range ev.ToolResults {
				s.Hub.BroadcastJSON(map[string]any{"type": "stream.tool_result", "conversationId": convID, "toolCallId": tr.ID, "name": tr.Name, "result": tr.Result, "isError": tr.IsErr})
			}
			if ev.Done {
				s.Hub.BroadcastJSON(map[string]any{"type": "stream.end", "conversationId": convID, "messageId": assistantID, "finishReason": ev.FinishReason})
			}
		},
	}
	content, _, err := orch.Run(chatMsgs)
	return content, err
}

func (s *Server) buildToolContext() *llm.ToolContext {
	tc := s.newToolContext()
	if p := s.Providers.GetActive(); p != nil {
		tc.APIBase = p.APIBase
		tc.APIKey = p.APIKey
		if mc, ok := p.ModelConfigs[p.SelectedModelID]; ok {
			tc.ShellOutputLimit = mc.ShellOutputLimit
			tc.FetchLimit = mc.FetchLimit
			tc.FileReadLimit = mc.FileReadLimit
		}
	}
	if s.MCP != nil {
		tc.McpExecutor = s.MCP.ExecuteTool
	}
	return tc
}

func (s *Server) getEnabledTools(mode string) []llm.ToolDef {
	// Load toggles from DB
	disabled := map[string]bool{}
	rows, err := s.DB.Query("SELECT toolId FROM tool_toggles WHERE enabled=0")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id string
			rows.Scan(&id)
			disabled[id] = true
		}
	}
	var tools []llm.ToolDef
	for _, t := range llm.BuiltinTools {
		if disabled[t.Name] {
			continue
		}
		if mode == "plan" && (t.Name == "run_sh" || t.Name == "write_file") {
			continue
		}
		tools = append(tools, t)
	}
	// Add MCP tools
	if s.MCP != nil {
		for _, mt := range s.MCP.GetToolDefs() {
			if disabled[mt.Name] {
				continue
			}
			tools = append(tools, llm.ToolDef{Name: mt.Name, Description: mt.Description, Parameters: mt.InputSchema})
		}
	}
	return tools
}

func (s *Server) buildHistory(convID, mode string) []llm.ChatMessage {
	msgs, _ := s.Messages.List(convID)
	allSettings, _ := s.Settings.All()
	sysPrompt := defaultSystemPrompt
	if extra := llm.BuildSystemPrompt(allSettings, mode); extra != "" {
		sysPrompt += "\n\n" + extra
	}
	chatMsgs := []llm.ChatMessage{{Role: "system", Content: sysPrompt}}
	for _, m := range msgs {
		last := &chatMsgs[len(chatMsgs)-1]
		if last.Role == m.Role {
			if s, ok := last.Content.(string); ok {
				last.Content = s + "\n" + m.Content
			}
		} else {
			chatMsgs = append(chatMsgs, llm.ChatMessage{Role: m.Role, Content: m.Content})
		}
	}
	return chatMsgs
}

func (s *Server) handleRetry(_ context.Context, client *ws.Client, convID string, atIndex int, mode string) {
	msgs, _ := s.Messages.List(convID)
	if atIndex < 0 || atIndex >= len(msgs) {
		return
	}
	// Delete from atIndex onward
	s.Messages.DeleteAfter(convID, msgs[atIndex].Timestamp-1)
	s.Hub.BroadcastJSON(map[string]any{"type": "messages.truncated", "conversationId": convID, "atIndex": atIndex})

	chatMsgs := s.buildHistory(convID, mode)
	assistantID := uuid.NewString()
	fullContent, err := s.streamToConv(convID, assistantID, mode, chatMsgs)
	if err != nil {
		s.Hub.BroadcastJSON(map[string]any{"type": "stream.error", "conversationId": convID, "error": err.Error()})
		return
	}
	if fullContent != "" {
		aMsg, _ := s.Messages.Add(convID, "assistant", fullContent)
		if aMsg != nil {
			s.Hub.BroadcastJSON(map[string]any{"type": "message.created", "message": aMsg})
		}
	}
	s.Conversations.Touch(convID)
}

func (s *Server) handleEditResend(_ context.Context, client *ws.Client, convID, text string, atIndex int, mode string) {
	msgs, _ := s.Messages.List(convID)
	if atIndex < 0 || atIndex >= len(msgs) {
		return
	}
	// Delete from atIndex onward
	s.Messages.DeleteAfter(convID, msgs[atIndex].Timestamp-1)
	s.Hub.BroadcastJSON(map[string]any{"type": "messages.truncated", "conversationId": convID, "atIndex": atIndex})

	// Add edited user message
	userMsg, _ := s.Messages.Add(convID, "user", text)
	if userMsg != nil {
		s.Hub.BroadcastJSON(map[string]any{"type": "message.created", "message": userMsg})
	}

	chatMsgs := s.buildHistory(convID, mode)
	assistantID := uuid.NewString()
	fullContent, err := s.streamToConv(convID, assistantID, mode, chatMsgs)
	if err != nil {
		s.Hub.BroadcastJSON(map[string]any{"type": "stream.error", "conversationId": convID, "error": err.Error()})
		return
	}
	if fullContent != "" {
		aMsg, _ := s.Messages.Add(convID, "assistant", fullContent)
		if aMsg != nil {
			s.Hub.BroadcastJSON(map[string]any{"type": "message.created", "message": aMsg})
		}
	}
	s.Conversations.Touch(convID)
}

func (s *Server) handleFork(client *ws.Client, convID string, atIndex int) {
	msgs, _ := s.Messages.List(convID)
	if atIndex < 0 || atIndex > len(msgs) {
		return
	}

	// Title from first user message
	title := "Fork"
	for _, m := range msgs[:atIndex] {
		if m.Role == "user" {
			t := m.Content
			if len(t) > 30 {
				t = t[:30]
			}
			title = "Fork: " + t
			break
		}
	}

	newConv, err := s.Conversations.Create(title)
	if err != nil {
		return
	}
	for _, m := range msgs[:atIndex] {
		s.Messages.Add(newConv.ID, m.Role, m.Content)
	}
	s.Hub.BroadcastJSON(map[string]any{"type": "conversation.created", "conversation": newConv})
	client.SendJSON(map[string]any{"type": "forked", "conversationId": convID, "newConversationId": newConv.ID})
}

func (s *Server) handleCompact(client *ws.Client, convID string, atIndex int) {
	msgs, _ := s.Messages.List(convID)
	if atIndex <= 0 || atIndex > len(msgs) {
		return
	}

	// Build transcript of messages to compact
	var transcript strings.Builder
	for _, m := range msgs[:atIndex] {
		c := m.Content
		if len(c) > 2000 {
			c = c[:2000] + "..."
		}
		fmt.Fprintf(&transcript, "[%s] %s\n\n", m.Role, c)
	}

	// Use summary task model
	toolCtx := s.newToolContext()
	if active := s.Providers.GetActive(); active != nil {
		toolCtx.APIBase = active.APIBase
		toolCtx.APIKey = active.APIKey
	}
	_, summaryModel := toolCtx.ResolveTaskModel("summary")

	s.mu.RLock()
	prov := s.Provider
	mainModel := s.Model
	s.mu.RUnlock()
	if summaryModel == "" {
		summaryModel = mainModel
	}
	if summaryModel == "" {
		summaryModel = "gpt-4o"
	}

	prompt := []llm.ChatMessage{
		{Role: "user", Content: "Summarize this conversation concisely, preserving all key context needed to continue. Start with [Summary].\n\n" + transcript.String()},
	}

	client.SendJSON(map[string]any{"type": "compact.start", "conversationId": convID})

	var summary strings.Builder
	log.Printf("compact: using model %s for %d messages", summaryModel, atIndex)
	err := prov.Stream(context.Background(), prompt, summaryModel, nil, func(ev llm.StreamEvent) {
		if ev.Delta != "" {
			summary.WriteString(ev.Delta)
		}
	})
	if err != nil {
		log.Printf("compact: stream error: %v", err)
	}

	sumText := strings.TrimSpace(summary.String())
	log.Printf("compact: summary length=%d", len(sumText))
	if sumText == "" {
		client.SendJSON(map[string]any{"type": "stream.error", "conversationId": convID, "error": "Compaction failed — empty summary"})
		return
	}

	// Delete all messages, re-insert summary + remaining
	if len(msgs) > 0 {
		s.Messages.DeleteAfter(convID, 0)
	}
	s.Messages.Add(convID, "system", sumText)
	s.Messages.Add(convID, "system", "⟳ Context compacted — earlier messages summarized")
	for _, m := range msgs[atIndex:] {
		s.Messages.Add(convID, m.Role, m.Content)
	}

	// Tell client to reload messages
	newMsgs, _ := s.Messages.List(convID)
	client.SendJSON(map[string]any{"type": "compact.done", "conversationId": convID, "messages": newMsgs})
}

func (s *Server) maybeAutoCompact(client *ws.Client, convID string) {
	// Check if auto_compact is enabled in settings
	v, _ := s.Settings.Get("auto_compact")
	if v != "true" && v != "1" {
		return
	}
	// Get context token limit from active model config
	contextTokens := 128000
	if p := s.Providers.GetActive(); p != nil {
		if mc, ok := p.ModelConfigs[p.SelectedModelID]; ok && mc.ContextTokens > 0 {
			contextTokens = mc.ContextTokens
		}
	}
	msgs, _ := s.Messages.List(convID)
	if len(msgs) <= 4 {
		return
	}
	total := 0
	for _, m := range msgs {
		total += llm.EstimateTokens(m.Content)
	}
	threshold := contextTokens * 95 / 100
	if total > threshold {
		log.Printf("auto-compact: %d tokens > %d threshold, compacting conv %s", total, threshold, convID)
		s.handleCompact(client, convID, len(msgs)/2)
	}
}

func (s *Server) handleTranslate(client *ws.Client, convID, messageID, language string) {
	if language == "" {
		language = "English"
	}

	// Find message content
	msgs, _ := s.Messages.List(convID)
	var content string
	for _, m := range msgs {
		if m.ID == messageID {
			content = m.Content
			break
		}
	}
	if content == "" {
		return
	}

	toolCtx := s.newToolContext()
	if active := s.Providers.GetActive(); active != nil {
		toolCtx.APIBase = active.APIBase
		toolCtx.APIKey = active.APIKey
	}
	_, transModel := toolCtx.ResolveTaskModel("translation")

	s.mu.RLock()
	prov := s.Provider
	transModelFallback := s.Model
	s.mu.RUnlock()
	if transModel == "" {
		transModel = transModelFallback
	}

	prompt := []llm.ChatMessage{
		{Role: "user", Content: "Translate the following text to " + language + ". Output ONLY the translated text, no explanations, no original text, no labels:\n\n" + content},
	}

	prov.Stream(context.Background(), prompt, transModel, nil, func(ev llm.StreamEvent) {
		if ev.Delta != "" {
			client.SendJSON(map[string]any{"type": "translation.delta", "conversationId": convID, "messageId": messageID, "delta": ev.Delta})
		}
		if ev.Done {
			client.SendJSON(map[string]any{"type": "translation.done", "conversationId": convID, "messageId": messageID})
		}
	})
}

func noCacheHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		h.ServeHTTP(w, r)
	})
}