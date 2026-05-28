package main

import (
	"database/sql"
	"embed"
	"io/fs"
	"log"
	"os"

	"github.com/XNet-NGO/AIOPE-Headless/internal/config"
	"github.com/XNet-NGO/AIOPE-Headless/internal/conversation"
	"github.com/XNet-NGO/AIOPE-Headless/internal/db"
	"github.com/XNet-NGO/AIOPE-Headless/internal/gateway"
	"github.com/XNet-NGO/AIOPE-Headless/internal/llm"
	"github.com/XNet-NGO/AIOPE-Headless/internal/mcp"
	"github.com/XNet-NGO/AIOPE-Headless/internal/message"
	"github.com/XNet-NGO/AIOPE-Headless/internal/provider"
	"github.com/XNet-NGO/AIOPE-Headless/internal/remote"
	"github.com/XNet-NGO/AIOPE-Headless/internal/server"
	"github.com/XNet-NGO/AIOPE-Headless/internal/settings"
	"github.com/XNet-NGO/AIOPE-Headless/internal/vecstore"
	"github.com/XNet-NGO/AIOPE-Headless/internal/ws"
)

//go:embed web/*
var webEmbed embed.FS

func main() {
	cfg := config.Load()

	database, err := db.Open(cfg.DBPath)
	if err != nil {
		log.Fatal("db:", err)
	}
	defer database.Close()

	// Load provider from DB, seed defaults on first run
	provSvc := &provider.Service{DB: database}
	if ps, _ := provSvc.List(); len(ps) == 0 {
		seedDefaults(provSvc, database)
	}

	// Initialize gateway router
	gwStore := &gateway.Store{DB: database}
	gwStore.Init()
	gwRouter := gateway.NewRouter(gwStore)

	var prov llm.Provider
	var model string
	if active := provSvc.GetActive(); active != nil {
		// Try gateway resolution first
		if oai, resolvedModel, err := gwRouter.Resolve(active.SelectedModelID); err == nil {
			model = resolvedModel
			prov = oai
		} else {
			prov = &llm.OpenAI{APIKey: active.APIKey, APIBase: active.APIBase}
		}
		if model == "" { model = active.SelectedModelID }
	}
	if prov == nil {
		prov = &llm.OpenAI{}
	}

	webFS, _ := fs.Sub(webEmbed, "web")

	remoteSvc := remote.NewService(database)
	remoteSvc.SeedFromSSHConfig()

	vs := &vecstore.VecStore{DB: database, EmbedURL: "http://localhost:8091"}
	vs.Init()

	srv := &server.Server{
		Conversations: &conversation.Service{DB: database},
		Messages:      &message.Service{DB: database},
		Settings:      &settings.Service{DB: database},
		Providers:     provSvc,
		Hub:           ws.NewHub(),
		Provider:      prov,
		Model:         model,
		WebFS:         webFS,
		DB:            database,
		MCP:           mcp.NewManager(database),
		Remote:        remoteSvc,
		Gateway:       gwRouter,
		VecStore:      vs,
		Password:      cfg.Password,
		BasePath:      cfg.BasePath,
	}

	// Load password from DB if changed via UI
	var dbPw string
	if database.QueryRow("SELECT value FROM settings_kv WHERE key='password'").Scan(&dbPw) == nil && dbPw != "" {
		srv.Password = dbPw
	}

	log.Fatal(server.ListenAndServe(cfg.Bind, cfg.Port, srv.Handler()))
}

func seedDefaults(svc *provider.Service, database *sql.DB) {
	key := "63b282b9-2952-4c88-84e4-91a1eb91c007"
	if v := os.Getenv("AIOPE_GATEWAY_KEY"); v != "" {
		key = v
	}
	boolPtr := func(b bool) *bool { return &b }
	f64Ptr := func(f float64) *float64 { return &f }
	strPtr := func(s string) *string { return &s }
	mc := func(id string, tools, vision bool, ctx int, reasoning *string) provider.ModelConfig {
		return provider.ModelConfig{
			ModelID: id, ToolsOverride: boolPtr(tools), VisionOverride: boolPtr(vision),
			Temperature: f64Ptr(0.6), ReasoningEffort: reasoning,
			ContextTokens: ctx, AutoCompact: true,
		}
	}
	auto := strPtr("auto")
	p := &provider.Profile{
		ID:              "default_gateway",
		BuiltinID:       "aiope_gateway",
		Label:           "AIOPE Gateway",
		APIKey:          key,
		APIBase:         "https://inf.xnet.ngo/v1",
		SelectedModelID: "google-ai-studio/models-gemma-4-31b-it",
		IsActive:        true,
		ModelConfigs: map[string]provider.ModelConfig{
			"google-ai-studio/models-gemma-4-31b-it":    mc("google-ai-studio/models-gemma-4-31b-it", true, true, 256000, auto),
			"google-ai-studio/models-gemma-4-26b-a4b-it": mc("google-ai-studio/models-gemma-4-26b-a4b-it", true, true, 256000, auto),
			"google-ai-studio/models-gemma-3-27b-it":    mc("google-ai-studio/models-gemma-3-27b-it", false, true, 128000, auto),
			"google-ai-studio/models-gemma-3-12b-it":    mc("google-ai-studio/models-gemma-3-12b-it", false, true, 128000, nil),
			"google-ai-studio/models-gemma-3-4b-it":     mc("google-ai-studio/models-gemma-3-4b-it", false, true, 128000, nil),
			"cline/minimax-minimax-m2.5":                mc("cline/minimax-minimax-m2.5", true, false, 200000, auto),
			"zen/minimax-m2.5-free":                     mc("zen/minimax-m2.5-free", true, false, 200000, auto),
			"zen/nemotron-3-super-free":                 mc("zen/nemotron-3-super-free", true, false, 1000000, auto),
			"zen/big-pickle":                            mc("zen/big-pickle", true, false, 200000, auto),
			"cline/z-ai-glm-5":                         mc("cline/z-ai-glm-5", true, false, 200000, auto),
			"pollinations/openai":                       mc("pollinations/openai", true, false, 128000, auto),
			"pollinations/openai-fast":                  mc("pollinations/openai-fast", true, false, 128000, auto),
			"openrouter/openrouter-free":                mc("openrouter/openrouter-free", false, true, 128000, auto),
		},
	}
	if existing, err := svc.Get(p.ID); err == nil && existing != nil {
		if existing.APIKey != "" {
			p.APIKey = existing.APIKey
		}
		if existing.APIBase != "" {
			p.APIBase = existing.APIBase
		}
	}
	svc.Create(p)
	svc.SetActive(p.ID)

	for task, model := range llm.TaskModelDefaults {
		database.Exec("INSERT OR IGNORE INTO settings_kv(key,value) VALUES(?,?)", "task_model_"+task, model)
	}

	log.Println("Seeded default AIOPE Gateway provider")
}
