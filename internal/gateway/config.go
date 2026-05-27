package gateway

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ProviderConfig represents an upstream LLM provider (e.g., Google AI Studio, Pollinations).
type ProviderConfig struct {
	Name    string `json:"name"`    // e.g. "google-ai-studio"
	APIKey  string `json:"apiKey"`
	APIBase string `json:"apiBase"` // e.g. "https://generativelanguage.googleapis.com/v1beta/openai"
	Enabled bool   `json:"enabled"`
}

// ModelRoute maps a display model ID to its upstream provider and actual model name.
type ModelRoute struct {
	DisplayID string `json:"displayId"` // what the client requests, e.g. "google-ai-studio/models-gemma-4-31b-it"
	Provider  string `json:"provider"`  // references ProviderConfig.Name
	Model     string `json:"model"`     // actual model ID sent upstream
	Enabled   bool   `json:"enabled"`
}

// Store persists gateway provider configs and model routes in SQLite.
type Store struct{ DB *sql.DB }

func (s *Store) Init() error {
	_, err := s.DB.Exec(`
		CREATE TABLE IF NOT EXISTS gateway_providers (
			name TEXT PRIMARY KEY,
			json TEXT NOT NULL,
			updatedAt INTEGER
		);
		CREATE TABLE IF NOT EXISTS gateway_routes (
			displayId TEXT PRIMARY KEY,
			provider TEXT NOT NULL,
			model TEXT NOT NULL,
			enabled INTEGER DEFAULT 1,
			FOREIGN KEY(provider) REFERENCES gateway_providers(name)
		);
	`)
	return err
}

func (s *Store) ListProviders() ([]ProviderConfig, error) {
	rows, err := s.DB.Query("SELECT json FROM gateway_providers ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProviderConfig
	for rows.Next() {
		var j string
		rows.Scan(&j)
		var p ProviderConfig
		json.Unmarshal([]byte(j), &p)
		out = append(out, p)
	}
	return out, nil
}

func (s *Store) GetProvider(name string) (*ProviderConfig, error) {
	var j string
	if err := s.DB.QueryRow("SELECT json FROM gateway_providers WHERE name=?", name).Scan(&j); err != nil {
		return nil, err
	}
	var p ProviderConfig
	json.Unmarshal([]byte(j), &p)
	return &p, nil
}

func (s *Store) SaveProvider(p *ProviderConfig) error {
	j, _ := json.Marshal(p)
	_, err := s.DB.Exec(
		"INSERT OR REPLACE INTO gateway_providers(name,json,updatedAt) VALUES(?,?,?)",
		p.Name, string(j), time.Now().UnixMilli(),
	)
	return err
}

func (s *Store) DeleteProvider(name string) error {
	_, err := s.DB.Exec("DELETE FROM gateway_providers WHERE name=?", name)
	return err
}

func (s *Store) ListRoutes() ([]ModelRoute, error) {
	rows, err := s.DB.Query("SELECT displayId,provider,model,enabled FROM gateway_routes ORDER BY displayId")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ModelRoute
	for rows.Next() {
		var r ModelRoute
		var en int
		rows.Scan(&r.DisplayID, &r.Provider, &r.Model, &en)
		r.Enabled = en == 1
		out = append(out, r)
	}
	return out, nil
}

func (s *Store) SaveRoute(r *ModelRoute) error {
	en := 0
	if r.Enabled {
		en = 1
	}
	_, err := s.DB.Exec(
		"INSERT OR REPLACE INTO gateway_routes(displayId,provider,model,enabled) VALUES(?,?,?,?)",
		r.DisplayID, r.Provider, r.Model, en,
	)
	return err
}

func (s *Store) DeleteRoute(displayID string) error {
	_, err := s.DB.Exec("DELETE FROM gateway_routes WHERE displayId=?", displayID)
	return err
}

func (s *Store) DeleteRoutesByProvider(providerName string) error {
	_, err := s.DB.Exec("DELETE FROM gateway_routes WHERE provider=?", providerName)
	return err
}

func (s *Store) FetchUpstreamModels(p *ProviderConfig) ([]string, error) {
	base := p.APIBase
	if base == "" {
		return nil, fmt.Errorf("no apiBase configured")
	}
	req, _ := http.NewRequest("GET", base+"/models", nil)
	if p.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.APIKey)
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(b))
	}
	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	var out []string
	for _, m := range result.Data {
		out = append(out, m.ID)
	}
	return out, nil
}
