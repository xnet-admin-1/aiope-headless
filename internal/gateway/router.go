package gateway

import (
	"fmt"
	"strings"
	"sync"

	"github.com/XNet-NGO/AIOPE-Headless/internal/llm"
)

// Router resolves model display IDs to configured upstream providers.
type Router struct {
	store     *Store
	mu        sync.RWMutex
	providers map[string]*ProviderConfig
	routes    map[string]*ModelRoute
}

func NewRouter(store *Store) *Router {
	r := &Router{store: store}
	r.Reload()
	return r
}

func (r *Router) Reload() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers = make(map[string]*ProviderConfig)
	r.routes = make(map[string]*ModelRoute)
	if ps, err := r.store.ListProviders(); err == nil {
		for i := range ps {
			r.providers[ps[i].Name] = &ps[i]
		}
	}
	if rs, err := r.store.ListRoutes(); err == nil {
		for i := range rs {
			r.routes[rs[i].DisplayID] = &rs[i]
		}
	}
}

// Resolve maps a display model ID to an LLM client and upstream model name.
// Supports explicit routes and prefix-based inheritance (provider/model).
func (r *Router) Resolve(displayID string) (*llm.OpenAI, string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Explicit route
	if route, ok := r.routes[displayID]; ok && route.Enabled {
		if prov, ok := r.providers[route.Provider]; ok && prov.Enabled {
			return &llm.OpenAI{APIKey: prov.APIKey, APIBase: prov.APIBase}, route.Model, nil
		}
	}

	// Prefix inheritance: "provider-name/model-id"
	if idx := strings.Index(displayID, "/"); idx > 0 {
		provName := displayID[:idx]
		model := displayID[idx+1:]
		if prov, ok := r.providers[provName]; ok && prov.Enabled {
			return &llm.OpenAI{APIKey: prov.APIKey, APIBase: prov.APIBase}, model, nil
		}
	}

	return nil, "", fmt.Errorf("no provider for model: %s", displayID)
}

// ListModels returns all enabled display IDs.
func (r *Router) ListModels() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for id, route := range r.routes {
		if route.Enabled {
			if prov, ok := r.providers[route.Provider]; ok && prov.Enabled {
				out = append(out, id)
			}
		}
	}
	return out
}

func (r *Router) AddProvider(p *ProviderConfig) error {
	if err := r.store.SaveProvider(p); err != nil {
		return err
	}
	r.Reload()
	return nil
}

func (r *Router) AddRoute(route *ModelRoute) error {
	if err := r.store.SaveRoute(route); err != nil {
		return err
	}
	r.Reload()
	return nil
}

func (r *Router) RemoveProvider(name string) error {
	r.store.DeleteRoutesByProvider(name)
	r.store.DeleteProvider(name)
	r.Reload()
	return nil
}

func (r *Router) Store() *Store { return r.store }

func (r *Router) GetProvider(name string) (*ProviderConfig, error) {
	return r.store.GetProvider(name)
}

func (r *Router) UpdateRoute(displayID string, enabled bool) error {
	r.mu.RLock()
	route, ok := r.routes[displayID]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("route not found: %s", displayID)
	}
	route.Enabled = enabled
	if err := r.store.SaveRoute(route); err != nil {
		return err
	}
	r.Reload()
	return nil
}

func (r *Router) RemoveRoute(displayID string) error {
	if err := r.store.DeleteRoute(displayID); err != nil {
		return err
	}
	r.Reload()
	return nil
}

func (r *Router) UpdateProvider(name string, apiKey, apiBase string, enabled *bool) error {
	p, err := r.store.GetProvider(name)
	if err != nil {
		return err
	}
	if apiKey != "" && !strings.Contains(apiKey, "****") {
		p.APIKey = apiKey
	}
	if apiBase != "" {
		p.APIBase = apiBase
	}
	if enabled != nil {
		p.Enabled = *enabled
	}
	if err := r.store.SaveProvider(p); err != nil {
		return err
	}
	r.Reload()
	return nil
}

func (r *Router) DiscoverModels(name string) ([]string, error) {
	p, err := r.store.GetProvider(name)
	if err != nil {
		return nil, err
	}
	return r.store.FetchUpstreamModels(p)
}
