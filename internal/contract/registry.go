package contract

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
)

// ErrContractExists 同名同版本契约重复注册时返回。
var ErrContractExists = errors.New("contract version already registered")

// memoryRegistry 是线程安全的内存契约注册表。
type memoryRegistry struct {
	mu     sync.RWMutex
	byName map[string]map[string]model.Contract // name -> version -> contract
}

// NewRegistry 创建内存版契约注册表。
func NewRegistry() Registry {
	return &memoryRegistry{byName: map[string]map[string]model.Contract{}}
}

func (r *memoryRegistry) Register(c model.Contract) error {
	if c.Name == "" {
		return errors.New("contract name is required")
	}
	if c.Version == "" {
		return errors.New("contract version is required")
	}
	if len(c.Fields) == 0 {
		return errors.New("contract must declare at least one field")
	}
	seen := map[string]bool{}
	for _, f := range c.Fields {
		if f.Name == "" {
			return errors.New("field name is required")
		}
		if seen[f.Name] {
			return fmt.Errorf("duplicate field in contract: %s", f.Name)
		}
		seen[f.Name] = true
		switch f.Type {
		case model.TypeString, model.TypeInt, model.TypeFloat, model.TypeBool:
		default:
			return fmt.Errorf("field %s has unsupported type %q", f.Name, f.Type)
		}
	}
	keySeen := map[string]bool{}
	for _, k := range c.KeyFields {
		if !seen[k] {
			return fmt.Errorf("key field %s not declared in contract %s", k, c.Name)
		}
		if keySeen[k] {
			return fmt.Errorf("duplicate key field: %s", k)
		}
		keySeen[k] = true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	vs, ok := r.byName[c.Name]
	if !ok {
		vs = map[string]model.Contract{}
		r.byName[c.Name] = vs
	}
	if _, exists := vs[c.Version]; exists {
		return fmt.Errorf("%w: %s@%s", ErrContractExists, c.Name, c.Version)
	}
	vs[c.Version] = c
	return nil
}

func (r *memoryRegistry) Get(name, version string) (model.Contract, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if vs, ok := r.byName[name]; ok {
		c, ok := vs[version]
		return c, ok
	}
	return model.Contract{}, false
}

func (r *memoryRegistry) Active(name string) (model.Contract, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	vs, ok := r.byName[name]
	if !ok || len(vs) == 0 {
		return model.Contract{}, false
	}
	var active model.Contract
	var activeSV semver
	first := true
	for _, c := range vs {
		sv := parseSemver(c.Version)
		if first || sv.compare(activeSV) > 0 {
			active, activeSV, first = c, sv, false
		}
	}
	return active, true
}

func (r *memoryRegistry) Versions(name string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	vs, ok := r.byName[name]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(vs))
	for v := range vs {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		return parseSemver(out[i]).compare(parseSemver(out[j])) < 0
	})
	return out
}
