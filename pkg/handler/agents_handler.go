// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"encoding/json"
	"net/http"

	"github.com/bborbe/k8s"
	"github.com/golang/glog"

	agentv1 "github.com/bborbe/agent/task/executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

// NewAgentsHandler returns an http.Handler that lists all known agent
// configs from the in-memory store as JSON.
// If authSecret is non-empty, requests must include X-Agent-Auth header with the secret.
func NewAgentsHandler(provider k8s.Provider[agentv1.Config], authSecret string) http.Handler {
	return &agentsHandler{
		provider:   provider,
		authSecret: authSecret,
	}
}

type agentsHandler struct {
	provider   k8s.Provider[agentv1.Config]
	authSecret string
}

func (h *agentsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// AGENTS_AUTH_SECRET enables authentication for the /agents endpoint.
	// If empty, authentication is disabled (for development).
	if h.authSecret != "" {
		if r.Header.Get("X-Agent-Auth") != h.authSecret {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	configs, err := h.provider.Get(r.Context())
	if err != nil {
		glog.Warningf("list agent configs: %v", err)
		http.Error(w, "failed to list configs", http.StatusInternalServerError)
		return
	}
	type agentEntry struct {
		Name            string `json:"name"`
		Assignee        string `json:"assignee"`
		Image           string `json:"image"`
		Heartbeat       string `json:"heartbeat"`
		SecretName      string `json:"secretName,omitempty"`
		VolumeClaim     string `json:"volumeClaim,omitempty"`
		VolumeMountPath string `json:"volumeMountPath,omitempty"`
	}
	entries := make([]agentEntry, 0, len(configs))
	for _, c := range configs {
		entries = append(entries, agentEntry{
			Name:            c.Name,
			Assignee:        c.Spec.Assignee,
			Image:           c.Spec.Image,
			Heartbeat:       c.Spec.Heartbeat,
			SecretName:      c.Spec.SecretName,
			VolumeClaim:     c.Spec.VolumeClaim,
			VolumeMountPath: c.Spec.VolumeMountPath,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(entries); err != nil {
		glog.Warningf("encode agent configs: %v", err)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}
