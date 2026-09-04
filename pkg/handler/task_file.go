// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	"context"
	"strings"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/errors"
	"github.com/golang/glog"
	"gopkg.in/yaml.v3"
)

// extractFrontmatter returns the YAML frontmatter block between the leading
// "---" and the closing "\n---" delimiters. Ported from the controller's
// pkg/scanner/frontmatter.go (spec 005 — the reconcile loop reads vault task
// files through git-rest and needs the same parsing the controller uses).
func extractFrontmatter(ctx context.Context, content []byte) (string, error) {
	s := string(content)
	const delim = "---"
	if !strings.HasPrefix(s, delim) {
		return "", errors.Errorf(ctx, "no frontmatter delimiter found")
	}
	rest := strings.TrimPrefix(s, delim)
	if strings.HasPrefix(rest, "\r\n") {
		rest = rest[2:]
	} else if strings.HasPrefix(rest, "\n") {
		rest = rest[1:]
	}
	idx := strings.Index(rest, "\n---")
	if idx == -1 {
		return "", errors.Errorf(ctx, "frontmatter not closed")
	}
	return rest[:idx], nil
}

// extractBody returns the markdown body after the closing frontmatter delimiter.
func extractBody(content []byte) string {
	s := string(content)
	const delim = "---"
	if !strings.HasPrefix(s, delim) {
		return s
	}
	rest := strings.TrimPrefix(s, delim)
	if strings.HasPrefix(rest, "\r\n") {
		rest = rest[2:]
	} else if strings.HasPrefix(rest, "\n") {
		rest = rest[1:]
	}
	idx := strings.Index(rest, "\n---")
	if idx == -1 {
		return s
	}
	after := rest[idx+4:] // skip "\n---"
	if strings.HasPrefix(after, "\r\n") {
		after = after[2:]
	} else if strings.HasPrefix(after, "\n") {
		after = after[1:]
	}
	return after
}

// parseTaskFile parses a vault task file into a lib.Task. ok=false when the
// file has no valid frontmatter or no task_identifier (the file is not a task
// the executor can drive). The Content is the markdown body — it is preserved
// verbatim because SpawnJob renders it into the spawned Job's TASK_CONTENT env
// (renderTaskContent in pkg/spawner), so a dropped body would spawn an agent
// with no instructions.
func parseTaskFile(ctx context.Context, content []byte) (lib.Task, bool) {
	fmYAML, err := extractFrontmatter(ctx, content)
	if err != nil {
		glog.Warningf("event=reconcile_skip reason=invalid_frontmatter err=%v", err)
		return lib.Task{}, false
	}
	var fmMap map[string]interface{}
	if err := yaml.Unmarshal([]byte(fmYAML), &fmMap); err != nil {
		glog.Warningf("event=reconcile_skip reason=unparseable_frontmatter err=%v", err)
		return lib.Task{}, false
	}
	taskID, _ := fmMap["task_identifier"].(string)
	if taskID == "" {
		glog.Warningf("event=reconcile_skip reason=missing_task_identifier")
		return lib.Task{}, false
	}
	return lib.Task{
		TaskIdentifier: lib.TaskIdentifier(taskID),
		Frontmatter:    lib.TaskFrontmatter(fmMap),
		Content:        lib.TaskContent(extractBody(content)),
	}, true
}
