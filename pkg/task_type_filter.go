// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

// EffectiveTaskTypes computes the union of a singular taskType and a taskTypes list.
// The singular taskType is first in the result when non-empty. Duplicates are removed
// while preserving order. The result is the set an agent accepts.
func EffectiveTaskTypes(taskType string, taskTypes []string) []string {
	seen := make(map[string]struct{})
	var result []string
	if taskType != "" {
		seen[taskType] = struct{}{}
		result = append(result, taskType)
	}
	for _, t := range taskTypes {
		if _, ok := seen[t]; !ok {
			seen[t] = struct{}{}
			result = append(result, t)
		}
	}
	return result
}

// TaskTypeInSet reports whether taskType is in the effectiveTypes set.
// Empty taskType never matches — strict semantics, no bypass for legacy tasks.
func TaskTypeInSet(taskType string, effectiveTypes []string) bool {
	if taskType == "" {
		return false
	}
	for _, t := range effectiveTypes {
		if t == taskType {
			return true
		}
	}
	return false
}
