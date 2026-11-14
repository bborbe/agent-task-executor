// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"reflect"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
)

// The installed CRD schema is a hand-maintained copy of the ConfigSpec fields, and
// the CRD sets preserveUnknownFields: false — so a field declared on the Go type but
// missing from desiredCRDSpec is PRUNED by the API server. Nothing errors: the value
// is accepted by the client, silently dropped by the server, and the setting simply
// does not exist. That is how runWindow was lost on the first deploy of the window
// config, after it had passed tests, a render check and a review.
//
// This asserts the two copies agree, so the next added field fails here instead of
// failing silently in a cluster.
var _ = Describe("desiredCRDSpec", func() {
	It("declares a schema property for every ConfigSpec field", func() {
		specSchema := desiredCRDSpec().
			Versions[0].
			Schema.OpenAPIV3Schema.
			Properties["spec"]

		specType := reflect.TypeOf(agentv1.ConfigSpec{})
		var missing []string
		for i := 0; i < specType.NumField(); i++ {
			field := specType.Field(i)
			tag := field.Tag.Get("json")
			if tag == "" || tag == "-" {
				continue
			}
			name := strings.SplitN(tag, ",", 2)[0]
			if name == "" {
				continue
			}
			if _, ok := specSchema.Properties[name]; !ok {
				missing = append(missing, name)
			}
		}

		Expect(missing).To(BeEmpty(),
			"these ConfigSpec fields have no CRD schema property, so the API server "+
				"will prune them from every Config and the setting will silently do nothing")
	})
})
