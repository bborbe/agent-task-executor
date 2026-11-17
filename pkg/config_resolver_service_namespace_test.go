// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
)

var _ = DescribeTable("convert carries serviceNamespace",
	func(serviceNamespace string, expected string) {
		provider := &fakeProvider{
			items: []agentv1.Config{
				{
					Spec: agentv1.ConfigSpec{
						Assignee:         "claude",
						Image:            "foo/bar",
						Heartbeat:        "30m",
						ServiceNamespace: serviceNamespace,
					},
				},
			},
		}

		resolved, err := pkg.NewConfigResolver(provider, "dev", "personal").
			Resolve(context.Background(), "claude")
		Expect(err).NotTo(HaveOccurred(), "Resolve failed")
		Expect(resolved.ServiceNamespace).To(Equal(expected))
	},
	Entry("explicit serviceNamespace survives the resolver", "agent-sandbox", "agent-sandbox"),
	Entry("unset serviceNamespace stays empty", "", ""),
)
