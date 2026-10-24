// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner_test

import (
	"context"
	stderrors "errors"
	"flag"
	"io"
	"os"
	"strings"

	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	"github.com/golang/glog"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/bborbe/agent-task-executor/pkg/spawner"
)

const testPriorityClass = "github-pr-review-agent"

// captureSpawnerLog runs fn with glog writing to a pipe at V(2) — the level
// the deployed executors run at — and returns everything written. The suite
// runs serially, so the global os.Stderr swap cannot race another spec's
// logging.
func captureSpawnerLog(fn func()) string {
	Expect(flag.Set("logtostderr", "true")).To(Succeed())
	Expect(flag.Set("v", "2")).To(Succeed())
	defer func() {
		Expect(flag.Set("v", "0")).To(Succeed())
		Expect(flag.Set("logtostderr", "false")).To(Succeed())
	}()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	Expect(err).ToNot(HaveOccurred())
	os.Stderr = writer

	fn()
	glog.Flush()

	os.Stderr = original
	Expect(writer.Close()).To(Succeed())
	out, err := io.ReadAll(reader)
	Expect(err).ToNot(HaveOccurred())
	Expect(reader.Close()).To(Succeed())
	return string(out)
}

// newScopedQuota builds a quota scoped PriorityClass In [priorityClassName]
// with the pods limit in status.hard.
func newScopedQuota(name, priorityClassName, pods string) *corev1.ResourceQuota {
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test-ns"},
		Spec: corev1.ResourceQuotaSpec{
			ScopeSelector: &corev1.ScopeSelector{
				MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
					{
						ScopeName: corev1.ResourceQuotaScopePriorityClass,
						Operator:  corev1.ScopeSelectorOpIn,
						Values:    []string{priorityClassName},
					},
				},
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse(pods)},
		},
	}
}

func newQuotaLimiter(
	fakeClient *fake.Clientset,
	currentDateTime libtime.CurrentDateTimeGetter,
) spawner.PodQuotaLimiter {
	return spawner.NewPodQuotaLimiter(fakeClient, currentDateTime)
}

func listResourceQuotaActions(fakeClient *fake.Clientset) int {
	count := 0
	for _, action := range fakeClient.Actions() {
		if action.Matches("list", "resourcequotas") {
			count++
		}
	}
	return count
}

var _ = Describe("PodQuotaLimiter", func() {
	var (
		ctx             context.Context
		fakeClient      *fake.Clientset
		currentDateTime libtime.CurrentDateTime
	)

	BeforeEach(func() {
		ctx = context.Background()
		fakeClient = fake.NewClientset()
		currentDateTime = libtime.NewCurrentDateTime()
		currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-24T10:00:00Z"))
	})

	DescribeTable(
		"PodQuota selects the quota scoping the priority class",
		func(quotas []*corev1.ResourceQuota, priorityClassName string, expected int) {
			objects := make([]runtime.Object, 0, len(quotas))
			for _, quota := range quotas {
				quota.Namespace = "test-ns"
				objects = append(objects, quota)
			}
			fakeClient = fake.NewClientset(objects...)

			limit, err := newQuotaLimiter(fakeClient, currentDateTime).
				PodQuota(ctx, libk8s.Namespace("test-ns"), priorityClassName)
			Expect(err).ToNot(HaveOccurred())
			Expect(limit).To(Equal(expected))
		},
		Entry(
			"matching priority class returns its pods limit",
			[]*corev1.ResourceQuota{newScopedQuota("agent-quota", testPriorityClass, "3")},
			testPriorityClass,
			3,
		),
		Entry(
			"quota scoped to a different priority class returns zero",
			[]*corev1.ResourceQuota{newScopedQuota("other-quota", "other-agent", "7")},
			testPriorityClass,
			0,
		),
		Entry(
			"unrelated quota alongside a matching quota is ignored, not treated as no match",
			[]*corev1.ResourceQuota{
				newScopedQuota("other-quota", "other-agent", "7"),
				newScopedQuota("agent-quota", testPriorityClass, "3"),
			},
			testPriorityClass,
			3,
		),
		Entry(
			"quota with a nil scopeSelector returns zero",
			[]*corev1.ResourceQuota{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "unscoped"},
					Status: corev1.ResourceQuotaStatus{
						Hard: corev1.ResourceList{
							corev1.ResourcePods: resource.MustParse("5"),
						},
					},
				},
			},
			testPriorityClass,
			0,
		),
		Entry("no quota at all returns zero", nil, testPriorityClass, 0),
		Entry(
			"pods limit falls back to spec.hard when status.hard is empty",
			[]*corev1.ResourceQuota{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "spec-only"},
					Spec: corev1.ResourceQuotaSpec{
						ScopeSelector: &corev1.ScopeSelector{
							MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
								{
									ScopeName: corev1.ResourceQuotaScopePriorityClass,
									Operator:  corev1.ScopeSelectorOpIn,
									Values:    []string{testPriorityClass},
								},
							},
						},
						Hard: corev1.ResourceList{
							corev1.ResourcePods: resource.MustParse("2"),
						},
					},
				},
			},
			testPriorityClass,
			2,
		),
		Entry(
			"literal PriorityClass/In strings match, pinning the constant values",
			[]*corev1.ResourceQuota{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "literal"},
					Spec: corev1.ResourceQuotaSpec{
						ScopeSelector: &corev1.ScopeSelector{
							MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
								{
									ScopeName: "PriorityClass",
									Operator:  "In",
									Values:    []string{testPriorityClass},
								},
							},
						},
					},
					Status: corev1.ResourceQuotaStatus{
						Hard: corev1.ResourceList{
							corev1.ResourcePods: resource.MustParse("4"),
						},
					},
				},
			},
			testPriorityClass,
			4,
		),
		Entry(
			"NotIn does not match; only the In operator does",
			[]*corev1.ResourceQuota{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "not-in"},
					Spec: corev1.ResourceQuotaSpec{
						ScopeSelector: &corev1.ScopeSelector{
							MatchExpressions: []corev1.ScopedResourceSelectorRequirement{
								{
									ScopeName: corev1.ResourceQuotaScopePriorityClass,
									Operator:  corev1.ScopeSelectorOpNotIn,
									Values:    []string{testPriorityClass},
								},
							},
						},
					},
					Status: corev1.ResourceQuotaStatus{
						Hard: corev1.ResourceList{
							corev1.ResourcePods: resource.MustParse("9"),
						},
					},
				},
			},
			testPriorityClass,
			0,
		),
	)

	It("returns zero without an API call for an empty priority class", func() {
		limit, err := newQuotaLimiter(fakeClient, currentDateTime).
			PodQuota(ctx, libk8s.Namespace("test-ns"), "")
		Expect(err).ToNot(HaveOccurred())
		Expect(limit).To(Equal(0))
		Expect(fakeClient.Actions()).To(BeEmpty())
	})

	It("takes the smaller limit and logs one ambiguity warning naming both quotas", func() {
		fakeClient = fake.NewClientset(
			newScopedQuota("agent-quota-large", testPriorityClass, "5"),
			newScopedQuota("agent-quota-small", testPriorityClass, "3"),
		)
		limiter := newQuotaLimiter(fakeClient, currentDateTime)

		var limit int
		out := captureSpawnerLog(func() {
			var err error
			limit, err = limiter.PodQuota(ctx, libk8s.Namespace("test-ns"), testPriorityClass)
			Expect(err).ToNot(HaveOccurred())
		})

		Expect(limit).To(Equal(3))
		Expect(strings.Count(out, "event=pod_quota_ambiguous")).To(Equal(1))
		Expect(out).To(ContainSubstring("agent-quota-large"))
		Expect(out).To(ContainSubstring("agent-quota-small"))
		Expect(out).To(ContainSubstring("using=3"))
	})

	It("serves a cached answer within 30s and re-reads after it", func() {
		fakeClient = fake.NewClientset(newScopedQuota("agent-quota", testPriorityClass, "3"))
		limiter := newQuotaLimiter(fakeClient, currentDateTime)

		currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-24T10:00:00Z"))
		limit, err := limiter.PodQuota(ctx, libk8s.Namespace("test-ns"), testPriorityClass)
		Expect(err).ToNot(HaveOccurred())
		Expect(limit).To(Equal(3))

		currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-24T10:00:10Z"))
		limit, err = limiter.PodQuota(ctx, libk8s.Namespace("test-ns"), testPriorityClass)
		Expect(err).ToNot(HaveOccurred())
		Expect(limit).To(Equal(3))

		currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-24T10:00:31Z"))
		limit, err = limiter.PodQuota(ctx, libk8s.Namespace("test-ns"), testPriorityClass)
		Expect(err).ToNot(HaveOccurred())
		Expect(limit).To(Equal(3))

		Expect(listResourceQuotaActions(fakeClient)).To(Equal(2))
	})

	It("does not cache a failed read, so the next call retries", func() {
		fakeClient = fake.NewClientset(newScopedQuota("agent-quota", testPriorityClass, "3"))
		listCalls := 0
		fakeClient.PrependReactor(
			"list",
			"resourcequotas",
			func(action k8stesting.Action) (bool, runtime.Object, error) {
				listCalls++
				if listCalls == 1 {
					return true, nil, stderrors.New("forbidden")
				}
				return false, nil, nil
			},
		)
		limiter := newQuotaLimiter(fakeClient, currentDateTime)

		limit, err := limiter.PodQuota(ctx, libk8s.Namespace("test-ns"), testPriorityClass)
		Expect(err).To(HaveOccurred())
		Expect(limit).To(Equal(0))

		limit, err = limiter.PodQuota(ctx, libk8s.Namespace("test-ns"), testPriorityClass)
		Expect(err).ToNot(HaveOccurred())
		Expect(limit).To(Equal(3))

		Expect(listCalls).To(Equal(2))
	})
})
