// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// podQuotaCacheTTL bounds how long one namespace/priority-class answer is
// served from cache. Admission runs on every task, so without it each task
// would pay a ResourceQuota list; the window keeps the cost at one list per
// namespace and priority class.
const podQuotaCacheTTL = 30 * time.Second

//counterfeiter:generate -o ../../mocks/pod_quota_limiter.go --fake-name FakePodQuotaLimiter . PodQuotaLimiter

// PodQuotaLimiter reports the pod capacity the namespace ResourceQuota enforces
// for one agent priority class.
type PodQuotaLimiter interface {
	// PodQuota returns the pod limit of the ResourceQuota whose scopeSelector
	// matches PriorityClass In [priorityClassName] in namespace.
	//
	// bounded distinguishes "no quota scopes this class" from "the quota admits
	// zero pods". Without it a quota carrying hard.pods: 0 would be
	// indistinguishable from an absent one, admission would fall back to the
	// declared cap, and the executor would claim work the quota can never start —
	// the exact defect this reader exists to remove, in the most restrictive
	// configuration.
	PodQuota(
		ctx context.Context,
		namespace libk8s.Namespace,
		priorityClassName string,
	) (limit int, bounded bool, err error)
}

// NewPodQuotaLimiter creates a PodQuotaLimiter backed by the K8s core/v1 API.
func NewPodQuotaLimiter(
	kubeClient kubernetes.Interface,
	currentDateTime libtime.CurrentDateTimeGetter,
) PodQuotaLimiter {
	return &podQuotaLimiter{
		kubeClient:      kubeClient,
		currentDateTime: currentDateTime,
		cache:           make(map[string]cachedPodQuota),
	}
}

// podQuotaLimiter implements PodQuotaLimiter by listing ResourceQuotas and
// memoizing the answer for podQuotaCacheTTL.
type podQuotaLimiter struct {
	kubeClient      kubernetes.Interface
	currentDateTime libtime.CurrentDateTimeGetter
	mu              sync.Mutex
	cache           map[string]cachedPodQuota
}

// cachedPodQuota is one priority class's answer and the instant it was read.
type cachedPodQuota struct {
	limit   int
	bounded bool
	readAt  time.Time
}

// PodQuota returns the pod limit of the quota scoping the priority class. An
// empty priority class means the Config names none, so no quota can bound it
// and no API call is made. A failed list is returned and never cached, so the
// next admission retries rather than inheriting the failure for a full window.
func (l *podQuotaLimiter) PodQuota(
	ctx context.Context,
	namespace libk8s.Namespace,
	priorityClassName string,
) (int, bool, error) {
	if priorityClassName == "" {
		return 0, false, nil
	}
	cacheKey := namespace.String() + "/" + priorityClassName
	now := l.currentDateTime.Now().Time()

	l.mu.Lock()
	cached, ok := l.cache[cacheKey]
	l.mu.Unlock()
	if ok && now.Sub(cached.readAt) < podQuotaCacheTTL {
		return cached.limit, cached.bounded, nil
	}

	quotaList, err := l.kubeClient.CoreV1().
		ResourceQuotas(namespace.String()).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, false, errors.Wrapf(ctx, err, "list resourcequotas in namespace %s", namespace)
	}
	limit, bounded := podQuotaFromQuotas(quotaList.Items, priorityClassName)

	l.mu.Lock()
	l.cache[cacheKey] = cachedPodQuota{limit: limit, bounded: bounded, readAt: now}
	l.mu.Unlock()
	return limit, bounded, nil
}

// quotaScopesPriorityClass reports whether the quota's scopeSelector limits
// exactly the priority class named. Only the In operator matches; Exists,
// NotIn and DoesNotExist select other sets.
func quotaScopesPriorityClass(quota *corev1.ResourceQuota, priorityClassName string) bool {
	if quota.Spec.ScopeSelector == nil {
		return false
	}
	for _, expr := range quota.Spec.ScopeSelector.MatchExpressions {
		if expr.ScopeName != corev1.ResourceQuotaScopePriorityClass {
			continue
		}
		if expr.Operator != corev1.ScopeSelectorOpIn {
			continue
		}
		for _, value := range expr.Values {
			if value == priorityClassName {
				return true
			}
		}
	}
	return false
}

// quotaPodLimit returns the quota's pods limit. status.hard is authoritative
// once the quota controller has processed the object; spec.hard is the
// fallback while it has not.
func quotaPodLimit(quota *corev1.ResourceQuota) (int, bool) {
	if quantity, ok := quota.Status.Hard[corev1.ResourcePods]; ok {
		return int(quantity.Value()), true
	}
	if quantity, ok := quota.Spec.Hard[corev1.ResourcePods]; ok {
		return int(quantity.Value()), true
	}
	return 0, false
}

// podQuotaFromQuotas returns the smallest pods limit among the quotas that scope
// the priority class, and whether any such quota bounds it at all. A quota
// carrying pods: 0 is a hard zero bound — the class may run nothing — and is
// reported as bounded, never as "no quota". Only a quota with no pods entry is
// skipped. More than one contributing quota is ambiguous: the smaller bound is
// enforced so admission cannot exceed either quota, and the ambiguity is logged
// once naming every contributor.
func podQuotaFromQuotas(
	quotas []corev1.ResourceQuota,
	priorityClassName string,
) (int, bool) {
	limit := 0
	bounded := false
	contributors := make([]string, 0, len(quotas))
	for i := range quotas {
		quota := &quotas[i]
		if !quotaScopesPriorityClass(quota, priorityClassName) {
			continue
		}
		podLimit, ok := quotaPodLimit(quota)
		if !ok {
			continue
		}
		contributors = append(contributors, quota.Name)
		if !bounded || podLimit < limit {
			limit = podLimit
			bounded = true
		}
	}
	if len(contributors) > 1 {
		glog.Warningf(
			"event=pod_quota_ambiguous priority_class=%s quotas=%s using=%d",
			priorityClassName,
			strings.Join(contributors, ","),
			limit,
		)
	}
	return limit, bounded
}
