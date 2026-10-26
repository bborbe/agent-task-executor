// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/bborbe/errors"
	libk8s "github.com/bborbe/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// gitRestGatewaySecretDataKey is the data key inside the referenced K8s Secret
// that holds the git-rest gateway secret value. Matches the controller chart's
// `gateway-secret` data key (the Secret git-rest and the controller share).
const gitRestGatewaySecretDataKey = "gateway-secret"

// ReadGitRestGatewaySecret returns the git-rest gateway secret value from the
// named K8s Secret, or "" when secretName is empty (gateway auth disabled).
// The value is read once at startup and held in memory only — it is never
// logged, never embedded in the image, and never in env (spec 005 Security;
// the env var GITREST_GATEWAY_SECRET references the secret by NAME, matching
// the JobKafkaClientCertSecret pattern). Returns an error when the Secret is
// missing, lacks the gateway-secret data key, or holds an empty value there, so
// a misconfigured deployment fails loudly at startup instead of silently
// sending no auth header. An empty secretName is the only way to disable
// gateway auth.
func ReadGitRestGatewaySecret(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
	secretName string,
) (string, error) {
	return readGatewaySecret(
		ctx,
		kubeClient,
		namespace,
		secretName,
		gitRestGatewaySecretDataKey,
		"git-rest",
	)
}

// readGatewaySecret is the one reader every gateway credential uses. The
// component only names the service in the error, so a misconfigured deployment
// says which secret is missing rather than a generic message; the data key is
// the caller's because a Secret's layout is the deployment's business.
func readGatewaySecret(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
	secretName string,
	dataKey string,
	component string,
) (string, error) {
	if secretName == "" {
		return "", nil
	}
	secret, err := kubeClient.CoreV1().
		Secrets(namespace.String()).
		Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return "", errors.Wrapf(ctx, err, "get %s gateway secret %q", component, secretName)
	}
	value, ok := secret.Data[dataKey]
	if !ok {
		return "", errors.Errorf(ctx, "secret %q lacks data key %q", secretName, dataKey)
	}
	if len(value) == 0 {
		return "", errors.Errorf(
			ctx,
			"secret %q data key %q is empty; unset the secret name to disable gateway auth",
			secretName,
			dataKey,
		)
	}
	return string(value), nil
}
