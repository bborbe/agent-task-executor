// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	libk8s "github.com/bborbe/k8s"
	"k8s.io/client-go/kubernetes"
)

// vaultAPIGatewaySecretDataKey is the data key inside the referenced K8s Secret
// that holds the vault-api gateway secret value. It is the same `gateway-secret`
// key every service in this fleet uses for its gateway credential.
const vaultAPIGatewaySecretDataKey = "gateway-secret"

// ReadVaultAPIGatewaySecret returns the vault-api gateway secret value from the
// named K8s Secret, or "" when secretName is empty (gateway auth disabled).
//
// It mirrors ReadGitRestGatewaySecret on purpose: the secret is referenced by
// NAME from env, read once at startup, and held in memory only — never logged,
// never embedded in the image, and never in env. A missing Secret, a missing
// data key or an empty value is an error, so a misconfigured deployment fails
// loudly at startup instead of silently sending no auth header and having every
// claim refused.
func ReadVaultAPIGatewaySecret(
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
		vaultAPIGatewaySecretDataKey,
		"vault-api",
	)
}
