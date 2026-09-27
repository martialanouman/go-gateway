#!/usr/bin/env bash
# Rend l'overlay de test avec un tag fictif et vérifie ce qui, s'il cassait, ne se verrait qu'au
# déploiement : un bind SMPP refusé, un pod Pending faute de CPU, une image jamais substituée.
set -euo pipefail

# v0.0.1, pas v0.0.0 : le garde-fou de render-manifests.sh sur le gabarit ("$PREFIX/svc:v0.0.0" non
# ancré en fin de ligne) matcherait n'importe quelle version commençant par "v0.0.0", y compris
# "v0.0.0-sha-...", et ferait échouer le rendu avant même d'atteindre kustomize.
VERSION="${1:-v0.0.1-sha-000000000000}"
cd "$(dirname "$0")/../.."
out=deploy/test/rendered
mkdir -p "$out"
scripts/render-manifests.sh "$VERSION" >"$out/base.yaml"
kubectl kustomize deploy/test >"$out/all.yaml"
all="$out/all.yaml"

fail() { echo "check.sh: $*" >&2; exit 1; }

grep -q 'ENVIRONMENT: staging' "$all" || fail "ENVIRONMENT n'est pas staging"
grep -q 'OTEL_SDK_DISABLED: "true"' "$all" || fail "le traçage n'est pas coupé"
grep -q 'KAFKA_BROKERS: redpanda:9092' "$all" || fail "KAFKA_BROKERS ne vise pas redpanda"
! grep -q 'SMPP_TRUSTED_PROXY_CIDRS' "$all" || fail "SMPP_TRUSTED_PROXY_CIDRS survit : tout bind serait refusé"
grep -q 'value: smsc-simulator:2775' "$all" || fail "CONNECTOR_ADDR ne vise pas le simulateur"
grep -q 'name: HTTP_ADMIN_TOKENS' "$all" || fail "admin-api-svc sans HTTP_ADMIN_TOKENS"
! grep -q 'ghcr.io/martialanouman/go-gateway/[a-z0-9-]*:v0.0.0$' "$all" || fail "un gabarit v0.0.0 a survécu"
! grep -qE '^ +replicas: ([2-9]|[1-9][0-9]+)$' "$all" || fail "un Deployment garde plus d'une réplique"
! grep -qE '^ +cpu: ([1-9][0-9]*|[1-9][0-9]{2,}m)$' "$all" || fail "une requête CPU de production survit"
grep -q 'gateway.test/phase: job' "$all" || fail "les Jobs ne portent pas leur phase"

kubeconform -strict -summary -ignore-missing-schemas -kubernetes-version 1.31.0 "$all"
echo "check.sh: overlay de test conforme ($VERSION)"
