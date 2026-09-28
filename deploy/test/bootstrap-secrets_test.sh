#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
openssl req -x509 -newkey rsa:2048 -nodes -subj /CN=origin -days 1 \
  -keyout "$tmp/o.key" -out "$tmp/o.crt" 2>/dev/null
XDG_CONFIG_HOME="$tmp/cfg" deploy/test/bootstrap-secrets.sh --print \
  --origin-cert "$tmp/o.crt" --origin-key "$tmp/o.key" >"$tmp/out.yaml"

fail() { echo "bootstrap-secrets_test: $*" >&2; exit 1; }
for s in gateway-secrets test-deps clickhouse-archive smsc-simulator-config api-origin-tls \
  billing-svc-tls content-key-svc-tls session-manager-svc-tls smpp-server-svc-tls \
  mo-dlr-router-svc-tls admin-api-svc-tls router-svc-tls connector-pool-svc-tls rest-api-svc-tls \
  operator-tls; do
  grep -q "^  name: $s$" "$tmp/out.yaml" || fail "secret absent : $s"
done
op=$(awk -v RS='---\n' 'index($0,"name: operator-tls")' "$tmp/out.yaml")
grep -q 'kind: Secret' <<<"$op" || fail "operator-tls n'est pas un Secret"
for k in ca.crt tls.crt tls.key; do
  grep -q "  $k: " <<<"$op" || fail "operator-tls sans $k"
done
for k in POSTGRES_URL REDIS_URL CLICKHOUSE_PASSWORD CONTENT_KMS_MASTER_KEY CONNECTOR_SYSTEM_ID \
  CONNECTOR_PASSWORD HTTP_ADMIN_TOKENS POSTGRES_PASSWORD REDIS_PASSWORD RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY; do
  grep -q "^  $k: " "$tmp/out.yaml" || fail "clé absente : $k"
done
pw=$(grep '^  CONNECTOR_PASSWORD: ' "$tmp/out.yaml" | awk '{print $2}' | base64 -d)
(( ${#pw} <= 8 )) || fail "CONNECTOR_PASSWORD fait ${#pw} caractères, SMPP en permet 8"
kms=$(grep '^  CONTENT_KMS_MASTER_KEY: ' "$tmp/out.yaml" | awk '{print $2}' | base64 -d | base64 -d | wc -c)
(( kms == 32 )) || fail "CONTENT_KMS_MASTER_KEY décode en $kms octets, 32 attendus"
[[ -f "$tmp/cfg/go-gateway-test/operator.crt" ]] || fail "certificat exploitant non conservé"
[[ -z $(git status --porcelain --ignored -- .tls) ]] || fail "des certificats ont atterri dans le dépôt"

# Panne transitoire SSH sur la vérification d'existence, puis apply qui "réussit" : un faux ssh qui
# n'échoue que sur le "get secret" reproduit exactement le scénario dangereux (panne lue comme
# absence, écrasement d'un environnement déjà initialisé) sans jamais toucher un vrai hôte.
fakebin="$tmp/fakebin"; mkdir -p "$fakebin"
cat >"$fakebin/ssh" <<'FAKESSH'
#!/usr/bin/env bash
case "$*" in
  *"get secret gateway-secrets"*) exit 255 ;;
esac
cat >/dev/null
exit 0
FAKESSH
chmod +x "$fakebin/ssh"

cfg2="$tmp/cfg2"
if PATH="$fakebin:$PATH" XDG_CONFIG_HOME="$cfg2" deploy/test/bootstrap-secrets.sh --host x \
     --origin-cert "$tmp/o.crt" --origin-key "$tmp/o.key" >/dev/null 2>&1; then
  fail "une panne SSH transitoire sur la vérification d'existence n'aurait pas dû laisser l'apply passer"
fi
[[ ! -f "$cfg2/go-gateway-test/admin-token" ]] || fail "accès exploitant écrits malgré une vérification d'existence en échec"

echo "bootstrap-secrets_test: ok"
