#!/usr/bin/env bash
# Génère tous les secrets de l'environnement de test sur le poste de l'exploitant et les applique par
# SSH. Une seule fois : Postgres fige son mot de passe à l'initdb, un second tirage le désynchronise.
set -euo pipefail

usage() { echo "usage: $0 (--host root@IP | --print) --origin-cert FILE --origin-key FILE" >&2; exit 2; }
host="" print=0 origin_cert="" origin_key=""
while (($#)); do
  case $1 in
    --host) host=$2; shift 2 ;;
    --print) print=1; shift ;;
    --origin-cert) origin_cert=$2; shift 2 ;;
    --origin-key) origin_key=$2; shift 2 ;;
    *) usage ;;
  esac
done
[[ -n $origin_cert && -n $origin_key ]] || usage
[[ $print == 1 || -n $host ]] || usage

cd "$(dirname "$0")/../.."
ns=gateway
keep="${XDG_CONFIG_HOME:-$HOME/.config}/go-gateway-test"
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT

if [[ $print == 0 ]] && ssh "$host" kubectl -n "$ns" get secret gateway-secrets >/dev/null 2>&1; then
  echo "$0: gateway-secrets existe déjà sur $host — refus de régénérer" >&2
  exit 1
fi

hex() { openssl rand -hex "$1"; }
pg_pw=$(hex 24) redis_pw=$(hex 24) ch_pw=$(hex 24)
s3_key=$(hex 10) s3_secret=$(hex 24)
bind_id=gateway bind_pw=$(hex 4)
admin_token=$(hex 32)
kms=$(openssl rand -base64 32)

svcs=billing-svc,content-key-svc,session-manager-svc,smpp-server-svc,mo-dlr-router-svc,admin-api-svc,router-svc,connector-pool-svc,rest-api-svc
go run ./test/tlsgen -out "$work/tls" -ns "$ns" -services "$svcs,operator" >/dev/null

cat >"$work/named_collections.xml" <<EOF
<clickhouse>
  <named_collections>
    <cdr_archive>
      <url>http://rustfs:9000/cdr-archive/</url>
      <access_key_id>$s3_key</access_key_id>
      <secret_access_key>$s3_secret</secret_access_key>
    </cdr_archive>
  </named_collections>
</clickhouse>
EOF

cat >"$work/config.yml" <<EOF
observability:
  http_port: 9000
virtual_smscs:
  - name: carrier
    port: 2775
    bind_credentials: { system_id: "$bind_id", password: "$bind_pw" }
    addr_ton: 1
    addr_npi: 1
    address_range: ".*"
    tls: { enabled: false }
    seed: 42
    pdu_buffer_size: 10000
    scenario:
      profile: healthy
      latency: { distribution: fixed, params: { ms: 5 } }
EOF

k() { kubectl -n "$ns" create "$@" --dry-run=client -o yaml; echo "---"; }
{
  k secret generic gateway-secrets \
    --from-literal=POSTGRES_URL="postgres://gateway:$pg_pw@postgres:5432/gateway?sslmode=disable" \
    --from-literal=REDIS_URL="redis://:$redis_pw@redis:6379/0" \
    --from-literal=CLICKHOUSE_PASSWORD="$ch_pw" \
    --from-literal=CONTENT_KMS_MASTER_KEY="$kms" \
    --from-literal=CONNECTOR_SYSTEM_ID="$bind_id" \
    --from-literal=CONNECTOR_PASSWORD="$bind_pw" \
    --from-literal=HTTP_ADMIN_TOKENS="$admin_token:admin:read|admin:write|content:read|audit:read"
  k secret generic test-deps \
    --from-literal=POSTGRES_PASSWORD="$pg_pw" \
    --from-literal=REDIS_PASSWORD="$redis_pw" \
    --from-literal=RUSTFS_ACCESS_KEY="$s3_key" \
    --from-literal=RUSTFS_SECRET_KEY="$s3_secret"
  k secret generic clickhouse-archive --from-file=named_collections.xml="$work/named_collections.xml"
  k secret generic smsc-simulator-config --from-file=config.yml="$work/config.yml"
  k secret tls api-origin-tls --cert="$origin_cert" --key="$origin_key"
  for svc in ${svcs//,/ }; do
    k secret generic "$svc-tls" --from-file=tls.crt="$work/tls/$svc.crt" \
      --from-file=tls.key="$work/tls/$svc.key" --from-file=ca.crt="$work/tls/ca.crt"
  done
} >"$work/secrets.yaml"

mkdir -p "$keep"; chmod 700 "$keep"
cp "$work/tls/ca.crt" "$work/tls/operator.crt" "$work/tls/operator.key" "$keep/"
printf '%s\n' "$admin_token" >"$keep/admin-token"
printf 'system_id=%s\npassword=%s\n' "$bind_id" "$bind_pw" >"$keep/bind-credentials"
chmod 600 "$keep"/*

if [[ $print == 1 ]]; then
  cat "$work/secrets.yaml"
else
  ssh "$host" kubectl apply -f - <"$work/secrets.yaml"
  echo "$0: secrets appliqués ; accès exploitant dans $keep"
fi
