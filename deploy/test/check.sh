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
kubectl kustomize deploy/test | sed "s#go-gateway/test-env:v0.0.0\$#go-gateway/test-env:$VERSION#" >"$out/all.yaml"
all="$out/all.yaml"

fail() { echo "check.sh: $*" >&2; exit 1; }

grep -q 'ENVIRONMENT: staging' "$all" || fail "ENVIRONMENT n'est pas staging"
grep -q 'OTEL_SDK_DISABLED: "true"' "$all" || fail "le traçage n'est pas coupé"
grep -q 'KAFKA_BROKERS: redpanda:9092' "$all" || fail "KAFKA_BROKERS ne vise pas redpanda"
! grep -q 'SMPP_TRUSTED_PROXY_CIDRS' "$all" || fail "SMPP_TRUSTED_PROXY_CIDRS survit : tout bind serait refusé"
# access_management et le bloc grants de zz-grants.xml s'excluent : ClickHouse refuse alors de démarrer.
! grep -q 'CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT' "$all" || fail "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT empêcherait ClickHouse de démarrer"
grep -q 'value: smsc-simulator:2775' "$all" || fail "CONNECTOR_ADDR ne vise pas le simulateur"
grep -q 'name: HTTP_ADMIN_TOKENS' "$all" || fail "admin-api-svc sans HTTP_ADMIN_TOKENS"
! grep -q 'ghcr.io/martialanouman/go-gateway/[a-z0-9-]*:v0.0.0$' "$all" || fail "un gabarit v0.0.0 a survécu"
! grep -qE '^ +replicas: ([2-9]|[1-9][0-9]+)$' "$all" || fail "un Deployment garde plus d'une réplique"
! grep -qE '^ +cpu: ([1-9][0-9]*|[1-9][0-9]{2,}m)$' "$all" || fail "une requête CPU de production survit"
grep -q 'gateway.test/phase: job' "$all" || fail "les Jobs ne portent pas leur phase"
grep -q 'gateway.test/phase: seed$' "$all" || fail "pas de Job de seed"
grep -q 'gateway.test/phase: smoke$' "$all" || fail "pas de Job de smoke"
grep -q "go-gateway/test-env:$VERSION\$" "$all" || fail "l'image test-env ne porte pas la version déployée"
# item complet, pas une fenêtre de N lignes : un `value:` littéral ajouté avant `valueFrom` décale
# sinon `name: test-seed` hors d'un `-A` fixe, et la garde du dessous ne tombe plus jamais seule.
cid=$(awk '
  /^[[:space:]]*- name: CONNECTOR_ID$/ { match($0, /^[[:space:]]*/); indent = RLENGTH; print; found = 1; next }
  found {
    match($0, /^[[:space:]]*/); cur = RLENGTH
    if ($0 ~ /^[[:space:]]*- name:/ && cur <= indent) { exit }
    print
  }
' "$all")
grep -q 'name: test-seed' <<<"$cid" || fail "CONNECTOR_ID ne vient pas de la ConfigMap test-seed"
# Sans optional, le premier déploiement (ConfigMap pas encore créée par le seed) bloque
# connector-pool-svc en CreateContainerConfigError : le rollout attend 600 s et le seed ne part jamais.
grep -q 'optional: true' <<<"$cid" || fail "CONNECTOR_ID sans optional: true : le premier déploiement bloquerait avant le seed"
! grep -q 'value:' <<<"$cid" || fail "CONNECTOR_ID garde sa valeur de production à côté de valueFrom"

for dep in postgres redis redpanda clickhouse rustfs smsc-simulator; do
  grep -q "^  name: $dep$" "$all" || fail "dépendance absente : $dep"
done
grep -q 'gateway.test/phase: deps-job' "$all" || fail "le Job du bucket ne porte pas sa phase"
grep -B3 '^  name: gateway-config$' "$all" | grep -q 'gateway.test/phase: deps' \
  || fail "gateway-config n'est pas en phase deps : les Jobs (envFrom non optional) resteraient en CreateContainerConfigError"
# 6 Services, 5 StatefulSets, le Deployment du simulateur, le ConfigMap des droits ClickHouse, le
# ConfigMap gateway-config.
# Et le ConfigMap des journaux système ClickHouse (step-287b).
[[ $(grep -c 'gateway.test/phase: deps$' "$all") -eq 15 ]] || fail "une dépendance n'est pas en phase deps : elle partirait avec l'application"
# step-287g : un document par StatefulSet, son nom et sa place (sélecteur, tolérance) sur une ligne.
placed=$(awk '
  /^---/ { if (sts) print name, sel, tol; sts = 0; name = ""; sel = 0; tol = 0; next }
  /^kind: StatefulSet$/ { sts = 1 }
  /^  name: / && !name { name = $2 }
  /gateway.test\/role: deps/ { sel = 1 }
  /key: gateway.test\/role/ { tol = 1 }
  END { if (sts) print name, sel, tol }
' "$all")
for dep in postgres redpanda clickhouse rustfs; do
  grep -qx "$dep 1 1" <<<"$placed" || fail "$dep n'est pas épinglé au nœud de dépendances (sélecteur et tolérance)"
done
grep -qx "redis 0 0" <<<"$placed" || fail "redis quitte la passerelle : chaque message lui fait plusieurs allers-retours"

# shellcheck disable=SC2016 # backticks littéraux : la règle Traefik Host(`...`) rendue par kustomize
grep -q 'Host(`api-test.manouman.com`)' "$all" || fail "l'API REST n'est pas routée"
grep -q 'serverName: rest-api-svc' "$all" || fail "Traefik ne vérifie pas le certificat du backend"

kubeconform -strict -summary -ignore-missing-schemas -kubernetes-version 1.31.0 "$all"
echo "check.sh: overlay de test conforme ($VERSION)"
