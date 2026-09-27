# Environnement de test k3s et pipeline CD — plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal :** chaque merge vert sur `main` se déploie automatiquement sur un VPS Contabo sous k3s, en rejouant
les manifests de production `deploy/k8s` via un overlay kustomize de test.

**Architecture :** un overlay `deploy/test/` (kustomize intégré à `kubectl`) prend comme base la sortie de
`scripts/render-manifests.sh`, ajoute les dépendances en StatefulSets, et patche ce qui ne tient pas sur
un nœud. Un workflow GitHub Actions build les images en snapshot GoReleaser, les pousse sous
`v0.0.0-sha-<12 hex>`, rend le YAML et le passe par SSH à un script hôte à commande forcée qui
l'applique par phases étiquetées.

**Tech Stack :** k3s (Traefik v3, ServiceLB, local-path), kustomize v5 (via `kubectl kustomize`),
GoReleaser v2 (`dockers_v2`), GitHub Actions, bash, Cloudflare (DNS + Origin CA).

**Spec :** `docs/superpowers/specs/2026-09-27-environnement-de-test-k3s-design.md`

## Global Constraints

- `deploy/k8s/` n'est **pas modifié**. Tout ce qui est propre au test vit sous `deploy/test/`.
- `scripts/render-manifests.sh` n'est **pas modifié** ; le tag de test doit passer sa regex
  `^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$`.
- Tag des images de test : `v0.0.0-sha-<12 premiers hex du commit>` — un seul identifiant de pré-version,
  jamais purement numérique (SemVer interdit un identifiant numérique à zéro de tête).
- Aucun secret dans git. Les secrets sont générés sur le poste de l'exploitant et appliqués par SSH.
- Namespace : `gateway`. `ENVIRONMENT=staging`.
- Mot de passe de bind SMPP ≤ 8 caractères, `system_id` ≤ 15 (mémoire *smpp-bind-field-length-limits*).
- Commentaires : zéro par défaut, seulement le *pourquoi* non évident (CLAUDE.md).
- Chaque script bash passe `shellcheck` ; le workflow passe `actionlint`.

## Faits établis pendant la préparation (ne pas re-vérifier)

- admin-api-svc démarre hors production avec `OIDC_*` vides et utilise alors `HTTP_ADMIN_TOKENS`
  (`token:scope|scope`, séparés par des virgules) — `cmd/admin-api-svc/wiring.go:472-477`,
  `internal/config/config.go:1422`. Avec `TLS_ENABLED=true`, l'API Admin exige un **certificat client**
  signé par la CA de la passerelle.
- `OTEL_SDK_DISABLED=true` coupe le traçage (`internal/config/config.go:120-135`,
  `internal/observability/tracing.go:63`).
- `test/tlsgen -out DIR -ns gateway -services a,b,…` écrit `ca.crt`, `<svc>.crt`, `<svc>.key` ; SAN
  `<svc>` et `<svc>.gateway.svc`. **Neuf** `Secret` `<svc>-tls` (clés `tls.crt`, `tls.key`, `ca.crt`) :
  billing, content-key, session-manager, smpp-server, mo-dlr-router, admin-api, router, connector-pool,
  rest-api (`deploy/k8s/tls/README.md`).
- `internal/deploy` et `make manifests` ne scannent que `deploy/k8s` : `deploy/test/` n'est pas vu.
- GoReleaser `--snapshot` construit les images `dockers_v2` **sans les pousser**, une par plateforme,
  suffixée : `<image>:<tag>-amd64`. `{{ .Tag }}` suit `GORELEASER_CURRENT_TAG`.
- `smpp-server-svc` porte `SMPP_TRUSTED_PROXY_CIDRS=10.0.0.0/8` : derrière le ServiceLB de k3s (pas
  d'en-tête PROXY, source en 10.x), **tout bind échouerait**. L'overlay retire la variable.
- Le port SMPP public est en **TLS** (`TLS_ENABLED=true`, certificat tlsgen, SAN `smpp-server-svc`).
- rest-api-svc sert en TLS public (sans certificat client) sur 8080 : Traefik doit parler HTTPS au
  backend avec la CA de la passerelle.
- `CONNECTOR_ID` doit égaler l'id d'une ligne `smsc_connectors` visée par une route, sinon le
  connector-pool ignore tout. Aucun mécanisme de seed n'existe — objet de **step-275** (Task 7).
- Les Deployments portent `app.kubernetes.io/part-of: go-gateway` ; un seul conteneur, à l'index 0.
- ClickHouse : l'archive (step-407) passe par la collection nommée `cdr_archive` du serveur
  (`internal/storage/clickhouse/testdata/s3archive/`). RustFS `rustfs/rustfs:1.0.0`, client `mc`
  `cgr.dev/chainguard/minio-client` (digests dans `archive_s3_integration_test.go:20-21`).

## Carte des fichiers

| Fichier | Rôle |
|---|---|
| `deploy/test/kustomization.yaml` | l'overlay : ressources, patches, étiquettes de phase |
| `deploy/test/patches/*.yaml` | ConfigMap, réduction d'échelle, HPA, smpp-server, connector-pool, admin-api, phase des Jobs |
| `deploy/test/gateway-oidc.yaml` | ConfigMap OIDC vide (hors production : jetons statiques) |
| `deploy/test/deps/*.yaml` | Postgres, Redis, Redpanda, ClickHouse, RustFS (+ Job du bucket), smsc-simulator |
| `deploy/test/ingress.yaml` | IngressRoute + ServersTransport Traefik vers rest-api-svc |
| `deploy/test/check.sh` | rend l'overlay avec un tag fictif et vérifie ses invariants (la garde) |
| `deploy/test/host/gateway-deploy` | script hôte à commande forcée : applique par phases |
| `deploy/test/host/gateway-deploy_test.sh` | test du script avec un `kubectl` factice |
| `deploy/test/host/install.sh` | préparation de l'hôte (root, une fois) |
| `deploy/test/bootstrap-secrets.sh` | génère et applique tous les secrets depuis le poste |
| `deploy/test/README.md` | runbook |
| `.github/workflows/deploy-test.yml` | la CD |
| `Makefile`, `.github/workflows/ci.yml`, `.gitignore` | cible `test-env`, job CI, `deploy/test/rendered/` |
| `tasks-todo/step-275.md`, `tasks-todo/INDEX.md` | seed + preuve bout-en-bout, reportés |
| `debts/api-de-test-joignable-hors-cloudflare.md` | dette |

## Étiquettes de phase (interface entre overlay et script hôte)

Le script hôte sélectionne par l'étiquette `gateway.test/phase` :

| Valeur | Objets | Traitement |
|---|---|---|
| `deps` | dépendances (StatefulSets, Deployment du simulateur, Services, ConfigMaps) | `apply`, puis `rollout status` |
| `deps-job` | Job `rustfs-bucket` | `delete`, `apply`, `wait complete` |
| `job` | `migrate-postgres`, `migrate-clickhouse`, `kafka-provision` | `delete`, `apply`, `wait complete` |
| *(absente)* | tout le reste (application) | `apply` avec `-l '!gateway.test/phase'`, puis `rollout status` |

Les objets `deps` portent aussi `app.kubernetes.io/part-of: test-deps` — c'est ce qui les exclut du
patch de réduction d'échelle (ciblé sur `part-of=go-gateway`).

---

### Task 1 : La garde de l'overlay, puis l'overlay applicatif

**Files :**
- Create : `deploy/test/check.sh`, `deploy/test/kustomization.yaml`, `deploy/test/gateway-oidc.yaml`,
  `deploy/test/patches/gateway-config.yaml`, `deploy/test/patches/scale-down.yaml`,
  `deploy/test/patches/hpa.yaml`, `deploy/test/patches/smpp-server-svc.yaml`,
  `deploy/test/patches/connector-pool-svc.yaml`, `deploy/test/patches/admin-api-svc.yaml`,
  `deploy/test/patches/job-phase.yaml`
- Modify : `.gitignore`, `Makefile` (cible `test-env` près de `manifests:` ligne ~236)

**Interfaces :**
- Produces : `deploy/test/check.sh [VERSION]` → écrit le rendu dans `deploy/test/rendered/all.yaml` et
  sort non nul au premier invariant violé. `make test-env`. Le chemin `deploy/test/rendered/all.yaml`
  est consommé par Task 5.

- [ ] **Step 1 : écrire la garde (le test)**

`deploy/test/check.sh` :

```bash
#!/usr/bin/env bash
# Rend l'overlay de test avec un tag fictif et vérifie ce qui, s'il cassait, ne se verrait qu'au
# déploiement : un bind SMPP refusé, un pod Pending faute de CPU, une image jamais substituée.
set -euo pipefail

VERSION="${1:-v0.0.0-sha-000000000000}"
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
! grep -qE '^ +cpu: ([1-9][0-9]*|[1-9][0-9]{3,}m)$' "$all" || fail "une requête CPU de production survit"
grep -q 'gateway.test/phase: job' "$all" || fail "les Jobs ne portent pas leur phase"

kubeconform -strict -summary -ignore-missing-schemas -kubernetes-version 1.31.0 "$all"
echo "check.sh: overlay de test conforme ($VERSION)"
```

`chmod +x deploy/test/check.sh`. Ajouter à `.gitignore`, après le bloc GoReleaser :

```gitignore
# Rendu de l'overlay de test (deploy/test/check.sh) : dérivé, jamais versionné.
/deploy/test/rendered/
```

Ajouter au `Makefile`, juste après la cible `manifests` :

```makefile
.PHONY: test-env
test-env: ## Render the k3s test overlay (deploy/test) and check its invariants (kubectl + kubeconform)
	deploy/test/check.sh
```

- [ ] **Step 2 : lancer la garde, constater l'échec**

Run : `make test-env`
Expected : FAIL — `kubectl kustomize` ne trouve pas `deploy/test/kustomization.yaml`.

- [ ] **Step 3 : écrire l'overlay applicatif**

`deploy/test/kustomization.yaml` :

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: gateway
resources:
  - rendered/base.yaml
  - gateway-oidc.yaml
patches:
  - path: patches/gateway-config.yaml
  - path: patches/smpp-server-svc.yaml
  - path: patches/connector-pool-svc.yaml
  - path: patches/admin-api-svc.yaml
  - path: patches/scale-down.yaml
    target:
      kind: Deployment
      labelSelector: app.kubernetes.io/part-of=go-gateway
  - path: patches/hpa.yaml
    target:
      kind: HorizontalPodAutoscaler
  - path: patches/job-phase.yaml
    target:
      kind: Job
      labelSelector: app.kubernetes.io/part-of=go-gateway
```

`deploy/test/gateway-oidc.yaml` :

```yaml
# Vide exprès : hors production, admin-api-svc retombe sur les jetons statiques HTTP_ADMIN_TOKENS
# (cmd/admin-api-svc/wiring.go). Le ConfigMap doit exister : le Deployment le lit sans optional.
apiVersion: v1
kind: ConfigMap
metadata:
  name: gateway-oidc
data:
  OIDC_ISSUER: ""
  OIDC_AUDIENCE: ""
  OIDC_JWKS_URL: ""
```

`deploy/test/patches/gateway-config.yaml` :

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: gateway-config
data:
  ENVIRONMENT: staging
  KAFKA_BROKERS: redpanda:9092
  KAFKA_TOPIC_REPLICATION_FACTOR: "1"
  OTEL_SDK_DISABLED: "true"
```

`deploy/test/patches/scale-down.yaml` :

```yaml
# Les requêtes de production totalisent ~21,5 vCPU : sur un nœud de 8, la moitié des pods resterait
# Pending. Les limites sont gardées.
- op: replace
  path: /spec/replicas
  value: 1
- op: replace
  path: /spec/template/spec/containers/0/resources/requests
  value:
    cpu: 50m
    memory: 64Mi
```

`deploy/test/patches/hpa.yaml` :

```yaml
- op: replace
  path: /spec/minReplicas
  value: 1
- op: replace
  path: /spec/maxReplicas
  value: 2
```

`deploy/test/patches/job-phase.yaml` :

```yaml
- op: add
  path: /metadata/labels/gateway.test~1phase
  value: job
```

`deploy/test/patches/smpp-server-svc.yaml` :

```yaml
# Le ServiceLB de k3s n'envoie pas d'en-tête PROXY et la source arrive en 10.x : avec la liste de
# confiance de production, chaque bind serait refusé (internal/smppserver/proxyproto.go).
apiVersion: apps/v1
kind: Deployment
metadata:
  name: smpp-server-svc
spec:
  template:
    spec:
      containers:
        - name: smpp-server-svc
          env:
            - name: SMPP_TRUSTED_PROXY_CIDRS
              $patch: delete
```

`deploy/test/patches/connector-pool-svc.yaml` :

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: connector-pool-svc
spec:
  template:
    spec:
      containers:
        - name: connector-pool-svc
          env:
            - name: CONNECTOR_ADDR
              value: smsc-simulator:2775
```

`deploy/test/patches/admin-api-svc.yaml` :

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: admin-api-svc
spec:
  template:
    spec:
      containers:
        - name: admin-api-svc
          env:
            - name: HTTP_ADMIN_TOKENS
              valueFrom:
                secretKeyRef:
                  name: gateway-secrets
                  key: HTTP_ADMIN_TOKENS
```

- [ ] **Step 4 : relancer la garde**

Run : `make test-env`
Expected : `check.sh: overlay de test conforme (v0.0.0-sha-000000000000)` et un résumé kubeconform
sans `Invalid` ni `Errors`. Si kustomize refuse les documents vides que `render-manifests.sh` émet
(lignes `---` en tête), filtrer dans `check.sh` avant l'écriture de `base.yaml` et relancer.

- [ ] **Step 5 : prouver que la garde mord (mémoire *hollow-test-fixtures*)**

Pour chacune des trois mutations, `cp` le fichier avant, muter, `make test-env` doit échouer avec le
message attendu, puis restaurer depuis la copie :
1. supprimer le bloc `env` de `patches/smpp-server-svc.yaml` → `SMPP_TRUSTED_PROXY_CIDRS survit`
2. retirer l'entrée `scale-down.yaml` de `kustomization.yaml` → `garde plus d'une réplique`
3. retirer `ENVIRONMENT: staging` du patch ConfigMap → `ENVIRONMENT n'est pas staging`

- [ ] **Step 6 : commit**

```bash
git add deploy/test .gitignore Makefile
git commit -m "feat(deploy): overlay k3s de test et sa garde de rendu"
```

---

### Task 2 : Les dépendances

**Files :**
- Create : `deploy/test/deps/postgres.yaml`, `deploy/test/deps/redis.yaml`,
  `deploy/test/deps/redpanda.yaml`, `deploy/test/deps/clickhouse.yaml`, `deploy/test/deps/rustfs.yaml`,
  `deploy/test/deps/smsc-simulator.yaml`
- Modify : `deploy/test/kustomization.yaml` (resources), `deploy/test/check.sh` (assertions)

**Interfaces :**
- Consumes : secrets créés par Task 4 — `gateway-secrets` (`CLICKHOUSE_PASSWORD`), `test-deps`
  (`POSTGRES_PASSWORD`, `REDIS_PASSWORD`, `RUSTFS_ACCESS_KEY`, `RUSTFS_SECRET_KEY`),
  `clickhouse-archive` (clé `named_collections.xml`), `smsc-simulator-config` (clé `config.yml`).
- Produces : Services `postgres:5432`, `redis:6379`, `redpanda:9092`, `clickhouse:9000`,
  `rustfs:9000`, `smsc-simulator:2775` — les adresses que Task 4 met dans `gateway-secrets`.

- [ ] **Step 1 : étendre la garde**

Ajouter dans `check.sh`, avant `kubeconform` :

```bash
for dep in postgres redis redpanda clickhouse rustfs smsc-simulator; do
  grep -q "^  name: $dep$" "$all" || fail "dépendance absente : $dep"
done
grep -q 'gateway.test/phase: deps-job' "$all" || fail "le Job du bucket ne porte pas sa phase"
# 6 Services, 5 StatefulSets, le Deployment du simulateur, le ConfigMap des droits ClickHouse.
[[ $(grep -c 'gateway.test/phase: deps$' "$all") -eq 13 ]] || fail "une dépendance n'est pas en phase deps : elle partirait avec l'application"
```

- [ ] **Step 2 : constater l'échec**

Run : `make test-env` → Expected : FAIL `dépendance absente : postgres`.

- [ ] **Step 3 : écrire les dépendances**

`deploy/test/deps/postgres.yaml` :

```yaml
apiVersion: v1
kind: Service
metadata:
  name: postgres
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  selector: {app: postgres}
  ports: [{name: postgres, port: 5432}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  serviceName: postgres
  replicas: 1
  selector: {matchLabels: {app: postgres}}
  template:
    metadata:
      labels: {app: postgres, app.kubernetes.io/part-of: test-deps}
    spec:
      containers:
        - name: postgres
          image: postgres:18-alpine
          env:
            - {name: POSTGRES_USER, value: gateway}
            - {name: POSTGRES_DB, value: gateway}
            - name: POSTGRES_PASSWORD
              valueFrom: {secretKeyRef: {name: test-deps, key: POSTGRES_PASSWORD}}
          ports: [{containerPort: 5432}]
          readinessProbe:
            exec: {command: [pg_isready, -U, gateway, -d, gateway]}
            periodSeconds: 5
          volumeMounts: [{name: data, mountPath: /var/lib/postgresql}]
  volumeClaimTemplates:
    - metadata: {name: data}
      spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 10Gi}}}
```

`deploy/test/deps/redis.yaml` :

```yaml
apiVersion: v1
kind: Service
metadata:
  name: redis
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  selector: {app: redis}
  ports: [{name: redis, port: 6379}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  serviceName: redis
  replicas: 1
  selector: {matchLabels: {app: redis}}
  template:
    metadata:
      labels: {app: redis, app.kubernetes.io/part-of: test-deps}
    spec:
      containers:
        - name: redis
          image: redis:7-alpine
          env:
            - name: REDIS_PASSWORD
              valueFrom: {secretKeyRef: {name: test-deps, key: REDIS_PASSWORD}}
          args: [redis-server, --appendonly, "yes", --requirepass, $(REDIS_PASSWORD)]
          ports: [{containerPort: 6379}]
          readinessProbe:
            exec: {command: [sh, -c, 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning ping | grep -q PONG']}
            periodSeconds: 5
          volumeMounts: [{name: data, mountPath: /data}]
  volumeClaimTemplates:
    - metadata: {name: data}
      spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 2Gi}}}
```

`deploy/test/deps/redpanda.yaml` (flags repris de `docker-compose.yml`, adresse annoncée = le Service) :

```yaml
apiVersion: v1
kind: Service
metadata:
  name: redpanda
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  selector: {app: redpanda}
  ports: [{name: kafka, port: 9092}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redpanda
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  serviceName: redpanda
  replicas: 1
  selector: {matchLabels: {app: redpanda}}
  template:
    metadata:
      labels: {app: redpanda, app.kubernetes.io/part-of: test-deps}
    spec:
      containers:
        - name: redpanda
          image: redpandadata/redpanda:v24.2.18
          args:
            - redpanda
            - start
            - --mode=dev-container
            - --smp=1
            - --memory=1G
            - --overprovisioned
            - --kafka-addr=PLAINTEXT://0.0.0.0:9092
            - --advertise-kafka-addr=PLAINTEXT://redpanda:9092
          ports: [{containerPort: 9092}]
          readinessProbe:
            exec: {command: [sh, -c, "rpk cluster health | grep -q 'Healthy:.*true'"]}
            periodSeconds: 5
            timeoutSeconds: 5
          volumeMounts: [{name: data, mountPath: /var/lib/redpanda/data}]
  volumeClaimTemplates:
    - metadata: {name: data}
      spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 20Gi}}}
```

`deploy/test/deps/clickhouse.yaml` (droits repris de `testdata/s3archive/grants.xml`) :

```yaml
apiVersion: v1
kind: Service
metadata:
  name: clickhouse
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  selector: {app: clickhouse}
  ports:
    - {name: native, port: 9000}
    - {name: http, port: 8123}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: clickhouse-grants
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
data:
  zz-grants.xml: |
    <clickhouse>
      <users>
        <gateway>
          <grants>
            <query>GRANT ALL ON *.*</query>
          </grants>
        </gateway>
      </users>
    </clickhouse>
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: clickhouse
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  serviceName: clickhouse
  replicas: 1
  selector: {matchLabels: {app: clickhouse}}
  template:
    metadata:
      labels: {app: clickhouse, app.kubernetes.io/part-of: test-deps}
    spec:
      containers:
        - name: clickhouse
          image: clickhouse/clickhouse-server:24.8-alpine
          env:
            - {name: CLICKHOUSE_USER, value: gateway}
            - {name: CLICKHOUSE_DB, value: gateway}
            - {name: CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT, value: "1"}
            - name: CLICKHOUSE_PASSWORD
              valueFrom: {secretKeyRef: {name: gateway-secrets, key: CLICKHOUSE_PASSWORD}}
          ports: [{containerPort: 9000}, {containerPort: 8123}]
          readinessProbe:
            httpGet: {path: /ping, port: 8123}
            periodSeconds: 5
          volumeMounts:
            - {name: data, mountPath: /var/lib/clickhouse}
            - {name: archive, mountPath: /etc/clickhouse-server/config.d/named_collections.xml, subPath: named_collections.xml}
            - {name: grants, mountPath: /etc/clickhouse-server/users.d/zz-grants.xml, subPath: zz-grants.xml}
      volumes:
        - name: archive
          secret: {secretName: clickhouse-archive}
        - name: grants
          configMap: {name: clickhouse-grants}
  volumeClaimTemplates:
    - metadata: {name: data}
      spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 20Gi}}}
```

`deploy/test/deps/rustfs.yaml` :

```yaml
apiVersion: v1
kind: Service
metadata:
  name: rustfs
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  selector: {app: rustfs}
  ports: [{name: s3, port: 9000}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rustfs
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  serviceName: rustfs
  replicas: 1
  selector: {matchLabels: {app: rustfs}}
  template:
    metadata:
      labels: {app: rustfs, app.kubernetes.io/part-of: test-deps}
    spec:
      containers:
        - name: rustfs
          image: rustfs/rustfs:1.0.0@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff
          env:
            - name: RUSTFS_ACCESS_KEY
              valueFrom: {secretKeyRef: {name: test-deps, key: RUSTFS_ACCESS_KEY}}
            - name: RUSTFS_SECRET_KEY
              valueFrom: {secretKeyRef: {name: test-deps, key: RUSTFS_SECRET_KEY}}
          ports: [{containerPort: 9000}]
          readinessProbe:
            tcpSocket: {port: 9000}
            periodSeconds: 5
          volumeMounts: [{name: data, mountPath: /data}]
  volumeClaimTemplates:
    - metadata: {name: data}
      spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 20Gi}}}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: rustfs-bucket
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps-job}
spec:
  backoffLimit: 4
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: mc
          image: cgr.dev/chainguard/minio-client@sha256:b2bd7824d23d3e3b15bedd7e87fbc3be29d2e213307b4f901e4a1d92356dc20f
          args: [mb, --ignore-existing, root/cdr-archive]
          env:
            - name: RUSTFS_ACCESS_KEY
              valueFrom: {secretKeyRef: {name: test-deps, key: RUSTFS_ACCESS_KEY}}
            - name: RUSTFS_SECRET_KEY
              valueFrom: {secretKeyRef: {name: test-deps, key: RUSTFS_SECRET_KEY}}
            - name: MC_HOST_root
              value: http://$(RUSTFS_ACCESS_KEY):$(RUSTFS_SECRET_KEY)@rustfs:9000
```

`deploy/test/deps/smsc-simulator.yaml` (le tag suit la CI de `go-smsc-simulator` ; confirmer le nom
publié avant le premier déploiement) :

```yaml
apiVersion: v1
kind: Service
metadata:
  name: smsc-simulator
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  selector: {app: smsc-simulator}
  ports: [{name: smpp, port: 2775}]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: smsc-simulator
  labels: {app.kubernetes.io/part-of: test-deps, gateway.test/phase: deps}
spec:
  replicas: 1
  selector: {matchLabels: {app: smsc-simulator}}
  template:
    metadata:
      labels: {app: smsc-simulator, app.kubernetes.io/part-of: test-deps}
    spec:
      containers:
        - name: smsc-simulator
          image: ghcr.io/martialanouman/go-smsc-simulator:v0.7.0
          ports: [{containerPort: 2775}, {containerPort: 9000}]
          readinessProbe:
            httpGet: {path: /health, port: 9000}
            periodSeconds: 5
          volumeMounts:
            - {name: config, mountPath: /etc/smsc/config.yml, subPath: config.yml}
      volumes:
        - name: config
          secret: {secretName: smsc-simulator-config}
```

Dans `kustomization.yaml`, ajouter sous `resources:` :

```yaml
  - deps/postgres.yaml
  - deps/redis.yaml
  - deps/redpanda.yaml
  - deps/clickhouse.yaml
  - deps/rustfs.yaml
  - deps/smsc-simulator.yaml
```

- [ ] **Step 4 : relancer la garde** → `make test-env` : conforme.

- [ ] **Step 5 : mutation** — `cp`, retirer `gateway.test/phase: deps` des métadonnées du StatefulSet
  `postgres` : la garde doit échouer (`une dépendance n'est pas en phase deps`). Restaurer.

- [ ] **Step 6 : commit**

```bash
git add deploy/test
git commit -m "feat(deploy): dépendances de l'environnement de test (Postgres, Redis, Redpanda, ClickHouse, RustFS, simulateur SMSC)"
```

---

### Task 3 : L'API REST derrière Traefik et Cloudflare

**Files :**
- Create : `deploy/test/ingress.yaml`
- Modify : `deploy/test/kustomization.yaml`, `deploy/test/check.sh`

**Interfaces :**
- Consumes : `Secret` `api-origin-tls` (Task 4 : certificat Origin CA, clés `tls.crt`/`tls.key`) et
  `rest-api-svc-tls` (clé `ca.crt`, lue par Traefik pour vérifier le backend).

- [ ] **Step 1 : étendre la garde**

```bash
grep -q 'Host(`api.test.manouman.com`)' "$all" || fail "l'API REST n'est pas routée"
grep -q 'serverName: rest-api-svc' "$all" || fail "Traefik ne vérifie pas le certificat du backend"
```

- [ ] **Step 2 : constater l'échec** → `make test-env` : FAIL `l'API REST n'est pas routée`.

- [ ] **Step 3 : écrire l'ingress**

`deploy/test/ingress.yaml` :

```yaml
# rest-api-svc sert en TLS sous la CA de la passerelle (tlsgen) : Traefik doit la connaître pour
# vérifier le backend plutôt que d'ignorer son certificat.
apiVersion: traefik.io/v1alpha1
kind: ServersTransport
metadata:
  name: rest-api-backend
spec:
  serverName: rest-api-svc
  rootCAs:
    - secret: rest-api-svc-tls
---
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: rest-api
spec:
  entryPoints: [websecure]
  routes:
    - kind: Rule
      match: Host(`api.test.manouman.com`)
      services:
        - kind: Service
          name: rest-api-svc
          port: 8080
          scheme: https
          serversTransport: rest-api-backend
  tls:
    secretName: api-origin-tls
```

Ajouter `- ingress.yaml` sous `resources:`.

- [ ] **Step 4 : relancer la garde** → conforme (kubeconform ignore les CRD Traefik faute de schéma :
  c'est le rôle de `-ignore-missing-schemas`, et la vraie validation est l'`apply` de Task 8).

- [ ] **Step 5 : commit**

```bash
git add deploy/test
git commit -m "feat(deploy): l'API REST de test derrière Traefik, backend vérifié par la CA de la passerelle"
```

---

### Task 4 : Amorçage des secrets depuis le poste de l'exploitant

**Files :**
- Create : `deploy/test/bootstrap-secrets.sh`

**Interfaces :**
- Produces : `deploy/test/bootstrap-secrets.sh --print --origin-cert F --origin-key F` (YAML sur
  stdout, rien d'appliqué) et `deploy/test/bootstrap-secrets.sh --host root@IP --origin-cert F
  --origin-key F` (applique par SSH, refuse si `gateway-secrets` existe). Écrit les accès exploitant
  dans `${XDG_CONFIG_HOME:-$HOME/.config}/go-gateway-test/` (0700) : `ca.crt`, `operator.crt`,
  `operator.key`, `admin-token`, `bind-credentials`.
- Secrets créés : `gateway-secrets` (`POSTGRES_URL`, `REDIS_URL`, `CLICKHOUSE_PASSWORD`,
  `CONTENT_KMS_MASTER_KEY`, `CONNECTOR_SYSTEM_ID`, `CONNECTOR_PASSWORD`, `HTTP_ADMIN_TOKENS`),
  `test-deps`, `clickhouse-archive`, `smsc-simulator-config`, neuf `<svc>-tls`, `api-origin-tls`.

- [ ] **Step 1 : écrire le test**

`deploy/test/bootstrap-secrets_test.sh` :

```bash
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
  mo-dlr-router-svc-tls admin-api-svc-tls router-svc-tls connector-pool-svc-tls rest-api-svc-tls; do
  grep -q "^  name: $s$" "$tmp/out.yaml" || fail "secret absent : $s"
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
echo "bootstrap-secrets_test: ok"
```

Ajouter au `Makefile` sous `test-env` (même cible, deuxième ligne) : `deploy/test/bootstrap-secrets_test.sh`.

- [ ] **Step 2 : constater l'échec** → `make test-env` : FAIL (script absent).

- [ ] **Step 3 : écrire le script**

`deploy/test/bootstrap-secrets.sh` :

```bash
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
```

`chmod +x deploy/test/bootstrap-secrets.sh deploy/test/bootstrap-secrets_test.sh`.

- [ ] **Step 4 : relancer** → `make test-env` : `bootstrap-secrets_test: ok`.

- [ ] **Step 5 : mutation** — `cp`, remplacer `bind_pw=$(hex 4)` par `bind_pw=$(hex 5)` : le test doit
  échouer sur la borne de 8. Restaurer.

- [ ] **Step 6 : shellcheck et commit**

```bash
shellcheck deploy/test/*.sh
git add deploy/test Makefile
git commit -m "feat(deploy): amorçage des secrets de l'environnement de test depuis le poste de l'exploitant"
```

---

### Task 5 : Le script hôte `gateway-deploy`

**Files :**
- Create : `deploy/test/host/gateway-deploy`, `deploy/test/host/gateway-deploy_test.sh`
- Modify : `Makefile` (`test-env` : troisième ligne `deploy/test/host/gateway-deploy_test.sh`)

**Interfaces :**
- Consumes : sur stdin, `deploy/test/rendered/all.yaml` (Task 1) ; les étiquettes de phase.
- Produces : code de sortie 0 si et seulement si deps prêtes, Jobs `Complete`, Deployments déployés.

- [ ] **Step 1 : écrire le test**

`deploy/test/host/gateway-deploy_test.sh` :

```bash
#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
cat >"$tmp/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
echo "$*" >>"$KUBECTL_LOG"
case "$*" in
  *"get statefulset,deployment"*) echo statefulset.apps/postgres ;;
  *"get deployment"*) echo deployment.apps/router-svc ;;
  *"wait"*) [[ -n ${FAIL_WAIT:-} ]] && exit 1 ;;
esac
exit 0
EOF
chmod +x "$tmp/bin/kubectl"
fail() { echo "gateway-deploy_test: $*" >&2; exit 1; }

export KUBECTL_LOG="$tmp/log"
echo 'kind: ConfigMap' | PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null
order=$(grep -nE 'apply .*phase=deps$|delete job -l gateway.test/phase=deps-job|delete job -l gateway.test/phase=job|apply .*!gateway.test/phase|rollout status deployment.apps/router-svc' "$KUBECTL_LOG" | cut -d: -f1 | tr '\n' ' ')
[[ $(wc -w <<<"$order") == 5 ]] || fail "étapes manquantes : $(cat "$KUBECTL_LOG")"
[[ $order == "$(tr ' ' '\n' <<<"$order" | sort -n | tr '\n' ' ')" ]] || fail "ordre violé : $(cat "$KUBECTL_LOG")"

: >"$KUBECTL_LOG"
if echo 'kind: ConfigMap' | FAIL_WAIT=1 PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null 2>&1; then
  fail "un Job en échec n'arrête pas le déploiement"
fi
! grep -q '!gateway.test/phase' "$KUBECTL_LOG" || fail "l'application est appliquée malgré un Job en échec"
echo "gateway-deploy_test: ok"
```

- [ ] **Step 2 : constater l'échec** → `deploy/test/host/gateway-deploy_test.sh` : FAIL (script absent).

- [ ] **Step 3 : écrire le script**

`deploy/test/host/gateway-deploy` :

```bash
#!/usr/bin/env bash
# Commande forcée de la clé de CD (authorized_keys) : lit le YAML rendu sur stdin et l'applique par
# phases. Un Job est supprimé avant d'être réappliqué : son template est immuable.
set -euo pipefail

ns=gateway
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
cat >"$work/all.yaml"
k() { kubectl -n "$ns" "$@"; }

run_jobs() {
  k delete job -l "gateway.test/phase=$1" --ignore-not-found --wait=true
  k apply -f "$work/all.yaml" -l "gateway.test/phase=$1"
  k wait --for=condition=complete job -l "gateway.test/phase=$1" --timeout=300s
}

k apply -f "$work/all.yaml" -l gateway.test/phase=deps
for r in $(k get statefulset,deployment -l app.kubernetes.io/part-of=test-deps -o name); do
  k rollout status "$r" --timeout=300s
done
run_jobs deps-job
run_jobs job
k apply -f "$work/all.yaml" -l '!gateway.test/phase'
for r in $(k get deployment -l app.kubernetes.io/part-of=go-gateway -o name); do
  k rollout status "$r" --timeout=600s
done
echo "gateway-deploy: déployé"
```

`chmod +x deploy/test/host/gateway-deploy deploy/test/host/gateway-deploy_test.sh`.

- [ ] **Step 4 : relancer** → `gateway-deploy_test: ok`.

- [ ] **Step 5 : mutation** — `cp`, retirer `run_jobs job` : le test doit échouer (`étapes
  manquantes`). Restaurer.

- [ ] **Step 6 : shellcheck et commit**

```bash
shellcheck deploy/test/host/gateway-deploy deploy/test/host/gateway-deploy_test.sh
git add deploy/test/host Makefile
git commit -m "feat(deploy): script hôte de déploiement par phases, Jobs recréés avant l'application"
```

---

### Task 6 : Préparation de l'hôte et runbook

**Files :**
- Create : `deploy/test/host/install.sh`, `deploy/test/README.md`
- Modify : `deploy/README.md` (une ligne de renvoi en tête de « Ce que ces manifests ne contiennent pas »)

**Interfaces :**
- Consumes : `deploy/test/host/gateway-deploy` (Task 5), copié par `scp` avant l'exécution.
- Produces : utilisateur `deploy`, kubeconfig restreint au namespace `gateway` dans
  `/home/deploy/.kube/config`, `/usr/local/bin/gateway-deploy`, `authorized_keys` à commande forcée.

- [ ] **Step 1 : écrire `install.sh`**

```bash
#!/usr/bin/env bash
# Préparation d'un VPS Ubuntu 24.04 neuf, en root, une fois : k3s, pare-feu, compte de CD.
# usage : install.sh "<clé publique ssh de la CD>"
set -euo pipefail
pubkey=${1:?usage: $0 "<clé publique ssh de la CD>"}
here=$(cd "$(dirname "$0")" && pwd)

apt-get update -q && apt-get install -yq ufw
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
# Réseaux des pods et des Services de k3s : sans eux, ufw coupe le trafic entre pods.
ufw allow from 10.42.0.0/16
ufw allow from 10.43.0.0/16
ufw --force enable
sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication no/' /etc/ssh/sshd_config
systemctl reload ssh

curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 600
until kubectl get nodes >/dev/null 2>&1; do sleep 2; done
systemctl show k3s -p LimitNOFILE

kubectl create namespace gateway --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata: {name: deploy, namespace: gateway}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: deploy, namespace: gateway}
rules:
  - apiGroups: ["*"]
    resources: ["*"]
    verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: deploy, namespace: gateway}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: deploy}
subjects: [{kind: ServiceAccount, name: deploy, namespace: gateway}]
---
apiVersion: v1
kind: Secret
metadata:
  name: deploy-token
  namespace: gateway
  annotations: {kubernetes.io/service-account.name: deploy}
type: kubernetes.io/service-account-token
EOF
until token=$(kubectl -n gateway get secret deploy-token -o jsonpath='{.data.token}' | base64 -d) && [[ -n $token ]]; do sleep 1; done

id deploy >/dev/null 2>&1 || useradd -m -s /bin/bash deploy
install -d -m 700 -o deploy -g deploy /home/deploy/.kube /home/deploy/.ssh
KUBECONFIG=/home/deploy/.kube/config kubectl config set-cluster k3s \
  --server=https://127.0.0.1:6443 \
  --certificate-authority=/var/lib/rancher/k3s/server/tls/server-ca.crt --embed-certs
KUBECONFIG=/home/deploy/.kube/config kubectl config set-credentials deploy --token="$token"
KUBECONFIG=/home/deploy/.kube/config kubectl config set-context deploy --cluster=k3s --user=deploy --namespace=gateway
KUBECONFIG=/home/deploy/.kube/config kubectl config use-context deploy
chown deploy:deploy /home/deploy/.kube/config && chmod 600 /home/deploy/.kube/config

install -m 755 "$here/gateway-deploy" /usr/local/bin/gateway-deploy
printf 'command="/usr/local/bin/gateway-deploy",no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty %s\n' "$pubkey" \
  >/home/deploy/.ssh/authorized_keys
chown deploy:deploy /home/deploy/.ssh/authorized_keys && chmod 600 /home/deploy/.ssh/authorized_keys
echo "install.sh: prêt. Empreinte de l'hôte pour TEST_SSH_KNOWN_HOSTS :"
ssh-keyscan -t ed25519 127.0.0.1 2>/dev/null | sed "s/^127.0.0.1/$(curl -s4 ifconfig.me)/"
```

Puis `shellcheck deploy/test/host/install.sh`.

- [ ] **Step 2 : écrire le runbook `deploy/test/README.md`**

Sections, dans l'ordre, chacune avec ses commandes exactes :

1. **Ce que c'est** : environnement de test, pas la production ; overlay de `deploy/k8s`, qui n'est pas
   modifié ; la garde `make test-env`.
2. **Prérequis** : VPS Ubuntu 24.04 ≥ 8 vCPU / 16 Go ; DNS Cloudflare — `api.test` A → IP, **proxifié**,
   SSL **Full (strict)** ; `smpp.test` A → IP, **DNS only** ; certificat **Origin CA** pour
   `api.test.manouman.com` téléchargé (`origin.crt`, `origin.key`) ; image smsc-simulator publiée.
3. **Préparer l'hôte** : `ssh-keygen -t ed25519 -f cd-key -N ''` ;
   `scp deploy/test/host/install.sh deploy/test/host/gateway-deploy root@IP:/root/` ;
   `ssh root@IP bash /root/install.sh "$(cat cd-key.pub)"` ; noter la ligne d'empreinte.
4. **Secrets GitHub** (environnement `test`) : `TEST_SSH_KEY` = `cd-key`, `TEST_SSH_HOST` = IP,
   `TEST_SSH_KNOWN_HOSTS` = la ligne d'empreinte. Puis `shred -u cd-key`.
5. **Secrets du cluster** :
   `deploy/test/bootstrap-secrets.sh --host root@IP --origin-cert origin.crt --origin-key origin.key`.
   Une seule fois ; le script refuse s'ils existent. Pour tout régénérer :
   `kubectl delete namespace gateway` (perte des données, c'est un environnement de test), puis
   recréer le namespace et relancer.
6. **Premier déploiement** : Actions → *Deploy test* → *Run workflow* avec le SHA de `main`.
7. **Rollback** : même geste avec un SHA antérieur.
8. **Accès exploitant** : Admin API par un seul tunnel —
   `ssh -L 8081:127.0.0.1:8081 root@IP kubectl -n gateway port-forward svc/admin-api-svc 8081:8080`,
   puis dans un autre terminal `curl --cacert ~/.config/go-gateway-test/ca.crt
   --cert …/operator.crt --key …/operator.key --resolve admin-api-svc:8081:127.0.0.1
   -H "Authorization: Bearer $(cat …/admin-token)" https://admin-api-svc:8081/v1/admin/customers`.
   SMPP : `smpp.test.manouman.com:2775` en **TLS** sous la CA de la passerelle ; le certificat porte le
   SAN `smpp-server-svc`, donc le client pose `ServerName=smpp-server-svc`.
9. **Passer les paquets GHCR en privé** : `/etc/rancher/k3s/registries.yaml` avec un PAT
   `read:packages`, `systemctl restart k3s` — **avant** de basculer la visibilité, sinon
   `ImagePullBackOff` au prochain redéploiement.
10. **Ce qui n'y est pas** : observabilité, sauvegardes, charge (step-280), seed et preuve bout-en-bout
    (step-275).

Dans `deploy/README.md`, ajouter en tête de la section « Ce que ces manifests ne contiennent pas » :

```markdown
L'environnement de test k3s rejoue ces manifests sans les modifier, par l'overlay `deploy/test/` : voir
son `README.md`.
```

- [ ] **Step 3 : commit**

```bash
shellcheck deploy/test/host/install.sh
git add deploy/test deploy/README.md
git commit -m "docs(deploy): runbook et préparation de l'hôte de l'environnement de test"
```

---

### Task 7 : Le workflow de CD, la CI de l'overlay, la fiche reportée et la dette

**Files :**
- Create : `.github/workflows/deploy-test.yml`, `tasks-todo/step-275.md`,
  `debts/api-de-test-joignable-hors-cloudflare.md`
- Modify : `.github/workflows/ci.yml` (job `manifests`, ligne ~152), `tasks-todo/INDEX.md` (section M12),
  `docs/superpowers/specs/2026-09-27-environnement-de-test-k3s-design.md`

- [ ] **Step 1 : vérifier localement le nommage des images snapshot** (le risque principal du workflow)

Run :
```bash
GORELEASER_CURRENT_TAG=v0.0.0-sha-0123456789ab goreleaser release --snapshot --clean
docker images --format '{{.Repository}}:{{.Tag}}' | grep 'go-gateway/.*v0.0.0-sha-0123456789ab' | sort
```
Expected : 12 lignes `ghcr.io/martialanouman/go-gateway/<img>:v0.0.0-sha-0123456789ab-amd64` (et
`-arm64`). Si le suffixe diffère, reporter le motif exact dans la boucle de push du Step 2. Nettoyer :
`docker images … | xargs docker rmi`.

- [ ] **Step 2 : écrire `.github/workflows/deploy-test.yml`**

```yaml
# Déploie chaque commit vert de main sur l'environnement de test (deploy/test/README.md). Les images
# portent v0.0.0-sha-<commit> : ni latest, ni Release, ni tag git — Release reste un geste manuel.
name: Deploy test

on:
  workflow_run:
    workflows: [CI]
    types: [completed]
    branches: [main]
  workflow_dispatch:
    inputs:
      sha:
        description: Commit de main à déployer (rollback compris)
        required: true

concurrency:
  group: deploy-test
  cancel-in-progress: false

permissions:
  contents: read
  packages: write

jobs:
  deploy:
    if: github.event_name == 'workflow_dispatch' || (github.event.workflow_run.conclusion == 'success' && github.event.workflow_run.event == 'push')
    runs-on: ubuntu-latest
    environment: test
    env:
      SHA: ${{ inputs.sha || github.event.workflow_run.head_sha }}
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ env.SHA }}
          fetch-depth: 0

      - name: Version de test
        run: echo "VERSION=v0.0.0-sha-${SHA:0:12}" >>"$GITHUB_ENV"

      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
          cache: true

      - uses: docker/setup-buildx-action@v3

      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - uses: goreleaser/goreleaser-action@v6
        with:
          version: "~> v2"
          args: release --snapshot --clean
        env:
          GORELEASER_CURRENT_TAG: ${{ env.VERSION }}

      # --snapshot ne pousse pas et suffixe chaque image par sa plateforme : on pousse l'amd64 sous le
      # tag nu, celui que render-manifests.sh substitue.
      - name: Pousser les images
        run: |
          images=$(docker images --format '{{.Repository}}:{{.Tag}}' | grep -E "^ghcr.io/martialanouman/go-gateway/[a-z0-9-]+:${VERSION}-amd64$")
          [[ $(wc -l <<<"$images") -eq 12 ]] || { echo "::error::attendu 12 images, obtenu : $images"; exit 1; }
          for img in $images; do
            docker tag "$img" "${img%-amd64}"
            docker push "${img%-amd64}"
          done

      - name: kubeconform
        run: make kubeconform && echo "$(go env GOPATH)/bin" >>"$GITHUB_PATH"

      - name: Rendre l'overlay
        run: deploy/test/check.sh "$VERSION"

      - name: Déployer
        env:
          SSH_KEY: ${{ secrets.TEST_SSH_KEY }}
          KNOWN_HOSTS: ${{ secrets.TEST_SSH_KNOWN_HOSTS }}
          HOST: ${{ secrets.TEST_SSH_HOST }}
        run: |
          install -m 700 -d ~/.ssh
          printf '%s\n' "$SSH_KEY" >~/.ssh/id_ed25519 && chmod 600 ~/.ssh/id_ed25519
          printf '%s\n' "$KNOWN_HOSTS" >~/.ssh/known_hosts
          ssh -o StrictHostKeyChecking=yes "deploy@$HOST" <deploy/test/rendered/all.yaml
```

- [ ] **Step 3 : garder l'overlay dans la CI**

Dans `.github/workflows/ci.yml`, job `manifests`, remplacer `run: make kubeconform manifests` par :

```yaml
        run: |
          make kubeconform manifests
          PATH="$(go env GOPATH)/bin:$PATH" make test-env
```

- [ ] **Step 4 : `actionlint`**

Run : `actionlint .github/workflows/deploy-test.yml .github/workflows/ci.yml` → aucune sortie.

- [ ] **Step 5 : la fiche reportée `tasks-todo/step-275.md`**

```markdown
# step-275 — Environnement de test : seed du plan de contrôle et preuve bout-en-bout

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** l'environnement de test k3s (docs/superpowers/specs/2026-09-27-environnement-de-test-k3s-design.md) · **Bloque :** —

## Pourquoi cette fiche existe

L'environnement de test déploie et prouve que chaque pod est prêt ; il ne prouve pas qu'un SMS
traverse. Deux manques, reportés hors du premier livrable :

1. **Aucun seed.** Sans client, compte, identifiant de bind, sender ID actif, connecteur et route
   statique, rien ne s'envoie. Le connector-pool ne traite que les enregistrements dont `ConnectorID`
   égale son `CONNECTOR_ID` (`internal/connectorpool/submit.go:176`), qui doit donc être l'id d'une
   ligne `smsc_connectors` visée par une route. Le modèle du seed : `internal/e2e/e2e_test.go:221`
   (`seedControlPlane`) ; par l'API Admin, pour que `config:changed` invalide les caches.
2. **Aucune preuve bout-en-bout.** Un Job `smoke` qui binde en TLS, soumet un `submit_sm` avec
   `registered_delivery` et attend son DLR — à condition que smsc-simulator v0.7.0 émette des DLR avec
   la configuration `healthy` (à vérifier dans son dépôt ; `docs/specification-technique-simulateur-smsc.md`).

## Définition de terminé
- [ ] Seed idempotent par l'API Admin, rejouable après une remise à zéro du namespace.
- [ ] `CONNECTOR_ID` de l'overlay égal à l'id du connecteur seedé.
- [ ] Job `smoke` lancé par `gateway-deploy` en dernière phase ; le workflow échoue s'il échoue.
```

Dans `tasks-todo/INDEX.md`, section M12, ajouter avant la ligne de step-280 :
`- [ ] step-275 — Environnement de test : seed du plan de contrôle et preuve bout-en-bout`.

- [ ] **Step 6 : la dette `debts/api-de-test-joignable-hors-cloudflare.md`**

```markdown
# L'API REST de test est joignable sans passer par Cloudflare

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** l'environnement de test k3s (2026-09-27) · **Portée par :** —

**Ce qu'on a fait à la place.** `api.test.manouman.com` est proxifié par Cloudflare, mais le port 443
du VPS accepte toute source : Traefik le publie par le ServiceLB de k3s, en iptables, en amont de
`ufw` (`deploy/test/host/install.sh`). Qui connaît l'IP atteint l'API directement, sous un certificat
Origin CA qu'aucun navigateur ne reconnaît.

**Pourquoi.** Filtrer aux plages Cloudflare exige une règle iptables hors `ufw` ou une politique
réseau Traefik (`ipAllowList`), à tenir à jour avec les plages publiées ; disproportionné pour un
environnement de test sans données réelles.

**Ce qu'il en coûte si on ne la paie jamais.** L'API de test reste exposée aux scans directs, sans le
filtrage ni la limitation de débit de Cloudflare.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier jeu de données réel ou client externe sur
l'environnement de test.
```

- [ ] **Step 7 : aligner la spec** sur ce que la préparation a établi : SMPP public en **TLS** (pas en
  clair) ; **neuf** `Secret` TLS ; retrait de `SMPP_TRUSTED_PROXY_CIDRS` ; `HTTP_ADMIN_TOKENS` et
  certificat client exploitant ; `OTEL_SDK_DISABLED=true` ; tag `v0.0.0-sha-<12 hex>` ; seed et
  smoke reportés à step-275 ; dettes réduites à `api-de-test-joignable-hors-cloudflare`.

- [ ] **Step 8 : commit**

```bash
git add .github/workflows tasks-todo debts docs/superpowers/specs
git commit -m "ci: déploiement continu de main sur l'environnement de test k3s"
```

---

### Task 8 : Premier déploiement réel (avec l'utilisateur)

Prérequis humains : VPS commandé, DNS posés, certificat Origin CA, image smsc-simulator publiée,
PR mergée (le workflow `workflow_run` ne se déclenche que depuis la branche par défaut).

- [ ] **Step 1 :** dérouler le runbook sections 3 à 5.
- [ ] **Step 2 :** Actions → *Deploy test* → *Run workflow* avec le SHA de `main`.
- [ ] **Step 3 :** vérifier, dans l'ordre, et consigner le résultat dans la PR ou la fiche :
  - `ssh root@IP kubectl -n gateway get pods` : tout `Running`/`Completed` ;
  - `curl -sS -o /dev/null -w '%{http_code}\n' https://api.test.manouman.com/<chemin authentifié de
    get-account, lu dans api/openapi-public.yaml>` → `401`
    (Cloudflare → Traefik → rest-api-svc en TLS vérifié, auth refusée faute de clé) ;
  - `openssl s_client -connect smpp.test.manouman.com:2775 -servername smpp-server-svc
    -CAfile ~/.config/go-gateway-test/ca.crt </dev/null` → `Verify return code: 0 (ok)` ;
  - l'appel Admin du runbook §8 → `200`.
- [ ] **Step 4 :** merger un commit anodin sur `main` et constater que *Deploy test* part seul après *CI*.
```
