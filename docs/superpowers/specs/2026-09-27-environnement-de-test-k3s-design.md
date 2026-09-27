# Environnement de test k3s et pipeline CD — design

> **Date :** 2026-09-27 · **Statut :** validé en brainstorming, à planifier
> **Précède :** les steps restantes (280, 396, 400, 405, 410, 420)

## But

Chaque merge vert sur `main` se déploie automatiquement sur un environnement de **test** qui exécute les
manifests de production `deploy/k8s` — pas un second descripteur. La production reste Kubernetes ;
cet environnement est la répétition de ce que step-410 déroulera.

## Décisions et leurs raisons

| Décision | Raison | Écarté |
|---|---|---|
| **k3s mono-nœud** | rejoue `deploy/k8s` tel quel : ConfigMap, Secrets, Jobs, probes, drain, `status.podIP` | Dokploy : un compose parallèle aux manifests, non gardé, qui dériverait |
| **Contabo Cloud VPS 8** (8 vCPU, 24 Go, ~17 $/mois), Rocky Linux 10 (SELinux `Enforcing`) | seule offre ≥ 16 Go disponible à ce prix (Hetzner CAX/CX43/CX53 indisponibles, CPX42 à 69 €) | AWS (~120 $/mois, crédits CPU) ; le design ne dépend pas du fournisseur |
| **Images `v0.0.1-sha-<12 hex>`** | pré-version SemVer que `scripts/render-manifests.sh` accepte déjà : aucun changement du script | `v0.0.0-sha-…` : sous-chaîne du gabarit `:v0.0.0` que le script neutralise (correspondance non ancrée fin de ligne) — le rendu se rejetterait lui-même |
| **Build dans GitHub Actions, déploiement par SSH** | le `Dockerfile` ne compile pas (il copie le binaire GoReleaser) ; l'API k8s n'est jamais exposée | build sur l'hôte ; kubeconfig distant |
| **`Release` reste manuel** | step-270b : une Release SemVer est un geste. Les images de test ne sont ni `latest`, ni Release, ni tag git | — |
| **Pair SMSC = smsc-simulator** | publié en image par la CI de son propre dépôt (prérequis externe) | `fake-smsc`, que GoReleaser exclut |

**Hétérogénéité assumée :** `deploy/README.md` dit « ni kustomize ni Helm » — cela reste vrai pour
`deploy/k8s`. L'overlay de test utilise kustomize, intégré à `kubectl`, et ne modifie aucun fichier de
`deploy/k8s`.

## Architecture

```
GitHub Actions                                   VPS (k3s, namespace gateway)
─────────────                                    ────────────────────────────
CI verte sur main ─► deploy-test.yml             deps : postgres, redis, redpanda,
  1. goreleaser --snapshot                              clickhouse, rustfs, smsc-simulator
     → push ghcr v0.0.1-sha-<12 hex>             jobs : migrate-postgres, migrate-clickhouse,
  2. render-manifests.sh + kustomize                    kafka-provision
     → rendered.yaml                             app  : 10 Deployments de deploy/k8s, patchés
  3. ssh deploy@vps < rendered.yaml ──────────►  gateway-deploy (commande forcée)
                                                 exposé : 22, 2775 (SMPP/TLS), 443 (REST via Traefik)
```

## Composants

### 1. `deploy/test/` — l'overlay

- `kustomization.yaml` : base = `.rendered/base.yaml` (sortie de `render-manifests.sh`, ignorée par git,
  écrite dans le dossier de l'overlay pour rester sous la racine kustomize).
- `deps/` : un StatefulSet mono-réplica + `Service` par dépendance, volume `local-path` :
  Postgres 18, Redis 7, Redpanda (un broker), ClickHouse 24.8, RustFS, smsc-simulator (Deployment,
  sans état). Versions alignées sur `docker-compose.yml` et les tests d'intégration.
- Patches :
  - **ConfigMap `gateway-config`** : `ENVIRONMENT=staging`, `KAFKA_BROKERS=redpanda:9092`,
    `KAFKA_TOPIC_REPLICATION_FACTOR=1`, `OTEL_SDK_DISABLED=true`.
  - **Tous les Deployments** (patch JSON 6902, cible `kind: Deployment`) : `replicas: 1`, `requests`
    ramenés à ~¼ ; `limits` gardées. Motif : les `requests` de production totalisent ~21,5 vCPU.
  - **HPA** : `minReplicas: 1`, `maxReplicas: 2`.
  - **smpp-server-svc** : `SMPP_TRUSTED_PROXY_CIDRS` supprimé — le ServiceLB de k3s n'envoie aucun
    en-tête PROXY, et la liste de confiance de production ferait refuser tout bind.
  - **connector-pool-svc** : `CONNECTOR_ADDR=smsc-simulator:2775`.
  - **admin-api-svc** : `HTTP_ADMIN_TOKENS` statique (`Secret gateway-secrets`), pas d'IdP OIDC hors
    `production`. L'accès exploitant ajoute un certificat client (mTLS, SAN `operator`, même CA
    `tlsgen`) : jeton **et** certificat, tous deux réservés à ce qui est hors production.
  - **rest-api-svc** : un `Ingress` Traefik sur `api.test.manouman.com`, TLS par le `Secret`
    `api-origin-tls` : un certificat **Cloudflare Origin CA** (15 ans), créé à la main depuis le runbook.
    Il n'est reconnu que par le proxy Cloudflare : l'enregistrement `api.test` est donc **proxifié**, en
    mode SSL **Full (strict)**. Aucun ACME, aucune dépendance au port 80. Le proxy masque l'IP cliente :
    sans effet, rest-api-svc ne lit pas l'adresse distante (seul le listener SMPP le fait).
  - **`gateway-oidc`** : ConfigMap de valeurs de test (rien de secret).
- Conservé **tel quel** : TLS inter-services et sur le port SMPP public (**neuf** `Secret` générés par
  `test/tlsgen`, un par service — `smpp-server-svc-tls` sert aussi le port public 2775, SAN
  `smpp-server-svc`, donc un client externe doit poser `ServerName=smpp-server-svc`), probes, drain,
  PDB, `SMPP_POD_ADDR` ← `status.podIP`, le `Service` `LoadBalancer` de smpp-server-svc (le ServiceLB
  de k3s le publie sur le port 2775 de l'hôte).
- Seed du plan de contrôle (client, compte, bind, sender ID, connecteur, route) et Job de preuve
  bout-en-bout : reportés à step-275 (`tasks-todo/step-275.md`) — ni l'un ni l'autre n'existe encore.

### 2. `deploy/test/host/gateway-deploy` — le script côté hôte

Lit le YAML rendu sur stdin, puis, dans l'ordre, en échouant au premier écart :

1. applique les dépendances, attend `Ready` ;
2. **supprime puis recrée** les Jobs (template immuable : un `apply` avec une nouvelle image échoue) ;
   scrute chaque Job et sort en échec dès qu'un passe à `Failed`, sans attendre son délai —
   `kubectl wait --for=condition=complete` ne reviendrait jamais sur un Job en échec ;
3. applique le reste, `kubectl rollout status` sur chaque Deployment.

Il tourne avec un kubeconfig restreint au namespace `gateway` (ServiceAccount + RoleBinding), pas
l'admin k3s.

### 3. `.github/workflows/deploy-test.yml`

- Déclencheurs : `workflow_run` sur `CI`, `conclusion == success`, branche `main` ; `workflow_dispatch`
  avec un `sha` pour le rollback.
- `concurrency: deploy-test`, `cancel-in-progress: false`.
- Un seul job `deploy` : `goreleaser release --snapshot` (`.goreleaser.yaml` inchangé), retague et
  pousse les 12 images `v0.0.1-sha-<12 hex>` (`--snapshot` ne pousse rien et suffixe chaque image par
  sa plateforme — le workflow retire le `-amd64`), rend l'overlay (`deploy/test/check.sh`), puis
  `ssh deploy@$TEST_SSH_HOST < rendered/all.yaml`.
- Secrets GitHub : `TEST_SSH_KEY`, `TEST_SSH_HOST`, `TEST_SSH_KNOWN_HOSTS` (jamais
  `StrictHostKeyChecking=no`).

### 4. `deploy/test/README.md` — le runbook

Préparation de l'hôte (Rocky Linux 10, firewalld limité à 22/80/443/2775, `kernel-modules-extra` du noyau courant, SSH par clé seule, `LimitNOFILE` du
service k3s — prérequis de smpp-server-svc) ; installation de k3s ; utilisateur `deploy` et
`authorized_keys` avec `command="/usr/local/bin/gateway-deploy"` ; `bootstrap-secrets.sh` crée
`gateway-secrets` (mots de passe et jeton admin aléatoires), les **neuf** `Secret` TLS et le `Secret`
`api-origin-tls` (depuis le certificat Origin CA), et dépose sur le poste de l'exploitant le
certificat client `operator` (mTLS Admin API) et les identifiants de bind SMPP ; premier
déploiement ; rollback ; bascule des paquets GHCR en privé (`/etc/rancher/k3s/registries.yaml` + PAT
`read:packages` — sans quoi tout part en `ImagePullBackOff`).

## Exposition

| Port | Service | Exposé |
|---|---|---|
| 22 | SSH, clé seule | oui |
| 2775 | SMPP (ServiceLB), `smpp.test.manouman.com`, en **TLS** sous la CA de la passerelle | oui |
| 443 | REST (Traefik), `api.test.manouman.com`, derrière le proxy Cloudflare, certificat Origin CA | oui, à tous : aucun filtrage aux plages IP Cloudflare (`debts/api-de-test-joignable-hors-cloudflare.md`) |
| 6443 | API k8s | non (firewalld) |
| Admin API, ops 9090, dépendances | — | non : `ssh -L`, et `HTTP_ADMIN_TOKENS` + certificat client `operator` (mTLS) requis même par le tunnel |

Le ServiceLB publie les ports des `Service` `LoadBalancer` ; firewalld n'ouvre que 22/80/443/2775 : seuls les
`Service` de type `LoadBalancer` sont donc exposés, et l'overlay n'en ajoute aucun.

## Gestion des erreurs

Tout écart (un Job passé `Failed`, détecté sans attendre son délai ; un rollout non terminé dans le
sien) fait échouer le workflow ; le déploiement précédent reste en place pour les Deployments dont le
rollout n'a pas abouti. Le rollback est un `workflow_dispatch` sur un SHA antérieur.

## Résolu pendant l'implémentation

- `goreleaser --snapshot` ne pousse rien et suffixe chaque image par sa plateforme
  (`<tag>-amd64`/`-arm64`) : `deploy-test.yml` retague et pousse l'`amd64` sous le tag nu.
- `admin-api-svc` hors `production` accepte `HTTP_ADMIN_TOKENS` statique ; l'accès exploitant ajoute
  un certificat client `operator` (mTLS, même CA `tlsgen`).
- L'export OTLP se coupe par `OTEL_SDK_DISABLED=true`, sans boucle d'erreurs.
- `test/tlsgen` émet **neuf** `Secret` (un par service exposé en TLS, `smpp-server-svc` compris) plus
  un certificat `operator` gardé hors cluster.
- `internal/deploy` et `make manifests` ne lisent que `deploy/k8s` (`internal/deploy/deploy.go:28`) :
  `deploy/test/` leur est invisible par construction.

## Dettes (`debts/`)

- `debts/api-de-test-joignable-hors-cloudflare.md` — Traefik expose 443 à toute source, sans
  filtrage aux plages Cloudflare.

## Hors périmètre

Observabilité (Prometheus/Grafana), sauvegardes (tout se recrée), charge (step-280, sur hôte dédié),
seed du plan de contrôle et preuve bout-en-bout (step-275).

## Prérequis externes

- Le VPS Contabo commandé, Rocky Linux 10, accès root par clé.
- DNS : enregistrements `A` `api.test.manouman.com` et `smpp.test.manouman.com` vers l'IP du VPS,
  posés avant le premier déploiement. Zone Cloudflare : `api.test` **proxifié** (nuage orange, SSL
  Full (strict)), `smpp.test` en **DNS only** — le proxy ne transporte pas SMPP sur 2775.
- Un certificat Cloudflare Origin CA pour `api.test.manouman.com`, chargé dans le `Secret`
  `api-origin-tls` (runbook) ; jamais dans git.
- La CI de `go-smsc-simulator` publie une image `linux/amd64` sur GHCR, taguée par version.
