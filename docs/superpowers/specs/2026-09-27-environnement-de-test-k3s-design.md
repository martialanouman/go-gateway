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
| **Contabo Cloud VPS 8** (8 vCPU, 24 Go, ~17 $/mois), Ubuntu 24.04 | seule offre ≥ 16 Go disponible à ce prix (Hetzner CAX/CX43/CX53 indisponibles, CPX42 à 69 €) | AWS (~120 $/mois, crédits CPU) ; le design ne dépend pas du fournisseur |
| **Images `v0.0.0-sha.<court>`** | pré-version SemVer que `scripts/render-manifests.sh` accepte déjà : aucun changement du script | un tag `sha-…` hors regex |
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
     → push ghcr v0.0.0-sha.<court>              jobs : migrate-postgres, migrate-clickhouse,
  2. render-manifests.sh + kustomize                    kafka-provision, smoke
     → rendered.yaml                             app  : 10 Deployments de deploy/k8s, patchés
  3. ssh deploy@vps < rendered.yaml ──────────►  gateway-deploy (commande forcée)
                                                 exposé : 22, 2775 (SMPP), 443 (REST via Traefik)
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
    `KAFKA_TOPIC_REPLICATION_FACTOR=1`, endpoint OTLP neutralisé.
  - **Tous les Deployments** (patch JSON 6902, cible `kind: Deployment`) : `replicas: 1`, `requests`
    ramenés à ~¼ ; `limits` gardées. Motif : les `requests` de production totalisent ~21,5 vCPU.
  - **HPA** : `minReplicas: 1`, `maxReplicas: 2`.
  - **connector-pool-svc** : `CONNECTOR_ADDR=smsc-simulator:2775`.
  - **rest-api-svc** : un `Ingress` Traefik sur `api.test.manouman.com`, TLS par le `Secret`
    `api-origin-tls` : un certificat **Cloudflare Origin CA** (15 ans), créé à la main depuis le runbook.
    Il n'est reconnu que par le proxy Cloudflare : l'enregistrement `api.test` est donc **proxifié**, en
    mode SSL **Full (strict)**. Aucun ACME, aucune dépendance au port 80. Le proxy masque l'IP cliente :
    sans effet, rest-api-svc ne lit pas l'adresse distante (seul le listener SMPP le fait).
  - **`gateway-oidc`** : ConfigMap de valeurs de test (rien de secret).
- `smoke/` : un Job qui ouvre un bind SMPP, soumet un `submit_sm` et attend son DLR — la preuve
  bout-en-bout de chaque déploiement.
- Conservé **tel quel** : TLS inter-services (8 `Secret` générés par `test/tlsgen`), probes, drain,
  PDB, `SMPP_POD_ADDR` ← `status.podIP`, le `Service` `LoadBalancer` de smpp-server-svc (le ServiceLB
  de k3s le publie sur le port 2775 de l'hôte).

### 2. `deploy/test/host/gateway-deploy` — le script côté hôte

Lit le YAML rendu sur stdin, puis, dans l'ordre, en échouant au premier écart :

1. applique les dépendances, attend `Ready` ;
2. **supprime puis recrée** les Jobs (template immuable : un `apply` avec une nouvelle image échoue),
   attend `Complete` ;
3. applique le reste, `kubectl rollout status` sur chaque Deployment ;
4. relance le Job `smoke`, attend `Complete`.

Il tourne avec un kubeconfig restreint au namespace `gateway` (ServiceAccount + RoleBinding), pas
l'admin k3s.

### 3. `.github/workflows/deploy-test.yml`

- Déclencheurs : `workflow_run` sur `CI`, `conclusion == success`, branche `main` ; `workflow_dispatch`
  avec un `sha` pour le rollback.
- `concurrency: deploy-test`, `cancel-in-progress: false`.
- Job images : `goreleaser release --snapshot` avec `.goreleaser.yaml` inchangé si possible, puis push
  des images `v0.0.0-sha.<court>`.
- Job deploy : rendu sur le runner, puis `ssh deploy@$TEST_SSH_HOST < rendered.yaml`.
- Secrets GitHub : `TEST_SSH_KEY`, `TEST_SSH_HOST`, `TEST_SSH_KNOWN_HOSTS` (jamais
  `StrictHostKeyChecking=no`).

### 4. `deploy/test/README.md` — le runbook

Préparation de l'hôte (Ubuntu 24.04, `ufw` refus par défaut, SSH par clé seule, `LimitNOFILE` du
service k3s — prérequis de smpp-server-svc) ; installation de k3s ; utilisateur `deploy` et
`authorized_keys` avec `command="/usr/local/bin/gateway-deploy"` ; création de `gateway-secrets`
(mots de passe aléatoires) et des 8 `Secret` TLS ; premier déploiement ; rollback ; bascule des paquets
GHCR en privé (`/etc/rancher/k3s/registries.yaml` + PAT `read:packages` — sans quoi tout part en
`ImagePullBackOff`) ; création du `Secret` `api-origin-tls` depuis le certificat Origin CA.

## Exposition

| Port | Service | Exposé |
|---|---|---|
| 22 | SSH, clé seule | oui |
| 2775 | SMPP (ServiceLB), `smpp.test.manouman.com`, en clair | oui |
| 443 | REST (Traefik), `api.test.manouman.com`, derrière le proxy Cloudflare, certificat Origin CA | oui, à tous : ServiceLB publie en amont de `ufw`, un filtrage aux IP Cloudflare y serait sans effet |
| 6443 | API k8s | non (`ufw`) |
| Admin API, ops 9090, dépendances | — | non : `ssh -L` |

Le ServiceLB et Traefik publient leurs ports par iptables, **en amont de `ufw`** : seuls les
`Service` de type `LoadBalancer` sont donc exposés, et l'overlay n'en ajoute aucun.

## Gestion des erreurs

Tout écart (Job en échec, rollout non terminé dans son délai, smoke rouge) fait échouer le workflow ;
le déploiement précédent reste en place pour les Deployments dont le rollout n'a pas abouti. Le
rollback est un `workflow_dispatch` sur un SHA antérieur.

## À vérifier pendant le plan

- `goreleaser --snapshot` pousse-t-il avec `dockers_v2` ? Sinon : tag + push explicites des images
  produites.
- Ce qu'admin-api-svc accepte sans IdP hors `production` (`internal/config/config.go:1409`).
- Comment neutraliser l'export OTLP sans erreurs en boucle.
- Les entrées attendues par `test/tlsgen` pour les 8 `Secret`.
- La garde `internal/deploy` et `make manifests` ne doivent pas balayer `deploy/test/`, ou doivent
  l'accepter.

## Dettes à ficher (`debts/`, même PR)

- SMPP en clair sur le port 2775 de l'environnement de test.
- Métriques `External` des HPA sans adaptateur.
- Aucun collecteur OTel.
- Dépendance à l'image smsc-simulator publiée par un autre dépôt.

## Hors périmètre

Observabilité (Prometheus/Grafana), sauvegardes (tout se recrée), charge (step-280, sur hôte dédié),
TLS sur SMPP public, exposition de l'API Admin.

## Prérequis externes

- Le VPS Contabo commandé, Ubuntu 24.04, accès root par clé.
- DNS : enregistrements `A` `api.test.manouman.com` et `smpp.test.manouman.com` vers l'IP du VPS,
  posés avant le premier déploiement. Zone Cloudflare : `api.test` **proxifié** (nuage orange, SSL
  Full (strict)), `smpp.test` en **DNS only** — le proxy ne transporte pas SMPP sur 2775.
- Un certificat Cloudflare Origin CA pour `api.test.manouman.com`, chargé dans le `Secret`
  `api-origin-tls` (runbook) ; jamais dans git.
- La CI de `go-smsc-simulator` publie une image `linux/amd64` sur GHCR, taguée par version.
