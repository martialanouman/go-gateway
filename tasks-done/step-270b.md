# step-270b — Images conteneur : Dockerfiles et publication GHCR

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-270 · **Bloque :** step-280, step-410

## Pourquoi cette fiche existe

step-270 a posé les manifests. Ils nomment des images
(`ghcr.io/martialanouman/go-gateway/<svc>:<version>`) que **rien ne construit** : le dépôt n'a aucun
`Dockerfile`, et le bloc `dockers:` de `.goreleaser.yaml` est commenté depuis M0 avec la mention
« enable once the service Dockerfiles land ».

Séparer les deux n'est pas un découpage cosmétique : « comment on déploie » et « comment on
construit » ont des revues différentes — l'une porte sur des ports, des probes et des budgets de
drain, l'autre sur une surface d'attaque et une chaîne de publication. Mais la dette a une date
d'échéance : **step-280 ne peut pas monter un environnement représentatif sans image**, et step-410
ne peut pas cocher son item manifests sur un déploiement que personne n'a jamais lancé.

## Périmètre (ce que fait CETTE PR)

- Un `Dockerfile` par binaire déployé — les dix services, plus `migrate` et `kafka-provision` que les
  Jobs de `deploy/k8s/jobs/` référencent.
- Activation du bloc `dockers:` de `.goreleaser.yaml`, une entrée par image, taguée `{{ .Version }}`,
  poussée sur GHCR depuis `release.yml`.
- Le tag `v0.0.0` que `deploy/k8s` porte aujourd'hui est un gabarit : décider et documenter comment
  la version publiée y est substituée au déploiement.

## Points d'implémentation clés

- **Base distroless, utilisateur non-root, pas de shell.** GoReleaser produit déjà des binaires
  statiques (`CGO_ENABLED=0`), donc rien n'exige une image avec un système de fichiers complet — et
  `content-key-svc` détient la KEK : sa surface doit rester la plus petite du lot (ADR-0011).
- ~~**`smpp-server-svc` a besoin de son ulimit.**~~ **Corrigé en cours de step : l'affirmation était
  fausse.** Le runtime Go relève lui-même le *soft* `RLIMIT_NOFILE` jusqu'au *hard*, sans condition
  (`$GOROOT/src/syscall/rlimit.go:32-45`) — un `Setrlimit` maison serait mort-né, et une image n'hérite
  pas d'un plafond qu'elle pourrait corriger. Le *hard* limit vient du nœud (`LimitNOFILE` de l'unit
  containerd) : ni le Dockerfile, ni un `securityContext`, ni un initContainer ne le changent
  (`setrlimit` n'est pas partagé entre conteneurs d'un pod). Le prérequis est donc **documenté** dans
  `deploy/README.md`, et sa vérification appartient à **step-280**, qui a un cluster.
- L'archive `content-key-svc` de `.goreleaser.yaml`, corrigée en step-270, est un prérequis : sans
  elle le service n'est publié nulle part.
- **L'image `migrate` doit embarquer `migrations/`.** Le Job de `deploy/k8s/jobs/` l'invoque avec
  `-dir migrations` ; un binaire statique sans ce répertoire échoue au premier démarrage, et la
  migration ClickHouse a le même besoin pour ses fichiers.
- **`release.yml` a besoin de `packages: write`** et d'un login GHCR ; les permissions du workflow
  sont aujourd'hui `contents: read`.
- Vérifier que chaque image démarre et **échoue proprement** sur une configuration de production
  incomplète (adresse loopback, `CLICKHOUSE_PASSWORD` par défaut, `CONTENT_KMS_MASTER_KEY` absent) :
  c'est le comportement que `ENVIRONMENT=production` garantit, et une image qui l'aurait perdu ne se
  verrait qu'au déploiement.

## Tests (écrits dans la même PR)

- Une garde qui exige un `Dockerfile` par binaire publié, et une entrée `dockers:` par image que
  `deploy/k8s` référence — les deux sens, sur le patron de `internal/deploy`.
- Construction effective d'au moins une image en CI, et démarrage jusqu'au refus de configuration
  attendu.

## Design arrêté

**Cinq arbitrages.**

1. **Deux Dockerfiles, pas douze.** Onze images ne diffèrent que par le binaire copié : un
   `Dockerfile` générique (`ARG BINARY`) les couvre. Douze copies seraient douze endroits où la ligne
   `USER` peut disparaître sans qu'une revue le voie — et `deploy/k8s` n'a aucun `securityContext`,
   donc cette ligne est la **seule** chose qui empêche le workload de tourner en root.
   `Dockerfile.migrate` est à part parce qu'il est le seul à embarquer des fichiers : y fondre
   `COPY migrations` dans le générique mettrait le schéma SQL dans l'image de `content-key-svc`, dont
   ADR-0011 exige la surface la plus étroite du lot.
2. **La release passe en manuel, en une seule exécution.** `on: push branches:[main]` →
   `on: workflow_dispatch`. Une release publie douze images sur un registre public : c'est un geste,
   pas un effet de bord d'un merge. En un seul `goreleaser release`, donc une seule config — le
   découplage qui aurait imposé un second fichier n'existe pas si rien ne s'est publié tout seul avant.
3. **amd64 et arm64.** Les Dockerfiles n'ont **aucun `RUN`** : QEMU n'exécute jamais rien, la seconde
   arche coûte une extraction de layer. `dockers_v2` produit le manifeste multi-arch en un seul
   `buildx --push`, donc douze tags dans les deux cas. Ne publier qu'amd64 laisserait une archive
   arm64 sans image — une asymétrie que personne ne saurait expliquer.
4. **Une image par binaire DÉPLOYÉ.** `mt-replay` est bâti et archivé mais aucun manifeste ne le
   déploie : pas d'image. Une image que personne ne déploie est de la surface d'attaque publiée
   gratuitement, et elle se périme sans que rien ne le signale — le même raisonnement qui garde
   `fake-smsc` hors des `builds:`. La garde tient les deux sens.
5. **Le gabarit `v0.0.0` reste un gabarit.** `make deploy-render VERSION=vX.Y.Z` substitue au
   déploiement, et le script **relit sa propre sortie** : un rendu partiel ne peut pas atteindre
   `kubectl`. Une règle de garde refuse toute version figée à la main dans un manifeste.

**Le trou que cette step a trouvé avant d'écrire une ligne d'image.** `smpp-server-svc` n'avait ni
`builds:` ni `archives:` dans `.goreleaser.yaml` — l'ingress SMPP, le service le plus exposé du dépôt,
n'était publié nulle part. Même nature que l'archive `content-key-svc` corrigée en step-270 ; il en
restait un. C'est le rouge d'entrée du TDD, et la règle qui l'a nommé garde les deux directions.

## Definition of Done

- [x] `make check` vert · `make manifests` vert
- [x] une image par binaire déployé, non-root, sans shell · publiée sur GHCR au tag de version
- [x] `deploy/k8s` référence des images qui existent réellement

## Hors périmètre

Le déploiement lui-même et la campagne NFR → step-280. La checklist de go-live → step-410. TLS et
certificats → step-300.
