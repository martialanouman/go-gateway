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
- **`smpp-server-svc` a besoin de son ulimit.** `SMPP_MAX_CONNS` vaut 16384 : une image qui hérite
  d'un `nofile` de 1024 plafonne les binds bien avant la configuration.
- L'archive `content-key-svc` de `.goreleaser.yaml`, corrigée en step-270, est un prérequis : sans
  elle le service n'est publié nulle part.
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

## Definition of Done

- [ ] `make check` vert · `make manifests` vert
- [ ] une image par binaire déployé, non-root, sans shell · publiée sur GHCR au tag de version
- [ ] `deploy/k8s` référence des images qui existent réellement

## Hors périmètre

Le déploiement lui-même et la campagne NFR → step-280. La checklist de go-live → step-410. TLS et
certificats → step-300.
