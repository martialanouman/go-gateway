# step-407 — La production archive ses CDR avant de les supprimer

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-165 (livrée) · **Bloque :** step-410, step-420

## Pourquoi cette fiche existe

La spec prévoit un tiède (8–90 j, stockage objet et Parquet), puis une archive froide immuable jusqu'à la
limite légale, par exemple 13 mois (§6.14). step-165 a livré le mécanisme : le Retainer supprime des
partitions entières, et `PartitionArchiver` fait écrire la partition par ClickHouse, la **relit**, et ne la
supprime que si l'archive est vérifiée. Le mécanisme n'est branché sur rien en production :

- la seule `Destination` du dépôt est `FileDestination` (`internal/storage/clickhouse/retention.go:76`), qui
  écrit sur le disque du serveur ClickHouse. Son commentaire : « Production supplies an s3(...)
  Destination ». Personne ne l'a écrite ;
- `cmd/admin-api-svc/wiring.go:229-237` ne câble que `FileDestination` ;
- aucun manifeste de `deploy/k8s` ne pose `CLICKHOUSE_ARCHIVE_PREFIX`. Le défaut vide veut dire « supprimer
  sans archiver » (`internal/config/config.go:403-407`).

**Chaque jour, la partition qui atteint `CDR_RETENTION` (90 jours) est donc perdue pour de bon.** Sortie de
step-420 (le 2026-09-27) pour passer avant le go-live : après le démarrage de la production, chaque jour sans
archive est un jour qu'aucune step ne pourra rendre.

## Périmètre (ce que fait CETTE PR)

1. **Destination objet** : une `Destination` `s3(url, …, 'Parquet')` choisie par configuration.
   `FileDestination` reste pour le poste local. Les identifiants de l'objet arrivent par `secretKeyRef` (garde
   `secrets-by-reference` de `internal/deploy`), et ne figurent ni dans un log, ni dans une erreur, ni dans la
   requête journalisée. L'expression `s3(...)` porte les identifiants si on les y interpole : préférer une
   collection nommée ou une configuration côté serveur ClickHouse (à arbitrer).
2. **Catalogue des archives** : chaque tentative écrit son propre objet (`<prefix>-<jour>-<token>.parquet`),
   et une tentative échouée peut laisser derrière elle un objet **complet**. Le lecteur de step-420 doit
   savoir lequel fait foi. L'archiveur inscrit donc l'objet vérifié du jour **après** la relecture et
   **avant** la suppression de la partition. Si l'inscription échoue, rien n'est supprimé.
3. **Manifestes** : la destination et le préfixe sont posés pour admin-api-svc, qui porte le Retainer.

## Points d'implémentation clés

- **À arbitrer avant tout code (échelle : spec → Fable → humain)** :
  - **La production doit-elle refuser de booter sans destination d'archive ?** C'est le modèle des gardes de
    production du dépôt (`TLS_ENABLED`, `OIDC_*`) : une purge qui détruit sans archiver est exactement le
    défaut silencieux qu'une garde évite. Mais cela impose un bucket dès le premier déploiement.
  - La forme du catalogue : une table `control_plane` en Postgres (changement de schéma **et** migration,
    `.claude/rules/db-schema.md`), une table ClickHouse, ou un objet manifeste dans le bucket.
  - Comment ClickHouse reçoit les identifiants S3 : `named_collections`, configuration du serveur ou rôle
    IAM. L'interpolation dans la requête est écartée d'office.
- **Pièges connus** (step-165, `cdr-retention-tiering-traps`) : un seul propriétaire de la rétention (le TTL
  de table reste à 400 jours) ; ne jamais supprimer ce qu'on n'a pas relu ; la projection vient de
  `system.columns`, et le préfixe reste validé par `ValidArchivePrefix`.
- **RGPD** : l'archive porte `source_addr` et `dest_addr` hors des deux chemins d'effacement. ADR-0018 l'a
  déjà accepté (« responsabilité de l'exploitant ») ; la politique de cycle de vie du bucket (13 mois) est une
  ligne de la checklist de step-410.

## Tests (écrits dans la même PR)

- Sur un stockage S3 de test (conteneur MinIO), une partition expirée est archivée, relue, inscrite au
  catalogue, puis supprimée.
- Écriture refusée par le stockage : la partition n'est pas supprimée.
- Inscription au catalogue qui échoue : la partition n'est pas supprimée.
- Deux tentatives pour un même jour (la première complète mais non inscrite) : le catalogue n'en désigne
  qu'une.
- Aucun identifiant S3 dans les logs ni dans les erreurs.
- Si la garde de production est retenue : un boot en production sans destination est refusé.

## Definition of Done

- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] en production, une partition expirée est archivée sur le stockage objet, inscrite au catalogue, puis
      supprimée — et jamais supprimée sinon
- [ ] manifestes à jour, garde `internal/deploy` verte
- [ ] step-410 porte la ligne « bucket d'archive CDR : existe, cycle de vie 13 mois, identifiants provisionnés »

## Hors périmètre

La **lecture** de l'archive par l'export : step-420. La rétention du bucket (cycle de vie) : exploitant.
