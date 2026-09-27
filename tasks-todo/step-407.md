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

1. **Destination objet** : une `Destination` `s3(<collection nommée>, filename = …, format = 'Parquet')`,
   choisie par configuration. `FileDestination` reste pour le poste local. Les identifiants S3 vivent dans la
   configuration du serveur ClickHouse, jamais dans la passerelle, ni dans une requête, ni dans un log.
2. **Catalogue des archives** : chaque tentative écrit son propre objet (`<prefix>-<jour>-<token>.parquet`),
   et une tentative échouée peut laisser derrière elle un objet **complet**. Le lecteur de step-420 doit
   savoir lequel fait foi. L'archiveur inscrit donc l'objet vérifié du jour **après** la relecture et
   **avant** la suppression de la partition. Si l'inscription échoue, rien n'est supprimé.
3. **Manifestes** : la destination et le préfixe sont posés pour admin-api-svc, qui porte le Retainer.

## Points d'implémentation clés

- **Tranché (utilisateur, 2026-09-27)** :
  - **La production refuse de booter sans destination d'archive**, sur le modèle de `TLS_ENABLED` et
    `OIDC_*`. Une purge qui supprime sans archiver est un défaut silencieux.
  - **Des identifiants S3 dédiés** (clé d'accès et secret d'une identité propre à l'archivage), pour que
    tout service compatible S3 convienne (AWS, MinIO, Scaleway, OVH…). Un rôle IAM au sens AWS, par
    profil d'instance ou IRSA, n'existe que chez AWS. Droits minimaux : écriture et lecture sous le préfixe,
    **aucune suppression**, pour que l'archive reste immuable (§6.14).
  - **Mécanisme retenu, sous réserve de revue** : ClickHouse reçoit ces identifiants par une **collection
    nommée** déclarée dans la configuration du serveur ClickHouse, et la requête ne cite que son nom
    (`s3(cdr_archive, filename = '…')`). Pourquoi : des identifiants passés en arguments de `s3(...)`
    finissent en clair dans `system.query_log`. Conséquence : ClickHouse n'est pas déployé par ce dépôt,
    donc la collection nommée devient une ligne de la checklist de step-410, et la passerelle ne détient
    jamais ce secret.
  - **Catalogue : une table Postgres `control_plane.cdr_archives`** (`day` en clé primaire, `object`,
    `row_count`, `archived_at`). Postgres est déjà la source de vérité du plan de contrôle, admin-api-svc y
    écrit et porte à la fois le Retainer et l'export, et l'écriture est transactionnelle. Une table ClickHouse
    mêlerait du contrôle aux CDR, sans transaction ; un objet témoin dans le bucket ne se corrigerait pas sans
    droit de suppression. Le schéma change **et** une migration l'accompagne (`.claude/rules/db-schema.md`).
    Une ligne ne se réécrit jamais : si le jour est déjà inscrit, la partition a déjà été supprimée, et un
    second archivage n'a rien à inscrire.
- **Pièges connus** (step-165, `cdr-retention-tiering-traps`) : un seul propriétaire de la rétention (le TTL
  de table reste à 400 jours) ; ne jamais supprimer ce qu'on n'a pas relu ; la projection vient de
  `system.columns`, et le préfixe reste validé par `ValidArchivePrefix`.
- **RGPD** : l'archive porte `source_addr` et `dest_addr` hors des deux chemins d'effacement. ADR-0018 l'a
  déjà accepté (« responsabilité de l'exploitant ») ; la politique de cycle de vie du bucket (13 mois) est une
  ligne de la checklist de step-410.

## Design arrêté

Arbitré par Fable le 2026-09-27 (points 1 à 3), sans conflit avec la fiche ; le reste découle de la spec.

- **Configuration** : `CLICKHOUSE_ARCHIVE_COLLECTION` nomme la collection nommée ClickHouse. Posée →
  `s3(<collection>, filename = '<prefix>-<jour>-<token>.parquet', format = 'Parquet')` ; vide avec un préfixe
  → `FileDestination` (poste local). `CLICKHOUSE_ARCHIVE_PREFIX` reste l'interrupteur. La collection est
  validée comme identifiant (`^[A-Za-z_][A-Za-z0-9_]{0,63}$`) dans `newRetainer`, à côté de
  `ValidArchivePrefix` ; une collection sans préfixe est refusée (« would have no effect »).
- **`Destination`** devient `func(object string) string` : l'archiveur nomme l'objet (préfixe, jour, jeton),
  la destination ne fait que l'envelopper. Le nom d'objet est ce que le catalogue retient.
- **Catalogue** : `control_plane.cdr_archives (day date PK, object text, row_count bigint ≥ 0,
  archived_at timestamptz)`, schéma + migration 0022. Interface `ArchiveCatalog` déclarée côté consommateur
  (package `clickhouse`), obligatoire pour `PartitionArchiver`. Une seule requête, jamais de réécriture :
  `INSERT … ON CONFLICT (day) DO NOTHING RETURNING row_count UNION ALL` la ligne préexistante → rend le
  `row_count` de la ligne qui fait foi. **Si ce `row_count` < lignes de la partition** (un DROP raté après
  inscription, puis la partition a grossi), l'archiveur échoue et la partition reste : `archive_failed`
  visible plutôt qu'un catalogue silencieusement incomplet. Sinon le second objet reste orphelin, non inscrit.
- **Garde de production** : `validateAdminConfig` (admin-api-svc est le seul porteur du Retainer ; six autres
  services chargent la section ClickHouse sans rien purger). Production ET (préfixe vide OU collection vide) →
  boot refusé ; `FileDestination` est donc refusée en production. **Pas d'exemption** pour
  `RETENTION_INTERVAL = 0` : elle rouvrirait un « planificateur externe » qui supprime sans archiver.
- **Constats du spike** (ClickHouse 24.8, MinIO) : une identité à `PutObject`/`GetObject`/`ListBucket` seuls
  suffit ; ClickHouse refuse de réécrire une clé existante ; l'utilisateur ClickHouse de la passerelle a besoin
  de `GRANT NAMED COLLECTION ON cdr_archive` ; `query_log` ne voit que le nom de la collection. Les deux
  derniers vont à la checklist de step-410.

## Tests (écrits dans la même PR)

- Sur un stockage S3 de test (conteneur MinIO), une partition expirée est archivée, relue, inscrite au
  catalogue, puis supprimée.
- Écriture refusée par le stockage : la partition n'est pas supprimée.
- Inscription au catalogue qui échoue : la partition n'est pas supprimée.
- Deux tentatives pour un même jour (la première complète mais non inscrite) : le catalogue n'en désigne
  qu'une.
- Aucune requête émise ne contient d'identifiant S3 : la destination ne cite que le nom de la collection.
- Un boot en production sans destination d'archive est refusé.

## Definition of Done

- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] en production, une partition expirée est archivée sur le stockage objet, inscrite au catalogue, puis
      supprimée — et jamais supprimée sinon
- [ ] manifestes à jour, garde `internal/deploy` verte
- [ ] step-410 porte la ligne « bucket d'archive CDR : existe, cycle de vie 13 mois, identité S3 sans droit de
      suppression, collection nommée déclarée sur le serveur ClickHouse »

## Hors périmètre

La **lecture** de l'archive par l'export : step-420. La rétention du bucket (cycle de vie) : exploitant.
