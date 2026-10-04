# step-408 — Le grand livre se partitionne vraiment : billing-svc crée les partitions journalières

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-141 (livrée) · **Bloque :** step-409, step-410
> Unité faute de multiple de dix libre avant step-409, que la campagne finale doit trouver partitionnée.

## Pourquoi cette fiche existe

`control_plane.billing_ledger` est déclaré partitionné par jour (`RANGE (created_at)`,
`migrations/0001_init.up.sql:553-587`), mais seule `billing_ledger_default` existe. Le commentaire du schéma
renvoie la création des partitions à « a scheduler (pg_partman / cron) » : ce planificateur n'existe nulle
part (ni code, ni extension, ni CronJob, ni fiche). step-141 est pourtant cochée « grand livre partitionné ».
`db/schema_passerelle_sms.sql:785-790` déclare en plus deux partitions d'exemple (14 et 15/07/2026) qu'aucune
migration ne crée : le schéma et les migrations divergent.

Constaté le 04/10/2026 sur le VPS de test : `DEFAULT` contient 17,8 M lignes (index de 1 à 1,15 Go). Il s'ensuit :

- toutes les écritures s'accumulent dans une seule table ;
- la purge « par drop de partition, jamais `DELETE WHERE` » et la rétention de 13 mois (guide §13.2, spec
  §6.14.2) sont inapplicables ;
- Postgres refuse de créer une partition dont la plage a déjà des lignes dans `DEFAULT`, et parcourt `DEFAULT`
  sous verrou à chaque création ;
- la branche « partition détachée » de `ListOrphanedReservations` (`queries/billing.sql:198-200`) n'est jamais
  exercée.

## Décisions humaines (04/10/2026, fermées)

1. Une step avant le go-live, numérotée 408 ; step-409 et step-410 en dépendent.
2. **Go dans billing-svc, pas pg_partman** : pg_partman exige une image Postgres sur mesure et ne sait pas
   archiver, alors que la purge passera par un archivage vers le stockage objet.
3. **Pas de production** : le contenu de `DEFAULT` peut être supprimé.

## Périmètre

- Au démarrage de billing-svc puis à intervalle régulier, sur le modèle de `billing.Folder`, la création des
  partitions journalières d'aujourd'hui à J+N ; idempotente, sûre avec plusieurs réplicas.
- `DEFAULT` reste le filet : une partition manquante ne fait jamais échouer une écriture. Une jauge des lignes
  présentes dans `DEFAULT`, et l'expression d'alerte qui fait foi (guide §13), signalent qu'il a servi.
- Le vidage de `DEFAULT` et la remise en cohérence de la facturation du VPS, sans casser l'invariant
  « somme du grand livre = solde » (ADR-0022).
- Le commentaire du schéma corrigé, les exemples retirés.
- Une fiche de dette : détacher, archiver vers l'objet, purger à 13 mois (§6.14.2).

## Vérifié dans la doc PostgreSQL 18 (`ctx7`, `/websites/postgresql_18`)

- `CREATE TABLE … PARTITION OF` prend **ACCESS EXCLUSIVE sur le parent** (sql-createtable) : il bloquerait le
  `COPY` groupé de `billing_batch.go` et `RecordDurable` le temps de la création.
- `ALTER TABLE … ATTACH PARTITION` ne prend que **SHARE UPDATE EXCLUSIVE** sur le parent, qui ne conflit pas
  avec les écritures, mais **ACCESS EXCLUSIVE** sur la table attachée et sur `DEFAULT`, qu'il parcourt pour
  vérifier qu'aucune ligne n'appartient à la plage (sql-altertable). Il échoue si une ligne y appartient.
- L'ATTACH crée sur la partition les index, `PRIMARY KEY` et `UNIQUE` du parent qui lui manquent :
  `billing_ledger_idem_idx` et la PK suivent chaque partition.

## Design arrêté

Arbitré par Fable le 04/10/2026 (huit points, aucun non tranché, aucun conflit avec la spec ni la fiche). Le
mécanisme (ATTACH plutôt que `PARTITION OF`) découle de la doc ci-dessus et de l'exigence « ne pas bloquer le
chemin chaud ».

**Fait structurant.** L'ATTACH parcourt `DEFAULT` sous ACCESS EXCLUSIVE, verrou déjà pris : `lock_timeout` ne
borne pas ce parcours. Sur un `DEFAULT` de 17,8 M lignes, chaque ATTACH bloquerait le chemin chaud des
secondes, et celui du jour courant échouerait à chaque passe. **`DEFAULT` doit être vide avant la première
passe**, et le rester : c'est ce que surveille la jauge. Comme le CD déploie `main` au merge, le vidage du VPS
se fait **avant le merge**.

- **Création** : `(*postgres.BillingRepo).EnsureLedgerPartitions(ctx, from time.Time, days int) error`, dans
  `internal/storage/postgres/billing.go`, hors sqlc (DDL). Pour chaque jour UTC de `from` à `from+days-1`,
  **une transaction par jour** (un jour en échec ne bloque pas les suivants) :
  `pg_try_advisory_xact_lock(<constante du dépôt>)` — non obtenu → la réplique saute ce jour ;
  `SET LOCAL lock_timeout = '1s'` ; `to_regclass(...)` non nul → rien à faire ;
  `CREATE TABLE billing_ledger_YYYYMMDD (LIKE billing_ledger INCLUDING DEFAULTS INCLUDING CONSTRAINTS)` ;
  `ALTER TABLE billing_ledger ATTACH PARTITION … FOR VALUES FROM ('J 00:00:00+00') TO ('J+1 00:00:00+00')` ;
  commit. Les échecs de jour sont joints et rendus. La transaction unique supprime la fenêtre « créée mais pas
  attachée » ; le verrou consultatif rend `to_regclass` fiable sans distinguer les codes d'erreur.
- **Frontière** : UTC (`created_at` est un instant ; jours de 24 h, sans DST). Nom `billing_ledger_YYYYMMDD`,
  layout `20060102` comme `partitionDayLayout` des CDR. Les littéraux sortent de `time.Format`, jamais d'une
  entrée : le `Sprintf` du DDL n'est pas injectable.
- **Boucle** : `runLedgerPartitions` dans `cmd/billing-svc/main.go`, une passe immédiate au démarrage puis
  toutes les heures, Warn sur erreur, dans la `supervisor.Group`. Constantes `ledgerPartitionDays = 8`
  (aujourd'hui + 7) et `ledgerPartitionInterval = time.Hour`, pas de variable d'environnement. Pas de type
  `billing.Partitioner` : il n'envelopperait qu'un appel.
- **Jauge** : `billing_ledger_default_rows`, `GaugeFunc` lue au scrape sur le modèle d'`OutboxLag` (une jauge
  posée par la passe se figerait si elle pend), comptage plafonné
  `SELECT count(*) FROM (SELECT 1 FROM billing_ledger_default LIMIT 10000)`, NaN sur erreur. Câblée dans
  `wiring.go` comme `eventRelayLag`. Expression qui fait foi, au guide §13 (jauge de groupe → `max`) :
  `max(billing_ledger_default_rows) > 0` (for: 5m). L'alerte ne se résout pas seule : rien ne sort une ligne
  de `DEFAULT` (fiche de dette).
- **Schéma** : retirer de `db/schema_passerelle_sms.sql` les deux partitions d'exemple qu'aucune migration ne
  crée, réécrire le commentaire. **Aucune migration** : le DDL migré ne change pas ; le commentaire de
  `0001_init.up.sql` reste (une migration appliquée ne se réécrit pas). Checklist guide §15 mise à jour.
- **Vidage du VPS : opération manuelle, jamais une migration.** Une migration de vidage resterait dans
  l'historique, tournerait pendant que les anciens pods écrivent, et ne toucherait pas Redis : elle casserait
  l'invariant qu'elle prétend garder. Procédure (lancée par l'humain, **avant le merge**) :
  1. `kubectl scale deploy/billing-svc --replicas=0` ; aucune opération de facturation Admin pendant
     l'opération (topup/transfer écrivent `balances` et le grand livre) ;
  2. une transaction : `TRUNCATE control_plane.billing_ledger, control_plane.billing_idempotency,
     control_plane.balances, control_plane.balance_deltas, control_plane.billing_events_outbox` — le grand
     livre et le solde valent 0 tous deux ; sans `billing_idempotency`, des réclamations sans mouvement ;
     sans l'outbox, des planchers d'un monde disparu ;
  3. Redis : `SCAN` + `UNLINK` de `billing:*` (pas `FLUSHALL`) ; garder `billing:balance:*` rendrait du crédit
     fantôme jusqu'au TTL de 10 min ;
  4. merge : le CD déploie billing-svc step-408, qui crée J..J+7 au démarrage sur un `DEFAULT` vide ;
  5. recréditer les clients de test par l'API Admin (topup : solde et mouvement dans la même transaction).

  Résidu toléré sur un VPS de test : une capture dont la réserve a précédé l'arrêt tombe sur un grand livre
  vide (chemin « capture sans réserve », Warn). Pour zéro résidu, couper aussi l'ingress avant l'étape 1.
- **Tests** (intégration, une base migrée fraîche par test : la base partagée du processus a des lignes du
  jour dans `DEFAULT`, sur lesquelles l'ATTACH du jour échouerait) :
  1. une passe crée les N partitions (`pg_inherits`), avec PK et index hérités ; une ligne du jour atterrit
     dans sa partition, pas dans `DEFAULT` ;
  2. une seconde passe ne change rien ;
  3. deux passes concurrentes réussissent toutes deux, chaque partition une fois ;
  4. une écriture hors horizon réussit, atterrit dans `DEFAULT`, et la jauge la compte ;
  5. une passe réussit pendant qu'une transaction d'écriture du jour reste ouverte (mutation : `PARTITION OF`
     doit la faire tomber sur `lock_timeout`).

  Plus la boucle : une passe au démarrage, avant le premier tick (unité, store factice).
