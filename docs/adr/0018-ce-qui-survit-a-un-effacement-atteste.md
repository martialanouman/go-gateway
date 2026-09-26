# ADR-0018 : Ce qui survit à un effacement attesté, combien de temps, et à quel titre

**Status:** Accepted
**Date:** 2026-09-26
**Deciders:** Équipe plateforme (arbitrage Fable puis utilisateur, step-297)
**Réf spec:** §6.14.3, §6.14.4, §6.20 ; ADR-0017 ; step-166 → step-290d → step-315 → step-297

## Context

L'effacement RGPD (`POST /admin/gdpr/erase`) délivre une attestation qui nomme son périmètre :
`scope=cdr+content_keys+unrouted_mo(excludes:cold_archives,kafka_log,audit_log)`. Ce que l'effacement ne
touche pas continue de porter le numéro de la personne. Jusqu'ici, la durée et la justification de cette
survie n'étaient écrites que dans un commentaire Go (`attestationScope`). `control_plane.audit_log` n'avait
aucune rétention, et le log applicatif recevait l'attestation entière, sujet compris, quand elle ne
pouvait pas s'enregistrer.

## Decision

Un auditeur qui lit l'attestation trouve ici, pour chaque exclusion, sa durée et sa base.

| Ce qui survit | Durée | Base |
|---|---|---|
| `control_plane.audit_log` (numéros en clair dans `target`) | `POSTGRES_AUDIT_LOG_RETENTION`, défaut **365 jours**, borné à [365 j, 7×365 j] | Imputabilité des accès aux données : qui a lu ou modifié quoi. La piste d'audit est immuable par conception (step-315), et effacer la trace de l'accès effacerait la preuve du traitement. |
| Opt-out (`suppressions`) | Sans expiration | L'obligation de ne plus contacter la personne survit à l'effacement (§6.20) ; l'effacer la ré-exposerait. L'attestation le dit (`opt_out_preserved=true`). |
| Archives froides (Parquet) | La rétention d'archive de l'exploitant | Bornées par leur propre politique, hors de la plateforme : responsabilité de l'exploitant. |
| Log Kafka | La rétention des topics | Bornée par la rétention des topics ; responsabilité de l'exploitant. |
| Logs applicatifs | La collecte de logs (courte, §6.14.3) | **Aucun numéro effacé** : le log de dernier recours d'une attestation non enregistrée porte `job_id` et l'attestation **sans** son sujet, qui vit sur la ligne de job. |

- **Défaut au plancher.** La spec dit « 1 à 7 ans selon conformité ». Une exigence plus longue est celle de
  l'exploitant ; le défaut suit la minimisation.
- **Le plancher est dans la base.** Le trigger `audit_log_append_only` (migration 0021) laisse passer un
  `DELETE` seulement sous le réglage transactionnel `audit_log.purge` **et** au-delà de 365 jours. Aucune
  configuration ne purge sous la spec, et un `DELETE` accidentel reste refusé. Le plancher est compté en
  **jours** : `interval '1 year'` vaut 366 jours une année bissextile, et une seule ligne jugée trop jeune
  annulerait toute la purge.
- **Purge par `DELETE WHERE`, pas par partition.** C'est un écart assumé avec §6.14.3 : la table n'est pas
  partitionnée (step-290c), et son volume est de quelques dizaines de lignes par jour. La purge tourne dans
  `admin-api-svc`, au rythme de `CLICKHOUSE_RETENTION_INTERVAL`.

## Consequences

- `DELETE` est rendu au propriétaire de la table, et le `REVOKE` de step-315 ne porte plus que `TRUNCATE`.
  Un rôle de purge distinct n'aurait rien ajouté tant que le rôle applicatif est propriétaire : `SET LOCAL
  ROLE` et `SET LOCAL` sont le même geste délibéré. Il aurait en revanche exigé `CREATEROLE` du migrateur.
- L'immuabilité protège contre l'accident, pas contre l'intention. La dette
  `debts/audit-log-immuable-contre-tout-sauf-son-proprietaire.md` reste ouverte : seul un propriétaire
  distinct du rôle applicatif rend le plancher inaltérable.
- Le texte d'erreur d'un effacement échoué (`erasure failed: <err>`) vient du pilote ClickHouse, et rien ne
  garantit qu'il soit exempt de numéro.
