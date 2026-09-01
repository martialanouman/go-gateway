---
paths:
  - "db/**"
  - "migrations/**"
---

# Schéma et migrations

**Les deux, en même temps.** Changer le schéma, c'est éditer
`db/schema_passerelle_sms.sql` **et** ajouter la migration `golang-migrate`
correspondante dans `migrations/` (`NNNN_description.up.sql` / `.down.sql`) —
les migrations sont *dérivées* du fichier de schéma, qui reste le contrat
référencé par le code (plan d'exécution §1.7).

Migrations ClickHouse : `migrations/clickhouse/`, **une instruction par fichier**
(le découpeur casse sur un `;` en commentaire).

**Rien ne garde ce couplage.** `internal/controlplane/enums_test.go` prouve que
les enums Go correspondent aux `CHECK (col IN (...))`, mais de `0001_init`
seulement ; la CI ne teste que `migrate up/down/up`. Aucun test ne vérifie que
`db/schema_passerelle_sms.sql` et `migrations/` restent synchrones — c'est à la
revue de le faire.
