# step-260f — Le mot de passe ClickHouse de développement est refusé en production

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** LIVRÉE (2026-09-04)
> **Dépend de :** — · **Bloque :** —

## Pourquoi cette fiche existe

L'audit du 2026-09-03 a vérifié que `internal/config/config.go` pose `envDefault:"gateway"` sur
`ClickHouse.Password` sans aucune garde, alors que les autres défauts de développement sont refusés
sur le palier production : l'URL Postgres (`postgresProblems`), l'URL Redis (`redisProblems`),
l'adresse ClickHouse loopback (`clickhouseProblems`), et les adresses gRPC internes. Un déploiement
production avec `CLICKHOUSE_ADDR` réel et `CLICKHOUSE_PASSWORD` oublié démarre avec `gateway`, le mot
de passe de `docker-compose.yml`.

## Ce que l'exploration a établi

- Les sept gardes existantes suivent un seul patron : `IsProduction() && champ == constante`, message
  qui **nomme la variable** et dit quoi faire, sans jamais écho la valeur. `LogValue` réduit déjà le mot
  de passe à `clickhouse_password_set` (`config.go:1165`).
- Il n'existe aucune constante nommée pour ce mot de passe : `"gateway"` est un littéral dans le tag.
- La garde doit vivre dans `clickhouseProblems()` pour hériter du mécanisme « section déclarée » :
  un binaire ne valide que les sections qu'il déclare (`TestLoadValidatesOnlyDeclaredSections`), et une
  section déclarée garde sa garde production (`TestDeclaredSectionKeepsItsProductionGuard`).
- Le développement doit continuer à booter sans aucune variable (`TestDevelopmentAcceptsLocalhostDefaults`).
- step-290 (secrets **stockés**, argon2id/SHA-256, temps constant) est disjointe : chevauchement
  thématique, aucune collision de fichiers.

## Design arrêté

- Constante `defaultClickHousePassword = "gateway"` à côté de `defaultClickHouseAddr` ; le tag
  `envDefault` reste la seule autre occurrence du littéral.
- Dans `clickhouseProblems()`, après la garde d'adresse : `if c.Environment.IsProduction() &&
  c.ClickHouse.Password == defaultClickHousePassword` → `"CLICKHOUSE_PASSWORD is the development
  default: set it explicitly in production"`. Le message ne contient pas la valeur.
- Un mot de passe **vide** est couvert par la même garde : caarlos0/env résout une variable posée
  mais vide à son `envDefault` (`TestEmptyValueFallsBackToDefault`), donc `CLICKHOUSE_PASSWORD=""`
  en production vaut `gateway` et est refusé.

## Chaîne de preuves

1. Nouveau cas dans la table de `TestProductionRejectsLocalhostDefaults` : environnement production
   complet, `CLICKHOUSE_ADDR: ch1:9000`, sans `CLICKHOUSE_PASSWORD` ⇒ l'erreur nomme
   `CLICKHOUSE_PASSWORD`. Rouge attendu : `Load()` accepte.
2. Les tests qui posent un environnement production sans le mot de passe **et attendent un succès**
   sont complétés avec `CLICKHOUSE_PASSWORD: s3cret` dans le même commit ; ceux qui attendent une
   erreur nommant une autre variable ne sont pas touchés (les problèmes s'agrègent) — sauf
   `TestRunRequiresAdminTokensInProduction` (admin-api-svc), dont la garde des jetons vit après
   `Load` : le recensement par `grep "ENVIRONMENT": "production"` l'avait manqué parce qu'il passe
   par `t.Setenv` ; `make check` l'a trouvé.
3. Mutations : retirer `IsProduction()` → `TestDevelopmentAcceptsLocalhostDefaults` tombe ; retirer la
   comparaison → le nouveau cas tombe.

## Commits

1. Cette fiche.
2. `config` : constante + garde + tests.
3. Fiche → `tasks-done/`.

## Definition of Done

- [x] `make check` vert (deux passes : la première a trouvé le test d'admin-api-svc manqué par le recensement)
- [x] `ENVIRONMENT=production` sans `CLICKHOUSE_PASSWORD` refuse le boot en nommant la variable — cas
      « clickhouse password defaulted » de `TestProductionRejectsLocalhostDefaults`
- [x] le développement boote sans variable — `TestDevelopmentAcceptsLocalhostDefaults`
- [x] aucun test ni message n'écho la valeur du secret (seul `s3cret` apparaît dans les tests)
- [x] rouge lu (`Load() accepted a localhost development default in production`) ; mutations tombées :
      garde hors `IsProduction()` → le développement ne boote plus ; comparaison retirée → le défaut passe

## Revue

Un sous-agent en lecture seule : aucun bloquant ; un Required sur la fiche (la phrase « mot de passe
vide hors périmètre » était fausse : une valeur vide retombe sur le défaut et tombe sous la garde),
corrigé avant l'archivage. FYI retenu : le Job de migration ClickHouse doit porter la variable en
production (noté dans step-270).

## Hors périmètre

step-290 ; `docker-compose.yml` garde sa valeur de développement. À porter dans step-270 : le Job de
migration ClickHouse (`cmd/migrate` déclare `SectionClickHouse`) doit aussi recevoir `CLICKHOUSE_PASSWORD`.
