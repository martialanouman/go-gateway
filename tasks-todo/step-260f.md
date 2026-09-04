# step-260f — Le mot de passe ClickHouse de développement est refusé en production

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** À FAIRE
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
- Un mot de passe **vide** en production n'est pas traité ici : c'est un autre défaut (ClickHouse
  refuserait la connexion, pas silencieusement), hors périmètre.

## Chaîne de preuves

1. Nouveau cas dans la table de `TestProductionRejectsLocalhostDefaults` : environnement production
   complet, `CLICKHOUSE_ADDR: ch1:9000`, sans `CLICKHOUSE_PASSWORD` ⇒ l'erreur nomme
   `CLICKHOUSE_PASSWORD`. Rouge attendu : `Load()` accepte.
2. Les tests du package qui posent un environnement production sans le mot de passe sont recensés
   avant le rouge et complétés avec `CLICKHOUSE_PASSWORD: s3cret` dans le même commit ; ceux qui
   attendent une erreur nommant une autre variable continuent de passer (les problèmes s'agrègent).
3. Mutations : retirer `IsProduction()` → `TestDevelopmentAcceptsLocalhostDefaults` tombe ; retirer la
   comparaison → le nouveau cas tombe.

## Commits

1. Cette fiche.
2. `config` : constante + garde + tests.
3. Fiche → `tasks-done/`.

## Definition of Done

- [ ] `make check` vert
- [ ] `ENVIRONMENT=production` sans `CLICKHOUSE_PASSWORD` refuse le boot avec un message nommant la variable
- [ ] le développement boote sans variable
- [ ] aucun test ni message n'écho la valeur du secret
- [ ] rouge lu, deux mutations vues tomber

## Hors périmètre

step-290 ; un mot de passe vide ; `docker-compose.yml` garde sa valeur de développement.
