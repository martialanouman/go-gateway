---
paths:
  - "**/*_test.go"
---

# Tests

Détail : `docs/strategie-de-test-passerelle.md`.

- **TDD** : le rouge lu d'abord, la mutation vue tomber ensuite — la règle
  complète est dans `CLAUDE.md`, toujours chargé.
- Pyramide : beaucoup d'unitaires (logique de domaine), des intégrations
  (`testcontainers-go` : Postgres/Redis/Kafka/ClickHouse), peu de bout-en-bout.
- Toute nouvelle étape de pipeline porte un test qui vérifie qu'elle **ne logge
  pas le corps** *(invariant a)*.

## Le pair SMPP : lequel, et pourquoi

Deux pairs, chacun son usage — un test choisit par ce qu'il **exerce**, pas par le
jalon (stratégie de test §2) :

- **faux SMSC in-repo** (`internal/testutil/fakesmsc`, `make fake-smsc`) — dès
  qu'il s'agit de réponses applicatives (`OK`, `Throttled`, `SysErr`, `Delay`).
- **vrai simulateur** (`internal/testutil/smscsim`, `make smsc-sim`) — requis pour
  l'injection de pannes réaliste (disjoncteur, reroute, reconnexion).

## En CI, un test qui saute se lit comme un test qui passe

`internal/testutil/ciguard` transforme un saut en échec dès que `CI` est posée.
Ce n'est pas une exception du simulateur, c'est la règle de toute la suite
d'intégration : **dix tests de résilience ont traversé un jalon entier sans jamais
s'exécuter** avant qu'on le remarque (step-250b/250c).
