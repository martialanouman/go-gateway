# CLAUDE.md — Passerelle SMS (Go)

Manuel de travail pour Claude Code sur ce dépôt. Il ne porte que ce qu'aucune commande ni aucun fichier
ne dit déjà : les invariants, les couplages invisibles, l'ordre qu'on ne peut pas deviner. Les
commandes : `make help`. Les services : `ls cmd/`. Ce que le linter fait échouer n'est pas répété ici.

## Avant d'ouvrir un fichier

Tout travail de code s'ouvre par le skill `using-agent-skills`, invoqué *avant* la première lecture
de code — pas une fois le travail commencé.

- **TDD** — un rouge lu, échouant pour la bonne raison (« symbole inexistant » le prouve ;
  « connexion refusée » ne prouve rien), avant toute implémentation ; aucun « vert » avant d'avoir vu
  une mutation tomber. Une assertion jamais vue échouer n'en est pas une.
- **DoD** — aucune PR ne sort sans la Definition of Done ci-dessous, satisfaite en entier.

## Ce qu'on construit

Une **passerelle SMS** en Go : elle reçoit des SMS (SMPP entrant + REST), les route vers des SMSC
opérateurs (SMPP sortant), et gère la voie retour (MO/DLR). Double protocole sur **un pipeline
unique**. Cible : agrégateur national, 8 000 SMS/s soutenu, 15 000 en pic. Ce n'est **pas** une
plateforme de campagnes (pas de listes, modèles ni envoi programmé côté client).

## Architecture

Trois plans : **contrôle** (config dans PostgreSQL, exposée par `admin-api-svc`, poussée au plan de
données par `config-sync`), **données** (le traitement), **observabilité** (Prometheus/OTel/ClickHouse).

Magasins, et pourquoi chacun : **PostgreSQL 18** = plan de contrôle + autorité des soldes ;
**Redis/Dragonfly** = état opérationnel (sessions, débit, Bloom, cache de solde) ; **Kafka** = plan de
données durable (`mt.inbound` → `mt.routed` → …) ; **ClickHouse** = CDR/analytique.

**Ordre du pipeline MT (NON réordonnable)** : auth → ACK durable Kafka → E.164 → autorisation sender
ID → opt-out → anti-spam → résolution de route (numéro exact → script → déclaratif) →
encodage/segmentation → débit → réservation crédit MT → envoi SMSC → capture/libère → CDR.

Tout le code métier vit sous `internal/` ; `cmd/<service>/main.go` ne fait que câbler. Interfaces
définies **côté consommateur**. Détail : `convention-style-go.md` §2.

## Règles d'or (toujours / jamais)

- **JAMAIS le corps d'un message dans un log, un span ou un label** *(invariant a)*. Type `Body`
  masquant, `Reveal()` pour le clair. Réf : guide de codage §11.
- **Ordre du pipeline figé** *(invariant b)*. Le court-circuit « numéro exact » saute la *résolution de
  route*, **jamais** la conformité (sender ID, opt-out, anti-spam).
- **Facturation idempotente par `message_id`** *(invariant c)* ; désactivée = zéro appel réseau
  (contrôle booléen en cache).
- Aucune goroutine sans condition d'arrêt.
- **Opérations Redis atomiques en Lua** (token-bucket, réserve/capture de crédit). Jamais un
  read-modify-write côté Go.
- **Secrets** (mots de passe bind, clés API) stockés en hash, révélés une seule fois à la
  création/rotation. Comparaison en temps constant.
- **Modèle d'erreur plat** `{ code, message, errors[] }` en `application/json` (surcharge
  `huma.NewError`). `code` = contrat partagé avec les `command_status` SMPP et `cdr.error_code`.
  Réf : guide d'ingénierie §11.
- **Les contrats sont la source de vérité** : implémente pour conformer `openapi-*.yaml` et
  `schema_passerelle_sms.sql`, jamais l'inverse.
- **Bibliothèques : la doc via `ctx7` avant d'écrire, jamais de mémoire** — chi, huma, pgx, franz-go,
  go-redis, goja. La procédure est une règle globale ; ce qui est propre à ce dépôt, c'est que pgx
  v4→v5 et huma v1→v2 ont cassé, et qu'un usage périmé compile parfois.
- **Fiches de travail** : une step vit dans `tasks-todo/step-NNN.md` et porte son design sous
  `## Design arrêté`. Elle passe en `tasks-done/` par un `git mv`, dernier commit de sa PR.

## Les 4 invariants (tests bloquants, verts à vie)

a) le corps ne fuit dans aucune sérialisation ; b) un message routé par numéro exact traverse toutes
les étapes de conformité ; c) la facturation est idempotente sous double livraison d'un même
`message_id` ; d) `max_sessions` refuse le bind au-delà du quota.

## Tests

Pyramide : beaucoup d'unitaires (logique de domaine), des intégrations (`testcontainers-go` :
Postgres/Redis/Kafka/ClickHouse), peu de bout-en-bout. Détail : `strategie-de-test-passerelle.md`.

Deux pairs SMPP, chacun son usage : le **faux SMSC in-repo** (`internal/testutil/fakesmsc`,
`make fake-smsc`) pour les tests ordinaires, et le **vrai simulateur** (`internal/testutil/smscsim`,
`make smsc-sim`) pour l'injection de pannes des tests de résilience.

## Definition of Done (chaque PR)

- **`make check` vert** — il agrège les quatre portes de la CI (lint, `test -race`, `govulncheck`,
  contrats). Les énumérer ici les ferait diverger de la CI ; c'est déjà arrivé.
- Critères d'acceptation de la tâche couverts par des tests ; aucun invariant violé ; PR petite et
  focalisée (une tâche du plan d'exécution).
- **Relire ce que la step vient de périmer.** Une step ne fait pas qu'ajouter du code : elle rend faux
  ce qui décrivait l'état d'avant — ici, un README, un godoc, une fiche encore ouverte. La PR qui
  périme une affirmation la corrige, dans la même PR. Ce fichier a porté pendant deux jalons un « le
  simulateur SMSC n'est pas encore prêt » qui interdisait des tests que le dépôt écrivait déjà : un
  document faux coûte plus cher qu'un document absent, parce qu'on lui obéit.

## Les trois couplages qu'on oublie

- **Ajouter un code d'erreur** : 3 endroits en même temps — `internal/platform/errors` (sentinelle +
  mapping HTTP/SMPP), le champ `code` des deux `api/openapi-*.yaml`, et la §11.3 du guide
  d'ingénierie.
- **Toucher un contrat API** : les contrats sont publiés comme package npm versionné et consommés par
  le tableau de bord (dépôt séparé). Tout changement d'un `api/openapi-*.yaml` — un endpoint Admin
  neuf compris — exige un bump de `api/package.json`, majeur si `oasdiff` classe la rupture `ERR`.
  Le contrat se déclare **avant** l'implémentation. `make contracts` le vérifie ; procédure :
  `api/README.md`.
- **Changer le schéma** : éditer `db/schema_passerelle_sms.sql` **et** ajouter une migration
  `golang-migrate` correspondante dans `migrations/`.

## Index documentaire (source de vérité)

Contrats, référencés par le code : `db/schema_passerelle_sms.sql`, `api/openapi-public.yaml`,
`api/openapi-admin.yaml`.

Sous `docs/` : quoi/pourquoi `specification-technique-passerelle-sms.md` · plan de construction
`plan-execution-passerelle.md` · patterns Go `guide-codage-go.md` · style `convention-style-go.md` ·
archi & exploitation `guide-ingenierie-passerelle-sms.md` · décisions `adr/` (ADR-0001…) · vocabulaire
`glossaire-domaine-sms.md` · tests `strategie-de-test-passerelle.md` · pair de test SMSC
`specification-technique-simulateur-smsc.md` · consommateur de l'Admin API
`specification-technique-tableau-de-bord.md`.
