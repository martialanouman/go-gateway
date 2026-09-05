# CLAUDE.md — Passerelle SMS (Go)

Manuel de travail pour Claude Code sur ce dépôt. Il ne porte que ce qu'aucune commande ni aucun fichier
ne dit déjà : les invariants, les couplages invisibles, l'ordre qu'on ne peut pas deviner. Les
commandes : `make help`. Les services : `ls cmd/`. Le reste vit dans `docs/` (index en bas) et dans
`.claude/rules/`, qui se charge tout seul quand on ouvre le territoire concerné.

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
unique**. Cible de conception : agrégateur national, 8 000 SMS/s soutenu (exigence : 5 000–10 000),
15 000 en pic. Ce n'est **pas** une plateforme de campagnes (pas de listes, modèles ni envoi
programmé côté client). Le quoi/pourquoi complet : `specification-technique-passerelle-sms.md` §1-2 ;
l'architecture : `guide-ingenierie-passerelle-sms.md` §2-§4.

**Ordre du pipeline MT (NON réordonnable)** : auth → ACK durable Kafka → E.164 → autorisation sender
ID → opt-out → anti-spam → résolution de route (numéro exact → script → déclaratif) →
encodage/segmentation → débit → réservation crédit MT → envoi SMSC → capture/libère → CDR.
Le court-circuit « numéro exact » saute la *résolution de route*, **jamais** la conformité (sender ID,
opt-out, anti-spam). Diagramme complet : guide d'ingénierie §5.1.

## Les 4 invariants (tests bloquants, verts à vie)

a) le corps ne fuit dans aucune sérialisation ; b) un message routé par numéro exact traverse toutes
les étapes de conformité ; c) la facturation est idempotente sous double livraison d'un même
`message_id` ; d) `max_sessions` refuse le bind au-delà du quota.

Méthode de test et jalon de chacun : `strategie-de-test-passerelle.md` §3.

## Les trois couplages qu'on oublie

Une règle `.claude/rules/` ne se charge qu'à la **lecture** d'un fichier de son territoire — créer un
fichier neuf n'en déclenche aucune. D'où ces trois déclencheurs, qui doivent se savoir d'avance :

- **Ajouter un code d'erreur** → 3 endroits en même temps. `.claude/rules/errors.md`
- **Toucher un contrat API** (`api/openapi-*.yaml`, un endpoint Admin neuf compris) → bump de
  `api/package.json`, contrat déclaré **avant** l'implémentation. `.claude/rules/contracts-api.md`
- **Changer le schéma** → `db/schema_passerelle_sms.sql` **et** une migration. `.claude/rules/db-schema.md`

## Definition of Done (chaque PR)

- **`make check` vert** — il agrège ce que la CI vérifie, et exige Docker et l'image du simulateur
  (`make smsc-sim`) : un test d'intégration qui ne peut pas démarrer sa dépendance y **échoue** au
  lieu de sauter. Ne pas énumérer ses portes ici : le décompte a déjà divergé de
  `.github/workflows/ci.yml` une fois ; ce qu'il laisse dehors est nommé dans le `Makefile`.
- Critères d'acceptation de la tâche couverts par des tests ; aucun invariant violé ; PR petite et
  focalisée (une tâche du plan d'exécution).
- **Relire ce que la step vient de périmer.** Une step ne fait pas qu'ajouter du code : elle rend faux
  ce qui décrivait l'état d'avant — ici, un README, un godoc, une fiche encore ouverte. La PR qui
  périme une affirmation la corrige, dans la même PR. Ce fichier a porté pendant deux jalons un « le
  simulateur SMSC n'est pas encore prêt » qui interdisait des tests que le dépôt écrivait déjà : un
  document faux coûte plus cher qu'un document absent, parce qu'on lui obéit.

## Index documentaire (source de vérité)

Contrats, référencés par le code : `db/schema_passerelle_sms.sql`, `api/openapi-public.yaml`,
`api/openapi-admin.yaml`.

Sous `docs/` : quoi/pourquoi `specification-technique-passerelle-sms.md` · plan de construction
`plan-execution-passerelle.md` · patterns Go `guide-codage-go.md` · style `convention-style-go.md` ·
archi & exploitation `guide-ingenierie-passerelle-sms.md` · décisions `adr/` (ADR-0001…) · vocabulaire
`glossaire-domaine-sms.md` · tests `strategie-de-test-passerelle.md` · pair de test SMSC
`specification-technique-simulateur-smsc.md` · consommateur de l'Admin API
`specification-technique-tableau-de-bord.md`.

Règles à chargement paresseux, sous `.claude/rules/` : `go-code.md` (internal, cmd) · `tests.md`
(`*_test.go`) · `contracts-api.md` (api) · `db-schema.md` (db, migrations) · `errors.md`
(platform/errors) · `tasks-steps.md` (fiches de travail).
