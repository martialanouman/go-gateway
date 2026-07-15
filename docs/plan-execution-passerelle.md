# Plan d'exécution — Implémentation de la passerelle SMS

**Composant :** Passerelle SMS principale (Go)
**Statut :** Plan d'exécution v1.0
**Méthode :** tranche verticale MVP d'abord (walking skeleton), puis épaississement capacité par capacité.
**Contexte outil :** implémentation assistée par **Claude Code CLI**.

> Ce plan découpe la construction en jalons (`M0`…`M12`). Chaque jalon liste ses **dépendances**, ses **tâches** (avec les packages Go touchés et la référence de spec), ses **livrables** et ses **critères d'acceptation testables** — la *definition of done*. Pas d'estimation en jours : la vélocité en dev assisté est trop variable pour être utile ; on pilote à l'acceptation, pas au calendrier.

---

## 0. Comment exécuter ce plan avec Claude Code CLI

### 0.1 La boucle par tâche

Une tâche du plan = **une session Claude Code ciblée = une PR petite et verte**. Pour chaque tâche, donne à Claude Code trois choses, dans cet ordre :

1. **Le contexte** : la ou les références de spec (`§6.x`), le fichier de contrat concerné (`openapi-*.yaml`, `schema_passerelle_sms.sql`), et le package cible.
2. **Le livrable** : ce qui doit exister à la fin (un service, un package, un endpoint, un test).
3. **Les critères d'acceptation** : recopie-les depuis ce plan. Demande à Claude Code d'**écrire les tests d'abord ou en parallèle**, puis de faire passer le code — les critères sont des tests, pas des intentions.

Termine chaque session par la *definition of done* globale (§0.4). Ne fusionne pas tant qu'elle n'est pas verte.

### 0.2 Le fichier `CLAUDE.md` (à produire en premier — voir §14)

C'est le levier le plus important. `CLAUDE.md` à la racine du dépôt est le contexte permanent que Claude Code lit à chaque session : commandes de build/test/run, pointeurs vers les guides, carte de l'architecture, règles « toujours / jamais », glossaire du domaine. Sans lui, tu réexpliques le projet à chaque prompt. **Crée-le pendant `M0`**, garde-le court et à jour.

### 0.3 Règle d'or du séquencement

On construit un **squelette qui marche** (`M2`) le plus tôt possible : REST → route statique → SMSC simulé → CDR → statut. À partir de là, **chaque jalon épaissit une capacité sans jamais casser le flux de bout en bout**. On ne construit jamais un sous-système « complet mais débranché ». Les étapes du pipeline non encore implémentées sont des **pass-through explicitement marqués** (`// STUB Mx: …`), jamais du code silencieusement absent.

### 0.4 Definition of Done (chaque PR)

`gofmt`/`goimports` verts • `golangci-lint` sans alerte (config du guide de style §9) • `go test -race ./...` vert • critères d'acceptation de la tâche couverts par des tests • aucun invariant violé (corps jamais loggé/tracé, voir §0.5) • godoc sur les symboles exportés • pas de `context` manquant ni de goroutine sans condition d'arrêt.

### 0.5 Les invariants testables (jamais négociables)

Quatre tests d'invariant doivent exister tôt et rester verts à vie (guide de codage §13) : **(a)** le corps d'un message ne fuit dans aucune sérialisation (log, span, label) ; **(b)** un message routé par numéro exact traverse quand même toutes les étapes de conformité ; **(c)** la facturation est idempotente sous double livraison d'un même `message_id` ; **(d)** `max_sessions` refuse le bind au-delà du quota. Le test (a) se pose dès `M0`, les autres à leur jalon.

### 0.6 Documents de référence (source de vérité)

| Besoin | Document |
|---|---|
| Quoi construire et pourquoi | `specification-technique-passerelle-sms.md` |
| Schéma de données | `schema_passerelle_sms.sql` |
| Contrats API | `openapi-public.yaml`, `openapi-admin.yaml` |
| Comment coder (patterns) | `guide-codage-go.md` |
| Style du code | `convention-style-go.md` |
| Architecture & exploitation | `guide-ingenierie-passerelle-sms.md` |
| Pair de test (SMSC) | `specification-technique-simulateur-smsc.md` |
| Consommateur de l'Admin API | `specification-technique-tableau-de-bord.md` |

---

## 1. Le squelette qui marche

Le premier objectif structurant (`M2`) est cette tranche verticale minimale, tout le reste s'y greffe :

```
POST /messages ──► mt.inbound (Kafka) ──► router-svc (E.164 + route statique)
                                              │
                                              ▼
                                          mt.routed ──► connector-pool-svc ──► SMSC simulé
                                                              │
                                                              ▼
                                                          CDR (ClickHouse)  ◄── GET /messages/{id}
```

Dès que ce flux passe un test de bout en bout, l'architecture (double protocole plus tard, Kafka comme frontière de durabilité, CDR comme magasin de statut) est prouvée. Les jalons `M3`+ ajoutent SMPP, MO/DLR, conformité, débit, résilience, facturation, contenu — chacun **branché sur ce squelette**.

---

## 2. Vue d'ensemble des jalons

| Jalon | Objectif | Débloque |
|---|---|---|
| **M0** | Fondations : dépôt, CI, `CLAUDE.md`, dépendances docker, migrations, config, observabilité, modèle d'erreur | tout |
| **M1** | Plan de contrôle minimal + Admin API (noyau de provisioning) | de quoi configurer un envoi |
| **M2** | **Squelette vertical MT (REST → SMSC simulé → CDR)** | l'architecture prouvée |
| **M3** | Ingress SMPP (serveur) + gestion des sessions | double protocole sur un pipeline unique |
| **M4** | Voie retour MO/DLR + webhooks + numéros entrants | bidirectionnel complet |
| **M5** | Conformité : autorisation sender ID, opt-out, anti-spam | envoi conforme |
| **M6** | Encodage/segmentation UDH + gestion du débit | messages longs + protection des connecteurs |
| **M7** | Routage avancé : numéros exacts, scripts, stratégies, hot reload | routage production |
| **M8** | Résilience : disjoncteur, fallback, pool de binds, reconnexion | tolérance aux pannes SMSC |
| **M9** | Facturation opt-in : soldes MT/MO, réserve/capture, grand livre | monétisation |
| **M10** | Contenu : stockage chiffré, RGPD, rétention/tiering | conformité données |
| **M11** | Observabilité complète : spans, métriques, temps réel, export | exploitabilité |
| **M12** | Durcissement : charge (NFR), chaos, sécurité, go-live | mise en production |

**Parallélisation possible** (si plusieurs sessions/branches) : le codec SMPP (`M3`, `internal/smpp`) et l'élargissement de l'Admin API (`M1`) peuvent avancer en parallèle du squelette `M2` car ils partagent peu de code. Le reste est majoritairement séquentiel par dépendance de flux.

---

## 3. M0 — Fondations & outillage

**Objectif :** un dépôt qui build, teste, lint, et démarre ses dépendances, avec les fondations transverses.
**Dépend de :** —

**Tâches**

- **Scaffold du dépôt** selon le layout du guide de style §1 : `cmd/`, `internal/`, `api/`, `migrations/`, `deploy/`. `go.mod`, `Makefile`/`Taskfile` (`build`, `test`, `lint`, `run`, `migrate`, `up`). Réf : guide de style §1.
- **CI** : `gofmt -l` vide, `golangci-lint` (config `.golangci.yml` du guide de style §9), `go test -race ./...`, `govulncheck`. Réf : guide de codage §3.
- **`docker-compose` de dev** : Postgres 18, Redis/Dragonfly, Kafka (Redpanda accepté), ClickHouse. Réf : guide d'ingénierie §4.
- **Migrations** : intégrer `schema_passerelle_sms.sql` via `golang-migrate` dans `migrations/`, appliquées en dev et CI. (Le DDL est déjà validé par exécution sur Postgres.)
- **`internal/config`** : chargement 12-factor depuis l'environnement, validation stricte au boot (`log.Fatal` si invalide — seul endroit toléré). Réf : guide de codage §10.
- **`internal/observability`** : `slog` JSON, bootstrap OpenTelemetry, endpoint Prometheus `/metrics`. Réf : guide de codage §12.
- **`internal/platform`** : `uuidv7` (application-side pour `message_id`/`trace_id`), `e164` (normalisation), et le **type `Body` masquant** (`String()`/`MarshalJSON()` → `[REDACTED]`, `Reveal()` explicite). Réf : guide de codage §11.
- **`internal/platform/errors`** : erreurs sentinelles + **table de mapping unique** erreur de domaine → `code` / statut HTTP / `command_status` SMPP (guide d'ingénierie §11.3), et la surcharge `huma.NewError` produisant le modèle plat `{ code, message, errors[] }`. Réf : guide d'ingénierie §11.
- **`CLAUDE.md`** (voir §14).

**Critères d'acceptation**

- `docker compose up` démarre les quatre dépendances ; `make migrate` applique le schéma sans erreur.
- Un endpoint `/health` répond `{"status":"ok"}`.
- **Invariant (a)** : un test échoue si sérialiser une struct contenant un `Body` révèle le clair (log JSON et attribut de span).
- CI vert sur une PR triviale ; `golangci-lint` actif et bloquant.

---

## 4. M1 — Plan de contrôle minimal + Admin API (noyau)

**Objectif :** pouvoir provisionner, via l'API, le minimum pour envoyer : client → compte → identifiants → connecteur → route statique.
**Dépend de :** M0.

**Tâches**

- **`internal/storage/postgres`** : repositories (`sqlc` + requêtes paramétrées) pour `customers`, `smpp_accounts`, `credentials`, `smsc_connectors`, `routes`/`route_targets`, `sender_ids`. Réf : DDL, guide de codage §7.1.
- **`cmd/admin-api-svc`** (chi + huma) : implémenter le sous-ensemble de `openapi-admin.yaml` nécessaire au provisioning — `customers` CRUD, `smpp-accounts` CRUD + `channels`/`session-limits`, `credentials` create/rotate/list, `connectors` CRUD, `routes` CRUD (statique), `sender-ids`. Auth opérateur (bearer) : implémentation simple d'abord, scopes câblés.
- **Contrat** : test vérifiant que l'OpenAPI généré par Huma pour ces opérations est cohérent avec `openapi-admin.yaml` (schémas `Error` et entités).

**Critères d'acceptation**

- Parcours API complet : créer `customer` → `smpp_account` → `credential(api_key)` (secret renvoyé **une seule fois**, masqué ensuite) → `connector` → `route` statique.
- La **cardinalité** est refusée par le schéma : un 2ᵉ `smpp_bind` ou `api_key` sur un compte → `409`.
- Les réponses d'erreur ont la forme plate `{ code, message, errors[] }` en `application/json`.
- Suspendre un client cascade sur ses comptes (statut effectif = min).

---

## 5. M2 — Squelette vertical MT (REST → SMSC simulé → CDR)

**Objectif :** le walking skeleton de bout en bout. **Jalon le plus important.**
**Dépend de :** M0, M1.

**Tâches**

- **`internal/storage/kafka`** : producteur (`acks=all`, idempotent) et consommateur (`franz-go`), constantes de topics, clé de partition (guide de codage §7.3).
- **`cmd/rest-api-svc`** : `POST /messages` (auth clé API → compte → client, contrôle canal `rest_enabled`), validation, **génération `message_id`/`trace_id` (UUIDv7) à l'ingestion**, publication `mt.inbound`, `202 AcceptedMessage` **après** confirmation d'écriture durable. `GET /messages/{id}` lu depuis le CDR. Réf : openapi-public.yaml, §4.2/§6.12.
- **`cmd/router-svc`** : consomme `mt.inbound` → normalisation E.164 → **résolution de route déclarative statique uniquement** → publication `mt.routed`. Les étapes 2–5 et 7–8 du pipeline sont des **STUB pass-through marqués** (implémentées en M5/M6). Span par étape dès maintenant.
- **`cmd/connector-pool-svc`** : un bind unique (`bind_pool_size=1`), consomme `mt.routed`, `submit_sm` vers le SMSC simulé, suit `submit_sm_resp`, écrit le CDR (`status=enroute`). Réf : §4.2.
- **`internal/storage/clickhouse`** : sink CDR + lecture de statut (schéma de l'appendice du `.sql`).
- **Faux SMSC in-repo** : le vrai simulateur (`specification-technique-simulateur-smsc.md`) n'étant **pas encore prêt**, écrire un double minimal `internal/testutil/fakesmsc` (bind, `submit_sm`/`submit_sm_resp` scriptable, `deliver_sm` MO/DLR à la demande, `enquire_link`) comme pair SMPP pour M2→M7. Voir `strategie-de-test-passerelle.md` §2. Le vrai simulateur ne sera branché qu'à M8.

**Critères d'acceptation**

- Test bout-en-bout (`testcontainers` + simulateur) : `POST /messages` → le message apparaît en CDR `enroute` → `GET /messages/{id}` renvoie le statut.
- `trace_id`/`message_id` générés à l'ingestion et présents dans le CDR et les en-têtes Kafka.
- L'accusé `202` n'a lieu qu'après écriture durable dans `mt.inbound` (test : couper le connecteur, le `202` sort quand même).
- Un span par étape est émis (vérifiable via l'exporteur de test OTel).

---

## 6. M3 — Ingress SMPP (serveur) + sessions

**Objectif :** un ESME client peut se binder et soumettre ; SMPP et REST partagent **le même** pipeline.
**Dépend de :** M2. *(Le codec `internal/smpp` peut démarrer dès M0 en parallèle.)*

**Tâches**

- **`internal/smpp`** : codec PDU v3.4 (v5.0 optionnel), support TLV/UDH, payload > 254 o ; machine à états de session ; pas de `replace_sm`/`data_sm`. Tests unitaires + **fuzz** sur le décodeur. Réf : §5.1, guide de codage §9/§13.
- **`cmd/smpp-server-svc`** : accepte les binds, auth (`system_id` → `credentials` `smpp_bind`, mot de passe, `allowed_bind_types`), `submit_sm` → `mt.inbound` (**pipeline identique**), `enquire_link`, `unbind`, fenêtre. `query_sm`/`cancel_sm` selon bascules. Réf : §6.3/§6.22.
- **`cmd/session-manager-svc`** : registre Redis faisant autorité, API gRPC bind/unbind/lookup, `max_sessions` appliqué **au bind contre le registre inter-pods**, supervision `enquire_link`. Réf : §6.3/§6.8.
- **Anti-brute-force** : compteur par `system_id`/IP (Redis TTL), backoff, événement de sécurité.

**Critères d'acceptation**

- Un client SMPP (simulateur en mode ESME ou client de test) bind, soumet, reçoit `submit_sm_resp` ; le message suit le même chemin CDR que REST.
- **Invariant (d)** : bind au-delà de `max_sessions` → `ESME_RBINDFAIL` ; un unbind libère le jeton.
- Op désactivée (`query_sm_enabled=false`) → `ESME_RINVCMDID`.
- **Parité protocole** : un test soumet le même message en REST et en SMPP et vérifie un traitement identique en aval.
- Rotation d'identifiant avec `gracePeriodSec` : l'ancien secret reste valide pendant la fenêtre, coupé après.

---

## 7. M4 — Voie retour MO/DLR + webhooks

**Objectif :** MO et DLR routés vers le bon compte ; bidirectionnel complet.
**Dépend de :** M3.

**Tâches**

- **`connector-pool-svc`** : réception `deliver_sm`, classification MO vs DLR, publication `mo.inbound` / `dlr.events`.
- **Repos + Admin** : `inbound_numbers`, `inbound_keywords`, endpoints `/admin/inbound-numbers*`, `/admin/mo/unrouted`. Réf : §6.21.
- **`cmd/mo-dlr-router-svc`** : MO → E.164 → résolution du compte (dédié → son compte ; partagé → mot-clé ; sinon file « non routés ») → remise SMPP `deliver_sm` (au pod détenteur via gRPC registre) ou webhook. DLR → corrélation `message_id` → maj CDR (`delivered_at`, `status`, `latency_ms`) → transmission au compte. Réf : §4.3/§6.8.
- **`internal/…/webhook`** : envoi signé HMAC-SHA256, retries avec backoff, dead-letter après N tentatives. Réf : §5.2.

**Critères d'acceptation**

- Le simulateur émet un MO sur un numéro entrant → remis au bon compte (webhook ou bind actif).
- Un DLR met à jour le CDR et est transmis ; corrélation par `message_id` correcte.
- MO non résolu → visible dans `/admin/mo/unrouted`, jamais abandonné silencieusement.
- Webhook : signature HMAC vérifiable, retry avec backoff, dead-letter après épuisement.

---

## 8. M5 — Conformité sur le chemin critique

**Objectif :** activer les étapes STUB de conformité, avant tout coût.
**Dépend de :** M4.

**Tâches**

- **Autorisation sender ID** (§6.19) : étape `router-svc`, politique par compte (`strict`/`allow_unregistered_numeric`/`disabled`), `source_addr` vs `sender_ids` `active` du client.
- **Opt-out / suppressions** (§6.20) : repos `suppressions`/`opt_out_keywords`, **filtre de Bloom par portée en mémoire** (rafraîchi via config-sync), étape MT **bloquante** (union des portées platform/customer/account/inbound_number), détection STOP côté MO écrivant une suppression + auto-réponse (MT jamais facturé). Endpoints `/admin/suppressions*`.
- **Anti-spam** (§6.5) : vélocité (MT + MO entrant), contenu (regex précompilées), doublons (Redis TTL), réputation ; actions `block`/`flag`/`throttle` ; politique **fail-open avec flag** sur perte Redis pour l'état partagé, règles de contenu statiques maintenues.

**Critères d'acceptation**

- Sender non autorisé → rejet (`code=sender_id_not_authorized`, `403`/`ESME_RINVSRCADR`), CDR `rejected`.
- Destinataire désabonné bloqué si **l'une** des portées matche ; **Invariant (b)** partiel : opt-out s'applique aussi sur un message routé par numéro exact (préparé pour M7).
- Un STOP crée une suppression scopée sur le numéro entrant ; le MO est **quand même remis** et **jamais facturé**.
- Propriété **pas de faux négatif** du Bloom : test de fuzz sur des MSISDN présents.
- Le matching de contenu lit le clair **en mémoire** ; test que rien n'est stocké ni loggé.

---

## 9. M6 — Encodage/segmentation + gestion du débit

**Objectif :** messages longs corrects et connecteurs protégés.
**Dépend de :** M5.

**Tâches**

- **`internal/…/encoding`** : détection GSM-7/UCS-2/8-bit (respect `data_coding_default` connecteur), calcul `segment_count`, découpe UDH ; réassemblage MO. Fuzz. Réf : §6.6.
- **Rate limiting** (§6.4) : token-bucket **Lua atomique** par compte/connecteur/route ; précédence : `throughput_limit_per_sec` connecteur = plafond dur, `rate_limits` ≤ ce plafond (validé à l'écriture) ; **fail-closed** sur perte Redis (plafond technique statique local).
- **Throttling adaptatif** : ajusteur AIMD piloté par les signaux `submit_sm_resp` (`ESME_RTHROTTLED`).

**Critères d'acceptation**

- Un message long est segmenté avec le bon `segment_count` ; MO concaténé réassemblé.
- Limite de débit appliquée atomiquement **sous concurrence** (test avec `-race` et N goroutines).
- Le plafond technique du connecteur n'est jamais dépassé.
- `ESME_RTHROTTLED` fait baisser le débit effectif (AIMD) puis remonter progressivement.
- La segmentation précède débit et (futur) crédit.

---

## 10. M7 — Routage avancé

**Objectif :** routage de niveau production (portabilité, scripts, stratégies, hot reload).
**Dépend de :** M6.

**Tâches**

- **Numéros exacts** (§6.1) : repo `exact_routes`, `exactroute:{msisdn}` Redis + **Bloom en mémoire**, import MNP en masse (async), **court-circuit L0** qui saute la résolution mais **pas** la conformité.
- **Scripts de routage** (§6.2) : runtimes `goja` (JS) et `gopher-lua` **poolés**, **plafond d'instructions = garde primaire**, timeout mur en filet, plafond mémoire ; contrat `resolveRoute(message) → routeId | null` ; résolution de portée `account → customer → platform` ; cycle `draft → validate → test → publish` ; isolement inter-comptes. Endpoints `/admin/routing-scripts*`.
- **Stratégies de distribution** : les 6 (`static`/`round_robin`/`weighted`/`failover_priority`/`least_loaded`/`hash_based`) + `fallback_route`.
- **Hot reload** : **instantané immuable + pointeur atomique** (guide de codage §5.1) ; `config-sync` pub/sub ; surcouche mutable séparée pour l'état volatil.

**Critères d'acceptation**

- **Invariant (b)** complet : un message routé par numéro exact traverse E.164, sender ID, opt-out, anti-spam, segmentation, débit — test dédié.
- Scénario de portabilité : un numéro porté est routé par `exact_routes`, pas par préfixe.
- Un script retourne un `routeId` valide ou `null` → repli déclaratif ; un script dépassant le plafond d'instructions → repli + log + métrique.
- Hot reload échange les routes **sans downtime** (test : trafic continu pendant un reload).
- Chaque stratégie distribue conformément (tests déterministes pour `weighted`/`hash_based`).

---

## 11. M8 — Résilience connecteurs

**Objectif :** tolérer un SMSC qui se dégrade ou tombe, monter en débit par connecteur.
**Dépend de :** M7.

**Tâches**

- **Disjoncteur** (§6.15) : machine à états par connecteur, agrégation multi-pod par hash `breaker:binds` + règle de majorité, `breaker:events` pub/sub, lecture par `router-svc` uniquement à la construction de l'instantané.
- **`fallback_chain`** en en-tête + reroute unilatéral ; reroutage de masse **borné** (draineur à débit limité) + `mt.reroute-park`.
- **Pool de binds** (§6.8) : `bind_pool_size > 1`, partition `mt.routed` par `(connector_id, shard_index)`, **clé = ID logique** (segments d'un concaténé sur le même bind).
- **Auto-reconnexion** (§6.13) : opt-in, backoff + jitter, `link_status` vs `breaker_state` **distincts** ; `ESME_RINVPASWD` arrête la boucle.
- **Dead-letter** + retraitement.

**Critères d'acceptation**

- Un connecteur qui se dégrade ouvre le disjoncteur ; le trafic bascule via `fallback_chain` ; l'excédent est parqué puis rejoué.
- L'agrégat de disjoncteur est correct avec des binds sur plusieurs pods (test multi-instances).
- `bind_pool_size=4` augmente le débit ; les segments d'un message restent sur un seul bind (test d'ordre).
- Un bind coupé avec auto-reconnexion revient ; sans auto-reconnexion, `link_status=down` et rebind manuel requis.
- `ESME_RINVPASWD` stoppe l'auto-retry.

---

## 12. M9 — Facturation (opt-in)

**Objectif :** facturation prépayée/postpayée, soldes MT/MO séparés, sans impact quand désactivée.
**Dépend de :** M6 (segmentation → coût). *(Peut suivre M8.)*

**Tâches**

- **`cmd/billing-svc`** : `balances` (MT/MO séparés), **réserve/capture/libère en Lua** (idempotent par `message_id`), compteur MO, `billing_ledger` (partitionné), propriétaire selon `balance_scope`, découvert, `charge_on` submission/delivery + remboursement.
- **Intégration** : réserve dans `router-svc`, capture/libère dans `connector-pool-svc` ; **saut total quand désactivé** (contrôle booléen en cache, aucun appel réseau).
- **Autorité du solde** : grand livre Postgres durable ; cache Redis réhydraté au failover (fail-closed strict pendant la fenêtre).
- **Adaptateur externe** (§6.10) : modes `balance_check`/`consume_delegate_async`/`consume_delegate_sync`.
- **Admin** : `/admin/customers/{id}/billing*` (topup/transfer/scope/ledger/balances), WS `billing-alerts`.

**Critères d'acceptation**

- Prépayé MT : réserve → capture au succès, libère à l'échec ; solde insuffisant → `402`/code d'extension, **aucune** entrée de grand livre.
- **Invariant (c)** : double livraison d'un même `message_id` ne facture qu'une fois.
- Le compteur MO ne bloque **jamais** le MT ; il s'arrête à `mo_billing_floor` avec alerte.
- Changement de `balance_scope` refusé (`409`) si un solde ≠ 0.
- **Facturation désactivée = zéro appel réseau** (test qui compte les I/O du chemin chaud).

---

## 13. M10 — Contenu, chiffrement & RGPD

**Objectif :** stockage de contenu configurable et chiffré, effacement RGPD, rétention.
**Dépend de :** M5 (CDR + policy), M9 (clés/`content_keys` partagent `billing-svc`).

**Tâches**

- **Politique de contenu** (§6.23) : `off`/`stored_plaintext`/`stored_encrypted` ; enveloppe KMS + **clé par client** (`content_keys`) ; lecture `content:read` gardée et **auditée** ; crypto-shred.
- **Rétention/partitionnement/tiering** (§6.14) : partitions quotidiennes, TTL, archive Parquet ; `content_retention_days` découplé et plus court.
- **Effacement RGPD** (§6.14.4) : client (crypto-shred + purge CDR) ; MSISDN (suppression ligne à ligne du contenu **et** des métadonnées across clients) + attestation ; suppressions/opt-out **conservées**.

**Critères d'acceptation**

- Corps chiffré lisible uniquement via `content:read` (accès audité) ; **Invariant (a)** re-vérifié sous chaque politique : jamais dans log/trace.
- Crypto-shred (clé `destroyed`) rend le contenu illisible sans réécrire le CDR.
- Effacement MSISDN retire contenu + métadonnées across clients, garde l'opt-out ; attestation émise.
- Purge par **drop de partition** à l'échéance (pas de `DELETE WHERE`).

---

## 14. M11 — Observabilité complète & temps réel

**Objectif :** exploitabilité — traçage, métriques, streams, export.
**Dépend de :** transverse ; finalisé ici.

**Tâches**

- **Tracing** : span par étape (nommage stable), 100 % sur erreur/rejet/timeout ; jamais le corps.
- **Métriques** : catalogue à labels **bornés** (compte/connecteur/route/statut) ; latences d'ingestion et bout-en-bout, profondeur de file, état de disjoncteur, timeouts de script, fraîcheur du cache de solde.
- **Temps réel** : gateway WS/SSE (`/admin/stream/metrics|sessions|billing-alerts`) alimentée par un topic de métriques Kafka.
- **Trace/recherche/export** : `/admin/messages/{id}/trace`, `/admin/messages/search`, export async (row-cap, masque MSISDN par rôle).
- **Alerting** : Alertmanager **indépendant** du tableau de bord.

**Critères d'acceptation**

- Le trace complet d'un message est visible via l'API sans aucun corps.
- Métriques fraîches < 5 s ; aucun label à cardinalité non bornée (test de garde).
- Les WS poussent les mises à jour ; l'export produit un fichier masqué ; une alerte se déclenche (solde bas, disjoncteur ouvert).

---

## 15. M12 — Durcissement, charge & mise en production

**Objectif :** atteindre les NFR et passer la checklist de prod.
**Dépend de :** tous.

**Tâches**

- **Charge** (§1.2) : soutenu 8 000 SMS/s, pic 15 000 ; budgets de latence (ingestion p99 < 250 ms, bout-en-bout p99 < 2 s). Tuning partitions Kafka, batch ClickHouse, pool `pgx`.
- **Chaos** : perte Redis (vérifier chaque politique de panne), flapping connecteur, redémarrage de pods (drain gracieux, `PodDisruptionBudget`), failover Postgres.
- **Sécurité** : TLS/SMPP-TLS/mTLS, hachage des identifiants, scan d'injection (`gosec`), gestion des secrets, piste d'audit, `govulncheck`.
- **Go-live** : dérouler la **checklist de mise en production** (guide d'ingénierie §15).

**Critères d'acceptation**

- Débit soutenu tenu (disjoncteur fermé) avec les budgets de latence respectés.
- Rolling deploy sans coupure des binds (drain + PDB).
- L'injection de pannes dégrade **conformément aux politiques documentées** sans perte de message (durable dans Kafka).
- Checklist de prod entièrement cochée et signée.

---

## 16. Graphe de dépendances (résumé)

```
M0 ─┬─► M1 ─► M2 ─► M3 ─► M4 ─► M5 ─► M6 ─┬─► M7 ─► M8 ─► M11 ─► M12
    │                                      └─► M9 ─► M10 ─────────┘
    └─► (internal/smpp : peut démarrer tôt, requis à M3)
```

`M2` est le point de bascule : avant, on outille ; après, chaque jalon épaissit un flux déjà vivant. `M9`/`M10` (facturation, contenu) peuvent avancer en parallèle de `M7`/`M8` (routage, résilience) une fois `M6` acquis, car ils touchent des services distincts (`billing-svc` vs `connector-pool-svc`).

---

## 17. Le test harness, transversal

Trois piliers, à mettre en place dès `M2` et à enrichir à chaque jalon :

Le pair de test SMSC arrive en deux temps. **Le simulateur SMSC (`specification-technique-simulateur-smsc.md`) n'est pas encore prêt**, donc de `M2` à `M7` on utilise un **faux SMSC minimal in-repo** (`internal/testutil/fakesmsc`) : il joue le SMSC en sortie et l'ESME en entrée, avec des réponses scriptables (succès, throttling, erreurs, délais) et l'émission de MO/DLR à la demande. Les scénarios de résilience qui exigent une injection de pannes réaliste (disjoncteur, reroute, reconnexion) sont **écrits puis `t.Skip("needs SMSC simulator — M8")`** jusqu'à ce que le vrai simulateur soit disponible, branché à `M8`. Les **tests de contrat** vérifient que l'implémentation chi+huma reste fidèle à `openapi-*.yaml` ; les **tests d'intégration** (`testcontainers-go`) montent Postgres/Redis/Kafka/ClickHouse ; les **quatre invariants** (§0.5) restent verts en permanence. Détail complet dans `strategie-de-test-passerelle.md`.
