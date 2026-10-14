# Audit d'architecture — passerelle SMS et tableau de bord

2026-10-14 · base : go-gateway `main@dfe1d2c`, go-gateway-bo `main@192e68a` · lecture seule.

Objet : décider, composant par composant, ce qui entre dans le monorepo **porté**, **porté puis
refactoré**, ou **réécrit**, et lister les décisions que la spec unique doit trancher avant.

Classes : **LOCAL** — se corrige dans le composant · **TRANSVERSE** — plusieurs services ou le
contrat, décision de conception requise · **FONDATEUR** — le découpage lui-même est faux.

Sources : 106 fiches `debts/` de la passerelle (92 ouvertes), 29 du dashboard, 24 ADR, 13 steps
restantes, ~30 mémoires vérifiées dans le code, lecture directe du code des deux dépôts. Sept constats
fondateurs ont été revérifiés à la main (marqués ✔).

---

## 1. Verdict

Ni « tout porter », ni « tout réécrire ». Les défauts fondateurs se concentrent sur **le cœur
économique du chemin chaud** — facturation, état du message, connecteurs — et sur **la frontière
d'autorisation BFF ↔ passerelle**. Ce sont aussi les causes directes du plafond mesuré (310–565
`submit_sm`/s au run 14 contre 8 000 visés) et de la moitié des dettes ouvertes.

Le reste — admission SMPP/REST, pipeline de conformité, registre de sessions, content-key-svc, session
et garde du BFF, hub WebSocket, SPA — est sain et se porte avec ses tests.

| Composant | Verdict | Motif |
|---|---|---|
| Admission (smpp-server-svc, REST), `internal/pipeline` | **Porter** | Frontière de durabilité ACK juste, pipeline unique testé (invariant b) |
| Registre de sessions (`internal/session`) | **Porter** + compléter | Bon pour l'invariant d ; manque `enquire_link` serveur, binds sortants (T12) |
| router-svc | **Porter puis refactorer** | Pipeline bon ; isolation des pannes (F6) et consommateurs CDR hébergés à sortir |
| Facturation (billing-svc, `internal/billing`) | **Réécrire le modèle** | F1 ; garder `billing_idempotency` et les deltas append-only |
| État du message | **Nouveau composant** | F2 ; le CDR ClickHouse est porté comme projection seule |
| connector-pool-svc | **Réécrire la partie configuration/assignation** | F3 ; porter le client SMPP et les disjoncteurs |
| mo-dlr-router-svc | **Porter puis refactorer** | Corrélation unique, isolation par client (F6) |
| config-sync | **Supprimer** | Doublon de coalescence, contourné par ses propres émetteurs (T2) |
| admin-api-svc | **Porter** + scopes fins + audit unique | F4 ; sortir les jobs de rétention |
| content-key-svc | **Porter** | Forme bonne ; stockage de la KEK à corriger (F5) |
| BFF : session, auth, garde, hub | **Porter** | Fermé par défaut, DTO imposés par le compilateur |
| BFF : couche de relais, audit, alertes | **Réécrire** | F4 ; façade à la main, audit doublé, alertes depuis un flux éphémère |
| SPA | **Porter** + fabrique de clés de cache | Seulement des défauts locaux |

---

## 2. Défauts fondateurs

### F1 — La facturation est synchrone par message sur le chemin chaud, et le solde a plusieurs autorités

- **Symptôme.**
  - Deux transactions Postgres par message (réserve, règlement), soit environ 62 tx/s de ~8 écritures au run 14.
  - Le débit par client vaut voies ÷ latence de la réserve (225 → 951/s, cible 8 000).
  - billing-svc reste en `Recreate` : un déploiement arrête tout le trafic facturé.
  - Le solde vit sous quatre formes : cache Redis, hold, `inflight`, grand livre plus deltas.
  - admin-api-svc instancie son propre `Accountant` et écrit au grand livre ✔.
  - Le règlement passe par trois chemins (consommateur `mt.outcome`, gRPC du pool, reaper), et le guide en décrit un quatrième.
  - `rate_plans` n'est lu que par l'Admin et le stockage ✔ : 1 crédit = 1 segment en dur, `EntryRefund` n'a aucun écrivain, `RecordMO` aucun appelant.
  - billing-svc refuse un Redis Cluster au démarrage ✔, alors que la spec en prévoit un.
- **Cause.** La réserve doit être durable avant `mt.routed`. L'exclusion entre capture et libération repose sur un verrou Redis et des lectures préalables, pas sur une contrainte de base. Le COMMIT ambigu n'est traité nulle part.
- **Sources.** `internal/billing/billing.go:328-433`, `internal/pipeline/pipeline.go:288`, `cmd/admin-api-svc/wiring.go:373-407`. ADR-0002, 0010, 0022, 0024. step-287, 401, 408b, 409. Une douzaine de fiches (`debit-par-client-borne-…`, `commit-ambigu-…`, `capture-fail-open-…`, `billing-svc-reste-en-recreate-…`, etc.).
- **À trancher.**
  1. Le modèle d'autorité du crédit. Trois options :
     - autorité en mémoire ou dans Redis, avec un grand livre projeté depuis Kafka ;
     - enveloppes de crédit pré-allouées par propriétaire, réglées par lots ;
     - synchrone assumé, avec un plafond de débit par client chiffré.
  2. Une seule API d'écriture du solde, y compris pour l'Admin.
  3. Un seul déclencheur de règlement : un événement terminal par message, « jamais envoyé » compris.
  4. Un état terminal exclusif par contrainte de base, et une résolution des COMMIT ambigus par relecture idempotente.
  5. Où le tarif est évalué, ou bien le retrait de `rate_plans` du contrat.
  6. Une clé de partition du grand livre dérivable du `message_id`, et des TTL de réserve, de cache et de reaper posés ensemble.
  7. Des hash tags compatibles cluster.
  8. Ce qui se passe pendant une panne de facturation (ADR-0024 comme base).

### F2 — Aucun magasin opérationnel de l'état d'un message : le CDR analytique en tient lieu

- **Symptôme.** Le CDR ClickHouse (`ReplacingMergeTree`, insert asynchrone, rang scalaire) sert à décider. Il est lu par :
  - `GET /messages/{id}`, qui peut répondre 404 juste après le 202 ;
  - `query_sm` et `cancel_sm` ;
  - la garde de rejeu des lettres mortes ;
  - **le reaper de facturation, qui décide capture ou libération d'après le statut CDR** ✔.

  Cinq services écrivent la même ligne par rang. Un rejet de rang 0 remplace physiquement l'`accepted`, `cancelled` a enterré `delivered`, et une ligne DLR écrase les colonnes du message. La corrélation `smsc_msg_id → message_id` n'existe que dans Redis.
- **Cause.** L'état du message est éclaté entre Kafka, Redis (`dlrmap`, jetons d'annulation, holds) et le CDR. Personne ne le possède, et le rang tient lieu d'horloge.
- **Sources.** `internal/billing/reaper.go:191-205`, `internal/restapi/messages.go:255-276`, `internal/smppserver/ops.go:61`, `internal/storage/clickhouse/cdr.go:198-206`. ADR-0009, 0012, 0013, 0023. Fiches `dlr-delivered-n-est-pas-collant`, `query-sm-final-date-vide-…`, `issue-de-remise-mo-non-projetee`, etc.
- **À trancher.**
  - Une machine d'état du message : états, transitions permises, terminaux collants, un horodatage par transition, grains message et segment séparés, cycle de vie MO.
  - Un propriétaire unique, avec un magasin indexé par `message_id` (OLTP/KV, ou journal d'états projeté).
  - Une corrélation DLR durable.
  - Le CDR devient une projection qu'aucune décision opérationnelle ni financière ne lit.

### F3 — Le connecteur est à la fois une ressource de déploiement et de configuration

- **Symptôme.**
  - Un pod de pool par connecteur, configuré par l'environnement (adresse, identité, douze champs de bind), tandis que `bind_pool_size` vient de la base : deux sources de vérité.
  - Chaque pool lit tout `mt.routed` et jette ce qui ne le concerne pas : N connecteurs, N lectures du trafic.
  - Un groupe neuf démarre `FromLatest` ✔ : un message routé vers un connecteur sans pod est perdu sans CDR terminal, et sa réservation reste ouverte.
  - Le routeur considère « disponible » un connecteur qui n'a pas de pod.
  - Créer un connecteur dans l'Admin ne crée aucun pool.
- **Sources.** `cmd/connector-pool-svc/wiring.go:276-296`, `internal/connectorpool/submit.go:177-184`, `internal/routing/snapshot.go:113-143`. ADR-0015, 0016. Fiches `ancre-de-confiance-par-connecteur`, `connector-auto-reconnect-du-manifest-sans-effet`.
- **À trancher.**
  - Pool générique avec assignation dynamique des connecteurs, ou déploiement par connecteur généré depuis la base.
  - Adressage de `mt.routed` : un topic par connecteur, ou une clé avec consommateur assigné.
  - Une source unique pour les paramètres de bind.
  - Ce que le routeur exige avant de router vers un connecteur.

### F4 — L'autorisation fine n'existe que dans le BFF ; l'audit est doublé ; le relais est réécrit à la main

- **Symptôme.**
  - 46 clés de permission du BFF se réduisent à `admin:read`/`admin:write`, plus six scopes sensibles ✔. Toute clé d'écriture (`groups:write`…) ouvre **toutes** les écritures amont. Le jeton personnel contourne les gardes fines du BFF.
  - Un rôle avec écriture sans lecture passe la garde du BFF puis prend un 403 amont.
  - Chaque mutation relayée est auditée deux fois, par le BFF hors transaction et par la passerelle, et le BFF relit l'état amont pour remplir le « before ». On ne sait pas quel journal fait foi.
  - Environ 33 opérations sur ~136 sont relayées, à ~75 lignes de Go chacune, avec des enums recopiés du contrat sans garde.
  - Divergences réelles sur les sender IDs : `maxLength` 11 contre 20, `traffic_category` absent, statut `pending_carrier_approval` manquant.
  - Une troisième copie du modèle existe dans `internal/fakegateway`.
  - Les 8 scopes sont identiques dans trois listes que seule une convention tient alignées.
- **Sources.** BO `internal/permissions/scopes.go:19-54`, `internal/bff/relay.go:75-104`, `internal/bff/sender_ids.go:141-226`, `api/openapi-bff.yaml:2997-3013`. G `internal/auth/auth.go:20-66`. step-071 (BO).
- **À trancher.**
  - Le point de décision d'autorisation : des scopes fins par ressource et par action portés par la passerelle, ou le BFF seul décideur et aucun jeton qui sorte de lui.
  - La passerelle est la source unique de l'audit du plan de contrôle ; le BFF ne journalise que ses propres faits (auth, opérateurs, rôles, refus).
  - Un relais générique piloté par une table par `operationId` (garde, action d'audit, sensibilité), avec des types générés une seule fois dans le monorepo.

### F5 — La frontière de confiance interne est plate

- **Symptôme.**
  - admin-api-svc n'a pas de `TLS_ALLOWED_CLIENTS` ✔ : tout détenteur d'un certificat de la CA est admis, alors que billing, content-key, session-manager et smpp-server nomment leurs appelants.
  - Personne ne dit qui émet le certificat client du BFF.
  - Le JWKS est servi sur l'écouteur public du BFF, alors que l'ADR-0019 le veut interne.
  - La KEK de content-key-svc est dans `gateway-secrets`, le même Secret que `POSTGRES_URL`/`REDIS_URL` de tous les pods.
  - Un seul rôle Postgres et un seul schéma pour tous les services ; Kafka sans authentification ; secrets de webhook en clair en base.
  - L'identité système des flux porte `admin:read` (tout le plan de contrôle).
  - step-287h demande des identifiants client du BFF pour des Jobs, ce que l'ADR-0019 interdit.
- **Sources.** G `deploy/k8s/admin-api-svc.yaml`, `internal/config/config.go:582-585`, `deploy/k8s/content-key-svc.yaml:61-69`. BO `internal/bff/router.go:102`, `internal/gateway/caller.go:16-33`. ADR-0011, 0015 à 0019. Fiches `kafka-sans-authentification`, `secret-de-webhook-en-clair-en-base`, `mot-de-passe-de-bind-en-argv`.
- **À trancher.**
  - Une identité par service sur chaque magasin (rôle Postgres, ACL Kafka par topic, ACL Redis par espace de clés), plus un migrateur propriétaire distinct.
  - Une PKI interne unique, qui émet le certificat `dashboard` de l'allowlist.
  - Un Secret par service, et une KMS hors du namespace.
  - Un JWKS interne.
  - Des comptes de service pour l'automatisation (amendement de l'ADR-0019) ou des jetons statiques hors production.
  - Un scope `stream:read`.
  - Une procédure de secours quand le BFF, seul émetteur d'identité, est tombé.

### F6 — Pas de domaine d'isolation : une panne ou un tenant bloque ses voisins

- **Symptôme.**
  - La lane du routeur est la partition, et `mt.inbound` mélange tous les clients. Un échec transitoire (billing en `Recreate`, ClickHouse en panne pour l'insert synchrone du CDR `rejected`) arrête toute la partition, clients non facturés compris.
  - Voie retour : un client injoignable a retardé 1,1 M de DLR des autres.
  - Pas de dead-letter sur `mt.inbound`.
  - Seul le MT est rejouable.
  - Chaque consommateur a improvisé sa politique d'échec. Les classes de trafic (ADR-0021) ont été posées tardivement.
- **Sources.** `internal/router/router.go:102-130,208,242-248`, `internal/modlrrouter/dlr_delivery.go:76-90`, `cmd/mt-replay/main.go:108`. ADR-0001, 0012, 0014, 0020, 0021. Fiches `un-poison-immobilise-…`, `dlr-d-un-client-injoignable-…`, `rejeu-en-place-pendant-un-reequilibrage`.
- **À trancher.** Pour chaque topic et chaque groupe de consommateurs, un contrat :
  - clé, ordre garanti, unité de la borne de doublons ;
  - classification des échecs en liste de refus ;
  - dead-letter dès le premier jour, avec âge maximal et estampille de rejeu ;
  - isolation par tenant, aussi sur la voie retour ;
  - dépendances fail-closed autorisées dans une lane, et sort d'un message dont la dépendance est en panne (garé ou bloquant).

  À poser aussi : la topologie par classe de trafic, et la contrainte HPA ↔ partitions.

---

## 3. Défauts transverses

| # | Défaut | Exigence pour la spec |
|---|---|---|
| T1 | **Réglages acceptés sans effet**, plus de 20 cas : TLS de connecteur, `max_per_day`, rétention du corps par client, réputation anti-spam lue mais jamais écrite, rotation de secret qui répond 200, plafond d'instructions des scripts inexistant (ADR-0003, `ErrInstructionLimit` RESERVED) | Chaque champ de config ou de réponse nomme son lecteur, son effet et son délai de propagation. Une garde vérifie « déclaré → appliqué ». Aucun champ sans lecteur n'est publié |
| T2 | **Distribution de config au cas par cas** : env, snapshot au boot, watcher, read-through, resync. Canal pub/sub unique `breaker:events`, chaque transition reconstruit tout (Bloom d'opt-out compris). config-sync coalesce deux fois et est contourné | Un seul mécanisme versionné : événements typés et ciblés, flux durable, resync bornée. Chaque champ a une source, un écrivain, des lecteurs et une fraîcheur. Les suppressions sortent du rechargement de config |
| T3 | **Redis unique porte tout** : sessions, débits, anti-spam, holds, cache de solde, annulation, pub/sub, `dlrmap` (12,5 M clés) | Des instances Redis par usage. Aucun état durable du message dans Redis |
| T4 | **Pas de matrice dépendance × appelant** : appels sans échéance (anti-brute-force, voie retour), aucun disjoncteur sur la facturation, un appel ClickHouse qui gèle un bind 300 s | Chaque appel sortant déclare échéance, politique de panne, disjoncteur et rôle dans `/readyz`, prouvés contre une dépendance **lente**, pas seulement coupée |
| T5 | **Observabilité déclarée, pas câblée** : métrique au catalogue mais non exposée, alertes hors du dépôt, `absent()` aveugle au NaN. Agrégation des réplicas sans propriétaire, hub sans abonnés (BO) | Une chaîne SLO → métrique → émetteur → règle versionnée, prouvée sur le câblage réel. Un propriétaire de l'agrégation inter-réplicas |
| T6 | **Alertes** : notifications durables écrites depuis un flux WS au-mieux-une-fois, sévérité devinée. Règles d'infrastructure à deux propriétaires (base BFF + Alertmanager) | Notifications issues d'une source durable avec offset persisté. Règles d'infra en GitOps, lecture seule dans le BFF |
| T7 | **Logique et secrets métier côté BFF** : impact d'une suspension, secret de webhook généré par le BFF, fiche message composée (dette 026) | Toute vue dérivée et toute génération de secret appartient à la passerelle |
| T8 | **PII hors périmètre RGPD** : MSISDN dans les curseurs et les erreurs, exports jamais purgés, corps en clair dans Kafka | Un inventaire des lieux d'un MSISDN ou d'un corps, chacun avec rétention, effacement et attestation |
| T9 | **Rétention sans propriétaire**, dans les deux dépôts : grand livre jamais purgé, `notifications`, sessions et challenges WebAuthn | Une matrice de rétention par objet, posée à la création de la table. Un exécutant de tâches unique (bail ou `SKIP LOCKED`) |
| T10 | **Validation chez l'appelant et décidée tard** : adresses SMPP non validées en UTF-8, format de sender ID. Durcir après publication = bump majeur | Les règles de champ sont dans la spec et appliquées au codec, avant toute publication du contrat |
| T11 | **Contrat Admin** : PATCH qui ne sait pas effacer, garde de contrat aveugle aux paramètres, collisions de noms corrigées par l'overlay BO | PATCH à trois états (absent / `null` / valeur). Garde bidirectionnelle (opérations, paramètres, sécurité, codes). Composants nommés, plus d'overlay |
| T12 | **Registre de sessions incomplet** : pas d'`enquire_link` serveur (un pair mort tient son quota 60 s), binds sortants invisibles, « suspendu » répond « mauvais mot de passe » | Un registre unique, entrant et sortant, avec keep-alive serveur et motifs de refus par cas |
| T13 | **Voie MO en une phrase** | Un pipeline MO ordonné comme le MT, chaque étape avec propriétaire et politique de panne |
| T14 | **Défauts ≠ production** (export CDR éteint, `max_message_age` inactif) et ordres de déploiement implicites | Les valeurs par défaut sont celles de la production. Chaque changement de format lu par deux versions déclare son ordre de déploiement |
| T15 | **Session BFF** : l'inactivité glisse sur l'interrogation REST à 10 s (le poste reste ouvert 12 h), pas sur la WS | L'activité est un geste de l'opérateur. Politique explicite pour un écran mural de NOC |
| T16 | **Aucun test ne fait parler le vrai BFF à la vraie passerelle** : le BFF est testé contre Prism, la passerelle via testcontainers. C'est là que F4 et F5 se seraient vus | Un parcours d'intégration BFF ↔ admin-api-svc dans le monorepo |
| T17 | **Décisions produit renvoyées en dette** : mono- ou multi-pays (opt-out), quiet hours, formule de réputation, plafond journalier, fournisseur de facturation | Chacune devient une exigence ou un non-objectif net, jamais un « report » |

---

## 4. Socle du monorepo : collisions et conventions

Toutes ces collisions sont classées **FONDATEUR pour le socle** : chacune doit être tranchée avant le premier commit.

- **Invariants** : les lettres (a)–(e) désignent des choses différentes dans les deux dépôts, et le code les cite. Il faut un préfixe ou une numérotation commune.
- **Debts** : les deux registres ont des règles contraires.
  - Passerelle : slug, la fiche payée garde son fichier, aucune garde.
  - Dashboard : numéro `NNN`, la fiche payée est supprimée, garde `debt_owners_test.go` fermée par défaut.

  La garde du dashboard est la seule vérifiée par un test, mais elle se ferme sur le porteur, pas sur le remède : la 067 est payée (#294) et toujours présente. Les fiches 049 et 051 ne sont pas des dettes selon les propres règles du registre.
- **Steps** : 35 numéros livrés en collision, deux sens du numéro (ordre d'exécution contre ordre de `todo.md`), et l'ADR-0019 cite déjà « go-gateway-bo step-070 ».
- **ADR** : la série n'existe que dans la passerelle. L'ADR-0019 a sa case n°4 non cochée alors que l'action est livrée, et sa référence « §517 » est un numéro de ligne.
- **Specs** : celle du dashboard existe en deux copies. Il faut garder celle du dashboard (775 lignes, plus récente), y reporter deux ajouts de l'autre (le commentaire ADR-0017 en §5.1 et la règle de catégorie du filtre CDR en §6.4), puis ajouter l'ADR-0019 et la table permissions → scopes, absentes des deux. Le **guide d'ingénierie diverge du code** : `config-svc` inexistant, `billing_customers` inexistant, « le pool capture », topics manquants, clé de `mo.inbound`. La spec unique part donc **du code et des ADR**, pas du guide.
- **Modules Go** : deux `go.mod`. Un module unique casse `contract_copy_test.go` et importe les dépendances `tool` du dashboard.
- **CI et release** : deux workflows nommés `CI` (`deploy-test` écoute `CI`), un `api/` des deux côtés (le filtre de publication `api/**`). Le tag de release vaut pour tout le dépôt : un `feat` du dashboard republierait les 12 images. Il faut des filtres `paths:` et des tags préfixés.
- **Livraison du dashboard** : rien n'existe (Dockerfile, release, manifestes). La garde « exactement 12 images » passera à 13. **Le CD de test de la passerelle est rouge** depuis le 11/10 (6 runs).
- **Outillage** : gofmt contre gofumpt, `revive exported` actif d'un côté seulement, `go install` contre directive `tool`, golang-migrate contre goose, porte « généré à jour » absente côté passerelle (sqlc, buf), dérive de versions (testcontainers, pgx, go-redis, TypeScript 5.9/7.0, Redis 7/8). Le schéma `dashboard` vide reste créé par la base de la passerelle.
- **Claude** : deux mémoires (142 et 70 fichiers) liées aux anciens chemins, les règles `.claude/rules/` d'un côté, les skills de l'autre.
- **À supprimer** : le clone `go-gateway-adr0019` (branche mergée, ADR identique).

---

## 5. Ce qui est sain et forme le socle commun

**Passerelle**
- Admission du débit avant l'ACK, puis ACK `acks=all` sur `mt.inbound.<catégorie>` (ADR-0001, 0021).
- `mt.outcome` comme événement de fait multi-consommateurs (ADR-0012, 0023, 0024). C'est le motif à généraliser pour F1 et F2.
- `internal/pipeline` partagé par REST et SMPP, et le catalogue d'erreurs unique REST ↔ SMPP.
- `billing_idempotency` (l'INSERT est le verrou) et les deltas de solde append-only.
- Le jeton d'annulation à vainqueur unique, le registre de sessions en Lua atomique, `pod_addr`.
- content-key-svc à surface minimale (mTLS avec allowlist par SAN).
- Le `Watcher` (coalescence, backoff, resync), réutilisable derrière un transport durable.
- La garde de contrats (oasdiff, refus sans bump, « toute opération déclarée est servie »).
- `ciguard` (un test sauté échoue), GoReleaser distroless, job migrate up/down/up, CD par SHA avec rollback.

**Dashboard**
- La garde de permissions fermée par défaut, et `oapi-codegen` en strict-server (DTO imposé par le compilateur).
- `signedTransport`, le rejeu des lectures uniquement.
- Le bail Redis du hub, la file bornée par client, l'état `stale`.
- La session à cookie scellé et double échéance.
- Les compteurs anti-brute-force en PostgreSQL avec HMAC de l'adresse.
- L'audit en champs scalaires, le worker `SKIP LOCKED`.
- Le contrôle de version du schéma avant `Listen`, la rotation des clés par `kid`.
- La CI : agrégateur, actions épinglées par SHA, `check-generated`.

**Pratiques**
- Les enseignements d'implémentation (annexe) et la discipline de mutation des tests.

---

## 6. Décisions à trancher avant la spec, dans l'ordre

L'ordre suit les dépendances : chaque décision contraint les suivantes.

1. **Modèle d'autorité du crédit** (F1) : il fixe le débit par client atteignable et la forme de `mt.routed`.
2. **Propriétaire et magasin de l'état du message** (F2) : la facturation, `query_sm`, l'annulation et le reaper en dépendent.
3. **Connecteur : ressource de configuration ou de déploiement** (F3).
4. **Contrat par topic et domaine d'isolation** (F6).
5. **Point de décision d'autorisation et journal d'audit unique** (F4).
6. **Modèle de confiance interne** (F5), y compris les comptes de service et l'amendement de l'ADR-0019.
7. **Mécanisme unique de distribution de config, et découpage de Redis** (T2, T3).
8. **Décisions produit** (T17).
9. **Conventions du socle** (§4) : invariants, debts, steps, ADR, modules, CI et release.

---

## Annexe — Enseignements d'implémentation à hériter

Aucune spec ne les capture : ils vont dans les règles `.claude/` et la mémoire du monorepo.

**Outillage**
- testcontainers sans socket exporté saute en silence ; ne jamais désactiver Ryuk.
- huma ne fusionne pas le `security:` global (une opération sans `scopeSecurity()` est publique) et injecte 500/default et `$schema`.
- sqlc 1.30 : qualifier les tables.
- ClickHouse :
  - le découpeur de migrations casse sur un `;` en commentaire ;
  - pagination keyset DateTime64 en `toUnixTimestamp64Milli` ;
  - une TTL de table concurrente détruit l'archive ;
  - `engine_file_truncate_on_insert` tronque avant d'écrire.
- franz-go :
  - le record timeout ne borne pas un lot en vol ;
  - un commit tardif peut faire reculer l'offset du nouveau propriétaire ;
  - `UniformBytesPartitioner` panique hors client.
- Réseau et TLS :
  - annoncer `h2` supprime `ReadHeaderTimeout` ;
  - go-oidc recharge le JWKS sans limite et aplatit les erreurs ;
  - `crypto/tls` côté client n'a pas de `GetConfigForClient`.
- Moteurs de scripts : gopher-lua `SetMx` fait `os.Exit` ; goja n'a pas de compteur d'instructions.
- Postgres :
  - `PARTITION OF` prend ACCESS EXCLUSIVE (utiliser `ATTACH`) ;
  - `'365 days'` suit l'heure d'été ;
  - un REVOKE est invisible en superuser ;
  - `LockBalance` sur une ligne absente provoque 40P01 ;
  - les compteurs de pgxpool ne montrent pas la famine.
- Redis de facturation en `noeviction`.
- PromQL : vérifier les labels de chaque opérande ; `absent(g >= 0)`.
- CI :
  - `exit 139` vient de `fs.aio-max-nr` ;
  - minio a disparu de Docker Hub (remplacé par RustFS) ;
  - cache golangci partagé entre worktrees ;
  - `make generate` efface un commentaire manuel ;
  - deux PR parallèles prennent le même numéro de migration et la même version de contrat ;
  - chaque `cmd/` neuf doit être ajouté à GoReleaser.

**Tests**
- Muter le code, la donnée et le câblage pour prouver qu'un test peut échouer.
- Pièges qui rendent un test creux :
  - un aller-retour circulaire ;
  - une fixture littérale sous `-count>1`, une valeur zéro dans une fixture ;
  - une garde testée sur une copie du câblage ;
  - un test de backoff à planchers seuls, un `sync.Once` dans un test de course, une comparaison à la constante.
- Un boot que rien ne construit en test est cassé.
- Une métrique au catalogue n'est pas exposée pour autant.
- Les tests de `cmd/` contournent `Validate`.
- Un test qui compte sur un ordonnancement est un flake : utiliser une barrière.

**Conception et revue**
- Classer un échec sur le chemin chaud par liste de refus.
- Une garde descendue dans le code partagé exige la même conséquence chez chaque appelant.
- Resserrer une porte oblige à vérifier les autres.
- Les correctifs de revue génèrent des défauts : d'abord demander si le code doit exister.
- Un commentaire invente des mécanismes.
- Une absence prouvée par une recherche tronquée est fausse.

**Mesure de charge**
- Un banc sans `DLRMap` mesure 1,6× la production.
- Les littéraux laissés à zéro faussent la mesure.
- Hôte au repos, jamais de mesure en parallèle.
- Le débit visé est traversant.
- La co-résidence des dépendances fausse le plafond.
- Vérifier l'image réellement déployée.
