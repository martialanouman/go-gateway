# Guide d'ingénierie — Passerelle SMS

**Composant :** Passerelle SMS principale (Go)
**Spécification de référence :** `specification-technique-passerelle-sms.md` (modèle RESHADED, v2.0)
**Statut :** Guide d'ingénierie v1.0
**Public :** ingénieurs backend, SRE, et intégrateurs de l'équipe plateforme.

> *Convention d'équipe : la prose est en français ; le code, les noms de tables/topics/services, les schémas et leurs commentaires restent en anglais. Ce guide suit cette règle.*

Ce document explique **comment la passerelle est construite et exploitée**. Il traduit la spécification en décisions d'ingénierie concrètes : découpage en services, contrats entre eux, magasins de données, pipeline de traitement, politiques de panne, déploiement et exploitation. Il se lit conjointement avec le DDL (`schema_passerelle_sms.sql`) et le guide de codage Go (`guide-codage-go.md`).

---

## 1. Périmètre et principes directeurs

La passerelle **reçoit, traite et route** des SMS. Elle n'est pas une plateforme de campagnes : ni listes de diffusion, ni modèles, ni programmation d'envoi côté client (§1.2bis de la spec). Le pic de débit provient de la charge agrégée des clients, pas d'un envoi de masse orchestré par la plateforme.

Cinq principes gouvernent chaque choix d'implémentation.

**Un seul pipeline, deux protocoles.** SMPP (`submit_sm`) et REST (`POST /messages`) convergent vers le même flux `mt.inbound` immédiatement après l'authentification. Toute règle métier — autorisation d'expéditeur, opt-out, anti-spam, routage, débit, facturation — est écrite **une fois** et s'applique aux deux protocoles. Aucune logique n'est dupliquée par protocole.

**L'ingestion est découplée de l'envoi.** Un soumetteur est acquitté dès que le message est durablement écrit dans Kafka (`mt.inbound`), pas quand le SMSC l'accepte. Routage, facturation et envoi se font hors bande. C'est ce qui tient la cible de latence d'ingestion (p99 < 250 ms) indépendamment de l'état des connecteurs.

**La conformité est sur le chemin critique, jamais rapportée après coup.** Autorisation de sender ID, opt-out scopé au canal, corps jamais journalisé, chiffrement par clé client, effacement RGPD : ce sont des étapes du pipeline, pas une couche optionnelle. Un raccourci de routage (numéro exact) ne court-circuite **jamais** la conformité.

**La facturation est entièrement optionnelle.** Désactivée, elle disparaît du chemin de requête : un contrôle booléen en cache, aucun appel réseau, aucune dépendance. Sa panne ne bloque jamais l'envoi sauf configuration fail-closed explicite.

**Le socle de traitement est sans état et s'étend horizontalement.** Tout état de session, de débit ou de réservation est externalisé (Redis) ou porté par le message (en-têtes Kafka). Seul `smpp-server-svc` a un état TCP, géré via un registre de sessions.

---

## 2. Vue d'ensemble de l'architecture

Le système se découpe en trois plans.

Le **plan de contrôle** détient la configuration (clients, comptes, routes, connecteurs, anti-spam, facturation) dans PostgreSQL, l'expose via l'API Admin, et pousse les changements vers le plan de données par un bus de configuration (pub/sub). Il diffuse aussi l'état agrégé des disjoncteurs.

Le **plan de données** fait le travail réel : ingestion (serveur SMPP + API REST), traitement (`router-svc`), envoi (`connector-pool-svc`), et routage retour (`mo-dlr-router-svc`). Il est nourri par Kafka (durabilité) et Redis/Dragonfly (état à faible latence).

Le plan d'**observabilité** collecte métriques (Prometheus → TSDB long terme), logs structurés (Loki/ELK), traces (OpenTelemetry) et CDR (ClickHouse). L'alerting infrastructure (Alertmanager) est indépendant de la disponibilité du tableau de bord.

```
Admin Dashboard (React) ──► Admin REST/WS API
                                   │
        ┌──────────────────────────┴──────────────────────────┐
        │  CONTROL PLANE                                        │
        │  config-svc → PostgreSQL 18                           │
        │  admin-api-svc  •  config change bus (pub/sub)        │
        │  circuit-breaker state broadcast (Redis pub/sub)      │
        └──────────────────────────┬──────────────────────────┘
                                   │ config sync (hot reload)
        ┌──────────────────────────┴──────────────────────────┐
        │  DATA PLANE                                          │
        │  Ingress: smpp-server-svc, rest-api-svc              │
        │      └─► Kafka mt.inbound                            │
        │  Processing: router-svc  ─► Kafka mt.routed          │
        │  Egress: connector-pool-svc ─► SMSC(s)               │
        │      └─► Kafka mo.inbound / dlr.events               │
        │  Return: mo-dlr-router-svc ─► deliver_sm / webhook   │
        │  session-manager-svc (Redis) • rate-limiter (Redis)  │
        │  billing-svc (opt-in)                                │
        └──────────────────────────┬──────────────────────────┘
                                   │
        ┌──────────────────────────┴──────────────────────────┐
        │  OBSERVABILITY                                       │
        │  Prometheus → Thanos/Mimir • Loki • OTel • ClickHouse │
        └─────────────────────────────────────────────────────┘
```

---

## 3. Les services déployables

La passerelle est un ensemble de services conteneurisés (Kubernetes). Chacun a une frontière nette et un contrat explicite.

### 3.1 `smpp-server-svc` — ingestion SMPP côté utilisateur

Gère les binds SMPP longue durée des clients ESME (5 000–20 000 sessions simultanées). C'est le **seul service à état TCP**. À l'authentification d'un bind, il résout l'identifiant vers son compte SMPP puis son client, vérifie le canal (`smpp_enabled`) et `max_sessions` contre le registre inter-pods (`session-manager-svc`), puis publie chaque `submit_sm` validé sur `mt.inbound`. Il consomme `mo.inbound`/`dlr.events` pour remettre au bind propriétaire via le registre. Il traite `query_sm`/`cancel_sm` quand ils sont activés par compte (§6.22). Scalé derrière un load-balancer L4 avec affinité de session ; la remise MO/DLR au bon pod passe par le registre de sessions et un endpoint gRPC de remise interne (§6.8).

### 3.2 `rest-api-svc` — ingestion REST

Service HTTP **sans état** pour `POST /messages`, la requête de statut, l'annulation et la lecture read-only du compte. Authentifie la clé API (Bearer ou HMAC), résout compte→client, valide, publie sur `mt.inbound`, acquitte. Scalé par HPA (CPU / connexions). HTTP/2 ou keep-alive avec pool pour supporter 10 000+ connexions concurrentes.

### 3.3 `router-svc` — le cœur du pipeline MT

Consommateur **sans état** de `mt.inbound`. Applique, dans l'ordre : normalisation E.164 → autorisation de sender ID (§6.19) → opt-out (§6.20) → anti-spam (§6.5) → résolution de route (§6.1) → encodage/segmentation (§6.6) → limite de débit (§6.4) → réservation de crédit MT (§6.9). Publie sur `mt.routed`. Émet un span OpenTelemetry par étape. Maintient en mémoire des filtres de Bloom (numéros exacts, suppressions) et un instantané immuable de la configuration de routage, échangé atomiquement au rechargement à chaud. Les comptes portant un script de routage sont isolés sur des pools/quotas séparés car leur enveloppe de coût est distincte (§6.2).

### 3.4 `connector-pool-svc` — envoi vers les SMSC

Un pool logique **par SMSC**, tenant `bind_pool_size` binds SMPP sortants parallèles (§6.8). Consomme `mt.routed`, applique le lissage de débit et le disjoncteur (§6.15), évalue la réécriture de sender ID juste avant l'envoi (§6.16), envoie `submit_sm`, suit `submit_sm_resp`. En cas de succès il **capture** la réservation de crédit ; en cas d'échec il **libère**. Écrit le CDR (statut `enroute`). Si le disjoncteur du connecteur cible est ouvert, il republie le message vers le connecteur suivant du `fallback_chain` porté en en-tête. Reçoit les `deliver_sm` entrants et publie sur `mo.inbound`/`dlr.events`. Publie l'état agrégé du disjoncteur et la charge dans Redis (par transition, pas par message).

### 3.5 `mo-dlr-router-svc` — routage retour

Consomme `mo.inbound`/`dlr.events`. Pour un MO : normalisation E.164 → détection de mot-clé STOP (§6.20) → résolution du compte via numéro entrant/mot-clé (§6.21) → remise immédiate (SMPP `deliver_sm` au bind actif, ou webhook) → comptage MO sur le solde MO (§6.9). Pour un DLR : corrélation par `message_id`, mise à jour du CDR, transmission au compte d'origine. Un MO non résolu part en file « MO non routés » exposée au tableau de bord — jamais un abandon silencieux.

### 3.6 `session-manager-svc` — registre de sessions

Registre faisant autorité (Redis) de tous les binds, dans les deux sens. Expose une API gRPC pour bind/unbind/lookup, applique `max_sessions` **au bind** contre le registre inter-pods (pas best-effort par pod), pilote la supervision `enquire_link`, et maintient la table `account → {pod_id, bind_id}[]` qui permet la remise MO/DLR au bon pod.

### 3.7 `billing-svc` — moteur de crédit (déployé si activé)

Possède `balances`, `billing_customers`, `billing_ledger`, `content_keys`. Expose l'API réserve/capture/libère (MT) et le compteur (MO), réconcilie le cache Redis avec le grand livre Postgres (l'autorité durable), et héberge l'adaptateur de facturation externe (§6.10). **Absent du chemin de requête quand la facturation est désactivée.**

### 3.8 `admin-api-svc` — API du plan de contrôle

CRUD de configuration, métriques temps réel, traçage, export CDR asynchrone, lecture de contenu gardée (`content:read`) et effacement RGPD. Consommée exclusivement par le tableau de bord Admin. Distincte de l'API REST publique.

### 3.9 `config-sync` — propagation de configuration

Pousse les diffs du plan de contrôle vers le plan de données via pub/sub pour rechargement à chaud, sans redémarrage. Alimente les instantanés immuables de `router-svc` et les filtres de Bloom.

---

## 4. Les magasins de données et leur rôle

La persistance est polyglotte : chaque magasin est choisi pour son motif d'accès. Ne jamais mélanger les responsabilités.

**PostgreSQL 18 — plan de contrôle (forte cohérence).** Toute la configuration et le grand livre de facturation. Clés primaires en UUIDv7 natif. C'est l'**autorité durable** des soldes. Le DDL complet est dans `schema_passerelle_sms.sql`. Non partitionné sauf `billing_ledger` (par jour).

**Redis / Dragonfly (cluster) — état opérationnel à faible latence.** Sessions, compteurs token-bucket, dédup anti-spam, réputation, cache/réservations de solde, files de délai courtes, état agrégé de disjoncteur, filtres. **Jamais** le magasin durable des messages : une perte Redis dégrade des fonctionnalités selon leur politique de panne (§16), sans perdre un message en vol.

**Kafka — plan de données (durabilité du pipeline).** Socle de durabilité : l'écriture dans `mt.inbound` est la frontière d'accusé de réception. Offre rejeu, dead-letter et parallélisme par partition. Topics : `mt.inbound`, `mt.routed`, `mo.inbound`, `dlr.events`, `mt.dead-letter`/`mo.dead-letter`, `mt.reroute-park`.

**ClickHouse (columnar) — CDR, analytique, recherche, traçage.** Un enregistrement par message, partitionné par jour, avec tiering TTL. Interrogé par le tableau de bord. Le DDL du CDR (dialecte ClickHouse) est en appendice du fichier `.sql`.

### 4.1 Partitionnement des topics Kafka

Le clé de partition n'est pas cosmétique — elle garantit l'ordre et le parallélisme.

`mt.inbound` et `mo.inbound` sont partitionnés par hash de compte/client, ce qui distribue la charge tout en gardant les messages d'un compte cohérents. `mt.routed` est partitionné par `(connector_id, shard_index)` où `shard_index = hash(message_key) % bind_pool_size` du connecteur cible. **`message_key` est l'ID de message logique** : tous les segments UDH d'un SMS concaténé le partagent, donc ils atterrissent sur le même shard, donc sur le même bind, dans l'ordre — exigence des SMSC qui réassemblent sur un seul bind.

---

## 5. Flux de données

### 5.1 MT (soumission) — le chemin critique

```
submit_sm | POST /messages
   │  AUTH: credential → smpp_account → customer   (les 2 IDs → en-têtes Kafka)
   │  CHANNEL: smpp_enabled / rest_enabled ?
   ▼
   ACK CLIENT dès écriture durable dans mt.inbound          ← frontière de durabilité (§6.7)
======================= router-svc =======================
   1. E.164 NORMALIZE (dest + source)
   2. SENDER-ID AUTHORIZATION      — pas de correspondance → REJECT
   3. OPT-OUT / SUPPRESSION        — hit → REJECT   (Bloom en mémoire, ~99% sans réseau)
   4. ANTI-SPAM                    — block → REJECT
   5. ROUTE RESOLUTION (3 niveaux, premier gagnant) :
        L0 numéro exact (MNP)  →  L1 script  →  L2 déclaratif
   6. ENCODING / UDH SEGMENTATION → segment_count
   7. RATE LIMIT
   8. MT CREDIT RESERVE (si facturation activée)
   publish → mt.routed (key = ID logique)
================= connector-pool-svc =====================
   breaker fermé ? sinon → reroute via fallback_chain
   9. SENDER-ID REWRITE (côté fournisseur, avant envoi)
   submit_sm → SMSC → submit_sm_resp
   succès → CAPTURE crédit ; échec → RELEASE ; écrit CDR (enroute)
   plus tard : deliver_sm (DLR) → update CDR → push au client
```

Point capital : **le court-circuit du niveau L0 ne saute que la résolution de route.** Les étapes 1–4 et 6–9 s'appliquent à *tout* message, y compris routé par numéro exact.

### 5.2 MO (réception)

Le `deliver_sm` d'un SMSC arrive sur `connector-pool-svc` → `mo.inbound` → `mo-dlr-router-svc` : normalisation E.164 → détection STOP (§6.20 — un STOP écrit une suppression scopée sur le numéro entrant ; le MO est **quand même** remis et **jamais** facturé) → résolution du compte via le numéro entrant (dédié → son compte ; partagé → mot-clé ; aucune correspondance → file « MO non routés ») → **remise immédiate** (jamais conditionnée à un solde) → comptage MO = `segment_count × credits_per_segment_mo` sur le solde MO → CDR écrit.

### 5.3 DLR (accusé de réception)

Le `deliver_sm` d'un DLR est corrélé au `message_id` d'origine, met à jour le CDR (`delivered_at`, `status`, `latency_ms`), clôt le span, et est transmis au compte d'origine via SMPP ou webhook. Si `rate_plans.charge_on = delivery`, un DLR `failed`/`expired` déclenche une entrée `refund` annulant la capture (§6.9).

---

## 6. Le moteur de routage

### 6.1 Trois niveaux, premier gagnant

**L0 — correspondance de numéro exact (priorité maximale, court-circuit).** Si le MSISDN de destination figure dans `exact_routes`, sa cible est utilisée immédiatement. C'est la réponse à la **portabilité des numéros** : le matching par préfixe (L2) suppose à tort que le préfixe identifie l'opérateur, faux pour un numéro porté. Un filtre de Bloom en mémoire (rafraîchi par `config-sync`) élimine ~99 % du trafic sans appel réseau — jamais de faux négatif, donc « absent » signifie certainement pas d'override. Si la cible d'un numéro exact est indisponible (connecteur désactivé, disjoncteur ouvert), on **retombe** sur L1/L2 plutôt que dead-letter.

**L1 — script de routage.** Si le compte (ou la plateforme) a un script actif, il est invoqué (§6.2). Un ID de route valide retourné est utilisé directement ; `null` bascule vers L2.

**L2 — matching déclaratif.** Règles ordonnées par `priority`, première correspondance complète gagnante, repli sur une route par défaut. Prédicats composables en ET (compte, client, sender pattern, préfixe destination, contenu). Le matching de destination utilise un préfixe-trie (O(longueur du préfixe)) ; le contenu, des regex précompilées.

### 6.2 Stratégies de distribution

Une fois la route résolue, son **exécution** est identique quelle que soit la voie de résolution — toujours déclarative, via `distribution_strategy` :

| Stratégie | Comportement | Usage |
|---|---|---|
| `static` | Un seul connecteur (`target_connector_id`). | Route mono-opérateur. |
| `round_robin` | Alterne parmi `route_targets` (≥2). | Capacités équivalentes. |
| `weighted` | Aléatoire proportionnel au `weight`. | Capacités inégales. |
| `failover_priority` | `priority` le plus bas d'abord, bascule si indisponible. | Primaire/secours. |
| `least_loaded` | Cible au plus faible `connectorload`. | Débits fluctuants. |
| `hash_based` | Hash déterministe d'une clé (MSISDN dest par défaut). | Affinité par abonné. |

Toutes n'opèrent que parmi les cibles passant le contrôle disjoncteur/désactivé (§6.15).

### 6.3 Scripts de routage (admin uniquement, §6.2)

Pour la logique inexprimable en déclaratif, le fournisseur écrit un script isolé. Scopé `platform`/`customer`/`smpp_account` (résolution `smpp_account → customer → platform`, au plus un actif par portée), **jamais attaché à une route**. Moteur JS embarqué (`goja`, pur Go) principal, Lua (`gopher-lua`) alternatif, en processus dans `router-svc`. Contrat : `resolveRoute(message) → routeId | null`. Aucun accès réseau/fichier. Garde-fou **primaire = plafond d'instructions/bytecode** (déterministe, insensible aux pauses GC), timeout mur en filet (défaut 2 ms, jusqu'à ~20 ms), plafond mémoire. Toute violation → repli déclaratif, journalisé et remonté. Runtimes réutilisés depuis un pool, état réinitialisé par invocation (pas d'allocation par message).

### 6.4 Rechargement à chaud

`config-sync` pousse les diffs de routes/scripts. `router-svc` garde un **instantané immuable** échangé par pointeur atomique. L'état volatil (disjoncteur agrégé, charge connecteur) vit dans une **surcouche mutable séparée** mise à jour sur `breaker:events`, pour ne pas rebâtir l'instantané à chaque transition.

---

## 7. Sessions SMPP, connecteurs et fiabilité de liaison

### 7.1 Cycle de vie des sessions (§6.3)

Chaque bind est enregistré dans `session-manager-svc`. Supervision `enquire_link` (défaut 30 s, 3 réponses manquées → unbind forcé). Débit par fenêtre glissante SMPP (submit_sm en vol borné par la fenêtre), découplé du token-bucket métier. Au redéploiement d'un pod, unbind gracieux (drain) avec période de grâce ; les clients se reconnectent.

`max_sessions` s'applique **au bind** contre le registre inter-pods. Abaisser la limite sous le nombre de sessions vivantes ne coupe aucun bind existant : la convergence forcée exige `DELETE /admin/sessions/{id}`. Un unbind, une coupure ou une expiration `enquire_link` libère immédiatement le jeton.

### 7.2 Identifiants et rotation (§6.3)

Chaque compte a exactement 1 identifiant de bind + 1 clé API (`UNIQUE(account_id, type)`), stockés uniquement en hash. **Le secret n'est révélé qu'à la création et à la rotation** — aucune action « reveal ». La rotation est **manuelle** (`POST .../rotate`), avec un `gracePeriodSec` optionnel pendant lequel l'ancien secret reste valide en parallèle (un ESME tient des binds TCP longue durée ; une rotation dure couperait son trafic). Aucune rotation automatique. Révocation, passage `status != active`, suspension du compte ou du client force la déconnexion des sessions concernées. Anti-brute-force : échecs comptés par `system_id` et IP (Redis TTL), backoff, événement de sécurité auditable.

### 7.3 Disjoncteur (§6.15)

Machine à états par connecteur (`closed → open → half-open → closed`). Closed compte les issues `submit_sm_resp` sur fenêtre glissante ; Open déclenché au-delà d'un seuil (défaut 50 %, avec minimum de requêtes), arrête l'envoi, démarre un cool-down (défaut 30 s) ; Half-open laisse passer des sondes ; reprise si succès, sinon réouverture avec backoff.

Propagation d'état **sans dépendance synchrone par message** : (1) chaque `connector-pool-svc` publie l'état agrégé dans `breaker:state:{connector_id}` (dérivé par majorité du hash `breaker:binds`, §6.8) à chaque transition, avec notification `breaker:events` — `router-svc` ne le lit qu'en construisant son instantané ; (2) chaque message porte un `fallback_chain` en en-tête, permettant à `connector-pool-svc` de rerouter unilatéralement un message reçu pour un connecteur ouvert. Reroutage de masse **borné** par un draineur à débit limité ; l'excédent est parqué dans `mt.reroute-park` pour éviter une tempête de republication Kafka.

### 7.4 Reconnexion automatique (§6.13)

Opt-in par connecteur (`auto_reconnect_enabled`, défaut false). Désactivée, un bind rompu passe `link_status=down` et la reprise est manuelle (`POST /admin/connectors/{id}/rebind`). Activée, un superviseur retente avec backoff exponentiel + jitter, plafonné à `reconnect_max_attempts`. **Le disjoncteur ne reconnecte jamais** : ses sondes half-open exigent une connexion établie. Conséquence : sur perte de bind, le half-open ne peut sonder et le disjoncteur ne se referme pas seul — la reprise automatique n'existe que si l'auto-reconnexion est active. **Recommandation normative** : tout connecteur s'appuyant sur le disjoncteur doit activer l'auto-reconnexion ; l'UI/API avertit sinon. `link_status` (up|reconnecting|down) et `breaker_state` (closed|open|half_open) sont **distincts** et jamais confondus.

### 7.5 Gestion du débit (§6.4)

Deux niveaux : fenêtre SMPP au protocole (par session) et token-bucket métier (par compte/connecteur/route, Lua atomique dans Redis). Précédence : `throughput_limit_per_sec` du connecteur est le plafond technique absolu ; une ligne `rate_limits` est un gouverneur opérationnel qui ne peut jamais le dépasser (validé à l'écriture — voir la note dans le DDL). Le débit effectif est le minimum des deux. Throttling adaptatif AIMD piloté par les signaux `ESME_RTHROTTLED`. **Panne Redis : fail-closed conservateur** — chaque pod applique localement le plafond technique statique, jamais un débit non borné.

---

## 8. Facturation (opt-in, §6.9)

**Le solde est un compteur entier de crédits SMS, jamais monétaire.** Un SMS concaténé consomme plus d'un crédit : `credits = segment_count × credits_per_segment(destination, sender_type)`, consulté après segmentation et après la limite de débit.

Deux axes orthogonaux. **Direction** : MT et MO ont des soldes séparés. Le MT est un vrai solde (réserve → capture/libère ; en prépayé sans découvert, zéro bloque l'envoi). Le MO est un **compteur postpayé qui ne bloque rien** — il descend jusqu'à `mo_billing_floor`, après quoi l'accumulation cesse et une alerte est émise ; un dépassement MO n'a aucun effet sur le MT. La séparation supprime un vecteur de déni de service économique (inonder un long-code pour couper les envois MT). **Propriétaire** (`customers.balance_scope`) : `customer` (pool partagé par direction, défaut) ou `smpp_account` (soldes isolés, pas de point de sérialisation Redis). Verrou : changer `balance_scope` exige que **tous les soldes soient à zéro**.

Prépayé MT — réserve/capture/libère : `router-svc` réserve atomiquement (Lua) ; échec → rejet immédiat (REST `402`, SMPP code d'extension) sans entrée de grand livre ; `connector-pool-svc` capture sur succès, libère sur échec/expiration. **Idempotence par `message_id`** (clé de réservation unique + `UNIQUE(message_id, entry_type)`) car réserve et capture encadrent un hop Kafka au moins une fois. L'**autorité du solde est le grand livre Postgres durable** ; le cache Redis est une projection réhydratée depuis Postgres à la perte/failover (fail-closed pour les comptes en garantie stricte pendant la fenêtre).

Désactivée, `router-svc`/`connector-pool-svc` sautent l'étape par un contrôle booléen en cache. Interopérabilité externe (§6.10) : modes `balance_check`, `consume_delegate_async` (défaut), `consume_delegate_sync` (opt-in, appel synchrone sur le chemin critique), avec job de réconciliation périodique.

---

## 9. Conformité intégrée

### 9.1 Normalisation E.164 & autorisation de sender ID (§6.19)

La destination (et la source pour le MO) sont normalisées E.164 **avant toute autre étape** — sinon déduplication, opt-out et numéro exact seraient contournables par un simple écart de format. L'autorisation de sender ID vérifie que `source_addr` correspond à un `sender_ids` `active` du **client**, selon `sender_id_policy` : `strict` (défaut, rejet sinon), `allow_unregistered_numeric`, ou `disabled` (déconseillé, audité, averti).

### 9.2 Opt-out / STOP scopé au canal (§6.20)

Le désabonnement vise le **canal** (le numéro entrant auquel le destinataire a répondu STOP), portée par défaut `inbound_number`, portées plus larges disponibles. Chemin entrant : un MO comparé aux `opt_out_keywords` ; `suppress` écrit une suppression, `unsuppress` la retire, `help` déclenche une auto-réponse (MT jamais facturé). Chemin sortant : étape **bloquante** dans `router-svc`, avant anti-spam/routage/facturation, bloquant si le destinataire figure dans **l'une quelconque** des portées applicables (platform OU customer OU smpp_account OU inbound_number du `source_addr`). Filtre de Bloom en mémoire (jamais de faux négatif). Un expéditeur alphanumérique n'a pas de chemin de retour : seules les portées compte/client/plateforme s'appliquent.

### 9.3 Stockage & chiffrement du contenu (§6.23)

Le corps est la donnée la plus sensible. Son stockage est configurable (`customers.content_storage` : `off`, `stored_plaintext` déconseillé, `stored_encrypted` recommandé). **Cette politique ne gouverne qu'une surface : le stockage du corps dans le CDR.** Logs et traces ne portent **jamais** le corps, sous aucune politique — c'est un invariant testable, pas un réglage (§6.11). Chiffrement enveloppe + clé par client (`content_keys`) : détruire la clé (`status=destroyed`) rend tout le contenu du client illisible d'un geste (crypto-shred). Anti-spam, segmentation et détection opt-out lisent le clair **en mémoire, avant** stockage — « traiter le contenu » et « ne pas le stocker » ne sont pas en conflit. `content:read` est la frontière d'accès, auditée.

### 9.4 Effacement RGPD (§6.14.4)

Asymétrique. **Effacer un client** : crypto-shred de sa clé + purge de ses lignes CDR (le grand livre peut devoir être conservé). **Effacer une personne (MSISDN, cas DSAR)** : on ne peut crypto-shredder (clé partagée) → suppression ciblée ligne à ligne du contenu et des métadonnées `WHERE source_addr = :m OR dest_addr = :m`, across clients, job asynchrone + attestation. Exception : les suppressions/opt-out d'un MSISDN sont **conservées** (les effacer le ré-exposerait).

---

## 10. Fiabilité et sémantique de livraison (§6.7)

Toute écriture d'ingestion vers Kafka est la limite de durabilité ; l'accusé n'a lieu qu'après validation dans `mt.inbound`. **Aucune perte après accusé** (remise au SMSC au moins une fois). L'exactement-une-fois n'est pas garanti de bout en bout (SMPP est au moins une fois) — des clés d'idempotence sont disponibles côté client, et la facturation est idempotente par `message_id`. Files dead-letter pour les messages ayant épuisé leurs retries (y compris `fallback_chain` épuisé), remontées pour retraitement.

Matrice cible des NFR (§1.2) : disponibilité 99,95 %/région ; ingestion p50 < 50 ms / p99 < 250 ms ; bout-en-bout p50 < 400 ms / p99 < 2 s en charge nominale ; débit soutenu 5 000–10 000 SMS/s, pic 15 000 ; surcoût de réservation de crédit < 5 ms p99 (nul si désactivé).

---

## 11. Convention des codes d'erreur (API & pipeline)

Toute erreur exposée par la passerelle — que le client parle REST ou SMPP — se rattache à un **code d'erreur stable et unique**. Ce code est le contrat : il est identique quel que soit le protocole, documenté dans les deux specs OpenAPI (`openapi-public.yaml`, `openapi-admin.yaml`) et mappé une seule fois, à la frontière, vers un statut HTTP et un `command_status` SMPP. Le reste du code manipule des erreurs de domaine typées (voir le guide de codage Go, §4) ; la traduction en code/statut n'a lieu qu'au bord.

### 11.1 Format unique : un modèle JSON plat

Côté REST, **toute** réponse d'erreur utilise `application/json` et un modèle plat à deux champs obligatoires — `code` (clé machine) et `message` (texte humain) — plus un `errors[]` optionnel pour le détail de validation par champ :

```json
{
  "code": "recipient_opted_out",
  "message": "Destination +2250700000000 is suppressed on scope inbound_number.",
  "errors": []
}
```

`code` est la clé machine (le contrat, §11.2) ; `message` est pour l'humain. Le **statut HTTP est porté par la ligne de statut**, jamais dupliqué dans le corps. Les erreurs de validation renseignent `errors[]`, une entrée par champ (`field` façon `to` ou `messages.0.text`, `message`). Huma émet par défaut le format RFC 9457 (`problem+json`) : on le remplace par ce modèle plat via une surcharge `huma.NewError`, et l'OpenAPI généré reflète automatiquement la forme retenue.

### 11.2 Règles de nommage et de cycle de vie du `code`

Les codes sont en **`snake_case`**, courts, orientés cause métier (`sender_id_not_authorized`, pas `error_403_a`). Un code est **stable et immuable** : une fois publié il n'est jamais renommé ni réaffecté à un autre sens (les clients branchent leur logique dessus). On ajoute des codes, on n'en recycle pas. Ils ne sont pas versionnés et ne portent pas le statut HTTP dans leur nom (le statut peut varier selon le protocole, le code non). L'énumération de référence vit dans le champ `code` des specs OpenAPI ; toute nouvelle valeur y est ajoutée en même temps que le code applicatif.

### 11.3 Catalogue — mapping unifié REST ↔ SMPP

Chaque erreur de domaine a exactement un statut HTTP et un `command_status` SMPP. `command_status` est donné avec sa valeur SMPP v3.4 ; les erreurs sans code SMPP standard (facturation) utilisent la plage vendeur réservée `0x00000400+`.

| `code` | HTTP | SMPP `command_status` | Étape / origine | Rejouable ? |
|---|---|---|---|---|
| `unauthenticated` | 401 | `ESME_RINVPASWD` (0x0E) / `ESME_RINVSYSID` (0x0F) | Auth bind/clé API (§6.3) | Non (corriger l'identifiant) |
| `account_suspended` | 403 | `ESME_RBINDFAIL` (0x0D) | Statut effectif compte/client (§6.18) | Non |
| `channel_disabled` | 403 | `ESME_RBINDFAIL` (0x0D) | `smpp_enabled`/`rest_enabled` (§6.18) | Non |
| `max_sessions_exceeded` | — | `ESME_RBINDFAIL` (0x0D) | Registre de sessions au bind (§6.3) | Oui (après libération d'un jeton) |
| `invalid_destination` | 422 | `ESME_RINVDSTADR` (0x0B) | Normalisation E.164 (§6.19) | Non |
| `invalid_source` | 422 | `ESME_RINVSRCADR` (0x0A) | Normalisation E.164 (§6.19) | Non |
| `sender_id_not_authorized` | 403 | `ESME_RINVSRCADR` (0x0A) | Autorisation sender ID (§6.19) | Non |
| `recipient_opted_out` | 403 | `ESME_RSUBMITFAIL` (0x45) | Opt-out bloquant (§6.20) | Non |
| `content_blocked` | 403 | `ESME_RSUBMITFAIL` (0x45) | Anti-spam action=block (§6.5) | Non |
| `no_route` | 422 | `ESME_RINVDSTADR` (0x0B) | Résolution de route, aucun repli (§6.1) | Non |
| `payload_too_large` | 413 | `ESME_RINVMSGLEN` (0x01) | Encodage/segmentation (§6.6) | Non |
| `rate_limited` | 429 | `ESME_RTHROTTLED` (0x58) | Token-bucket / débit (§6.4) | Oui (respecter `Retry-After`) |
| `queue_full` | 503 | `ESME_RMSGQFUL` (0x14) | Backpressure aval saturé (§6.4/§6.12) | Oui (backoff) |
| `insufficient_credit` | 402 | ext. `0x00000400` | Réservation MT (§6.9) | Non (jusqu'à recharge) |
| `external_billing_unavailable` | 503 | `ESME_RSYSERR` (0x08) | Fournisseur de facturation externe injoignable sous `fail_closed` (§6.10) — crédit non confirmé refusé | Oui (backoff) |
| `message_not_found` | 404 | `ESME_RINVMSGID` (0x0C) | `query_sm` / statut (§6.22) | Non |
| `not_found` | 404 | — (API Admin) | Ressource du plan de contrôle inexistante (client, compte, connecteur, route…) | Non |
| `cancel_failed` | 409 | `ESME_RCANCELFAIL` (0x11) | `cancel_sm` déjà envoyé (§6.22) | Non |
| `operation_not_supported` | 405 | `ESME_RINVCMDID` (0x03) | Op SMPP désactivée / non supportée (§6.22) | Non |
| `validation_error` | 422 | `ESME_RINVMSGLEN` (0x01) | Validation de requête | Non |
| `idempotency_conflict` | 409 | — | Rejeu avec un `Idempotency-Key` divergent | Non |
| `forbidden_scope` | 403 | — (API Admin) | Scope opérateur manquant (`content:read`…) | Non |
| `conflict` | 409 | — (API Admin) | État conflictuel (soldes ≠ 0, script déjà actif) | Non |
| `internal_error` | 500 | `ESME_RSYSERR` (0x08) | Panne inattendue | Oui (backoff, idempotent) |
| `service_unavailable` | 503 | `ESME_RSYSERR` (0x08) | Dépendance dégradée fail-closed (§6.4/§6.9) | Oui (backoff) |
| `submit_failed` | — (issue sortante) | `ESME_RSUBMITFAIL` (0x45) | Le SMSC a rejeté le `submit_sm` — enregistré dans `cdr.error_code`, pas une erreur de requête REST | Non |
| `delivery_failed` | — (issue sortante) | — | Un accusé de réception rapporte le message comme non délivrable (message_state UNDELIV/DELETED/REJECTD) — enregistré dans `cdr.error_code` | Non |
| `delivery_expired` | — (issue sortante) | — | Un accusé de réception rapporte l'expiration du message avant remise (message_state EXPIRED) — enregistré dans `cdr.error_code` | Non |
| `fallback_exhausted` | — (issue sortante) | — | Le message a parcouru toute sa `fallback_chain` et tous les connecteurs se sont dégradés (breaker ouvert ou rejet santé-connecteur) — enregistré dans `cdr.error_code` et comme raison sur `mt.dead-letter` (step-125) | Non |
| `retries_exhausted` | — (issue sortante) | — | Un message sans chaîne de repli viable a subi un échec santé-connecteur au-delà de la fenêtre de retry — abandonné sur `mt.dead-letter` plutôt que redélivré indéfiniment sur un connecteur mort (step-129) | Non (rejouable par l'opérateur via l'outil de replay) |

### 11.4 Rejouabilité et idempotence

La colonne « Rejouable » indique si un client **devrait** réémettre. Les 4xx métier (autorisation, opt-out, crédit, route) ne sont pas rejouables tels quels — l'entrée est à corriger. `rate_limited` et `queue_full` le sont avec backoff, en respectant l'en-tête `Retry-After` quand il est présent. Les 5xx le sont aussi, mais **seulement** parce que la soumission est idempotente par `Idempotency-Key` côté REST et par `message_id` côté facturation (§6.9) : un rejeu après un `internal_error` survenu près de la frontière de durabilité ne double ni le message ni le débit. Un rejeu avec le même `Idempotency-Key` mais un corps différent renvoie `idempotency_conflict`.

### 11.5 Où le code est émis, et l'invariant CDR

Un rejet du pipeline produit toujours trois effets cohérents : la réponse JSON (`code` + `message`) / `submit_sm_resp` porte le `code`, le CDR est écrit avec `status=rejected` et `error_code=<code>` (§3.4), et le span de l'étape fautive est marqué en erreur (échantillonné à 100 %, §6.11). **Le même `code` circule dans les trois surfaces** — réponse client, CDR, trace — ce qui rend une erreur traçable de bout en bout par sa seule valeur. Comme pour tout le reste, aucune de ces surfaces ne contient le corps du message. Les codes propres au SMSC (échec de remise remonté par DLR) sont distincts des codes passerelle ci-dessus : ils sont conservés bruts dans `cdr.error_code` pour les états `failed`/`expired`, préfixés `smsc_` pour lever l'ambiguïté avec les rejets passerelle.

---

## 12. Scalabilité et déploiement

Tous les services du plan de données sont sans état et s'étendent via HPA (CPU / lag Kafka). `smpp-server-svc` fait exception (état TCP) : scalé aussi, la remise MO/DLR passant par le registre `session-manager-svc` qui remet directement au pod détenteur via gRPC (round-robin sur les binds vivants, repli webhook à défaut).

Dimensionnement (§2.5) : un worker sans état soutient ~1 000–2 000 msg/s → 8–16 vCPU en charge soutenue, auto-scalé 2–3× au pic. Un compte scripté a une enveloppe propre, inférieure, et est isolé sur des pools séparés. Kafka dimensionné pour 8 000–15 000 msgs/s en écriture, réplication 3. Multi-région : plan de données par région pour la latence, synchro de config cross-région, primaire Postgres avec réplicas en lecture (la reprise après sinistre n'est pas traitée, §1.2bis).

Déploiement Kubernetes : services conteneurisés, tout état de session externalisé ou affinité gérée explicitement. Le module de facturation est déployé conditionnellement (`billing-svc` absent quand désactivé). `PodDisruptionBudget` sur `smpp-server-svc` et `connector-pool-svc` pour préserver les binds ; drain gracieux au rolling update.

---

## 13. Observabilité

**Métriques** : Prometheus par pod → remote-write → TSDB long terme (Thanos/Mimir), scrape 10–15 s, fraîcheur < 5 s. Cardinalité (compte × connecteur × route) — dizaines de milliers de séries. Le groupe client n'est **pas** un label Prometheus : dérivable du compte, la ventilation somme les séries par compte (§6.17).

**Alerting infrastructure** : Alertmanager, **indépendant** de la disponibilité du tableau de bord.

**Stream temps réel** : gateway WebSocket/SSE alimentée par un topic de métriques Kafka, pour le tableau de bord (`/admin/stream/metrics`, `/sessions`, `/billing-alerts`).

**Logs** : JSON structuré → Loki/ELK. **Le corps du message n'apparaît jamais** (§6.11).

**Tracing** : OpenTelemetry, `trace_id` par message, un span par étape (ingestion/auth, autorisation, opt-out, anti-spam, routage, débit, réserve/capture, encodage, envoi SMSC + `submit_sm_resp`, DLR, remise finale). Échantillonnage 100 % pour tout message en erreur/rejet/timeout, configurable pour le succès. **Un span ne contient jamais le corps** — au plus une longueur et un `content_hash` tronqué.

**CDR** : Kafka → ClickHouse, partitionné par jour, pour recherche, traçage et réconciliation.

---

## 14. Configuration, rétention et exploitation

### 13.1 Rechargement à chaud

Les changements de configuration (routes, connecteurs, anti-spam, sender IDs, suppressions) sont poussés par `config-sync` en pub/sub et appliqués sans redémarrage. `router-svc` échange un instantané immuable ; les filtres de Bloom sont rafraîchis en incrémental.

### 13.2 Partitionnement et rétention (§6.14)

CDR (ClickHouse) et grand livre (Postgres) partitionnés par jour ; audit mensuel ; plan de contrôle non partitionné. Tiering : chaud 0–7 j (SSD), tiède 8–90 j (objet + Parquet, partitions détachées), froid > 90 j (archive immuable). Rétentions différenciées : corps `content_retention_days` (ex. 7 j) < métadonnées CDR (90 j) < grand livre (13 mois+) < audit (1–7 ans) ; suppressions **sans expiration**. Purge par **drop de partition**, jamais `DELETE WHERE`.

### 13.3 Runbook — opérations courantes

**Onboarding d'un client B2B.** Créer le client (plan tarifaire, `billing_enabled`, `balance_scope`, politique de contenu) → créer un ou plusieurs comptes SMPP (canaux, `max_sessions`, types de bind) → générer les identifiants (bind + clé API, secret révélé une fois) → enregistrer les sender IDs (approbation opérateur) → configurer webhooks MO/DLR → si numéros entrants dédiés, les assigner. Aucun libre-service : tout passe par l'API/Tableau de bord Admin.

**Rotation d'un identifiant.** `POST /admin/smpp-accounts/{id}/credentials/{credId}/rotate` avec `gracePeriodSec` couvrant la fenêtre de reconnexion du client. Communiquer le nouveau secret hors bande. Vérifier la bascule avant expiration de la grâce.

**Incident connecteur.** Distinguer `link_status` et `breaker_state` via `GET /admin/connectors/{id}/status`. Disjoncteur ouvert + lien up → problème SMSC applicatif (throttle/erreurs) : le trafic bascule via `fallback_chain`, surveiller `mt.reroute-park`. Lien down → bind mort : si auto-reconnexion active, surveiller le backoff ; sinon `POST /admin/connectors/{id}/rebind`.

**MO non routés.** Surveiller `GET /admin/mo/unrouted` : un afflux signale un numéro entrant mal assigné ou un mot-clé manquant.

**Effacement RGPD.** `POST /admin/gdpr/erase` (job asynchrone), suivre via `GET /admin/gdpr/erase/{jobId}` jusqu'à l'attestation.

---

## 15. Checklist de mise en production

Avant d'exposer un connecteur ou un client en production, vérifier : identifiants stockés en hash uniquement ; TLS actif sur REST et SMPP-TLS où requis ; `throughput_limit_per_sec` renseigné et `rate_limits` cohérents ; auto-reconnexion activée sur tout connecteur s'appuyant sur le disjoncteur ; sender IDs approuvés ; politique de contenu et clé de chiffrement provisionnées ; webhooks signés HMAC et testés ; partitions Kafka dimensionnées pour `bind_pool_size` ; partition du jour créée dans `billing_ledger` ; alerting Alertmanager indépendant du dashboard ; échantillonnage de trace 100 % sur les échecs ; test de bascule `fallback_chain` ; test de rotation d'identifiant avec grâce ; vérification que le corps n'apparaît dans aucun log ni span (test d'invariant).

---

## 16. Décisions structurantes (rappel)

Les compromis clés, détaillés en §7 de la spec : client et compte SMPP distincts (1..N) ; exactement 1 clé API + 1 bind par compte en contrainte de schéma ; soldes MT/MO séparés (MO = compteur postpayé) ; Kafka comme couche d'ingestion durable ; livraison au moins une fois + idempotence de facturation par `message_id` ; autorité du solde = grand livre Postgres ; routage à 3 niveaux avec court-circuit numéro exact ne sautant jamais la conformité ; script embarqué (goja/Lua) plutôt que FaaS ; disjoncteur par connecteur à état hybride ; pool de binds par connecteur ; opt-out scopé au canal avec union à l'application ; stockage de contenu chiffré par clé client jamais loggué ; effacement RGPD asymétrique ; groupes organisationnels uniquement ; gestion exclusivement admin.

Ce qu'on revisitera à mesure que le système grandit : partitionnement Kafka plus fin au-delà d'un certain nombre de binds ; tuning batch-write ClickHouse à > 10k/s ; modèle ML anti-spam si la fraude dépasse les règles statiques ; classification de contenu pour débloquer les quiet hours.
