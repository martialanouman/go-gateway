# Connecteur sortant — des comptes, un bail, une source

2026-10-15 · Décision 3 du monorepo (`docs/audit-architecture-2026-10-14.md` §2 F3, §6).
Statut : design validé.

**Nommage.** Ici, « compte » est le compte **opérateur** d'un connecteur : `connector_account_id`,
jamais `account_id`, qui reste le compte SMPP du client partout ailleurs.

## 1. Problème

Le connecteur est à la fois une ressource de déploiement et de configuration :

- **un Deployment par connecteur**, configuré par 25 variables `CONNECTOR_*` (adresse, identité,
  paramètres de bind), alors que `smsc_connectors` porte les mêmes champs et que `bind_pool_size`
  vient de la base : deux sources de vérité ;
- **chaque pool lit tout `mt.routed`** et jette ce qui ne le concerne pas : N connecteurs, N lectures
  du trafic ;
- **un groupe neuf démarre `FromLatest`** : un message routé vers un connecteur sans pod est perdu sans
  CDR terminal, et sa réservation reste ouverte ;
- **le routeur juge « disponible » un connecteur qui n'a pas de pod** ;
- **créer un connecteur dans l'Admin ne crée aucun pool.**

Deux faits manquaient à la spec initiale :

- un compte opérateur plafonne à **~25 SMS/s** (contractuel), et parfois en nombre de binds. Les
  8 000 SMS/s visés demandent donc ~320 comptes : le plafond appartient au **compte** (`system_id`),
  ni au bind ni à l'opérateur ;
- des opérateurs et agrégateurs n'offrent pas de SMPP mais une **API REST** : le connecteur sortant
  est multi-protocole.

## 2. Décisions

| # | Décision |
|---|---|
| D1 | **L'exploitation crée les connecteurs depuis le dashboard, sans déploiement.** Un compte créé binde en quelques secondes. |
| D2 | **Un connecteur est une destination qui contient N comptes.** Le connecteur porte ce que le routage manipule (protocole, profil, `priority_tier`, cible de `fallback_chain`) ; le compte porte le contrat (endpoint, identifiants, débit, binds). |
| D3 | **Un adaptateur Go par fournisseur HTTP**, derrière une interface commune. Pas de gabarit générique en base. |
| D4 | **Un compte a au plus un détenteur actif**, désigné par un **bail Postgres** à fencing. Le bail donne le droit de binder, de consommer et d'appliquer le débit du compte. |
| D5 | **Une partition par compte** dans chaque `mt.routed.<catégorie>`. Le routeur choisit le compte ; aucun blocage en tête de file entre comptes. |
| D6 | **La base est la seule source de configuration.** L'environnement du pod ne porte que l'infrastructure. |
| D7 | **Postgres injoignable arrête l'envoi** au terme du bail : l'exclusivité prime sur la disponibilité. |

## 3. Modèle

### 3.1 `connectors`

| Colonne | Rôle |
|---|---|
| `id`, `name`, `status` | `status` : `active`, `disabled` |
| `protocol` | `smpp` ou `http` ; commun à tous ses comptes |
| `smpp_profile` | `vendor_profile` et paramètres de session : TON/NPI, `data_coding_default`, `esm_class_default`, `registered_delivery_default`, `priority_flag_default`, `validity_period_default`, `interface_version`, `enquire_link_*`, timeouts. Requis si `protocol = smpp` |
| `http_adapter` | Nom de l'adaptateur (§6). Requis si `protocol = http` |
| `priority_tier` | ADR-0020 §4, inchangé |

Les routes, `route_targets` et `fallback_chain` ciblent des connecteurs. Le débit d'un connecteur se
calcule (somme de ses comptes disponibles) et n'est jamais stocké.

### 3.2 `connector_accounts`

| Colonne | Rôle |
|---|---|
| `id`, `connector_id` | |
| `endpoint` | `host:port` (SMPP) ou URL de base (HTTP). Porté par le compte : les opérateurs attribuent souvent une adresse par compte |
| `credentials_sealed`, `credentials_kms_key_ref` | Scellés par `ConfigSecrets` (ADR-0016) : `system_id`/mot de passe, clé d'API, client OAuth2, secret de callback |
| `throughput_limit_per_sec` | **`NOT NULL`**, plafond contractuel |
| `max_binds` | SMPP : plafond de binds simultanés ; remplace `bind_pool_size` |
| `max_in_flight` | Fenêtre : `window_size` par bind en SMPP, requêtes concurrentes en HTTP |
| `bind_type`, `tls_*` | SMPP |
| `partition` | Entier **unique, attribué à la création, jamais réutilisé** (§4.2) |
| `status` | `provisioning`, `active`, `draining`, `disabled` |

### 3.3 `connector_account_leases`

| Colonne | Rôle |
|---|---|
| `connector_account_id` | Clé primaire |
| `holder` | Identité du pod |
| `epoch` | Incrémenté à chaque réclamation |
| `expires_at` | Horloge de Postgres |

- **Réclamer** : `UPDATE … SET holder = $moi, epoch = epoch + 1, expires_at = now() + TTL
  WHERE connector_account_id = $1 AND expires_at < now()`. Une ligne est insérée à la création du compte, avec
  `expires_at = -infinity`.
- **Renouveler** : toutes les 5 s, `WHERE holder = $moi AND epoch = $mon_epoch`, TTL 15 s.
- **Auto-fencing du détenteur.** Un SMSC ne vérifie aucun jeton : le détenteur cesse d'envoyer et
  de commiter dès que `émission du dernier renouvellement réussi + TTL − 3 s` est dépassé, mesuré sur
  son horloge **monotone**. Le suivant ne réclame qu'après `expires_at`, mesuré sur l'horloge de
  Postgres. Aucun écart d'horloge entre pods n'entre dans l'inégalité.
- **Libérer** : arrêter de consommer, attendre les réponses en vol, commiter, se délier, puis
  `expires_at = now()`.

### 3.4 `pool_members` et équilibrage

Chaque pod inscrit `(pod, heartbeat_at)` toutes les 5 s ; un membre sans battement depuis 15 s ne
compte plus. Chaque pod vise sa part de la somme des `throughput_limit_per_sec` des comptes actifs :

- au-dessus de sa cible, il libère **un** compte par cycle ;
- en dessous, il réclame un compte sans bail (ou au bail expiré) ;
- une marge d'un compte autour de la cible empêche l'oscillation.

Un compte neuf est pris par le premier pod sous sa cible.

## 4. Chemin d'un MT

### 4.1 Choix du compte

Au routage, après la résolution du connecteur, le routeur choisit le compte parmi les comptes
**disponibles** du connecteur : `status = active`, statut récent (§4.5) avec `link_status = up` et
`breaker_state ≠ open`. Le choix est un **hachage rendez-vous pondéré** par
`throughput_limit_per_sec`, sur `message_id` :

- déterministe : un rejeu du routeur renvoie le message au même compte, à disponibilités égales ;
- tous les segments d'un message partent sur le même compte ;
- un compte qui devient indisponible ne déplace que ses propres messages.

Un connecteur est disponible si au moins un compte l'est. Sinon le routeur passe au connecteur
suivant de la route, comme aujourd'hui pour un connecteur ouvert.

### 4.2 Adressage

- Production sur `mt.routed.<catégorie>` avec `Partition = connector_account.partition` (partitionneur
  manuel). La clé reste `message_id` ; les en-têtes portent `connector_account_id` et `connector_id`.
- **Extension des partitions.** À la création d'un compte, l'Admin API attribue `partition` (séquence)
  et, si elle dépasse le nombre de partitions, étend les trois topics par blocs de 64, de façon
  idempotente, **avant** de passer le compte à `active`. Un échec répond 503 et laisse le compte en
  `provisioning`, que le routeur ignore.
- La partition d'un compte supprimé reste orpheline : jamais réattribuée.

### 4.3 Consommation

- Le détenteur s'assigne les trois partitions de chacun de ses comptes (assignation manuelle). La
  priorité entre catégories (ADR-0021) s'applique **au sein du compte**.
- Offsets commités dans un groupe unique `connector-pool`, en commit autonome, hors adhésion.
- Une partition jamais commitée se lit **depuis le début** : neuve, elle est vide. Le piège
  `FromLatest` disparaît.
- Un compte sans détenteur garde ses messages dans sa partition : ils attendent, ils ne se perdent
  pas.

### 4.4 Envoi

1. Seau de débit **local** au compte, à `throughput_limit_per_sec`.
2. Bind : `hash(message_id) % binds_actifs` (segments concaténés sur un bind, §6.8 inchangé).
3. Réécriture de sender ID (§6.16).
4. Réclamation du jeton d'annulation (ADR-0013, inchangé).
5. `submit_sm` ou `Submit` HTTP ; issue publiée sur `mt.outcome` (décision 2).

Le disjoncteur est **par compte** : sa machine d'états (§6.15) est portée telle quelle, sans
agrégation entre pods.

### 4.5 État publié

Le détenteur publie **`connector.account-status`** (topic compacté, clé `connector_account_id`) : `epoch`, `holder`,
`link_status` (`up`, `reconnecting`, `down`, `auth_failed`), `breaker_state`, en vol, lag. À chaque
transition et toutes les 5 s. Le routeur en fait sa surcouche mutable ; un état de plus de 15 s rend
le compte indisponible. Un `epoch` inférieur au dernier vu est ignoré.

### 4.6 Compte indisponible avec backlog

Lien `down` ou disjoncteur ouvert au-delà du seuil de reroute : le détenteur republie la partition
vers un **compte frère** (même fonction de choix, ce compte exclu), à débit borné, par le garage
`mt.reroute-park` existant. Sans frère : connecteur suivant de `fallback_chain`. Chaîne épuisée :
dead-letter. Les segments restent groupés, puisque le choix porte sur `message_id`.

### 4.7 Mise hors service

- `draining` : le routeur ne choisit plus le compte ; le détenteur vide la partition, puis libère.
- `disabled` : le backlog est republié vers un frère (§4.6).

## 5. Voie retour SMPP

Inchangée sur le fond : les `deliver_sm` arrivent sur les binds du détenteur et partent sur
`dlr.events` ou `mo.inbound`, avec `connector_account_id`, acquittés après production durable.

## 6. Connecteur HTTP

### 6.1 Interface

Un paquet Go par fournisseur implémente :

- `Submit(ctx, unité) → Accepted{remote_id} | Retryable{after, code} | Permanent{code}` ;
- **l'un** de : `ParseCallback(req) → []ÉvénementRetour` (webhook) ou
  `Poll(ctx, curseur) → []ÉvénementRetour, curseur` ;
- ses capacités : `Idempotent` (accepte une référence client, à qui l'on passe `message_id`).

Le classement des réponses est une **table** ; chaque ligne est vérifiée par un test contre une
réponse enregistrée du fournisseur. Une réponse inconnue est `Retryable` et comptée par le
disjoncteur.

### 6.2 Messages concaténés

Le contrat de `mt.routed` reste **un record par segment**, pour tous les protocoles : la décision 1
(fait de consommation par segment) ne change pas, et un repli entre connecteurs de protocoles
différents republie des records identiques.

L'adaptateur HTTP **réassemble** : il tamponne par `message_id` jusqu'à `segment_count`, envoie le
texte une fois et applique l'issue à tous les segments. L'offset commité ne dépasse jamais le plus
ancien record en attente. Un message incomplet après 30 s publie ses segments présents en
`not_sent` (`incomplete_message`) et alerte : c'est une anomalie.

### 6.3 Envoi ambigu

Délai dépassé après émission de la requête : retentative avec la même référence si l'adaptateur est
`Idempotent` ; sinon `submit_failed` (`ambiguous`), sans retentative — non facturé en
`charge_on = accept`.

### 6.4 Webhook

- **Ingress public distinct** : `POST /callbacks/{connector_account_id}`, servi par n'importe quel pod du pool
  (l'analyse n'exige pas le bail).
- Authentification par l'adaptateur : signature HMAC du fournisseur si elle existe, sinon jeton
  secret du compte (scellé, comparé en temps constant) ; allowlist d'IP optionnelle par compte.
- Corps ≤ 64 KiB, débit plafond par compte.
- **200 seulement après production durable** sur `dlr.events` ou `mo.inbound`. Tout autre code fait
  rejouer le fournisseur ; la corrélation absorbe les doublons.

### 6.5 Polling

Fait par le détenteur du bail. Le curseur vit dans `connector_account_poll_cursors`, écrit
`WHERE epoch = $mon_epoch` : un ancien détenteur ne peut pas le faire reculer.

### 6.6 Secrets

Le détenteur ouvre les identifiants par `ConfigSecrets.Open` (ADR-0016). Un jeton OAuth2 vit en
mémoire du détenteur, jamais persisté.

## 7. Pannes

| Panne | Effet |
|---|---|
| Crash d'un pod | Bail expiré en 15 s, réclamé, rebind. Les envois en vol restent dans la fenêtre de doublons bornée (§6.7) |
| Déploiement progressif | `OnDrain` libère les comptes un à un (§3.3). Aucun compte n'est réclamé avant d'être lâché |
| **Postgres injoignable** | Pas de renouvellement : **auto-fencing à l'échéance, envoi suspendu après 15 s.** Un basculement HA coûte 15 à 45 s. Les messages attendent dans leur partition, sans perte. Continuer exposerait, sous partition partielle, deux détenteurs d'une même partition, donc des SMS en double |
| Statut périmé | Le routeur écarte le compte ; backlog en attente ou repris (§4.6) |
| Bind refusé par l'opérateur | `auth_failed`, alerte, compte écarté |
| Kafka injoignable | Rien ne part, comme aujourd'hui |
| Redis injoignable | Seul le jeton d'annulation est touché, fail-open comme aujourd'hui (ADR-0013). Le débit et le disjoncteur ne dépendent plus de Redis |

## 8. Invariants (tests bloquants)

1. **Au plus un détenteur actif par compte**, sous horloges décalées et coupure de Postgres pour un
   seul pod (`Cuttable` + `tcpproxy`).
2. **Un message routé vers un compte sans détenteur n'est jamais perdu** : il part dès qu'un pod
   prend le bail.
3. **Les segments d'un message partent sur le même compte et le même bind**, y compris après
   republication vers un frère.
4. **Débit d'un compte ≤ `throughput_limit_per_sec` ; binds ≤ `max_binds`** (pendant sortant de
   l'invariant d).
5. **La configuration ne vient que de la base** : garde AST, aucune lecture d'environnement métier
   dans le pool.

## 9. Tests

- Coordinateur de baux contre un vrai Postgres (testcontainers) : réclamation concurrente,
  renouvellement, libération ordonnée, équilibrage sans oscillation.
- Adaptateurs HTTP : une réponse enregistrée par ligne de la table de classement ; réassemblage,
  y compris segments entrelacés et incomplets.
- Webhook de bout en bout : 200 seulement après production ; rejeu absorbé.
- E2E existant à deux simulateurs, étendu à un connecteur de deux comptes : perte d'un compte,
  republication vers le frère, retour.

## 10. Porté, réécrit, neuf

| | |
|---|---|
| **Porté** | Client SMPP (bind, fenêtre, `enquire_link`, auto-reconnexion), machine d'états du disjoncteur, réécriture de sender ID, codec, garage et draineur de reroute |
| **Réécrit** | Configuration, assignation, consommation, débit (seau Redis → local), disjoncteur (agrégation multi-pod supprimée), publication d'état |
| **Neuf** | Coordinateur de baux, extension des partitions, adaptateurs HTTP, ingress de callback, polling |

## 11. Effets sur la spec et les contrats actuels

| Où | Effet |
|---|---|
| Admin API | Ressources `connectors` et `connectors/{id}/accounts` ; rupture, **bump majeur**. Le dashboard suit |
| Schéma | `smsc_connectors` scindé en `connectors` + `connector_accounts` ; ajout de `connector_account_leases`, `pool_members`, `connector_account_poll_cursors` |
| Topics | `connector.account-status` (compacté) ; partitions de `mt.routed.*` croissantes ; `breaker:events` et `breaker:binds:*` (Redis) supprimés |
| Spec §1, §6 | Connecteur HTTP ajouté |
| Spec §6.4 | Seau du connecteur (Redis, partagé) → seau local par compte ; la politique de panne Redis du pool disparaît |
| Spec §6.8 | `bind_pool_size` → `max_binds` du compte |
| Spec §6.15 | Disjoncteur par compte ; état du connecteur dérivé de ses comptes |
| Décision 2 §5.2 | Clé de corrélation `(connector_account_id, remote_id)` ; `segment_seq = 0` pour un DLR qui porte sur tout le message, appliqué à chaque segment par le repli |
| Kubernetes | Un Deployment générique, dimensionné sur le débit contractualisé total ; manifests par connecteur supprimés |

## 12. Hors périmètre

- **Contrôleur de débit adaptatif** (AIMD sur `submit_sm_resp`, spec §2) : il s'applique au seau
  local du compte, sans changer ce design.
- **Gabarit HTTP générique en base** : écarté (D3), pas différé.
