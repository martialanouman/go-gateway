# État du message — un journal, un propriétaire

2026-10-15 · Décision 2 du monorepo (`docs/audit-architecture-2026-10-14.md` §2 F2, §6).
Statut : design validé. Une porte bloque son adoption : le banc du §9.

## 1. Problème

Aucun magasin opérationnel ne possède l'état d'un message. Le CDR ClickHouse en tient lieu, alors
que c'est une table analytique en insert asynchrone, qui versionne ses lignes par rang de cycle de
vie dans un `ReplacingMergeTree`.

**Le CDR sert pourtant à décider.** Il est lu par :
- `GET /messages/{id}`, qui peut répondre 404 juste après le 202 ;
- `query_sm` et `cancel_sm` ;
- la garde de rejeu des lettres mortes ;
- le reaper de facturation, qui choisit entre capture et libération d'après ce statut.

**Il a cinq écrivains**, qui se disputent une même ligne par rang. Un rejet de rang 0 remplace
physiquement l'`accepted`, `cancelled` a enterré `delivered`, et une ligne DLR écrase les colonnes
du message.

**La corrélation `smsc_msg_id → message_id` n'existe que dans Redis** (`dlrmap`, 12,5 M de clés
au run 14).

## 2. Décisions

| # | Décision |
|---|---|
| D1 | **Horizon.** L'état est exact, et lisible dès l'ACK, pendant la phase active puis la fenêtre DLR : `DLR_RETENTION`, ou la `validity_period` si elle est plus longue, avec un plafond de 72 h. Au-delà, le CDR répond, en tant qu'historique. |
| D2 | Un **journal de transitions en ajout seul** dans un Postgres dédié (base `message_state`), avec **un seul écrivain** : `message-state-svc`. |
| D3 | L'état se **replie à la lecture**, par une fonction pure. Aucun autre service ne lit la base : l'accès passe par l'API gRPC du service. |
| D4 | La **corrélation des DLR est unique** et vit dans `message-state-svc`. Elle publie un DLR enrichi sur `dlr.routed`, avec la clé `account_id`. |
| D5 | Le CDR devient une **projection à écrivain unique** du flux `message.state`. Le rang disparaît. |
| D6 | L'arbitrage d'annulation reste le jeton à vainqueur unique (ADR-0013). Le service inscrit `cancelled` et ne décide jamais d'une annulation. |
| D7 | Le service n'est **jamais sur le chemin d'envoi** d'un MT. |

## 3. Le journal

### 3.1 `message_transitions`

| Colonne | Contenu |
|---|---|
| `message_id` | UUIDv7 |
| `segment_seq` | 0 pour une transition du message, 1..N pour un segment |
| `attempt` | Incrémenté à chaque reroute vers un autre connecteur |
| `kind` | Type de transition (§3.2) |
| `occurred_at` | Heure de l'événement source, pas l'heure d'écriture |
| `account_id`, `customer_id` | Renseignés sur `accepted` et `received` |
| `attrs` | jsonb court : `connector_id`, `smsc_msg_id`, `error_code`, statut DLR brut. **Jamais le corps** (invariant a) |

- **Clé primaire** : `(message_id, segment_seq, attempt, kind)`. Une écriture déjà présente est
  ignorée par `ON CONFLICT DO NOTHING`.
- **Partitionnement par plage de `message_id`.** Un UUIDv7 encode son heure de création, donc les
  bornes d'une partition se calculent depuis un jour, et toutes les transitions d'un message tombent
  dans une seule partition.
- **Purge** : chaque jour, la partition qui dépasse 72 h est supprimée.

### 3.2 Transitions

| Niveau | `kind` | Source |
|---|---|---|
| MT, message | `accepted` | `mt.inbound.*` |
| MT, message | `rejected` (conformité, crédit, `billing_unavailable`) | `mt.outcome`, `segment_seq = 0`, publié par le routeur |
| MT, message | `parked`, `unparked` (garage, décision 1) | `mt.outcome`, `segment_seq = 0` |
| MT, message | `cancelled` | Gagnant du jeton d'annulation |
| MT, segment | `routed` | `mt.routed.*` |
| MT, segment | `submitted` (ROK), `submit_failed`, `not_sent` | `mt.outcome` |
| MT, segment | `delivered`, `undeliverable`, `expired` | DLR corrélé (§5) |
| MT, segment | `dlr_window_closed` | Échéance de `DLR_RETENTION` sans DLR |
| MT, segment | `outcome_missing` | `routed` sans issue après 1 h |
| MO | `received`, `routed_to_account`, `unrouted`, `delivered_to_account`, `delivery_failed` | `mo.inbound`, `mo.routed`, issue de remise |

### 3.3 Repli

1. **Par segment.** On retient la dernière `attempt`. Le **premier état terminal par
   `occurred_at`** gagne et reste collant ; une égalité se départage par une précédence fixe des
   `kind`. Le résultat ne dépend jamais de l'ordre de consommation.
2. **Par message.** On agrège selon les règles **déjà publiées** dans `MessageStatus` (contrat
   public), sans changement. `rejected` et `cancelled` excluent tout segment `routed`.
3. **L'horodatage de chaque transition** est servi avec l'état.
4. **MO.** `delivered_to_account` est exposé comme `delivered`, `delivery_failed` comme `failed`.

## 4. Lectures

### 4.1 API gRPC

| Opération | Appelants |
|---|---|
| `GetMessage(message_id, account_id)` | REST `GET /messages/{id}`, `query_sm`, fiche message Admin/BFF |
| `CheckCancellable(message_id, account_id)` | `cancel_sm` : inconnu, déjà soumis ou annulable |
| `IsTerminal(message_id)` | Garde de rejeu des lettres mortes |

Un message d'un autre compte reçoit la même réponse qu'un message inconnu. Cible : `GetMessage`
présent < 50 ms au p99. La limite de débit dédiée de `query_sm` est conservée.

### 4.2 Lecture juste après l'ACK

Quand un message est absent du journal :
1. Calculer sa partition dans chacun des `mt.inbound.<catégorie>`, avec la fonction de hachage du
   producteur.
2. Noter le dernier offset de ces partitions.
3. Attendre que l'**offset commité** du groupe de consommateurs les dépasse, au plus 2 s.
   L'offset n'est commité qu'après le commit Postgres, donc n'importe quelle réplique peut faire
   cette vérification.
4. Toujours absent : replier sur le CDR, puis répondre 404 (`ESME_RINVMSGID` côté SMPP).

### 4.3 Ce qui ne lit plus ClickHouse

`query_sm`, `cancel_sm`, `GET /messages/{id}` dans l'horizon, la garde de rejeu et la facturation.
ClickHouse reste la source de l'historique, de l'analytique et de la fiche d'un message de plus de
72 h.

## 5. Écritures et corrélation des DLR

### 5.1 Écritures

- **Topics consommés** : `mt.inbound.*`, `mt.routed.*`, `mt.outcome`, `dlr.events`, `mo.inbound`,
  `mo.routed`.
- **Lots** : par partition, jusqu'à 20 ms ou 5 000 lignes, insérés en un
  `INSERT … SELECT FROM unnest(…) ON CONFLICT DO NOTHING`. Le banc (§9) tranche entre cette forme et
  un `COPY` vers une table de transit.
- **Offsets** commités après le commit Postgres. Un crash rejoue le lot, et la clé primaire absorbe
  les doublons.
- **Rejets du routeur** : ils sont publiés sur `mt.outcome` et non plus insérés de façon synchrone
  dans le CDR depuis la lane.

### 5.2 Corrélation

1. **À chaque `submitted` portant un identifiant distant**, une ligne est ajoutée à `dlr_correlation` :
   `(connector_account_id, remote_id) → (message_id, segment_seq, attempt, account_id, customer_id)`.
   L'identifiant distant n'est unique qu'au sein d'un compte opérateur (décision 3). Un connecteur
   HTTP qui rend un seul `remote_id` par message inscrit `segment_seq = 0`, et le repli applique le
   DLR à chaque segment.
   La table est partitionnée par jour d'insertion et ses partitions sont supprimées au-delà de
   72 h.
2. **Pour chaque lot de `dlr.events`**, le service fait une seule recherche groupée, sur au plus
   quatre partitions. Il ajoute la transition au journal, puis publie le DLR enrichi sur
   **`dlr.routed`**, avec la clé `account_id`.
3. **Un DLR qui arrive avant sa corrélation** va dans `dlr.uncorrelated` et y est rejoué jusqu'à
   une **échéance absolue** de 5 min. Passé ce délai, il va dans `dlr.unmatched` : compté, alerté et
   rejouable.

**Consommateurs de `dlr.routed`** : la remise (`mo-dlr-router-svc`, qui ne corrèle plus), la
facturation en mode `delivery`. Le journal, lui, reçoit la transition directement (étape 2).

### 5.3 Flux d'état

Après chaque commit, l'état replié de chaque message modifié est publié sur **`message.state`**
(clé `message_id`). La version de l'état est le nombre de transitions. Un doublon de publication
n'a donc aucun effet sur le CDR, qui projette ce flux comme seul écrivain.

## 6. Pannes

| Panne | Effet |
|---|---|
| Postgres ou le service indisponible | L'envoi MT n'est pas touché. Les événements s'accumulent dans Kafka sans perte. `GET` et `query_sm` se replient sur le CDR et marquent la réponse « possiblement en retard ». `cancel_sm` répond `ESME_RSYSERR`. La remise des DLR est retardée, jamais perdue. Le retard de consommation est alerté |
| Perte du magasin | Reconstruction par rejeu des topics depuis 72 h. La rétention Kafka de ces topics doit valoir au moins 72 h plus une marge |
| DLR jamais corrélable | `dlr.unmatched` : compté, alerté, rejouable |
| Segment sans issue | `outcome_missing` après 1 h, alerte, aucune libération de crédit automatique |

## 7. Invariants

- **Écrivain unique.** Seul `message-state-svc` écrit dans `message_state`, et c'est une propriété
  du rôle Postgres (F5).
- **Repli indépendant de l'ordre.** Toute permutation des mêmes transitions donne le même état.
- **Journal idempotent.** Une transition rejouée ne modifie pas le journal.
- **Lecture après ACK.** Un message dont le 202 est rendu est trouvé par `GetMessage`.
- **(a)** Le corps n'est jamais écrit dans le journal ni dans `attrs`.

## 8. Tests bloquants

Ils tournent sur le vrai Kafka et le vrai Postgres (testcontainers), et le code est muté sous
chacun d'eux.

| Test | Prouve |
|---|---|
| Propriété : permutations aléatoires d'une même suite de transitions | Repli indépendant de l'ordre |
| Table des règles d'agrégation de `MessageStatus` | Conformité au contrat publié |
| Transitions rejouées ou doublées | Journal idempotent |
| `GET` immédiatement après le 202 (la mutation retire l'attente) | Lecture après ACK |
| Message d'un autre compte | Réponse identique à un message inconnu |
| DLR avant corrélation, puis échéance | `dlr.uncorrelated`, puis `dlr.unmatched` |
| Postgres coupé, puis ralenti | Débit MT inchangé, repli sur le CDR, résorption du retard |
| Magasin effacé puis reconstruit depuis Kafka | États repliés identiques |
| Message de plus de 72 h | Réponse servie depuis le CDR |
| Garde sur le schéma des `kind` et des `attrs` | Invariant a |
| Rôle Postgres | Écrivain unique |

## 9. Porte : le banc d'écriture

L'approche n'est adoptée qu'après ce banc, sur l'hôte cible, pendant 30 min :
- 75 000 transitions par seconde ;
- 8 000 recherches de corrélation par seconde ;
- une suppression de partition en pleine charge.

**Critères** : commit de lot < 200 ms au p99, retard de consommation stable, aucun blocage
d'autovacuum.

**En cas d'échec**, le magasin passe sur ScyllaDB avec TTL. Le journal, le repli et l'API restent
identiques.

## 10. Effets sur la spec et les contrats actuels

| Emplacement actuel | Devient |
|---|---|
| `MessageStatus`, « LATEST PROJECTION… No freshness figure is promised » | Exact dans l'horizon, immédiat après le 202, historique au-delà. Issue de remise MO exposée. L'enum ne change pas : bump mineur du contrat public |
| `query_sm` | `final_date` renseignée pour tout état terminal |
| §6.22 « résolu contre le magasin de statut/CDR » | Résolu par `message-state-svc` |
| Topics | Création de `dlr.routed`, `dlr.uncorrelated`, `dlr.unmatched` et `message.state`. `mt.outcome` porte aussi les transitions de message du routeur |
| CDR | Écrivain unique, projection de `message.state`. La version est le nombre de transitions et le rang disparaît |
| Redis (§3.2) | `dlrmap` est supprimé |
| Reaper de facturation | Supprimé |
| Corrélation dans `mo-dlr-router-svc` | Supprimée : il consomme `dlr.routed` |
| Composants | Création de `message-state-svc` et de la base `message_state`, avec son propre rôle |

**Dettes payées par construction** : `dlr-delivered-n-est-pas-collant`,
`query-sm-final-date-vide-hors-delivered`, `issue-de-remise-mo-non-projetee`.

**Dettes payées en partie** :
- `dlr-d-un-client-injoignable-retarde-tous-les-autres` : la clé par compte isole les comptes ;
  l'isolation de la remise elle-même relève de la décision 4.
- `cdr-ne-distingue-pas-non-facturable-de-capture-echouee` : la distinction vient de
  `billing.events` (décision 1).

## 11. Hors périmètre

- L'isolation de la remise par compte, au-delà de la clé de `dlr.routed` : décision 4 (contrat par
  topic, F6).
- Le schéma ClickHouse du CDR en tant que projection : il sera traité avec le socle (T13,
  rétention).
