# Contrats de topics et lanes — un catalogue, un runtime

2026-10-17 · Décision 4 du monorepo (`docs/audit-architecture-2026-10-14.md` §2 F6, §6).
Statut : design validé.

## 1. Problème

Il n'y a pas de domaine d'isolation : une panne ou un tenant bloque ses voisins.

- **La lane du routeur est la partition**, et `mt.inbound` mélange tous les clients. Un échec
  transitoire (facturation en `Recreate`, ClickHouse en panne pour l'insert synchrone du CDR
  `rejected`) arrête toute la partition, clients non facturés compris.
- **Voie retour** : 24 comptes sans bind ni webhook ont laissé 1 115 342 DLR en retard, écoulés à
  ~18/s, et les DLR des autres comptes attendaient derrière eux (dette
  `dlr-d-un-client-injoignable-retarde-tous-les-autres`).
- **Pas de dead-letter sur `mt.inbound`** ; un poison immobilise sa partition.
- **Seul le MT est rejouable.**
- **Chaque consommateur a improvisé sa politique d'échec.** Les classes de trafic (ADR-0021) ont été
  posées tardivement.

## 2. Décisions

| # | Décision |
|---|---|
| D1 | **On promet l'isolation des pannes, pas l'équité de volume.** Un poison, une dépendance propre à un tenant ou un endpoint client injoignable ne retarde jamais un autre tenant. Le volume reste borné par le plafond d'admission. |
| D2 | **Une lane ne bloque jamais sur une dépendance propre à un tenant** : le message est garé. Elle ne bloque que sur une dépendance globale. |
| D3 | **Âge maximal par catégorie** : `otp` 10 min, `transactional` 24 h, `marketing` 72 h, plafonné par la `validity_period` du message. **Surchargeable par client, à la baisse uniquement.** Au-delà : `expired`, terminal, non facturé, jamais envoyé. |
| D4 | **Remise au client : 24 h au maximum depuis la réception** de l'événement par la passerelle, puis dead-letter. Un compte sans moyen de réception déclaré ne met rien en file. |
| D5 | **Un catalogue unique** (`contracts/topics.yaml`) déclare chaque topic et chaque groupe ; la configuration Kafka en est générée. |
| D6 | **Un runtime unique** (`lane`) applique ce contrat : classification, garage, expiration, dead-letter, rejeu, commits. Aucun consommateur ne les réécrit. |
| D7 | **Le garage par tenant est une table Postgres du service**, pas un topic : il doit se vider tenant par tenant. |

L'équité de volume (file équitable par tenant dans les lanes) pourra s'ajouter à la lane du routeur si
la sursouscription devient un problème mesuré, sans changer les contrats. Pulsar (dead-letter,
retry topics, livraison différée natifs) a été écarté : trois décisions reposent déjà sur la sémantique
Kafka (partition manuelle, topics compactés, offsets).

## 3. Le catalogue

### 3.1 Topic

| Champ | Contenu |
|---|---|
| `key` | `message_id`, `account_id`, `connector_account_id`, une entité, ou `manual` (partition explicite, décision 3) |
| `order` | `per_key` ou `none` |
| `duplicate_unit` | Message, segment ou événement, **et où le doublon est absorbé** : clé primaire, repli d'état, ou rien (un SMS en double, dit comme tel) |
| `payload` | Message protobuf versionné |
| `body` | `none` ou `sealed`. Jamais `clear` (invariant a, T8) |
| `partitions`, `retention`, `compacted` | Configuration Kafka. La rétention minimale se **calcule** depuis les contraintes déclarées (par exemple 72 h + marge pour la reconstruction de la décision 2) |
| `producers` | Services autorisés à produire |

### 3.2 Groupe de consommateurs

| Champ | Contenu |
|---|---|
| `topic`, `service` | |
| `tenant` | Champ de l'enveloppe qui identifie le tenant (`account_id`, `customer_id`, `connector_account_id`) ou `none` |
| `dependencies` | Pour chaque dépendance, un mode (§4.3) : `blocking`, `park_by_tenant`, `park_global`, `fail_open` (avec justification écrite) |
| `concurrency` | `per_partition` (défaut) ou `per_tenant` avec un plafond en vol (§5.2) |
| `max_age` | Politique par catégorie (D3) ou durée fixe (24 h pour la remise) |
| `dead_letter` | Topic de dead-letter, **obligatoire dès le premier jour** |
| `start` | `earliest` pour un groupe neuf. `latest` exige une justification écrite |

### 3.3 Génération

Un Job idempotent applique partitions, rétention et `cleanup.policy` depuis le catalogue ; il remplace
toute création de topic à la main. Une page de documentation en est générée.

### 3.4 Gardes (tests bloquants)

1. **Correspondance dans les deux sens** : tout topic produit ou consommé dans le code a son entrée ;
   aucune entrée sans usage. Garde AST, qui nomme les constructeurs du runtime.
2. **Un service ne produit que sur les topics où il figure dans `producers`.**
3. **Une dépendance appelée dans une lane est déclarée** : le runtime ne fournit son client qu'à
   travers sa déclaration.
4. **Un topic où transite le corps** a `body: sealed` et une rétention couverte par l'inventaire RGPD
   (T8).
5. **Un topic `order: per_key` a un nombre de partitions figé** : la garde refuse de l'augmenter dans
   le catalogue (§6.1).
6. **`maxReplicas` d'un service consommant par groupe ≤ partitions du plus petit topic qu'il
   consomme** (croisement manifests `internal/deploy` × catalogue, §6.2).

## 4. Le runtime `lane`

### 4.1 Issues d'un record

L'unité est la partition : lecture par lots, traitement record par record, commit par partition
(motif lot + barrière, porté).

| Issue | Effet |
|---|---|
| `Ok` | Fait |
| `Refuse(code)` | Refus définitif : c'est un résultat métier, publié par le traitement (par exemple `rejected`) |
| `Transient(dépendance, err)` | Le runtime applique le mode déclaré de la dépendance |
| `Poison(err)` | Illisible ou contraire à un invariant : dead-letter **immédiate**, la lane continue |

### 4.2 Liste de refus

Seuls les codes que le traitement déclare définitifs donnent `Refuse`. Une erreur inconnue est
`Transient`, jamais `Refuse` : un message réessayé peut encore partir, un message refusé à tort ne
revient pas.

### 4.3 Modes d'une dépendance

- **`blocking`** — retentative sur place avec backoff ; la partition attend. Réservé à ce sans quoi
  personne n'avance : Kafka, le magasin propre au service.
- **`park_by_tenant`** — disjoncteur par **(tenant, dépendance)**. Ouvert, les records du tenant sont
  garés **sans appel** à la dépendance (une insertion, pas un timeout). Une sonde par tenant et par
  intervalle teste la dépendance. **Ordre** : tant qu'un tenant a des records garés pour une
  dépendance, ses nouveaux records y vont aussi.
- **`park_global`** — topic de garage vidé en entier au retour de la dépendance (`mt.billing-wait.*`,
  décision 1).
- **`fail_open`** — la lane continue sans la dépendance et compte l'écart. Justification écrite
  exigée ; seul le jeton d'annulation (ADR-0013) en a une.

### 4.4 Garage par tenant

- Table `lane_parking (group, tenant, dependency, origin_ts, parked_at, source_topic,
  source_partition, source_offset, headers, payload)`, dans le Postgres **du service**.
- L'offset source est commité **après** l'insertion.
- **Vidange** : disjoncteur refermé, les records du tenant sont repris dans l'ordre d'arrivée, à débit
  borné, sous `FOR UPDATE SKIP LOCKED` — deux pods ne vident jamais le même tenant. Ses nouveaux
  records ne repassent en direct qu'une fois le garage vide.
- **Borne** : un record plus vieux que `max_age` est expiré au lieu d'être traité ; la taille est
  bornée par débit du tenant × `max_age`.

### 4.5 Âge maximal

- Mesuré **toujours** depuis `origin_ts`, en-tête posé par le premier producteur (l'ACK d'un MT, la
  réception d'un DLR ou d'un MO) et conservé à travers garages, dead-letters et rejeux.
- Un record expiré passe par le `OnExpired` du traitement : `expired` pour un MT (terminal, non
  facturé, décision 2), abandon compté pour une remise. **Jamais envoyé.**

### 4.6 Dead-letter et rejeu

- Chaque groupe a sa dead-letter. Un record y porte : payload et en-têtes d'origine, `dlq_reason`,
  `dlq_at`, position source (topic, partition, offset), `origin_ts`, `replay_count`.
- **Un outil unique, `lane-replay`, pour tous les groupes.** Il republie sur le topic source avec
  `replayed_at` et `replay_count + 1` ; chaque rejeu va au journal d'audit (décision 5).
- **Un record expiré n'est pas rejouable.** Rompt avec step-129, qui mesurait l'âge depuis
  `replayed_at`.

## 5. Topics et isolation

### 5.1 Table des topics

| Topic | Clé | Ordre | Unité de doublon → absorbée par | Producteurs |
|---|---|---|---|---|
| `mt.inbound.<cat>` | `message_id` | aucun | message → clé primaire du journal d'état ; **SMS en double possible** à la frontière routeur (ADR-0014) | rest-api, smpp-server |
| `mt.billing-wait.<cat>` | `message_id` | aucun | message → idem | router |
| `mt.routed.<cat>` | manuelle (compte opérateur) | par partition | segment → **SMS en double possible** à la frontière pool (§6.7) | router, pool (republication) |
| `mt.reroute-park` | `message_id` | aucun | segment | pool |
| `mt.outcome` | `message_id` | par clé | transition → clé primaire du journal | router, pool |
| `dlr.events` | `connector_account_id` | par clé | événement → corrélation | pool |
| `dlr.uncorrelated`, `dlr.unmatched` | `connector_account_id` | aucun | événement | message-state |
| `dlr.routed` | `account_id` | par clé | événement → idempotence côté client, sinon rien | message-state |
| `mo.inbound` | `connector_account_id` | par clé | événement | pool |
| `mo.routed` | `account_id` | par clé | événement | router (MO) |
| `message.state` | `message_id` | par clé | version de l'état → CDR | message-state |
| `connector.account-status` | `connector_account_id`, compacté | par clé | dernier état | pool |
| `config.changes` | entité | par clé | événement → rechargement idempotent | admin-api |
| `<groupe>.dead-letter` | celle de la source | aucun | record | runtime |

`webhook.retry` et `webhook.dead-letter` disparaissent : le garage par tenant les remplace.

### 5.2 Lane du routeur (`mt.inbound.<cat>`)

| Dépendance | Mode |
|---|---|
| Kafka | `blocking` |
| Postgres de confirmation des opt-out | `blocking` : envoyer sans savoir si le destinataire s'est désinscrit enfreint la conformité, pour tout le monde |
| Facturation, enveloppe vide | `park_global` (décision 1) |
| Redis (cache des routes exactes) | `blocking`, en attendant la décision 7 |

L'insertion synchrone du CDR disparaît : un rejet est publié sur `mt.outcome` (décision 2). **Il ne
reste dans le routeur aucune dépendance propre à un tenant qui puisse bloquer une partition.**

### 5.3 Voie retour (`dlr.routed`, `mo.routed`)

`tenant = account_id` ; dépendance « endpoint client » (webhook ou bind SMPP) en `park_by_tenant` ;
`max_age = 24 h`.

- **Client lent mais vivant** : son disjoncteur ne s'ouvre jamais, le garage ne le couvre pas.
  `concurrency: per_tenant` confie les records d'une partition à des workers par tenant, avec un
  plafond en vol (8 par compte par défaut). L'offset commité ne dépasse jamais le plus ancien record
  non terminé. Un client lent ne ralentit que sa file.
- **Compte sans moyen de réception déclaré** : `Refuse(no_receiver)`, sans appel ni garage. Un DLR est
  compté et abandonné (l'état reste consultable par `GET /messages/{id}`, puis dans le CDR) ; un MO va
  en dead-letter. Cache négatif relu au rechargement de configuration.
- **Client SMPP déconnecté** : endpoint en panne. Garage, sonde de présence du bind toutes les 5 s,
  vidange au rebind, dans la limite des 24 h.

### 5.4 Autres lanes

- **Pool** : tenant = compte opérateur ; un compte en panne republie (décision 3), il ne gare pas.
- **message-state, facturation, projection CDR** : leur seule dépendance est leur magasin, `blocking`.

## 6. Topologie

### 6.1 Classes de trafic et partitions

Trois topics par étage (`otp`, `transactional`, `marketing`), priorité de l'ADR-0021. **Un topic
`order: per_key` a un nombre de partitions fixé à sa création** : en ajouter redistribue les clés et
casse l'ordre. Il est dimensionné large ; le changer est une migration vers un topic neuf. Les topics à
partition manuelle (`mt.routed.*`) grossissent librement.

### 6.2 HPA et partitions

Pour un service qui consomme par groupe, `maxReplicas` ≤ partitions du plus petit topic consommé :
au-delà, les pods en trop sont inactifs et l'HPA croit avoir monté en capacité. Garde §3.4-6. Le pool
n'est pas concerné : les baux l'assignent (décision 3).

## 7. Pannes

| Panne | Effet |
|---|---|
| Postgres du service | Le garage est impossible : `park_by_tenant` devient `blocking`. Le garage est le magasin du service, donc une dépendance globale |
| Production vers la dead-letter | `blocking` : on ne commite jamais un record qui n'est allé nulle part |
| Redémarrage d'un pod | Disjoncteurs par tenant repartis fermés, rouverts aux premiers échecs ; le garage est durable |
| Dépendance d'un tenant en panne au-delà de `max_age` | Ses records expirent dans le garage, qui ne grossit pas sans fin |

## 8. Tests bloquants

- **Runtime** : un poison ne bloque pas sa partition ; le garage par tenant respecte l'ordre par clé ;
  l'âge survit au garage, à la dead-letter et au rejeu ; la vidange est exclusive sous `SKIP LOCKED` ;
  un record expiré n'est jamais traité.
- **Non-régression de la dette** : 24 comptes sans moyen de réception et un client à endpoint lent, à
  côté d'un client normal : **p99 de remise des DLR du client normal < 1 s**. Variante : endpoint qui
  tombe puis revient, garage vidé dans l'ordre, sans perte.
- **Gardes du catalogue** : §3.4.

## 9. Porté, réécrit, neuf

| | |
|---|---|
| **Porté** (dans le runtime) | Lot + barrière + commit par partition ; rejeu en place sur une partition sans progrès (step-285) |
| **Réécrit sur le runtime** | Lane du routeur (logique métier portée), remise de `mo-dlr-router-svc` |
| **Neuf** | Catalogue et son Job, `lane_parking`, `lane-replay` (remplace `mt-replay`), gardes |

## 10. Effets sur la spec et les contrats actuels

| Où | Effet |
|---|---|
| Spec §6.7, guide §10 | Âge maximal depuis `origin_ts` ; un record expiré n'est pas rejouable ; dead-letter par groupe |
| Spec §5 (webhooks) | Retentative du runtime, 24 h au maximum. `webhooks.retry_policy_json` et `internal/webhook.RetryPolicy` sont supprimés |
| Âges maximaux | Politique par catégorie ; surcharge par client à la baisse : un champ sur `customers`, **bump mineur** de l'Admin API |
| Topics | `webhook.retry`, `webhook.dead-letter` supprimés ; `<groupe>.dead-letter` pour chaque groupe |
| Dettes soldées par conception | `un-poison-immobilise-son-consommateur-sans-redemarrage`, `dlr-d-un-client-injoignable-retarde-tous-les-autres`, `drainer-n-estampille-pas-le-rejeu` ; rejeu limité au MT |
| Décision 8 | API de collecte des MO et des DLR (`GET /inbound?cursor=…`) notée, non retenue ici |

## 11. Hors périmètre

- **Équité de volume par tenant** (D1) : ajoutable à la lane du routeur sur mesure de la
  sursouscription.
- **Fenêtre d'attente du DLR opérateur** : inchangée, fixée par la décision 2 (`validity_period`,
  plafond 72 h). D4 ne borne que notre remise au client.
