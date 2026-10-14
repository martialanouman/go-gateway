# Modèle d'autorité du crédit MT — enveloppes pré-débitées

2026-10-14 · Décision 1 du monorepo (`docs/audit-architecture-2026-10-14.md` §2 F1, §6).
Statut : design validé, à intégrer à la spec unique du monorepo.

## 1. Problème

La réservation MT écrit en durable avant chaque envoi : un Lua Redis, puis une transaction Postgres
pour la réserve, puis une seconde pour le règlement. Cela pose quatre problèmes.

- **Le débit par client est plafonné.** Il vaut voies ÷ latence de réserve, soit environ 951/s
  mesuré, contre 8 000 visés. Le coût fixe d'une transaction est d'environ 18 ms (step-287).
- **Le solde a quatre formes** (cache Redis, hold, `inflight`, grand livre plus deltas), et
  admin-api-svc écrit hors de billing-svc.
- **Le règlement passe par trois chemins** : `mt.outcome`, le gRPC du pool et le reaper qui lit le
  CDR.
- **La spec se contredit.** Elle promet à la fois « zéro dépassement strict ancré sur le durable »
  (§6.9, §8), « fail-open par défaut, ne bloque jamais l'envoi » (§1.2) et « < 5 ms p99 » (§1.3).

## 2. Décisions

| # | Décision |
|---|---|
| D1 | La garantie du solde MT bloquant est un **dépassement borné et chiffré**, et non un zéro strict. Hors panne, le seul dépassement est la marge de prêt décrite en D2. |
| D2 | **Borne = T × R**, avec T = 1 s, global. **R** est le débit plafond du propriétaire en crédits/s : la somme des plafonds de débit de ses comptes, exprimés en segments, multipliée par le `credits_per_segment` maximal de son plan. Un compte sans plafond fait prendre à R le plafond plateforme par propriétaire (`BILLING_OWNER_RATE_CAP`). |
| D3 | **Enveloppes de crédit pré-débitées** par propriétaire et par pod du routeur. billing-svc est l'unique autorité et l'unique écrivain du solde. |
| D4 | Le **fait de consommation** est le record `mt.routed` de chaque segment. Aucun topic n'est ajouté. |
| D5 | `customers.charge_on` ∈ {`submit`, `accept`, `delivery`}, défaut **`accept`**. Il remplace `rate_plans.charge_on`. |
| D6 | Le routeur évalue le tarif depuis son instantané. Le résultat voyage dans le fait et n'est jamais recalculé. |
| D7 | Facturation indisponible et enveloppe vide : le message est **garé**, jamais envoyé sans crédit ni rejeté avant l'âge maximal. Cette règle remplace le fail-open et le fail-closed. |
| D8 | billing-svc devient HA : ≥ 2 répliques en `RollingUpdate`. Redis sort entièrement du chemin de facturation. |

## 3. Périmètre

- **Concerné : tout MT bloquant.**
  - Le prépayé, avec un plancher à 0 ou à `-overdraft_limit`.
  - Le postpayé à limite dure (`credit_limit_is_hard`), avec un plancher à `-credit_limit`.
- **Non concernés, réglés depuis les faits sans enveloppe** : le postpayé à limite souple et le
  compteur MO, qui ne bloquent rien.
- **Facturation désactivée** : aucun composant de ce document n'existe, et l'étape de réservation
  est un no-op.

## 4. L'enveloppe

### 4.1 Objet

La table `credit_envelopes` n'a qu'un écrivain, billing-svc. Ses colonnes :

| Colonne | Contenu |
|---|---|
| `id` | UUIDv7 |
| `owner_type`, `owner_id` | Propriétaire, direction `mt` |
| `holder_pod`, `holder_epoch` | Détenteur : nom du pod et epoch de démarrage |
| `request_key` | `(pod, epoch, n° de demande)`, unique |
| `granted` | Crédits accordés |
| `issued_at`, `expires_at` | Émission et échéance du bail |
| `state` | `active` → `closing` → `settled` |
| `fence_offsets` | jsonb : offsets de `mt.routed` par partition, posés à la clôture propre |
| `refunded` | Crédits restitués |

### 4.2 Cycle de vie

1. **Octroi.** Un pod demande un octroi au premier message d'un propriétaire sans enveloppe, puis
   en avance, de façon asynchrone, dès qu'il passe sous le seuil θ.
   - billing-svc l'inscrit en une transaction : la ligne `credit_envelopes` et une ligne
     `envelope_grant` qui débite le grand livre.
   - Les demandes sont groupées par commit de groupe, une transaction toutes les 10 à 20 ms.
   - L'octroi est idempotent par `request_key` : un RPC rejoué rend la même enveloppe.
   - Le bail est rendu comme une **durée**. Le pod la convertit avec son horloge monotone, en
     retranchant la durée du RPC.
2. **Consommation.** À l'étape « réservation de crédit MT », dont la place dans le pipeline ne
   change pas, le routeur décrémente en mémoire `segment_count × credits_per_segment`.
   - Chaque record `mt.routed` porte `envelope_id`, les crédits du segment, le mode `charge_on` et
     la version du plan tarifaire.
   - Si l'enveloppe ne couvre pas le message et qu'un octroi refuse faute de crédit, le message est
     rejeté pour crédit insuffisant, comme aujourd'hui.
   - Si l'octroi échoue faute de facturation joignable, la règle du §7 s'applique.
3. **Clôture.** Le compteur local du pod **ne fait jamais foi** : un rejeu en place (ADR-0014) le
   décrémente deux fois. La vérité est la somme des faits dédoublonnés.
   - **Clôture propre**, au drain ou après 10 s sans consommation : le pod déclare la fin de
     consommation et transmet `fence_offsets`. billing-svc règle quand son consommateur de faits a
     dépassé ces offsets.
   - **Pod mort**, sans clôture : le pod s'interdit toute consommation après `expires_at − 5 s`.
     billing-svc règle après `expires_at + 30 s`, quand son consommateur a rattrapé ce point.
4. **Règlement de l'enveloppe.** Une ligne `envelope_refund` restitue
   `granted − Σ crédits des segments consommés non libérés`, et l'enveloppe passe en `settled`.

Une nouvelle epoch ne reprend jamais les enveloppes de l'epoch précédente.

## 5. Dimensionnement

| Paramètre | Valeur | Rôle |
|---|---|---|
| r | Débit observé du propriétaire sur le pod (moyenne glissante), plafonné à R/P | Base de la taille |
| S, taille | `max(S_min, r × H)`, H = 2 s, S_min = coût d'un message de taille maximale | Environ 2 s de trafic du pod |
| θ, seuil de recharge | S/2 | Il reste environ 1 s, bien plus que la latence d'un octroi groupé |
| D, bail | 30 s, prolongé à chaque recharge | Durée maximale d'immobilisation après un crash |
| Délai de garde | 30 s | Laisse arriver les faits produits avant la mort |
| Marge de prêt | `T × R` au total, toutes enveloppes du propriétaire confondues | Seul dépassement possible |

**Solde bas.** Soit A le solde au-dessus du plancher plus la marge de prêt non encore utilisée.
Quand A < P × S, les octrois rétrécissent à A / P. Quand A atteint 0, plus rien n'est accordé.

Le faux refus qui en résulte (des crédits restent tenus par d'autres pods) est borné par la même
marge. La borne est donc symétrique : au plus 1 s de trafic servie en trop, ou refusée à tort.

**Charge.** Environ 1 octroi par seconde, par pod et par propriétaire actif, quel que soit son
débit. Le commit de groupe ramène cela à 50 à 100 transactions par seconde.

## 6. Règlement

### 6.1 Faits et idempotence

billing-svc lit `mt.routed`, toutes catégories, dans un groupe dédié qui ne décode que les
en-têtes. Il écrit par lots dans `billing_settlements`, avec `INSERT … ON CONFLICT DO NOTHING`.

| Colonne | Contenu |
|---|---|
| `(message_id, segment_seq)` | Clé primaire |
| `envelope_id` | Enveloppe débitée |
| `credits` | Crédits du segment |
| `charge_on`, `rate_plan_version` | Figés au moment de la réservation |
| `state` | `consumed`, `captured` ou `released` |
| `accepted_at` | Renseigné à la réception d'un ROK |

Un rejeu, une double livraison ou un reroute republient la même clé et ne sont jamais refacturés
(invariant c).

### 6.2 Transitions

Les transitions se lisent sur `mt.outcome`, et sur le DLR corrélé pour le mode `delivery`. Le mode
appliqué est celui que le fait a figé, jamais le mode courant du client.

| Issue du segment | `submit` | `accept` (défaut) | `delivery` |
|---|---|---|---|
| `submit_sm_resp` ROK | capturé | capturé | en attente |
| `submit_sm_resp` en erreur | capturé | libéré | libéré |
| Aucune réponse (délai dépassé) | capturé | libéré | libéré |
| `not_sent` (annulation, lettre morte, repli épuisé) | libéré | libéré | libéré |
| DLR `delivered` | — | — | capturé |
| DLR `failed`, `expired`, `rejected` ou `undeliverable` | — | — | libéré |
| Pas de DLR à l'échéance de `DLR_RETENTION` (24 h) | — | — | capturé, marqué « sans DLR » |

- **Seul un échec explicite libère** en mode `delivery`.
- **La capture à l'échéance** est faite par un balayage par lots de billing-svc, exécuté sur une
  seule instance.
- **Le pool ne libère plus en gRPC.** Il publie un `mt.outcome` `not_sent`. Il n'existe plus qu'un
  seul déclencheur de règlement : les événements consommés par billing-svc.

### 6.3 Grand livre

Le grand livre ne contient que les mouvements du solde :
- `envelope_grant`, au débit ;
- `envelope_refund`, à la clôture : il absorbe les libérations arrivées avant le règlement de
  l'enveloppe ;
- `late_release`, une ligne par segment, pour toute libération arrivée après ce règlement
  (DLR tardif en mode `delivery`).

Les écritures hors flux (`Topup`, `Transfer`, changement de `balance_scope`) passent par l'API de
billing-svc. admin-api-svc n'écrit plus le grand livre. `SUM(grand livre) == solde`.

### 6.4 CDR

billing-svc publie chaque transition sur `billing.events`. Le CDR en projette le statut de
règlement : non facturable, en attente, capturé, capturé sans DLR ou libéré. Il en projette aussi
le montant.

## 7. Pannes

| Panne | Comportement |
|---|---|
| Crash du pod pendant un octroi | L'enveloppe orpheline expire, puis est restituée. Aucune perte |
| Redémarrage d'un pod | Les enveloppes de l'ancienne epoch suivent la règle du pod mort |
| Kafka indisponible | Ni envoi ni consommation. Réconciliation depuis les faits |
| Retard du consommateur de faits | Clôtures retardées, plus de faux refus près de zéro. Métré et alerté, jamais de dépassement |
| Décalage d'horloge | Aucun effet : bail en durée monotone, marge de 5 s |
| Déploiement de billing-svc | Aucun effet : HA, octroi transactionnel sans état local |
| `balance_scope` | La bascule est refusée tant qu'une enveloppe du client est `active` ou `closing` |
| **billing-svc ou Postgres indisponible** | Les pods consomment les enveloppes qu'ils tiennent (environ 2 s), puis **garent** |

**Garage.** Un message prépayé sans enveloppe, dont l'octroi échoue faute de facturation joignable,
est publié dans `mt.billing-wait.<catégorie>`, clé par propriétaire.
- Il ne bloque ni sa lane ni les propriétaires non facturés.
- Il est rejoué dès qu'un octroi réussit à nouveau.
- Il est rejeté explicitement, avec le code `billing_unavailable` et un CDR `rejected`, au-delà de
  son âge maximal. L'âge maximal est la validité du message si elle est posée, 1 h sinon.
- La borne T reste stricte.

## 8. Invariants

- **(c) refondu.** Un segment est facturé au plus une fois : c'est la clé primaire
  `(message_id, segment_seq)`.
- **Conservation.** Pour toute enveloppe `settled`,
  `granted = Σ crédits des segments consommés non libérés au règlement + refunded`, et
  `SUM(grand livre) == solde`.
- **Borne.** Le dépassement d'un propriétaire est ≤ T × R, crash et panne de facturation compris.
- **Chemin chaud sans réseau.** Une réservation couverte par une enveloppe ne fait aucun appel
  sortant.

## 9. Tests bloquants

Tous tournent avec le vrai Kafka et le vrai Postgres (testcontainers). Le code est muté sous chacun
d'eux : retrait de `ON CONFLICT`, formule de restitution, marge de bail, filtre d'isolation du
garage.

| Test | Prouve |
|---|---|
| Double livraison, rejeu en place et reroute du même segment | Invariant c |
| Table mode × issue : les 3 modes contre chaque ligne du §6.2 | §6.2 |
| P pods, propriétaire à zéro, consommation concurrente | Borne |
| Pod tué en pleine enveloppe (`Cuttable`) | Conservation après le délai de garde |
| billing-svc coupé, puis **ralenti** | Garage, isolation, rejeu ; une recharge lente n'arrête pas un propriétaire actif |
| Client de facturation qui panique s'il est appelé alors qu'une enveloppe couvre le message | Chemin chaud sans réseau |
| Séquences aléatoires d'octrois, de faits, d'issues et de clôtures | Conservation |

**Critère de réussite.** Sur le banc de step-287, un seul propriétaire tient **8 000 SMS/s
traversants**.

## 10. Effets sur la spec et le contrat actuels

| Emplacement actuel | Devient |
|---|---|
| §6.9 « Autorité du solde = grand livre Postgres… cache Redis réhydraté » | D3. Redis sort de la facturation, il n'y a plus de réhydratation |
| §1.2 « fail-open par défaut, fail-closed en option » | D7, le garage |
| §8 « garantie de zéro dépassement MT réellement stricte » | D1 et D2 |
| §6.9 « réservation atomique (Lua) sur `billing:balance:mt:{owner}` » | Consommation d'enveloppe en mémoire |
| §6.9 « un message jamais envoyé est libéré par le pool » | `mt.outcome` `not_sent` |
| `rate_plans.charge_on` (`submission`/`delivery`) | `customers.charge_on` (`submit`/`accept`/`delivery`). Rupture de contrat Admin : bump majeur |
| Reaper qui décide à partir du CDR | Supprimé. Voir le §11 |
| ADR-0022 (deltas repliés en différé) | Conservée pour les octrois (pas de verrou de ligne par propriétaire). Elle n'est plus sur le chemin d'un message |
| ADR-0024 (règlement depuis `mt.outcome`) | Conservée et généralisée |

## 11. Dépendances et hors périmètre

- **Segment consommé sans issue au-delà de l'âge maximal.** Il relève du magasin d'état du message
  (décision 2, F2). D'ici là : alerte, et aucune libération aveugle.
- **DLR enrichi** (propriétaire, segment) : il dépend de la corrélation unique de la voie retour.
- **Isolation du garage** : elle doit être cohérente avec le contrat par topic (décision 4, F6).
- **Hors périmètre** : la facturation externe (§6.10) et le partitionnement physique de
  `billing_settlements` (rétention, décision T13).
