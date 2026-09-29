# ADR-0020 : La catégorie de trafic est déclarée par sender ID ; elle fixe le `priority_flag` sortant et réserve des connecteurs

**Status:** Accepted
**Date:** 2026-09-29
**Deciders:** Équipe plateforme. Arbitrages utilisateur du 29/09/2026 : pas de ML ; les champs de priorité
servent les connecteurs sortants ; la catégorie se déclare **par sender ID**, avec `marketing` par défaut et une
déclaration explicite pour toute autre valeur ; les contrôles vérifient que le trafic correspond à sa
déclaration ; tout sender ID doit être enregistré, un expéditeur inconnu est rejeté.
**Réf spec:** passerelle §1.2bis (l.65, quiet hours), §6.1 (routage), §6.8 (connecteurs), §6.19 (sender ID),
§9 (l.1046) ; ADR-0004 ; step-282 ; `debts/quiet-hours-reportees.md`

## Context

La passerelle ne distingue pas OTP, transactionnel et marketing. La spec en fait une impossibilité : « une
classification du trafic […] n'est pas fiable sans modèle ML » (l.65), et c'est pour cette raison que les quiet
hours sont reportées.

Un même client envoie les trois types de message, souvent par le même compte. La catégorie ne peut donc pas
se déclarer au niveau du client ni du compte.

Les champs de priorité existent, mais aucun n'agit :
- `priority` (REST, 0–3, `internal/restapi/messages.go:41`) et `priority_flag` (SMPP,
  `internal/smppserver/submit.go:60`) sont acceptés et transportés sur `mt.inbound`
  (`internal/pipeline/wire.go:35`). Ils se perdent au routeur : ni `mt.routed` ni `buildSubmit`
  (`internal/connectorpool/mapping.go:25`) ne les portent, et le SMSC reçoit toujours `priority_flag = 0` ;
- `smsc_connectors.priority_flag_default` et `smsc_connectors.priority_tier` sont stockés et servis par l'API
  Admin, mais aucun code d'exécution ne les lit. La spec ne définit pas `priority_tier`.

Le `priority_flag` sur le fil ne suffit pas, parce que l'attente a lieu **dans la passerelle**. `mt.routed` est
partitionné par `(connector_id, shard)`, et chaque partition est FIFO. Un OTP qui arrive derrière dix minutes
de marketing sur un connecteur au débit plafonné attend ces dix minutes, quel que soit son flag.

## Decision

**1. Déclaration par sender ID, pas classification.** `sender_ids.traffic_category` :
`otp | transactional | marketing`, `NOT NULL DEFAULT 'marketing'`. L'opérateur la pose via l'API Admin,
expéditeur par expéditeur. C'est un **engagement contractuel du client** sur ce qu'il enverra sous ce nom, et
non une inférence de la passerelle.
- **Le défaut est `marketing`**, la catégorie la plus contrainte. Toute autre valeur est un acte explicite.
- **L'expéditeur qui compte est celui que le client soumet**, avant toute réécriture (§6.19). La catégorie
  se lit à l'étape d'autorisation du sender ID (`internal/pipeline/senderid`), qui consulte déjà
  `sender_ids`. L'ordre du pipeline ne change pas.
- **Tout expéditeur doit être enregistré, numérique compris.** Un `source_addr` sans ligne `sender_ids`
  `active` est rejeté (`ErrSenderIDNotAuthorized`). Les politiques `allow_unregistered_numeric` et
  `disabled` disparaissent, et `smpp_accounts.sender_id_policy` avec elles : un message n'a jamais de
  catégorie inconnue.

**2. Priorité effective d'un message.** Le routeur la calcule à l'étape de résolution de route, depuis la
catégorie de l'étape 1, et la porte sur `mt.routed` :

| Catégorie | Rang | Priorité par défaut | Plafond |
|---|---|---|---|
| `marketing` | 0 | 0 | 0 |
| `transactional` | 1 | 1 | 2 |
| `otp` | 2 | 3 | 3 |

`priorité effective = min(max(priority demandée, défaut), plafond)`. Le client peut relever un transactionnel
urgent jusqu'à 2. Aucune demande ne sort de sa catégorie. Un client qui n'envoie rien obtient le défaut :
REST et SMPP valent 0 par défaut, et 0 ne peut pas signifier « absent ».

**3. `priority_flag_default` : le flag sortant.** `buildSubmit` écrit la priorité effective quand elle est
non nulle, sinon le `priority_flag_default` du connecteur. C'est la sémantique de ses voisins `*_default`
(`registered_delivery_default`, `validity_period_default`) : la valeur du connecteur quand le message n'en
impose pas.

**4. `priority_tier` : le rang minimal qu'un connecteur accepte.** `0` accepte tout, `1` n'accepte que
transactionnel et OTP, `2` n'accepte que l'OTP. Un connecteur dont le `priority_tier` dépasse le rang du
message est traité **comme indisponible**, avec la même conséquence que pour un connecteur indisponible
aujourd'hui :
- au niveau L0 (numéro exact), on retombe sur L1/L2 (ADR-0004) ;
- dans une stratégie de distribution, la cible est sautée ;
- il est retiré de la `fallback_chain` que le routeur construit. Le reroute, la file de parking et le rejeu
  héritent donc du filtre sans code propre ;
- si plus rien ne reste, la réponse est `ErrNoRoute`. Il n'y a **aucun code d'erreur neuf**.

La garde s'applique après la résolution, aux trois niveaux, script compris. Elle ne court-circuite **aucune**
étape de conformité (invariant b) et ne réordonne pas le pipeline.

C'est ce qui donne à l'OTP une vraie priorité **aujourd'hui** : un connecteur réservé n'a pas de backlog
marketing devant lui.

**5. Contrôles de conformité à la déclaration.** Un `rule_type` anti-spam `category_mismatch`, avec les
portées habituelles (`global` / `customer` / `smpp_account`), vérifie que le trafic d'un expéditeur
ressemble à sa catégorie déclarée :
- `otp` : un code de 4 à 8 chiffres, aucune URL, un corps court ;
- `transactional` : aucun marqueur promotionnel (liste configurable) ;
- `marketing` : rien à vérifier, c'est la catégorie la plus contrainte.

L'action vaut `flag` par défaut : compteur par sender ID, visible au tableau de bord, qui alimente la
réputation (§6.5). `block` est configurable par règle. Les règles tournent **en mémoire, sur le corps
transitoire, à l'étape anti-spam existante**. Ni le corps ni un extrait ne sort dans un log, une métrique ou
un span (invariant a) : seuls l'identifiant de règle et le verdict en sortent.

Le ML reste hors périmètre. On le rouvrira si les règles laissent passer une fraude mesurée (§9, l.1046).

## Options Considered

### Option A : catégorie par sender ID + réservation par `priority_tier` (retenue)
| Dimension | Évaluation |
|---|---|
| Complexité | Faible : une colonne sur `sender_ids`, une table de 3 lignes, un filtre dans la résolution existante |
| Coût à chaud | Nul : lecture de l'instantané de sender ID déjà chargé, sans appel réseau |
| Isolation | Réelle pour un connecteur réservé ; nulle sur un connecteur partagé |
| Invariant a | Intact : la vérification lit le corps en mémoire, comme l'anti-spam aujourd'hui |

**Pour :** colle à l'usage (un client, trois flux, un expéditeur par flux) ; c'est la granularité des
enregistrements réglementaires et opérateurs (sender ID + cas d'usage) ; débloque les quiet hours ; donne un
sens aux trois champs existants ; aucun topic ni consommateur neuf.
**Contre :** un expéditeur ne peut porter qu'une catégorie, donc un client qui mélange sous un même nom doit
en enregistrer un second. Sur un connecteur `priority_tier = 0`, l'OTP reste dans la même file FIFO que le
marketing.

### Option B : catégorie déclarée par compte
**Contre :** un client envoie les trois types, souvent par un seul compte. L'obliger à ouvrir un compte par
catégorie multiplie les identifiants, les binds et les `max_sessions`, pour exprimer ce que l'expéditeur dit
déjà. Écartée.

### Option C : catégorie déclarée par message
**Contre :** chaque client se déclarerait OTP. Une déclaration qui change à chaque envoi n'engage personne.
Écartée.

### Option D : file prioritaire dans le pool (`mt.routed.priority`, ou sous-files par catégorie)
**Pour :** une préemption réelle sur un connecteur partagé.
**Contre :** double les partitions de `mt.routed`, impose une politique anti-famine, et duplique le chemin
reroute / parking / rejeu / dead-letter. Aucune mesure ne la justifie encore. **Différée** :
`debts/pas-de-file-prioritaire-sur-un-connecteur-partage.md`.

### Option E : classification ML du contenu
Écartée par arbitrage utilisateur. Elle exige un corpus en clair que l'invariant a et ADR-0008 interdisent de
constituer, et une inférence sur le chemin critique.

## Trade-off Analysis

A et D visent la même chose : que l'OTP n'attende pas derrière le marketing. A l'obtient par **séparation
physique** : l'opérateur réserve un bind ou un connecteur, et la file n'est plus partagée. D l'obtient par
**ordonnancement**, dans une file partagée. A ne coûte que de la configuration, mais exige un connecteur
réservé par opérateur de réseau pour en profiter. D sert tout le monde, mais coûte un second chemin de
données complet. Commencer par A ne ferme pas D : la priorité effective portée sur `mt.routed` est
exactement la clé dont D aurait besoin.

Entre A et B, la question est de savoir qui porte l'engagement. Le compte est un canal technique, alors que
l'expéditeur est ce que l'abonné voit et ce que les régulateurs enregistrent. C'est là qu'un mensonge se
détecte, et là qu'on le sanctionne sans couper les autres flux du client.

## Consequences

- **La prémisse de la spec tombe.** La classification n'est plus un problème de ML mais une déclaration :
  `debts/quiet-hours-reportees.md` perd sa raison technique et devient une dette purement produit.
- **La priorité au routeur appartient à step-282.** Une fois `mt.inbound` clé par `message_id`, une rafale
  marketing occupe toutes les partitions devant un OTP. L'ordonnancement que step-282 doit chiffrer doit
  prendre le rang de catégorie comme poids, ou dire pourquoi il s'en passe.
- **Un client qui envoyait depuis un numéro non enregistré est rejeté** dès la migration. Il faut inventorier
  les comptes en `allow_unregistered_numeric` ou `disabled` et enregistrer leurs expéditeurs **avant**.
- **Tout expéditeur existant devient `marketing`** à la migration. Pour les clients OTP et transactionnels,
  l'opérateur doit déclarer leurs expéditeurs **avant** d'activer une réservation par `priority_tier`, sinon
  leur trafic perd son connecteur réservé.
- **Plus facile :** reporting et litiges par catégorie et par expéditeur, dès que le CDR la porte.
- **Plus dur :** l'opérateur doit penser la réservation. Un connecteur `priority_tier = 2` sans autre cible
  rend `ErrNoRoute` à tout le trafic non-OTP qui y est routé. C'est une erreur de configuration, visible au
  simulateur de route du tableau de bord.
- **À revoir** si un SMSC rejette `priority_flag ≠ 0` (`ESME_RINVPRTFLG`). Il faudrait alors un plafond par
  connecteur, qu'on n'ajoute pas sans cas réel.

## Action Items

1. [ ] Schéma et migration : `sender_ids.traffic_category` ; suppression de `smpp_accounts.sender_id_policy`. Anti-spam : `rule_type` `category_mismatch`.
   CDR ClickHouse : `traffic_category` et `priority`.
2. [ ] Contrat Admin : `traffic_category` sur le sender ID (optionnel avec défaut, bump MINEUR),
   descriptions de `priority_flag_default` et `priority_tier`, `category_mismatch` dans l'enum des règles
   anti-spam. Suppression de `set-account-sender-id-policy` et du champ `sender_id_policy` du compte
   (bump MAJEUR). Contrat public : description de `priority` (défaut et plafond par catégorie, bump MINEUR).
   Contrats déclarés **avant** l'implémentation.
3. [ ] Routeur : catégorie lue à l'autorisation du sender ID ; priorité effective calculée et portée sur
   `mt.routed` ; garde `priority_tier` aux trois niveaux et sur la `fallback_chain`.
4. [ ] Pool : `buildSubmit` écrit le `priority_flag` (effective, sinon `priority_flag_default`).
5. [ ] Anti-spam : règle `category_mismatch`, avec un garde d'invariant a (aucun extrait du corps dans le
   verdict).
6. [ ] Spec §1.2bis (l.65), §6.1, §6.5, §6.8 (`priority_tier` défini), §6.19, §9 (l.1046) ; glossaire ;
   fiche des quiet hours mise à jour.
7. [ ] step-282 : l'ordonnancement au routeur tient compte du rang de catégorie.
8. [ ] Spec du tableau de bord : catégorie sur l'écran des sender IDs (défaut `marketing`, passage à
   `otp`/`transactional` confirmé) ; retrait de la politique de sender ID du compte (§ périmètre, route
   `sender-id-policy`) ; `priority_flag_default` et `priority_tier` expliqués dans le formulaire du
   connecteur, avec un avertissement sur un `priority_tier` sans autre cible ; bandeau de catégorie au
   simulateur de route ; `category_mismatch` dans l'UI anti-spam (§6.6) ; filtre et ventilation par
   catégorie au CDR Explorer (§6.4) et au trafic temps réel (§6.3).
