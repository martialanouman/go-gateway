# step-209 — `cancelled` ne doit plus enterrer un message réellement livré

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201c · **Bloque :** —

## But
Fermer la course entre l'annulation et l'envoi : aujourd'hui un `cancel_sm` accepté pendant que le
statut projeté est encore `accepted` écrit une ligne `cancelled` de rang **60**, qui enterre
définitivement les `enroute` (20) et `delivered` (40) écrits ensuite. `GET /messages/{id}` affiche donc
`cancelled` **pour toujours** sur un message livré à l'abonné et facturé.

## Le constat
- `internal/cancel/cancel.go:86-92` décide l'annulation sur le **statut lu** dans le CDR : `accepted` ⇒
  autorisé, `enroute` ou terminal ⇒ `ErrCancelFailed`.
- `internal/storage/clickhouse/cdr.go:39-53` : `cancelled` vaut **60**, au-dessus de `delivered` (40) et
  `failed` (50). Le `ReplacingMergeTree` garde le rang max **quel que soit l'ordre d'insertion**.
- La course **pré-existe** et était assumée (`cancel.go:65-68`). Ce que step-201c a changé, c'est sa
  **taille** : l'`enroute` n'est plus écrit synchrones par le connecteur mais projeté depuis
  `mt.outcome`. Fenêtre mesurée à ~30 ms en régime normal (backlog `mt.outcome` 20 → 33 sur le run de
  référence), mais bornée seulement par l'alerte de lag à 30 s sous saturation ClickHouse.

L'argent n'est pas touché : la facturation suit le grand livre réserve/capture, idempotent par
`message_id`, qui ne lit jamais ce statut. Le dommage est un dommage **d'affichage** — mais il est
définitif et visible du client, et le contrat public le documente désormais comme une limite connue
(`api/openapi-public.yaml`, schéma `MessageStatus`).

## Points d'implémentation clés
- **Le rang n'est pas le levier qu'il paraît.** Poser `cancelled` à 45 le laisse **au-dessus** de
  `delivered` (40) : le scénario central n'est pas corrigé. Le descendre sous 40 change le sens de
  `cancelled` dans le cas « course sans DLR », où le message a bien quitté la file. Et le rang étant la
  version du `ReplacingMergeTree`, le changer **rejuge toutes les lignes historiques**.
- **La vraie autorité est le connecteur**, pas la projection — `cancel.go` le dit déjà. Deux directions à
  arbitrer : le connecteur publie une issue corrective quand le drapeau d'annulation arrive après son
  propre contrôle ; ou la projection refuse d'écraser un `cancelled` par un `enroute` postérieur, ce qui
  demande une règle de résolution plus riche qu'un rang scalaire.
- Trancher d'abord **ce que `cancelled` signifie** quand le connecteur a dispatché malgré tout. C'est une
  décision de spec (§6.22), pas un choix d'implémentation — elle passe par l'échelle spec → Fable →
  arbitrage humain, et probablement par un ADR.

## Design arrêté (2026-08-07)

Réf : [ADR-0013](../docs/adr/0013-annulation-jeton-vainqueur-unique.md), qui porte le détail et les
options écartées.

### DN1 — Le sens de `cancelled` est déjà tranché par la spec
§6.22 : « annule un message **pas encore envoyé** au SMSC ; s'il a déjà été soumis,
`ESME_RCANCELFAIL` ». `cancelled` signifie donc **jamais parti**.
**Raison :** la spec tranche, on ne délibère pas. Corollaire qui gouverne tout le reste : écrire cette
ligne sur un message dispatché est *faux*, pas mal classé — ce qui écarte les solutions qui se
contentent de la reclasser (démotion de rang, statut provisoire, résolution à la lecture). La ligne ne
doit pas être écrite.

### DN2 — La clé `cancel:{id}` devient un jeton à vainqueur unique
`SET … NX GET` en une commande, une clé. Connecteur avant `submit_sm` : revendique `"dispatched"`
(TTL 5 min). Canceller : revendique `"cancel"` (TTL 72 h). L'ancienne valeur retournée tranche —
`"dispatched"` côté Canceller ⇒ `ErrCancelFailed` et **aucune ligne écrite**.
**Raison :** le connecteur est déjà l'autorité sur « déjà dispatché » (`cancel.go` le dit). Le jeton en
tire les conséquences au lieu de faire confiance à une projection qu'on sait retardée.
**Vérifié en source** (Context7, `/redis/go-redis` v9.21.0) : `SetArgs{Mode:"NX", TTL, Get:true}`
produit `SET key value EX <ttl> NX GET` et renvoie l'ancienne valeur (`redis.Nil` si absente).

### DN3 — Le `GET` est structurel
Sans lui, le connecteur ne distingue pas un jeton `cancel` de **son propre** jeton posé avant un crash :
après redélivrance Kafka il écrirait `cancelled` sur un message ni envoyé ni annulé.
**Raison :** sans cette distinction, le correctif introduit le même bug à l'envers.

### DN4 — Deux TTL qui se recouvrent
`cancel` = 72 h (survit au `validity_period` max). `dispatched` = 5 min.
**Raison :** le jeton ne couvre que la fenêtre où la projection ment. Au-delà, `mt.outcome` a écrit
`enroute` (alerte de lag à 30 s ⇒ 10× de marge) et la lecture CDR refuse l'annulation avant Redis.
Invariant à tenir, écrit dans le commentaire de la const : **le TTL dépasse le seuil de l'alerte de lag**.
**Coût :** une clé par message dispatché (~2,4 M à 8 000/s, ~300 Mo). Le chemin chaud échange une
lecture Redis contre une écriture — nombre d'allers-retours inchangé.

### DN5 — Le jeton est pris là où `Exists` était lu
`connectorpool.go`, avant le contrôle de reroute et l'attente AIMD.
**Raison :** diff minimal, et le biais va dans le bon sens. Prendre tôt refuse quelques annulations
légitimes (message rerouté, en attente de throttle) : un faux négatif coûte un `ESME_RCANCELFAIL` et
n'écrit rien de faux ; le faux positif inverse est le bug. En cas de doute, refuser.

### DN6 — Changement observable assumé
Un `cancel_sm` dans la fenêtre répondait `ESME_ROK` en mentant ; il répond `ESME_RCANCELFAIL`, ce que
§6.22 prescrit pour « déjà soumis ». Aucun code d'erreur nouveau.

### DN7 — Fail-open conservé, trou résiduel documenté
Erreur Redis côté connecteur ⇒ on envoie (l'annulation est best-effort ; on ne fige pas la livraison
sortante sur une panne Redis). Résiduel : si le jeton du connecteur échoue et que Redis revient avant
le `cancel_sm`, le Canceller gagne un jeton indu et la ligne fausse revient. Borné aux pannes Redis
partielles, documenté, non poursuivi.

### DN8 — Un jeton inconnu n'est pas un jeton libre (constat de revue, bloquant)
Les deux sites lisaient toute valeur non reconnue comme « libre ». Or l'ancien `Mark` écrivait `"1"`
dans **cette même clé**, avec 72 h de TTL : pendant un déploiement progressif, un message annulé juste
avant la bascule porte `cancel:{id} = "1"`. Le nouveau connecteur l'aurait envoyé quand même, et le
Canceller aurait écrit la ligne — la régression exacte que la step corrige.
**Décision :** seul `HolderNone` autorise à avancer. Côté Canceller, tout le reste refuse ; côté
connecteur, tout sauf `HolderNone` et son propre `HolderDispatched` est honoré comme une annulation.
**Raison :** application directe de DN5 (« en cas de doute, refuser »), donc pas de nouvel arbitrage.
Le défaut inverse était un fail-open silencieux là où le design exige un fail-closed.

### DN9 — On ne revendique pas le jeton comme `HolderNone` (constat de revue)
`Claim` acceptait `HolderNone` comme revendiquant. Elle écrivait alors une valeur **vide** avec 72 h de
TTL, que tout revendiquant ultérieur relit comme « jeton libre » : le connecteur envoie, le Canceller
écrit la ligne, les deux croient avoir gagné. L'arbitrage cesse d'arbitrer sans erreur ni test rouge.
**Décision :** `Claim` refuse `HolderNone` et n'écrit rien.
**Raison :** piège latent (aucun appelant ne le fait aujourd'hui) mais invisible à l'exécution, sur
l'unique primitive dont dépend tout le correctif. Le refus à la porte est le seul endroit où cette
mauvaise utilisation reste observable.

## Tests (écrits dans la même PR)
- Une annulation acceptée sur un statut `accepted` périmé, suivie d'un `enroute` puis d'un `delivered`,
  ne laisse pas le message en `cancelled` → `TestRaceScenarioLeavesADeliveredMessageDelivered`.
- Le cas légitime reste intact : un message réellement annulé avant dispatch lit bien `cancelled` →
  `TestConnectorSkipsCancelledMessage`, `TestConnectorReleasesOnCancel`,
  `TestConnectorStillWritesCancelledRowDirectly`.
- Aucune régression sur la résolution des lignes historiques : le rang ne bouge pas →
  `TestCancelledStillOutranksEveryOtherState`.

## Tableau des mutations

| Mutation | Test tombé | Message |
|---|---|---|
| `Mode: "NX"` retiré de `Claim` | `TestClaimReportsTheHolder`, `…WinnersTTL` | `token = "cancel" … the loser overwrote the winner` · `TTL grew from 5m0s to 72h0m0s` |
| Refus sur jeton perdu désactivé (Canceller) | `TestCancelLosesTheRaceToTheConnector` | `a lost race wrote 1 CDR row(s), want 0` |
| Idem, vu de bout en bout | `TestRaceScenarioLeavesADeliveredMessageDelivered` | `final status = "cancelled", want delivered` |
| `HolderDispatched` traité comme annulation (connecteur) | `TestConnectorSubmitsOnItsOwnToken` | `its own token must write no cancelled CDR row` |
| DN8 annulé — jeton inconnu relu comme libre (connecteur) | `TestConnectorHonoursAnUnknownToken` | `the message was sent` |
| DN8 annulé (Canceller) | `TestCancelRefusesAnUnknownHolder` | `an unknown holder wrote 1 row(s), want 0` |
| Garde du revendiquant libre retirée (DN9) | `TestClaimAsTheFreeHolderIsRefused` | `claiming as the free holder must be refused` · `a refused claim must not write the key` |

## Definition of Done
- [x] gofmt/goimports · golangci-lint (0 issue) · `go test -race ./...` (RC=0, 79 paquets, **0 skip**) ·
      govulncheck (0 vulnérabilité atteignant le code) verts
- [x] la course est fermée et testée sur toute sa chaîne de décision —
      `TestRaceScenarioLeavesADeliveredMessageDelivered` enchaîne accepted → cancel → enroute →
      delivered sur le **vrai** jeton Redis, le **vrai** Canceller et la **vraie** table de rangs, et
      il est vu tomber sous mutation (`final status = "cancelled", want delivered`).
      **Ce qu'il ne couvre pas :** les lignes `enroute` et `delivered` y sont fabriquées, pas tirées du
      projecteur `internal/outcome`. Ce n'est donc pas un bout-en-bout : il prouve que l'arbitrage
      refuse d'écrire la ligne fautive et que les rangs résolvent bien, pas que le projecteur écrit ces
      deux lignes-là — ce que couvrent ses propres tests. Un vrai bout-en-bout coûterait ClickHouse +
      Kafka pour ce seul chaînon, arbitrage assumé.
- [x] le sens de `cancelled` sous course est tranché et écrit — §6.22 le disait déjà, ADR-0013 le fige
- [x] le contrat public est mis à jour, la limite documentée disparaît, `api/package.json` 4.0.2 → 4.0.3
      (`oasdiff` : aucune rupture)

## Hors périmètre
Le lag de projection lui-même (step-201c). L'annulation REST — elle n'existe pas : `cancel_sm` est
protocol-only (ADR-0009).
