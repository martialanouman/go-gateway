# step-201f — Isoler le pool de connecteurs : le dernier étage jamais mesuré seul

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201e (livrée) · **Bloque :** step-201b, step-216 PR2 (elle ajoute un étage au
> chemin d'envoi et ne doit pas s'insérer entre cette mesure et step-201b)

## But

Faire pour `connector-pool-svc` ce que step-201e `D1` a fait pour le routeur : le mesurer **seul**, pour
savoir si le plafond de bout en bout lui appartient ou appartient à l'hôte qu'il partage.

C'est le seul étage du pipeline MT dont le plafond n'a jamais été mesuré isolément, et c'est
aujourd'hui **celui qui borne tout le reste**.

## Le constat

Après step-201e, les trois étages sont connus — sauf un :

| Étage | Débit | Isolé ? |
|---|---:|---|
| Ingestion (REST → `mt.inbound`) | ≥ 2 400/s, p99 11 ms, 0 erreur | non, mais aucun signe de saturation |
| **Routeur** | **20 741/s** à 8 lanes | **oui** (step-201e `D1`) |
| **Pool → SMSC** | **2 400/s** | **jamais** |
| Pair de test (simulateur) | 43 498/s à 80 binds | oui (step-201 `D3`) |

**Cible NFR : 8 000 SMS/s = 10 400 `submit_sm/s`** (×1,3 segment). L'écart est de **×4,3**, et il est
entièrement dans la patte sortante.

Le routeur *paraissait* plafonner à 4 702/s en plein-stack ; isolé, il fait 20 741/s — un facteur 4,4
qui n'appartenait pas au routeur mais à la co-résidence de neuf composants sur un portable. **Le pool
est exactement dans la position où était le routeur avant step-201e** : accusé sur la foi d'un chiffre
mesuré dans le bruit.

Tant que ce banc n'existe pas, on ne sait pas si les 2 400/s sont le pool, le nombre de binds, le pair,
ou l'hôte — et **aucun dimensionnement ne peut être écrit** pour step-207 ni step-201b.

## Design arrêté

### Ce que l'exploration a trouvé, et qui change la lecture du 2 400/s

**Le harnais de référence ne paie pas le `dlrmap`.** `DLRMap` est nil dans son câblage du pool
(`reference_test.go:424-447` ; `dlrmap` n'apparaît nulle part sous `internal/e2e/`), alors que la
production écrit une entrée Redis **synchrone à chaque `submit_sm` acquitté**
(`connectorpool.go:976` → `recordDLRMapping`). Le 2 400/s est donc le débit d'un pool **plus rapide que
la production**, et il plafonne quand même à 2 400.

C'est le motif de `loadref-harness-fidelity-traps` : le harnais mesure autre chose que la production,
par une dépendance laissée à nil. Le banc mesure donc **les deux** configurations — le delta nomme la
part de Redis, et c'est cela, « attribuer ».

### Les étages, tranchés

| Étage | Décision | Pourquoi |
|---|---|---|
| `Producer` → `mt.outcome` | **réel, non négociable** | `New` panique si nil ; c'est un produce acquitté par message, fail-closed, sur le chemin chaud |
| `DLRMap` → Redis | **balayage sans, palier de fidélité avec** | sans = comparable au 2 400/s ; avec = la vérité de production ; le delta est l'attribution |
| `Billing` | **nil** | facturation opt-in (M9) ; invariant (c) : désactivée = zéro appel réseau. Le banc mesure le déploiement facturation-off, et son godoc le dit |
| `Breaker` | **espion, pas nil** | à nil, **aucun breaker local n'est créé** — le pool serait plus rapide que la production *et* le garde du disjoncteur serait sans objet |
| `CancelFlags`, `Stream`, `RerouteLimiter`, `ConfigSource`, `StatusControl` | nil | hors du chemin d'envoi nominal |

### Les trois pièges qui rendraient le banc creux

1. **`ConnectorID`** — `routedBench()` en tire un au hasard ; le pool filtre dessus et
   **skippe-et-commite** en silence. Le banc rapporterait un débit consommateur flatteur et **zéro
   `submit_sm`**. Le pré-remplissage porte l'ID du banc.
2. **La géométrie de shard** — le fan-out est `FNV32a(MessageID) % len(binds)`, **par batch de poll**.
   Des UUID aléatoires laissent des binds inactifs à petit batch.
3. **`refKafkaConfig` obligatoire** — un `config.Kafka{…}` littéral laisse les champs fetch à zéro et
   franz-go substitue 1 MiB au lieu des 56 KiB de production : batch ~18× plus gros, toute conclusion
   sur le fan-out fausse.

### Le pair, et les leviers

**`fakesmsc` + `calibratePeer` au nombre de binds du palier**, pas le simulateur : `smscsim.Launch`
*skippe* si l'image est absente (un banc muet est pire qu'un banc lent), `fakesmsc` est le pair du run
de référence donc les chiffres sont comparables, et les 43 498/s du simulateur ont été mesurés à 80
binds — incomparables à un palier à 4.

Deux leviers : balayage A sur `bind_pool_size ∈ {1,2,4,8,16}` à `window` 64 ; balayage B sur
`window ∈ {1,10,64,256}` au coude de A. Partitions fixées à 8 et **nommées** : elles gouvernent la
taille de batch via `FetchMaxPartitionBytes`, donc le fan-out atteignable.

## Périmètre

Des **instruments**, aucun changement du chemin chaud de production. Même contrainte qu'en step-201e :
tout vit sous `internal/e2e`, `test/load` ou `internal/testutil`.

### D1 — Le banc « pool seul », sur le patron exact de step-201e `D1`
Pré-remplir un topic privé façonné comme `mt.routed`, puis lancer **uniquement** le pool : pas
d'injecteur, pas de REST, pas de routeur. Balayer `bind_pool_size`.

Question falsifiable : **le pool replafonne-t-il vers 2 400/s une fois seul ?**
- oui → le plafond lui appartient, et le levier est à chercher dans le bind ou la fenêtre SMPP ;
- il monte franchement → les 2 400/s étaient la co-résidence, comme pour le routeur, et le chiffre du
  README doit être annoté plutôt que porté dans step-201b.

Les trois gardes de step-201e `D1` sont **reprises telles quelles** — le backlog doit tenir par
partition, le chrono part à la première consommation, et le débit est recoupé par deux sources
indépendantes. Elles ont attrapé de vrais défauts ; les réécrire serait les repayer.

### D2 — Chronométrer le `submit_sm` *in situ*
Le pendant de step-201e `D3`. Un histogramme autour de l'envoi, posé **dans le harnais**, dit si le coût
par message est l'aller-retour SMPP ou autre chose. `produceBounds` / `produceBucket` / `p99Bucket` sont
déjà écrits, testés et hors build tag (step-201e) : les réutiliser, ne pas les redécliner.

### D3 — Blanchir ou accuser le pair, par la mesure
**C'est le piège central de cette fiche.** Le plafond du **faux SMSC in-repo n'a jamais été mesuré** —
seul le simulateur l'a été (43 498/s à 80 binds, step-201 `D3`). Un banc « pool seul » branché sur le
faux SMSC peut très bien mesurer le faux SMSC.

Donc, avant toute conclusion : soit le banc utilise le **vrai simulateur**, dont le plafond est connu et
trois fois supérieur à la cible, soit il mesure d'abord le plafond du faux SMSC comme `calibratePeer`
le fait déjà pour le run de référence. Aucun chiffre du banc n'est lisible sans ce préalable.

## Points d'implémentation clés

- **Le pool fait plus qu'un `submit_sm` par message**, et chaque étage écarté change ce qu'on mesure :
  écriture `dlrmap` dans Redis (corrélation DLR, §1.11), publication sur `mt.outcome` (la projection CDR
  depuis step-201c), capture de crédit (`BillingSettler`, no-op par défaut donc gratuit si la
  facturation n'est pas câblée). Les bouchonner rend le banc plus rapide que la production ; les garder
  ajoute Redis au banc. **Trancher explicitement, et nommer le coût dans le godoc** — c'est ce que
  step-201e a fait pour les quatre étages de conformité, en bornant l'écart par une mesure déjà
  existante plutôt que par une intuition.
- **Deux leviers, pas un.** `bind_pool_size` (le nombre de binds) et la fenêtre SMPP (`REF_WINDOW`,
  combien de `submit_sm` en vol par bind). Un balayage à une seule dimension attribuerait à l'un ce qui
  appartient à l'autre.
- **Le shard est l'identifiant de message** (step-124), pas la partition : le pool consomme `mt.routed`
  avec un fan-out par shard FNV. La géométrie du pré-remplissage doit le respecter, sinon des binds
  restent inactifs — l'analogue exact du piège de placement de step-201e.
- **Le disjoncteur peut s'ouvrir sous charge** et couper l'envoi au milieu de la fenêtre. Le banc doit
  le lire et refuser un palier où il s'est ouvert, plutôt que publier un débit qui est en réalité une
  coupure.
- **Réutiliser, ne pas redécliner** : `newCeilingTopic`, `prefill`, `backlogHeld`, `waitUntilConsuming`,
  `crossCheck` et le lecteur `redpandametrics` existent et sont éprouvés. Ce banc est le second du même
  genre ; s'il en réécrit la moitié, c'est que l'extraction n'a pas été faite.

## Tests

- `D1` : un test de plafond `loadref` sur le patron de `TestRouterConsumeCeiling` — unité de travail
  identique à la production, fenêtre par `context.WithTimeout`, débit divisé par la durée réelle, aucune
  assertion de seuil sauf « le chemin bouge » et les trois gardes.
- `D2`-`D3` : les lecteurs et rendus sont des fonctions **pures**, testées hors conteneur et hors build
  tag, comme `produceLatency` et `laneShape`.
- **Aucun vert déclaré avant d'avoir vu tomber une mutation par assertion.** step-201e a produit quatre
  tests creux, tous révélés par la mutation et aucun par relecture — dont un test d'aller-retour
  circulaire qui passait sous n'importe quelle convention.
- Balayage consigné en lignes **ajoutées** à `test/load/README.md`, aucune éditée.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] le plafond de 2 400 msg/s de bout en bout est **attribué** — pool, binds, fenêtre, pair ou hôte —
      ou l'échec à l'attribuer est consigné avec ce qui manque
- [ ] le plafond du pair réellement utilisé est **mesuré** et cité à côté de chaque chiffre, faute de
      quoi aucun palier n'est lisible
- [ ] step-207 reçoit ce que ce banc lui doit : combien de binds pour quel débit, et par quel levier
- [ ] aucun changement du chemin chaud de production

## Hors périmètre

Le verdict NFR pleine échelle et l'environnement représentatif → **step-201b**. L'optimisation du
chemin d'envoi elle-même — batcher, élargir la fenêtre, changer la stratégie de bind — n'est pas ici :
cette fiche produit une **attribution**, pas un correctif. Si elle montre que le coût est dans
l'aller-retour SMPP, le correctif aura sa propre fiche et, comme pour le produce du routeur, son propre
arbitrage de durabilité.
