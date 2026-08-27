# Harnais de charge (M12)

Deux instruments : un script **k6** qui martèle l'API REST, et un **générateur de binds SMPP** en Go
pour l'ingress. Ils encodent les NFR de §1.2 de la spec — soutenu 8 000 SMS/s, pic 15 000, ingestion
p99 < 250 ms.

## Prérequis

`k6` est un binaire natif, **hors `go.mod`** (plan §1.3) : `make tools` ne l'installe pas.

```bash
brew install k6          # macOS — sinon https://grafana.com/docs/k6/latest/set-up/install-k6/
```

Les cibles échouent en dur s'il manque, elles ne se skippent pas : un harnais qu'on croit vert parce
qu'il ne s'est pas exécuté est pire que pas de harnais.

## Ce que ce harnais prouve — et ce qu'il ne prouve pas

`make load-smoke` vérifie que **l'instrument fonctionne**, pas que le système tient la charge. Il
lance le même script contre un stub instantané (doit passer) puis contre le même stub ralenti à 300 ms
(doit échouer). Le second run est la vraie assertion — un run de charge contre un stub local passe
trivialement, donc un vert seul ne signifie rien. Trois runs de plus font subir le même traitement à
l'option `IDEMPOTENCY` (voir plus bas).

La tenue réelle à 8 000 req/s se mesure sur matériel réel, avec le pipeline complet : c'est
**step-201**, pas ici.

## Avant de pointer une vraie passerelle

**Un run contre une passerelle réelle envoie de vrais SMS.** Le profil `smoke` en émet ~500, `sustained`
~480 000. Les destinataires sont tirés dans le bloc `+22507000xxxx`, celui que le dépôt utilise pour
ses fixtures — n'élargissez ce bloc que contre un stub.

`BASE_URL` est une **origine nue** : le script ajoute `/v1/messages` lui-même. Recopier l'URL serveur
du contrat (`…/v1`) produit `/v1/v1/messages`, donc 100 % de 404, et le run échoue en accusant la
latence.

## Cibles

```bash
make load-smoke                                        # fumigation : passe à vide, tombe sous contrainte
make load BASE_URL=http://localhost:8080               # profil smoke — ENVOIE DE VRAIS SMS
make load LOAD_PROFILE=sustained BASE_URL=http://…     # 8 000 req/s  — jamais en CI
make load LOAD_PROFILE=peak      BASE_URL=http://…     # 15 000 req/s — jamais en CI
make load BASE_URL=http://… IDEMPOTENCY=on             # même profil, chemin Idempotency-Key
make load-binds BINDS=200 ADDR=127.0.0.1:2775          # N binds SMPP concurrents
```

Variables du script k6 : `PROFILE`, `BASE_URL`, `API_KEY`, `SENDER_ID`, `IDEMPOTENCY`.

## L'option `IDEMPOTENCY` (step-201, `D10`–`D12`)

`IDEMPOTENCY=on|off`, **défaut `off`** ; toute autre valeur lève à l'init, comme `PROFILE`.

Elle choisit **quel chemin serveur** est mesuré, pas l'intensité. `internal/restapi/messages.go` bascule
sur `submitIdempotent` dès que l'en-tête `Idempotency-Key` est présent, ce qui ajoute **deux allers-retours
Redis** autour de la publication Kafka (`Reserve` avant, `Finalize` après). Régler les leviers de capacité
sans cet en-tête optimise un chemin que les clients qui retentent n'empruntent pas : les NFR seraient
déclarés tenus sur le cas favorable.

```bash
make load BASE_URL=http://localhost:8080 IDEMPOTENCY=on
```

Clé émise : `` k6-<seed du run>-<exec.scenario.iterationInTest> ``, ~25 caractères contre les 128 du
contrat. `iterationInTest` est unique **par construction** sur tout le run — un pliage arithmétique de
`(__VU, __ITER)` peut collisionner, lui. Le **seed par run** (horodatage ms en base 36 + 6 caractères
aléatoires) existe parce que la passerelle mémorise une clé **24 h** : sans lui, deux runs du même jour
rejoueraient les mêmes clés et le second mesurerait le cache d'idempotence — exactement le défaut que
l'option existe pour éviter, à l'échelle du run entier.

Quand l'option est `off`, l'en-tête est **absent**, jamais présent et vide : la passerelle traite `""`
comme « pas d'idempotence », **silencieusement**, et un harnais qui en émettrait un passerait vert en
mesurant le chemin non idempotent.

### Ce que cela coûte au Redis de la cible — lisez avant de viser autre chose qu'un stub

Un run `peak` crée **~900 000 clés** `idem:{<accountID>}:k6-*`, soit **~100–150 Mo**, chacune à **24 h de
TTL**. Elles ne s'effacent donc pas à la fin du run : deux runs `peak` dans la même journée cumulent,
trois aussi.

**Ce n'est pas de la pollution, c'est du réalisme** : en production, des clients qui retentent rempliront
`idem:{accountID}:*` au même rythme, et cette empreinte fait partie du chemin que l'option existe pour
mesurer. La purger entre les runs fausserait la mesure ; rendre le TTL configurable côté serveur pour le
confort du harnais reviendrait à régler le système sur son test.

**Dimensionnez donc le `maxmemory` du Redis cible pour l'absorber, cumul compris.** Sous pression, une
politique d'éviction n'expulserait pas que ces clés : elle prendrait aussi les **sessions**, les
**token-buckets** et le **cache de solde**, et le run mesurerait une tempête d'éviction au lieu du chemin
idempotent. En `noeviction`, ce seraient des échecs de réservation.

Le préfixe `k6-` n'est pas décoratif — il rend les clés du harnais balayables, ce qui permet le balai
(hors mesure) :

```bash
redis-cli --scan --pattern 'idem:{<accountID>}:k6-*' | xargs -L 500 redis-cli UNLINK
```

> Les accolades font partie de la clé : `internal/idempotency` écrit `idem:{<accountID>}:<clé>`, le hash
> tag gardant les entrées d'un compte sur un seul slot. Un motif sans accolades ne balaie rien.

### Comment l'option est vérifiée

Le dépôt n'a ni `node_modules` ni jest, et la CI n'installe que Go et le binaire k6 : on ne teste pas le
JavaScript, on **observe ses effets** sur le stub, qui reçoit chaque requête. Le stub gagne
`-idempotency=ignore|require-unique|forbid` (défaut `ignore`, comportement step-200 strictement intact) —
`require-unique` refuse en 422 un en-tête absent, vide, de plus de 128 caractères ou déjà vu ; `forbid`
refuse sa seule présence. `make load-smoke` en tire trois runs :

| Run | Attendu | Ce qu'il prouve |
|---|---|---|
| `IDEMPOTENCY=on` contre `require-unique` | exit 0 | ~500 clés, toutes non vides, ≤ 128 et distinctes |
| `IDEMPOTENCY` absent contre `forbid` | exit 0 | aucun en-tête émis |
| `IDEMPOTENCY=on` contre le **même** stub `forbid` | exit 99 | l'en-tête est bien émis |

Le troisième est la vraie assertion : sans lui, les deux premiers passeraient trivialement contre un stub
qui ignore tout, et débrancher l'observateur ne ferait aucun bruit. Les deux derniers partagent **un seul
processus** de stub délibérément — le run vert prouve que ce processus sert le trafic, si bien que le
rejet du suivant ne peut venir que de l'en-tête, et non d'un stub qui n'aurait jamais démarré.

## Les seuils

| Seuil | Valeur | Où |
|---|---|---|
| Ingestion p99 | < 250 ms | `thresholds.http_req_duration` |
| Taux d'erreur HTTP | < 1 % | `thresholds.http_req_failed` |
| Réponses 202 | > 99 % | `thresholds` sur le check nommé |

k6 sort en **code 99** quand un seuil tombe : c'est ce code qui porte le critère « le run échoue si
p99 dépasse le budget ».

Le troisième seuil couvre ce que les deux autres laissent passer : une réponse **2xx qui n'est pas
202**. Un 401 est déjà attrapé par `http_req_failed`, qui compte tout ce qui sort de 2xx–3xx ; un 200,
lui, y est compté comme un succès et tiendrait le budget de latence sans que rien ne signale que la
passerelle n'a rien accepté.

`make load-smoke` vérifie en plus que ce seuil a **mesuré quelque chose** : k6 affiche un seuil sans
aucun échantillon comme respecté (`✓ 'rate>0.99' rate=0.00%`), si bien qu'un sélecteur de check devenu
obsolète passerait vert des deux côtés du couple.

**Le budget bout-en-bout p99 < 2 s n'est pas encodé ici**, et ce n'est pas un oubli : k6 mesure
soumission → réponse HTTP, il ne voit jamais la patte SMSC. Le mesurer exige de corréler les
horodatages de sortie (fake SMSC ou CDR ClickHouse) — c'est un prérequis de step-201. N'ajoutez pas
un seuil k6 pour ça, il mesurerait l'ingestion sous un autre nom.

## Plafond du pair de test (step-201, `D3`)

Avant de régler un levier de capacité, il faut savoir ce que le **simulateur SMSC** encaisse : un run de
référence au plafond du pair mesure le pair, pas la passerelle. `make smsc-ceiling` balaie le nombre de
binds, injecte des `submit_sm` sur tous pendant chaque palier, et lit le débit **sur le `/metrics` du
simulateur** — jamais au compteur de l'injecteur, qui dirait ce qu'il croit avoir envoyé.

L'outil **ne démarre pas le simulateur** : il prend une adresse SMPP et une URL de metrics. Le
`docker run` et le YAML (profil `healthy`, latence fixe 5 ms — celui du futur run de référence) sont
dans le commentaire de tête de `cmd/smsc-ceiling`.

```bash
make smsc-ceiling                                  # 10,20,40,80 binds · 60 s mesurés par palier
make smsc-ceiling BINDS=10,20 MEASURE=5s           # run de fumigation — PAS un chiffre à consigner
```

L'outil n'imprime `CEILING:` que si un palier a **réellement saturé** le pair (issue non-`success`, ou
courbe qui cesse de monter avec les binds). Sinon il imprime `LOWER BOUND:` — parce que le balayage a
mesuré la plus grosse charge qu'on lui a **demandé** de produire, pas la plus grosse que le pair
encaisse. Une fenêtre sous les 60 s est en plus estampillée `SMOKE RUN` sur les mêmes lignes.

### Mesure du 02/08/2026

Conditions : simulateur `smsc-simulator:dev` en conteneur (OrbStack, ports publiés), injecteur sur la
**même machine** (14 cœurs, arm64) ; profil `HealthyConfig` (latence fixe 5 ms) ; **60 s mesurés par
palier**, 10 s de chauffe, 5 s de marge, 5 s entre paliers ; fenêtre d'émission de 32 `submit_sm` en vol
par session (défaut `bindgen`).

| Binds | `submit_sm/s` absorbés | par bind | Latence servie (configurée †) | Issues non-`success` | Palier |
|---:|---:|---:|---:|---:|---|
| 10 | 1 664 | 166 | 5 ms | 0 | qualifié |
| 20 | **3 291** | 165 | 5 ms | 0 | qualifié |
| 40 | 6 870 | 172 | 5 ms | 0 | qualifié |
| 80 | 13 176 | 165 | 5 ms | 0 | qualifié |
| 160 ‡ | 23 629 | 148 | 5 ms | 0 | qualifié |
| 320 ‡ | **34 872** | 109 | 5 ms | 0 | qualifié — **courbe pliée** |

‡ hors du balayage de `D3`, ajoutés parce que la courbe ne pliait toujours pas à 80.

† **Ce n'est pas une latence mesurée** : le simulateur observe la latence que son scénario a *décidée*.
La colonne affiche la valeur configurée quoi qu'il arrive, elle ne peut pas signaler une saturation.
Détail au « piège consigné » plus bas — la sortie de l'outil porte désormais la même réserve.

**Plafond au nombre de binds du run de référence — 3 291 `submit_sm/s` à 20 binds.** C'est le chiffre
sous lequel le run de référence de `D2` doit se situer. 20 binds parce que `bind_pool_size` est borné à
1..32 par le schéma du plan de contrôle : c'est le plus grand palier du balayage qu'un pod de
`connector-pool-svc` puisse reproduire seul. Le run de référence vise ≥ 1 000 msg/s traversants, soit
≈ 1 300 `submit_sm/s` à 1,3 segment — **moins de la moitié** de ce que le pair tient à ce niveau.

**Cette marge est mesurée hors contexte, et il faut la revérifier.** Les 3 291/s ont été relevés avec
l'injecteur **seul** face au simulateur. Le run de référence fera tourner sur les **mêmes 14 cœurs** la
passerelle (9 services), 4 magasins dont Redpanda, k6 **et** le simulateur : si la contention ramène le
pair à 1 500/s dans ce contexte, la marge annoncée disparaît. Le chiffre à opposer au run de référence
est celui d'un balayage relancé **pendant** que la pile complète tourne — pas celui-ci.

**Plafond du pair — 34 872 `submit_sm/s` à 320 binds.** C'est un vrai plafond, pas une borne
inférieure : la courbe **plie** à ce palier — 34 872/s contre 23 629/s à 160 binds, soit **48 %** de ce
que le doublement des binds aurait dû acheter. Le balayage de `D3` (10→80) ne suffit pas à l'atteindre,
et l'outil y imprime honnêtement `LOWER BOUND` : il faut pousser jusqu'à 160/320 pour voir la limite.

*Réserve, chiffrée* : pour ne pas plier il aurait fallu 35 443/s à 320 binds ; il en manque **1,6 %**.
Or le tableau ci-dessus montre une dispersion inter-paliers de ~4 % (172/bind à 40 binds contre 165–166
chez ses voisins, sur une courbe censée décroître). **La marge qui décide du verdict est plus petite que
le bruit visible dans le même run.** Ce que la mesure établit solidement n'est donc pas la valeur exacte
du plafond, ni même la certitude qu'il ait été atteint, mais son **ordre de grandeur** — et que le débit
par bind, plat jusqu'à 80 binds, s'effondre au-delà. Un second balayage est nécessaire pour confirmer le
verdict de saturation.

**Ce que les chiffres désignent.** Le débit est **linéaire en nombre de binds**, avec une érosion lente
du débit par bind, plate jusqu'à 80 binds (166 → 165/s) puis en chute (148 à 160, 109 à 320). Le goulot est **vraisemblablement par bind**,
pas partagé : le simulateur sérialise le service sur la goroutine de lecture de chaque bind
(`serveLatency` appelé avant toute réponse), ce qui plafonne un bind à 1/5 ms = 200/s en théorie. Les débits par bind observés correspondent à 5,8–9,2 ms réels par `submit_sm` : les 5 ms d'attente plus le codec, l'`Append` du
recorder et les compteurs — l'injecteur et le simulateur se disputant les mêmes 14 cœurs. Le
`sync.RWMutex` du recorder était le suspect n° 1 pour une contention **inter-binds** : il n'est pas la
limite à ces débits, sinon le débit par bind s'effondrerait avec le nombre de binds au lieu de perdre
34 % sur un facteur 32.

**L'autre lecture n'est pas écartée, et il faut le dire.** « Érosion du débit par bind » est une
moyenne, et une fraction de binds *figés* — des sessions qui cessent d'être servies sans qu'aucune
erreur ne remonte — produirait exactement la même érosion en concentrant le débit sur les binds
restants. Le balayage refuse un palier dont la session la plus lente passe sous le quart de la plus
rapide (`maxSubmitSpread`), et aucun palier ne l'a été.

**Mais cette garde ne couvre qu'un gel précoce.** Une session gelée à l'instant *t* d'une fenêtre
d'injection de 75 s garde un ratio `t/75` : le seuil de 4 ne se déclenche donc que pour `t < 19 s`,
c'est-à-dire un gel qui détruit plus de 85 % du travail de la session. Un gel à mi-parcours laisse un
écart de 1,9× et passe. Or la configuration qui reproduirait précisément l'érosion observée — environ
deux tiers des sessions gelées à mi-fenêtre — tombe dans cet angle mort.

Ce que la mesure établit donc : le débit par bind **est** plat jusqu'à 80 binds puis chute, et aucun
gel *précoce* n'a eu lieu. Ce qu'elle n'établit pas : que la chute au-delà de 80 binds vienne du
service par bind plutôt que de sessions figées tardivement. Trancher demande un débit **par session sur
la durée**, que l'instrument ne relève pas — il n'en donne que le total (`per session min..max` dans la
ligne de palier). Consigné comme suivi de step-201b.

La garde porte sur la **dispersion** des soumissions entre sessions, pas sur la queue de `submit_sm`
sans réponse. Une version antérieure seuillait cette queue et refusait *tous* les paliers d'un run
sain : un injecteur fenêtré termine chaque run avec sa fenêtre entière en vol sur chaque session —
mesuré à exactement `binds × 32` — parce qu'un jeton n'est libéré que par une réponse et aussitôt
repris. Une session figée est à la même valeur qu'une session saine ; seule la dispersion les sépare.
Le rapport de la queue au total (`maxUnansweredFraction`, 0,27–0,41 % en run sain) reste utilisé, mais
pour un autre cas : un pair qui **accepte sans jamais répondre**, invisible pour toutes les autres
gardes.

La garde porte sur la **dispersion** des soumissions entre sessions, pas sur la queue de `submit_sm`
sans réponse. Une version antérieure seuillait cette queue et refusait *tous* les paliers d'un run
sain : un injecteur fenêtré termine chaque run avec sa fenêtre entière en vol sur chaque session —
mesuré à exactement `binds × 32` — parce qu'un jeton n'est libéré que par une réponse et aussitôt
repris. Une session figée est à la même valeur qu'une session saine ; seule la dispersion les sépare.

**Conséquence pour `D1`.** Les 10 400 `submit_sm/s` que la cible NFR implique en sortie (8 000 SMS/s ×
1,3 segment) sont déjà dépassés à 80 binds sur une machine de développement, et le pair tient plus du
triple avant de plier. Le simulateur ne sera pas la contrainte artificielle de step-201b — à condition
de lui donner assez de binds : il en faut **≥ 80**, pas les ~52 que le modèle 200/s par bind laissait
espérer. La marge entre la cible et le plafond mesuré est d'un facteur ~3, sur une machine partagée.

**Piège consigné.** `smsc_served_latency_seconds` affiche exactement 5 ms à tous les paliers, y compris
à 320 binds. Ce n'est pas un pair au repos : le simulateur observe la latence **configurée**, pas une
durée mesurée (dépôt **`go-smsc-simulator`**, `internal/smsc/session.go` :
`ObserveServedLatency(..., float64(decision.LatencyMS)/1000)` — ce fichier n'existe pas dans ce dépôt-ci).
Cette métrique ne peut donc **pas** distinguer « le pair sature » de « l'injecteur ne pousse pas »,
contrairement à ce qu'annoncent `D3` et le godoc de `smscmetrics`. Les deux seuls signaux de saturation
utilisables sont `smsc_submit_sm_outcome_total` (les issues non-`success`, qui disqualifient un palier)
et l'inflexion de la courbe. **Les deux sont implémentés** : l'outil marque `Saturated` dès que l'un des
deux se déclenche, et sans marqueur il imprime `LOWER BOUND` au lieu de `CEILING`.

## Run de référence local (step-201, `D2`)

`make load-reference` monte **toute la voie MT dans un seul processus** — `rest-api`, `router`,
`connector-pool` — contre un Postgres, un Kafka et un ClickHouse réels (testcontainers) et le faux SMSC
embarqué, tient un débit cible pendant une minute pleine, puis **note l'état stationnaire**.

```bash
make load-reference                                   # 1 200 msg/s visés, fenêtre de 60 s
make load-reference RATE=400 BIND_POOL=8 MEASURE=90s  # une autre configuration de leviers
```

Le run vit derrière l'étiquette de compilation `loadref` : sans elle le fichier n'est **pas compilé**,
donc `make test` n'en paie rien — pas même un test skippé, qui devrait quand même démarrer les
conteneurs pour décider de se skipper.

### Le critère, et pourquoi il est une conjonction

Tout en même temps, sur une fenêtre d'au moins 60 s : débit d'acceptation ≥ **1 000 msg/s** · débit de
**sortie** égal à l'acceptation à la marge de segmentation près · **lag consumer plat** · p99 d'ingestion
< 250 ms · **0 erreur** sur les deux pattes · disjoncteur fermé · sous le plafond du pair.

L'égalité entrée/sortie et le lag plat sont ce qui sépare un **débit** d'une **file qui se remplit**. Le
run ci-dessous le montre en grandeur nature : l'acceptation seule passait, et elle passait large.

Chaque entrée dont l'absence ressemble à de la santé est **refusée**, jamais sautée : fenêtre nulle
(aucune division, aucun débit infini), moins de 6 relevés de lag (deux points ne distinguent pas plat de
croissant), p99 sur zéro échantillon (qui se lirait comme le run le plus rapide jamais mesuré), plafond
du pair inconnu (`D2` place le run **sous** un chiffre ; ne pas en avoir signifie que le critère a été
sauté, pas tenu). `test/load/steady` est unitaire et sans infrastructure : les seuils se relisent sans
démarrer un conteneur.

### Mesure du 02/08/2026 — le critère **n'est pas tenu**

Conditions : une machine de 14 cœurs (arm64, OrbStack 12 Go), **tout dans un processus**, Postgres 18 /
Redpanda / ClickHouse 24.8 en conteneurs, faux SMSC embarqué, injecteur Go dans le même processus.
30 s de chauffe, **60 s mesurés**, 10 s de marge.

| Levier | Chauffe | Accepté | **Sorti** | Pente du lag | p99 ingestion | Erreurs |
|---|---:|---:|---:|---:|---:|---:|
| **défauts livrés** (`bind_pool=4`, CH 10/5) | 20 s | 1 200/s | **192/s** | +1 001 rec/s | 45 ms | 0 |
| `bind_pool=4`, CH 10/5 | 30 s | 1 200/s | **214/s** | +975 rec/s | 44 ms | 0 |
| `bind_pool=16`, CH 10/5 | 30 s | 1 200/s | **297/s** | +902 rec/s | 11 ms | 0 |
| `bind_pool=16`, CH 64/32 | 30 s | 1 200/s | **330/s** | +901 rec/s | 9 ms | 0 |
| `bind_pool=8`, CH 10/5, cible 400/s | 45 s | 400/s | **195/s** | +219 rec/s | 47 ms | 0 |

**L'ingestion tient sans effort ; la traversée non.** L'acceptation atteint la cible à 1 200/s avec un
p99 de 9 à 47 ms — très en deçà des 250 ms — et **zéro erreur**. La sortie plafonne entre **195 et
330 `submit_sm/s`**, soit moins d'un tiers du seuil de 1 000. Un run qui n'aurait regardé que
l'acceptation aurait publié « 1 200 msg/s tenus » : c'est exactement l'erreur que `D1` écarte et que
`D2` est construit pour rendre impossible.

Le pair n'y est pour rien : le faux SMSC calibré au même endroit absorbe **218 000 à 255 000
`submit_sm/s`** — trois ordres de grandeur au-dessus. *(Ce n'est pas le chiffre `D3` de 3 291/s : celui-là
mesure le **simulateur**, un autre pair, avec 5 ms de latence scriptée. Reporter l'un sur l'autre placerait
le run sous un plafond qui n'est pas le sien.)*

### Le goulot, nommé et isolé

Le relevé de lag **par topic** situe l'étape lente sans laisser deviner. Sur le run
`bind_pool=16`, CH 64/32 — celui où la sortie est la plus haute :

```
mt.inbound  14 412 -> 27 224     (+213 rec/s)  → le routeur tient ~990 msg/s
mt.routed   15 037 -> 52 904     (+631 rec/s)  → le pool de connecteurs sort ~330/s
```

Le routeur suit ; **c'est la patte connecteur qui plafonne**. Deux leviers l'ont à peine bougée :
quadrupler `bind_pool_size` (4 → 16) a acheté **1,39×**, élargir le pool ClickHouse (10 → 64
connexions) **1,11×**. Un goulot qui ne cède pas à la concurrence n'est pas une sérialisation par shard.

`TestCDRWriteCeiling` isole l'écriture CDR du pipeline et tranche
(`make load-reference RUN=TestCDRWriteCeiling`) :

| Écrivains concurrents | `Insert` mono-ligne / s | Requêtes ClickHouse / s |
|---:|---:|---:|
| 1 | 154 | 307 |
| 4 | **548** | 1 096 |
| 16 | 278 | 557 |
| 64 | 301 | 602 — **avec des `i/o timeout`** |

**Le débit s'effondre au-delà de 4 écrivains.** Les chiffres encadrent exactement la sortie observée du
pool (195–330/s), et les `i/o timeout` à 64 sont la signature de la contre-pression ClickHouse.

**La cause.** `connectorpool` écrit **une ligne à la fois** sur le `submit_sm_resp`
(`CDR.Insert` → `InsertBatch([]CDRRow{row})`), et `InsertBatch` frappe **deux tables** — `cdr` puis
`cdr_events` — soit un `PrepareBatch` + `Send` chacune : **quatre allers-retours ClickHouse par
message**, sur le chemin de consommation, avant le commit d'offset.

**Ce que cela dit de `D8`.** `D8` pose « le batch reste `poll Kafka = PrepareBatch/Send = commit` » et
juge les deux `Send()` par poll « invisibles à quelques batches/seconde ». C'est vrai du **projecteur
`accepted`**, qui écrit un batch par poll. Ce n'est **pas** vrai du pool de connecteurs, qui n'a jamais
été dans ce régime : il écrit par message. Le critère de réouverture que `D8` s'était fixé — de la
pression de parts côté ClickHouse — est **atteint**, et il l'est sur un chemin que `D8` croyait déjà
batché. C'est la première chose à trancher avant `step-201b` : la réponse n'est pas un buffer client
(`D8` l'exclut avec raison), c'est de faire écrire le pool **par batch de poll**, comme le projecteur,
ou l'`async_insert` serveur avec `wait_for_async_insert=1`.

### Mesure du 03/08/2026 (après step-201c) — le goulot a **changé de place**

Mêmes conditions que ci-dessus, mêmes défauts livrés (`make load-reference` sans variable), avec la
projection du CDR sortant en place (`step-201c` `D1`) : le pool publie l'issue sur `mt.outcome` et un
consommateur dédié écrit la ligne CDR.

| Levier | Chauffe | Accepté | **Sorti** | Pente du lag | p99 ingestion | Erreurs |
|---|---:|---:|---:|---:|---:|---:|
| **défauts livrés**, avant step-201c | 20 s | 1 200/s | **192/s** | +1 001 rec/s | 45 ms | 0 |
| **défauts livrés**, après step-201c | 20 s | 1 200/s | **892/s** | +291 rec/s | 11 ms | 0 |

**La patte sortante est libérée : ×4,6 sur la sortie, et la pente du lag divisée par 3,4.** Le critère
d'état stationnaire reste néanmoins **refusé**, sur deux clauses :

```
[FAIL] input/output balance  53 491 submit_sm out for 72 002 accepted (892/s out vs 1200/s in),
                             25,71 % d'écart, tolérance 2 %
[FAIL] kafka lag             +291,5 rec/s sur 20 relevés (3 513 -> 22 448), veut au plus +12,0
```

Le relevé par topic dit **où** le backlog s'accumule désormais, et ce n'est plus le même endroit :

```
mt.inbound  3 486 -> 22 403     → le ROUTEUR est le nouveau goulot
mt.routed       7 ->     12     → plat : le pool de connecteurs suit
mt.outcome     20 ->     33     → plat : la projection de CDR suit
```

`mt.routed` montait de **+631 rec/s** avant la step ; il est maintenant plat à une douzaine de records.
Le pool a cessé d'être le facteur limitant, et la projection introduite par `D1` ne crée pas de backlog
à son tour — son retard reste sous les 35 records, soit largement sous le seuil de `D4`. Le sujet de
`step-201c` — « le `connector-pool-svc` sort 192–330 `submit_sm/s` » — est donc **clos**.

Ce qui reste est un goulot **différent, non mesuré jusqu'ici** : le routeur consomme `mt.inbound` moins
vite que l'ingestion ne l'alimente. La latence bout-en-bout le confirme (p99 entre 10,2 et 20,5 s,
moyenne 11,2 s) : c'est de l'attente en file, pas du temps de traitement — l'ingestion, elle, répond en
11 ms au p99. Ce goulot appartient à `step-201b`, qui porte le verdict NFR ; le nommer était hors du
périmètre de cette step, le mesurer ne l'était pas.

Le pair reste hors de cause : calibré dans le run même, le faux SMSC a répondu à **236 274
`submit_sm/s`** sur 4 binds, soit 265 fois la sortie observée.

### Mesure du 08/08/2026 (step-201d) — le goulot est nommé, et le chiffre du 03/08 ne se reproduit pas

Le harnais a changé sur trois points **avant** toute mesure, tous consignés en `step-201d` `D3`-`D5` :

1. sa configuration Kafka **dérive désormais des défauts de production** (`config.Defaults()`) au lieu
   d'un littéral qui laissait les quatre leviers de fetch à zéro — `FetchMaxPartitionBytes` valait donc
   1 MiB, le défaut franz-go, au lieu des 56 KiB de l'ADR-0012. Épinglé par
   `TestRefKafkaCarriesProductionDefaults`, dans la suite ordinaire ;
2. `pipeline_duration_seconds` est **enfin alimenté** (`router.Deps.Metrics` était nil) ;
3. le backlog est relevé **par partition**, et la consommation CPU du processus est comptée.

#### Le constat du 03/08 n'est pas reproductible

| Arbre | Sortie | Pente `mt.inbound` | p99 e2e (moyenne) | Verdict |
|---|---:|---:|---:|---|
| code `73ad72a` (201c), mesuré le 03/08 | 892/s | +291 rec/s | 11,2 s | FAILED |
| **code `73ad72a`, rejoué le 08/08** | **1 141/s** | **+22,7** | 646 ms | FAILED |
| **HEAD (201d PR1), 08/08** | **1 180/s** | **+2,2** | 229 ms | **PASSED** |

Le pair calibre 174 168/s ce jour contre 236 274/s le 03/08 : **l'hôte n'est pas dans le même état**, et
la valeur de 892/s appartient à ces conditions-là. Ce qui est reproductible sur cet hôte, code du 03/08
inclus, c'est un ordre de grandeur de **1 100–1 200 `submit_sm/s`**. Une contre-épreuve à
`FETCH_MAX_PARTITION_BYTES=1048576` sort 1 176/s : **le réglage Kafka n'explique rien** (dans le bruit).

#### Le point de saturation, encadré

La réserve « le point d'équilibre n'a pas été encadré » est levée. Balayage du débit visé, défauts
livrés, `ACCOUNTS=1` :

| Visé | **Sorti** | Pente `mt.inbound` | `mt.routed` | Pipeline / budget | CPU du processus |
|---:|---:|---:|---|---:|---:|
| 1 200/s | **1 180/s** | +2,2 rec/s | plat (4→9) | 19 µs / 847 µs = 2,2 % | 1,08 cœur sur 14 |
| 2 400/s | **1 194/s** | +1 218 rec/s | plat (7→8) | 19 µs / 838 µs = 2,3 % | 1,10 cœur sur 14 |
| 4 800/s | **1 450/s** | +3 369 rec/s | plat (7→9) | 18 µs / 690 µs = 2,7 % | 1,58 cœur sur 14 |

**La sortie sature autour de 1 200 `submit_sm/s`.** Au-delà, tout s'empile sur `mt.inbound` pendant que
`mt.routed` et `mt.outcome` restent plats : le pool de connecteurs et la projection de CDR suivent, le
routeur non. Le goulot est bien celui que `step-201d` avait supposé — la valeur, non.

#### Ce que le goulot n'est pas

Quatre hypothèses écartées par une mesure chacune, pas par raisonnement :

| Hypothèse | Mesure | Verdict |
|---|---|---|
| L'hôte est saturé | `Getrusage` : **1,1 cœur sur 14 (8 %)** | écartée — la boucle **attend**, elle ne calcule pas |
| Le coût est dans le pipeline | `pipeline_duration_seconds` : **19 µs** d'un budget de 838 | écartée — **2,3 %** |
| Un verrou partagé dans le pipeline | `BenchmarkPipelineProcessParallel` 1→8 : 10,1 → **2,6 µs** (×3,9) | écartée — il parallélise |
| Un plafond côté broker | `TestRoutedProduceCeiling` 1→256 en vol : 6 177 → **188 182/s** (×30) | écartée — aucun plafond |

Reste, **par soustraction** : 838 µs de budget, 19 µs de pipeline, ~162 µs pour un `ProduceSync` acks=all
mesuré à vide ⇒ **~97 % du temps par message est passé bloqué hors du pipeline**, dans une boucle de
consommation à **une seule goroutine** qui publie **un record à la fois**.

#### Répartition par étape du pipeline (`BenchmarkPipelineStages`, 3 répétitions, dispersion < 5 %)

| Étape | ns/op | allocs | part du pipeline |
|---|---:|---:|---:|
| `e164` | **6 314** | 55 | **63 %** |
| `segment` | 1 067 | 6 | 11 % |
| `encoding` | 968 | 2 | 10 % |
| `opt_out` | 784 | 14 | 8 % |
| `anti_spam` | 110 | 4 | 1 % |
| `route` | 107 | 3 | 1 % |
| `sender_id` | 15 | 0 | ~0 % |

`e164.Normalize` domine le pipeline **et le processus le paie deux fois par message** (routeur +
projection CDR `accepted`), alors que l'ingestion l'a délibérément déporté de son chemin de requête. À
2,3 % du budget total, ce n'est pas un sujet aujourd'hui ; ce le deviendrait si le budget descendait d'un
ordre de grandeur.

#### Le compte unique est un confondant, et il coûte 37 %

`mt.inbound` est clé par compte, et le run n'en semait qu'un — donc **une seule partition**, ce que le
relevé par partition rend enfin visible. `REF_ACCOUNTS` (défaut 1) le corrige. Mesuré à 2 400 visés :

| | `ACCOUNTS=1` | `ACCOUNTS=32` |
|---|---:|---:|
| **Sorti** | 1 098/s | **1 507/s** (+37 %) |
| Backlog par partition | `p0=86 805` · p1..p3 = 0 | `10 283 / 5 030 / 17 513 / 34 040` |
| Pente du lag | +1 288 rec/s | +915 rec/s |
| CPU | 1,06 cœur | 1,18 cœur |

**Le routeur n'a pas changé d'une ligne.** Répartir les clés lui fait fetcher quatre partitions au lieu
d'une, ce qui amortit les allers-retours de poll. La boucle reste sérialisée — mais elle devient enfin
*parallélisable*, ce qu'aucun run à un seul compte n'aurait pu montrer.

#### Réserves propres à cette mesure

- **Un run par configuration**, comme les lignes précédentes. Les sorties sont serrées (1 141 / 1 176 /
  1 180 / 1 194, soit 4,6 % d'amplitude) ; **la pente du lag, elle, est très bruitée** (+2,2 / +6,8 /
  +22,7 sur des configurations voisines) parce qu'elle est ajustée sur une file quasi équilibrée. Lire
  la sortie, pas la pente, quand le système n'est pas franchement saturé.
- **La comparaison 03/08 vs 08/08 n'est pas contrôlée sur l'hôte.** Seule la ligne « code `73ad72a`
  rejoué le 08/08 » l'est, et c'est elle qui porte la conclusion.
- **Le ~97 % non expliqué est une soustraction, pas une observation directe.** Le `ProduceSync` en
  situation n'a pas été chronométré : les 162 µs viennent d'un banc à vide, sur un broker au repos. La
  décomposition fine appartient à la porte de correctif.
- **Le harnais reste un majorant** : tracer no-op, `Sealer` nil, pas d'agrégat de disjoncteur Redis, et
  les étages débit / anti-spam Redis / crédit toujours en pass-through (`step-201d` `D4`). Ce dernier
  point n'a pas été corrigé ici — la mesure a montré que ces trois étages ne sont pas sur le chemin
  critique — et revient à `step-201b`.

### Mesure du 08/08/2026 (step-201d PR2) — le fan-out du routeur, et le goulot qui repart en aval

Le routeur consomme désormais par lot avec **une goroutine par partition** (`step-201d` `D11`). Rien
d'autre n'a changé sur le chemin chaud.

#### Au débit de référence, le critère passe et la file se vide

À 2 400 msg/s injectés, `ACCOUNTS=32`, 4 partitions — la même configuration que PR1 avait mesurée :

| | avant (PR1) | **après (PR2)** |
|---|---:|---:|
| **Sorti** | 1 507/s | **2 400/s** |
| Backlog `mt.inbound` | 16 554 → 66 866 (**+915 rec/s**) | **137 → 7** (la file se **vide**) |
| Écart d'équilibre | 37,21 % | **0,01 %** (tolérance 2 %) |
| Pipeline / budget | 18 µs / 664 µs (2,7 %) | 17 µs / 417 µs (4,2 %) |
| CPU du processus | 1,18 cœur sur 14 | 1,40 cœur sur 14 |
| Verdict | **FAILED** (2 clauses) | **PASSED** (8 clauses) |

**+59 % de sortie, et le critère d'état stationnaire tient à un débit qui le mettait en échec avant.**

#### La courbe de lanes, à un débit saturant

`ACCOUNTS=32`, `RATE=4800`, balayage de `PARTITIONS` — donc du nombre de lanes, puisque la lane **est** la
partition :

| Lanes | Débit du **routeur** | Backlog `mt.inbound` | Backlog `mt.routed` | Sorti |
|---:|---:|---|---|---:|
| 1 | 1 692/s | 70 902 → **247 927** | plat (7 → 6) | 1 692/s |
| 2 | 2 990/s | 41 292 → **144 018** | plat (46 → 10) | 2 990/s |
| 4 | 3 422/s | 21 892 → 100 542 | 11 022 → **30 849** | 3 091/s |
| 8 | **4 702/s** | 5 109 → **9 648** | 33 164 → **147 244** | 2 721/s |

Le débit du routeur monte de façon monotone, **×2,8 de 1 à 8 lanes**, et à 8 lanes `mt.inbound` se vide
presque : 9 648 records de retard pour 288 000 injectés. Le goulot que `D9` avait nommé est levé.

#### Le goulot suivant est le pool de connecteurs, et c'est vérifié, pas déduit

À partir de 4 lanes la file migre sur `mt.routed`, et à 8 lanes elle y est entièrement. La contre-épreuve
tranche : mêmes 8 lanes, `BIND_POOL` porté de 4 à 16 —

```
BIND_POOL=4   mt.inbound  5 109 ->   9 648   ·  mt.routed 33 164 -> 147 244   (le pool ne suit pas)
BIND_POOL=16  mt.inbound     56 ->  74 332   ·  mt.routed     27 ->     213   (le pool suit, la file remonte)
```

`mt.routed` redevient plat dès qu'on élargit le pool : le retard de 147 244 records lui appartenait bien.

#### Réserves propres à cette mesure

- **Les deux étages se disputent le même hôte.** À `BIND_POOL=16` le débit du *routeur* retombe de
  4 702/s à 3 395/s : seize binds de plus s'ordonnancent sur les mêmes cœurs. Le processus ne brûle que
  1,6 cœur sur 14, donc ce n'est pas du CPU brut — c'est de la contention entre neuf composants et trois
  conteneurs sur une machine. **Les chiffres à 4 800 msg/s bornent des tendances, pas des capacités** ;
  ceux à 2 400, où le critère passe, sont les seuls à porter un verdict.
  **→ Réserve levée par la mesure de step-201e ci-dessous** : la contention soupçonnée ici est
  confirmée et chiffrée. Isolé, le routeur fait 19 747 msg/s à 8 lanes contre 4 702 ici. Les chiffres
  de ce tableau restent ce qu'ils sont — une mesure de la *machine*, pas du routeur — mais on sait
  désormais de quoi ils sont la mesure.
- **`PARTITIONS=1` handicape plus que le routeur** : `mt.routed` et `mt.outcome` sont eux aussi ramenés à
  une partition. Le pool shardant par identifiant de message et non par partition, l'effet dominant reste
  bien le routeur, mais la première ligne du tableau n'isole pas parfaitement.
- **Un run par configuration**, comme partout ailleurs dans ce fichier. La sortie est le chiffre robuste ;
  la pente du lag reste bruitée près de l'équilibre.
- Le harnais reste un **majorant** : tracer no-op, `Sealer` nil, étages débit / anti-spam Redis / crédit
  toujours en pass-through (`step-201d` `D4`), tout dans un processus.

### Mesure du 08/08/2026 (step-201e) — le plafond est **attribué** : ce n'était pas le routeur

`TestRouterConsumeCeiling` isole le routeur de tout ce qui partageait son hôte. Pas d'injecteur, pas de
REST, pas de pool, pas de pair : un topic privé est **pré-rempli** avant que le chrono ne parte, et la
seule chose qui tourne est `RunBatch → Pipeline.Process → Produce`.

```
make load-reference RUN=TestRouterConsumeCeiling PREFILL=600000
```

Conditions : 600 000 enregistrements pré-remplis par palier, fenêtre de 10 s, un compte par partition
(placement **vérifié** sur les end-offsets, pas prédit), `mt.routed` inchangé à 4 partitions sur tout le
balayage. Le débit est recoupé par deux sources indépendantes — le compteur du producteur et
`lag_début − lag_fin` — et le lecteur du broker (`D2`) scrute les deux bouts de la fenêtre.

| Lanes | **Routeur seul** | Recoupement backlog | Lanes réellement ouvertes | Plein-stack PR2 | Rapport |
|---:|---:|---:|---|---:|---:|
| 1 | **5 842/s** | 5 827/s (−0,3 %) | 1,0 sur 1 | 1 692/s | ×3,5 |
| 2 | 6 981/s | 6 729/s (−3,6 %) | 2,0 sur 2 | 2 990/s | ×2,3 |
| 4 | 13 158/s | 12 726/s (−3,3 %) | 4,0 sur 4 | 3 422/s | ×3,8 |
| 8 | **19 747/s** | 19 521/s (−1,1 %) | 8,0 sur 8 | **4 702/s** | **×4,2** |
| 16 | **27 856/s** | 27 385/s (−1,7 %) | 16,0 sur 16 | — | — |

#### La réponse, et elle est franche

La question posée d'avance était : *le routeur replafonne-t-il vers 4 700/s une fois seul ?* **Non.**
Et c'est la branche la plus forte de la table de décision qui se réalise : **une seule lane isolée
(5 842/s) bat déjà le 8-lanes plein-stack (4 702/s)**. À 8 lanes, à configuration égale, le routeur va
**4,2 fois plus vite** sans rien d'autre sur la machine — et la courbe **ne plafonne toujours pas** à
16 lanes.

Le plafond de 4 800 msg/s de step-201d n'était donc ni le routeur, ni le broker : **c'est la
co-résidence** de neuf composants et trois conteneurs sur un portable. `step-201d` `D11` (le shard par
clé de compte) n'a pas de courbe qui le réclame : le fan-out par partition achète encore du débit
là où on l'a poussé.

#### Le broker est blanchi, et c'est mesuré et non déduit

C'est ce que `D2` ajoute : la latence de service du broker, lue sur sa propre exposition
(`/public_metrics`), aux deux bouts de chaque fenêtre.

| Lanes | `produce` servis | Latence de service moyenne | CPU du **broker** |
|---:|---:|---:|---:|
| 1 | 58 429 | 39 µs | 0,29 cœur |
| 2 | 69 405 | 78 µs | 0,40 cœur |
| 4 | 124 018 | 94 µs | 0,55 cœur |
| 8 | 129 827 | 108 µs | 0,64 cœur |
| 16 | 108 339 | 131 µs | **0,56 cœur** |

La latence monte de 39 à 131 µs pendant que le débit est multiplié par 4,8 : le broker se charge, mais
il sert toujours en **131 microsecondes** et ne brûle que 0,56 cœur sur le shard qui lui est alloué.
Deuxième borne, gratuite : le pré-remplissage lui fait absorber **520 000 enregistrements/s** avec
batching. Le broker n'est le goulot d'aucun palier de ce tableau.

`cpuSeconds` ne comptait que le processus Go et le disait ; ce chiffre-là est celui du conteneur qui
manquait. `D4` (cgroups) devient sans objet ici — un seul conteneur tourne, et il publie son propre CPU.

#### Réserves propres à cette mesure

- **Un majorant du routeur, énoncé comme tel.** Les étages résolution / sender ID / opt-out / anti-spam
  sont des bouchons, et `RateLimiter`/`Credit` sont nuls — comme dans le run de référence auquel la
  courbe est comparée (`step-201d` `D4`). Le coût est **borné par une mesure déjà faite** et non
  supposé : `Pipeline.Process` pèse 2,3 % du budget par message et ces quatre étages ~10 % du pipeline
  (`step-201d` `D8`), soit **moins de 0,3 %** du budget. `e164` et la segmentation — 74 % du pipeline —
  tournent pour de vrai.
- **Le backlog est équilibré par construction ; le trafic réel ne l'est pas.** Un compte par partition,
  choisi pour ça. La courbe mesure donc ce que les lanes achètent **quand elles sont toutes
  alimentées** : la borne haute du fan-out, jamais la moyenne d'un trafic réel.
- **Un run par configuration**, comme partout ailleurs dans ce fichier — et la dispersion est réelle.
  Sur quatre runs : 1 lane entre 4 481 et 5 856/s, 2 lanes entre 6 981 et 9 639/s, 8 lanes entre 19 475
  et 21 226/s, 16 lanes entre 27 856 et 30 728/s. Le palier à 2 lanes est le plus instable ; **le
  constat ne repose sur aucun palier isolé** mais sur un rapport de 4 qui tient sur les quatre runs.
- **Le coût du produce n'est pas encore observé, il est déduit.** La branche « la courbe plie puis
  reste plate » de la table de décision ne se lit ici que par soustraction. `D3` (histogramme autour de
  `Producer.Produce`, PR2) l'observe — et devra **relancer ce balayage** pour la lire.
  **→ Réserve levée par la mesure de step-201e PR2 ci-dessous** : le produce est chronométré, et il
  explique le débit à 5 % près.
- **`offset_commit` n'est pas mesuré** : `/public_metrics` n'en publie aucune série curée (vérifié sur
  la capture). Il vit dans l'exposition interne, non lue ici.
- Un seul **shard** Redpanda sur cet hôte : l'agrégation par shard est exercée par la fixture, pas par
  la mesure.

### Mesure du 09/08/2026 (step-201e PR2) — le débit du routeur **est** le produce, divisé par les lanes

`D3` chronomètre `Producer.Produce` dans le harnais, aux deux bouts de la fenêtre de chaque palier. La
question était posée d'avance : *le coût par record est-il payé dans le produce, ou ailleurs ?*

```
make load-reference RUN=TestRouterConsumeCeiling PREFILL=600000
```

| Lanes | Débit | Produces | **Produce moyen** | p99 du produce | Latence de service du broker |
|---:|---:|---:|---:|---|---:|
| 1 | 4 810/s | 48 104 | **188 µs** | (256 µs, 512 µs] | 46 µs |
| 2 | 8 161/s | 81 613 | 225 µs | (256 µs, 512 µs] | 63 µs |
| 4 | 14 020/s | 140 204 | 264 µs | (256 µs, 512 µs] | 86 µs |
| 8 | 20 741/s | 207 413 | 362 µs | (512 µs, 1,024 ms] | 103 µs |
| 16 | 29 843/s | 298 437 | **507 µs** | (512 µs, 1,024 ms] | 125 µs |

#### La réponse : le coût est dans le produce, et le modèle se vérifie

C'est la première branche de la table de décision. **Le produce ralentit de 188 à 507 µs — ×2,7 —
pendant que le débit fait ×6,2.** Chaque lane passe donc une part croissante de son temps bloquée dans
son aller-retour `acks=all`, ce qui est exactement la forme d'un rendement par lane qui s'effondre.

Le modèle « chaque lane est bloquée dans son produce » suffit à prédire le débit :

| Lanes | `lanes ÷ produce moyen` | **Débit mesuré** | Écart |
|---:|---:|---:|---:|
| 1 | 5 319/s | 4 810/s | −10 % |
| 8 | 22 099/s | 20 741/s | −6 % |
| 16 | 31 558/s | 29 843/s | **−5 %** |

Le débit du routeur **est** le produce synchrone divisé par le nombre de lanes, à 5-10 % près. Le reste
— décodage, `Pipeline.Process`, encodage, commit d'offset — tient dans cet écart, ce qui recoupe les
2,3 % de budget mesurés pour le pipeline en step-201d `D8`.

#### Ce que le broker en dit, et ce qu'il n'explique pas

Sa latence de service monte aussi (46 → 125 µs), mais elle reste **quatre fois inférieure** au produce
vu du client. La différence n'est pas du travail de broker : c'est de l'attente. Le client attend
l'accusé de la réplication `acks=all`, et cette attente croît avec le nombre de produces concurrents
sur le même client Kafka et le même shard.

**Conséquence pour step-207**, qui complète le couplage déjà consigné : ajouter des lanes achète du
débit tant que le produce ne ralentit pas plus vite que les lanes n'augmentent. À 16 lanes on paie
déjà ×2,7 de latence unitaire pour ×16 de parallélisme — la marge existe, elle n'est pas infinie.

#### Réserves propres à cette mesure

- **La part du budget dépasse 100 % à partir de 2 lanes, et c'est normal** : le budget est l'inverse du
  débit de *tout* le routeur, la moyenne est celle d'*un* produce. Une part de 1 514 % à 16 lanes
  signifie ~15 produces en vol à chaque instant. La ligne le dit désormais explicitement, parce que le
  chiffre brut se lisait comme une erreur d'arithmétique.
- **La p99 est un intervalle**, jamais une valeur : sur des bornes log2 une interpolation porterait
  jusqu'à 100 % d'erreur en se lisant comme une mesure.
- Le chronomètre est **dans le harnais**, pas dans `internal/router` : ce que la production paierait
  n'est pas mesuré ici, et n'a pas à l'être (step-201d `D7`).
- Ce run est **plus lent que celui de PR1** sur les petits paliers (4 810 contre 5 842/s à 1 lane) :
  même dispersion qu'ailleurs dans ce fichier, un run par configuration. Le constat porte sur la
  **forme** des deux courbes, jamais sur un palier isolé.

### Réserves, nommément

- **Un seul hôte, un seul processus, une seule mesure par configuration.** Les quatre lignes du tableau
  sont des runs uniques : l'écart entre 297/s et 330/s est du même ordre que la dispersion visible dans
  le micro-banc CDR (154 → 548 → 278 → 301). Ce que la mesure établit solidement est **l'ordre de
  grandeur** de la sortie et **le fait qu'aucun levier de `D5` ne la déplace**, pas la valeur exacte.
- **Le débit soutenable maximal n'a pas été encadré.** Le run à 400/s visés sortait encore 195/s avec
  une file qui montait : le point d'équilibre est **sous 400 msg/s** et n'a pas été cherché par
  dichotomie. Le chiffre à retenir est « ≪ 1 000 », pas une valeur.
- **La latence bout-en-bout n'est pas un verdict ici.** L'histogramme de la passerelle lit un p99 dans
  `(40,96 s ; 81,92 s]` — c'est la file mesurée, pas la passerelle au repos, et il n'a de sens qu'une
  fois l'état stationnaire tenu. Il est imprimé en contexte, jamais comme clause.
- **La p99 d'ingestion vient de l'injecteur, pas d'une exposition.** `ingest_duration_seconds` est
  déclarée au catalogue et **observée nulle part** — même défaut mort que
  `message_e2e_duration_seconds` avant l'unité 5 — et ses bornes encadrent le budget des 250 ms
  (0,128 / 0,256), donc aucune exposition ne le déciderait aujourd'hui même alimentée. L'injecteur
  garde ses échantillons et calcule un percentile **exact**, sans bucket ni interpolation.
- **`QueueDepth` n'est pas alimentée sur ce chemin.** La jauge du catalogue est publiée par
  `pollQueueDepth` dans les `main` de `router-svc` et `connector-pool-svc` ; le harnais ne monte pas de
  `main`, il interroge donc `kafka.Consumer.Lag` directement — la même source que la jauge.
- **`chtest` laisse le pool ClickHouse à zéro**, ce que le driver relit silencieusement comme « non
  défini » et remplace par ses 5/10 (`clickhouse_options.go:412-417`). Tous les tests d'intégration du
  dépôt tournent donc sur les défauts de la bibliothèque, jamais sur les leviers exposés à l'unité 6 :
  le harnais les surcharge lui-même (`REF_CH_MAX_OPEN` / `REF_CH_MAX_IDLE`).

### Écart assumé avec la lettre de `D2`

`D2` parle du « débit d'acceptation **k6** ». Le run injecte **en Go, dans le même processus**. Raison :
la pile est montée par testcontainers sur des ports éphémères, et faire porter la mesure par un binaire
externe ajouterait un ordonnancement fragile (k6 n'est pas dans `go.mod`, la CI ne l'installe pas
partout) pour zéro gain — l'injecteur Go pose la même requête sur le même contrat, et **garde ses
échantillons**, ce qui donne un p99 exact au lieu d'un bucket. Le script k6 reste l'outil pour viser une
passerelle **déployée** ; c'est lui que `step-201b` utilisera. À corriger dans la fiche.

### Mesure du 12/08/2026 (step-201f) — le pool **n'est pas** le plafond, et la pente n'est pas mesurable ici

`make load-reference RUN=TestPoolSubmitCeiling`. Le pool de connecteurs seul : un topic `mt.routed`
privé pré-rempli, aucun routeur, aucun REST, aucun injecteur, un faux SMSC en face. Trois runs, dix-huit
paliers.

#### Le pool seul, contre les 2 400/s du plein-stack

| Binds (w64) | run 1 (10 s) | run 2 (30 s) | run 3 (30 s, lit unique) |
|---:|---:|---:|---:|
| 1 | 4 044/s | 4 351/s | 3 294/s |
| 2 | 7 724/s | 6 690/s | 4 814/s |
| 4 | 12 947/s | 9 547/s | 8 125/s |
| 8 | 16 888/s | 19 724/s | 12 573/s |
| 16 | 25 313/s | — | 29 313/s |

**Le plus mauvais palier du plus mauvais run — 3 294/s, à UN bind — dépasse déjà les 2 400/s mesurés en
plein-stack.** À 8 binds les trois runs donnent 12 573, 16 888 et 19 724/s, tous au-dessus de la cible
NFR de 10 400 `submit_sm/s`. Le pair, calibré à chaque palier au nombre de binds du palier, tient entre
123 000 et 190 000/s : **cinq à soixante fois au-dessus de toute mesure**, donc il n'est jamais la
contrainte — c'est la réponse au piège que la fiche appelait `D3`.

Les 2 400/s n'appartenaient donc **pas au pool**. Ils appartenaient à l'hôte, exactement comme les
4 702/s qui semblaient appartenir au routeur avant qu'il en fasse 20 741 isolé.

#### Ce que ce banc ne peut pas dire

**La pente.** Le balayage mesure deux fois la même configuration — 8 binds / w64, une fois dans chaque
courbe — et les deux lectures divergent de 17 % au run 1 et de 19 % au run 3. La garde `sweepsAgree`
refuse la courbe sur ce seul motif.

Ce n'est pas une dérive entre balayages : au run 3, le balayage B lit 19 540 · 19 257 · 14 972 · 19 025
sur un levier (la fenêtre SMPP) que le run 1 a montré inerte à 1,5 % près. **30 % de dispersion entre
quatre paliers de configuration équivalente**, tous en fin de run. Le bruit de cet hôte est du même
ordre que les pas que la courbe sert à lire.

Conséquence : aucun coude, aucune loi d'échelle par bind, aucun verdict sur la fenêtre SMPP ne sort de
ce banc **sur cette machine**. Le run 1 suggérait une fenêtre inerte de w1 à w256 ; le run 3 l'a
contredit. Ces deux lectures s'annulent.

#### Ce que step-207 peut retenir

Le seul énoncé robuste au bruit de ±30 % : **8 binds par connecteur passent la cible NFR dans les trois
runs** (minimum 12 573/s contre 10 400 visés), **4 binds ne la passent pas de façon fiable** (8 125,
9 547 puis 12 947 — à cheval). C'est un plancher de dimensionnement, pas un optimum ; l'optimum demande
l'environnement représentatif de `step-201b`.

#### Réserves propres à cette mesure

- **Tous ces chiffres sont des MAJORANTS de la production.** `DLRMap` est nil, donc aucun palier ne paie
  l'écriture Redis de corrélation DLR que la production effectue à chaque `submit_sm` acquitté. Le run
  de référence faisait la même omission — en silence — ce qui rend ses 2 400/s comparables à ces lignes
  et fait qu'aucune des deux n'est une capacité de production. **Ce majorant vaut 1,6× : mesuré le
  27/08/2026, section suivante.**
- `Billing` est nil : le banc mesure un déploiement facturation-off, ce qu'autorise l'invariant (c).
- Le disjoncteur n'est **pas** bouchonné, et aucun palier ne l'a vu s'ouvrir.
- Le produce `mt.outcome` acquitté est bien sur le chemin mesuré. La ligne de latence de produit
  l'annonce à 83 % du budget par message à 1 bind : **le coût par message est le produce, pas
  l'aller-retour SMPP** — ce qui est cohérent avec une fenêtre SMPP sans effet visible. **Confirmé le
  27/08/2026 par décomposition** (section suivante) ; le `submit_sm` lui-même n'a pas de chronomètre, et
  la section dit pourquoi.


### Mesure du 27/08/2026 (step-201f PR2) — ce que coûte l'écriture DLR, et où passe vraiment la milliseconde

`make load-reference RUN=TestPoolDLRMapFidelity`. Même banc que ci-dessus, même lit, une seule
configuration — **8 binds / fenêtre 64**, le plancher de dimensionnement que PR1 avait nommé — mesurée
six fois : trois paliers avec le vrai store Redis câblé (`dlrmap.NewRedisMap`), trois sans.

Les couples sont **entrelacés et alternés** : sans/avec, avec/sans, sans/avec. Trois fenêtres sans puis
trois avec auraient ramassé la dérive de l'hôte — PR1 a lu 12 573 puis 14 972/s sur *une même*
configuration — et l'auraient rendue sous le nom de Redis. L'alternance interdit en plus qu'un côté soit
systématiquement celui qui passe en second.

#### Le prix de la corrélation DLR

| Couple | ordre | sans le store | avec le store |
|---:|---|---:|---:|
| 1 | sans d'abord | 19 763/s | 12 847/s |
| 2 | avec d'abord | 19 673/s | 12 178/s |
| 3 | sans d'abord | 19 567/s | 12 242/s |
| **moyenne** | | **19 668/s** | **12 422/s** |

**Dispersion à l'intérieur d'un côté : 5 %. Écart entre les deux : 37 %.** L'écart est sept fois la
dispersion, ce qui est la condition que `fidelityDelta` exige avant de laisser nommer un coût. Un run de
fumée indépendant (fenêtres de 5 s, deux couples) avait lu 34 % : les deux se recoupent.

**Second run, instrument corrigé.** La revue a trouvé un confondant : les enregistrements du banc ne
portent pas de `validity_period`, donc `ttlForValidity` retombe sur `maxTTL` et chaque entrée vit
**72 heures**. Rien n'expirait pendant le run, et le troisième palier « avec » mesurait un Redis d'un
million de clés que le premier n'avait jamais vu — l'appariement existe précisément pour exclure cela.
Le store est désormais vidé avant chaque palier « avec ». **Le chiffre n'a pas bougé : 37 % à nouveau**
(16 918/s sans, 10 740/s avec), sur un hôte pourtant plus lent — le pair calibrait à 136 000–141 000/s
contre 156 000–170 000 au premier run. Le confondant était réel en principe et sans effet mesurable ;
trois runs indépendants donnent 34 %, 37 %, 37 %.

Ce second run est aussi celui où la garde travaille près de sa limite : le côté « sans » y disperse de
15 % (18 046 · 17 226 · 15 482), et l'écart de 37 % ne le dépasse plus que d'un facteur 2,5. À la
dispersion qu'avait lue PR1 sur cet hôte — 30 % entre paliers équivalents — le verdict serait devenu
illisible, et c'est ce que `fidelityDelta` dirait alors plutôt que de publier un coût.

Les six paliers sont propres : `crossCheck` entre −0,7 % et +0,0 %, aucun disjoncteur ouvert, les huit
binds actifs à chaque fois, `putsMatchSubmits` vérifiant que le store a bien été appelé une fois par
`submit_sm` **compté par le pair** — c'est cette paire-là que la garde compare, et le journal ne la
publie pas ; il montre 365 392 écritures pour 365 338 `mt.outcome` produits, même ordre de grandeur.
**Une garde neuve, et elle est le cœur du palier** : `recordDLRMapping` saute un `submit_sm_resp` non-ROK et une réponse sans
`smsc_msg_id`, si bien qu'un palier peut détenir un vrai store, l'ouvrir, et ne jamais l'appeler. Les
deux côtés seraient alors la même configuration et leur écart serait du bruit publié sous le nom de
Redis — sans que le débit, le disjoncteur ni le recoupement n'aient rien à en dire.

#### Où passe la milliseconde : la décomposition, faute de chronomètre

La fiche demandait de **chronométrer le `submit_sm` in situ**. Ce n'est pas ce que cette PR livre, et le
motif n'est pas un manque de temps : **il n'y a pas de couture**. `bind.Submit` est concret, non exporté,
et atteint depuis un unique site d'appel à l'intérieur du chemin d'envoi ; le chronométrer exigerait
d'ajouter une interface à `connectorpool.Deps`, c'est-à-dire un changement du chemin chaud de production
que la fiche s'interdit deux fois. La réponse vient donc d'une décomposition, et elle est bornée en
conséquence.

À 8 binds / w64 avec le store, à 12 242 `submit_sm/s` (budget 82 µs par message, 8,0 lanes par batch) :

| Étage | Moyenne mesurée | Occupation (loi de Little) |
|---|---:|---:|
| produce `mt.outcome` (acks=all) | 347 µs | 4,2 lanes sur 8 |
| écriture DLR Redis | 213 µs | 2,6 lanes sur 8 |
| **reste** (décodage, `buildSubmit`, **aller-retour SMPP**, encodage) | — | **≤ 1,2 lane sur 8** |

Le reste est un **majorant** : la concurrence réelle vaut au plus les 8 goroutines du fan-out, jamais
plus, donc 8 − 6,8 borne par le haut ce que tous les autres étages consomment ensemble. Et le pair,
calibré à 156 318–170 531/s sur ces mêmes 8 binds, place l'aller-retour SMPP autour de 49 µs par
message, soit environ la moitié de ce reste.

L'occupation de l'écriture DLR se reproduit au second run — 2,7 lanes sur 8 aux trois paliers « avec »,
contre 2,6 ici — sur un hôte plus lent où sa moyenne monte à 243–255 µs. Ce qui se déplace, c'est la
durée absolue ; la part, non.

**Le coût par message est le produce et l'écriture Redis — pas l'aller-retour SMPP**, qui pèse au plus
15 % et vraisemblablement la moitié de cela. C'est ce que PR1 soupçonnait à 1 bind sans pouvoir le
confirmer, et cela explique une fenêtre SMPP restée inerte de w1 à w256 : élargir la fenêtre n'élargit
que l'étage qui ne coûte rien.

#### Ce que step-207 doit corriger de PR1

PR1 concluait « 8 binds passent la cible NFR dans les trois runs ». **Cette conclusion ne survit pas au
store.**

- Lecture directe : à 8 binds avec le store, 12 178–12 847/s, au-dessus des 10 400 visés. Marge 17–23 %.
- Mais PR1 avait lu, *sans* le store, 12 573 / 16 888 / 19 724/s sur la même configuration à trois runs
  — 57 % d'étalement. Le côté « sans » d'aujourd'hui (19 567–19 763) est le haut de cette plage.
  Appliquer les 37 % au bas de la plage donne ≈ 7 900/s, **sous la cible**.

Autrement dit : **la marge à 8 binds est plus petite que la variation entre runs.** Ce n'est plus un
plancher défendable. Ce que ce banc autorise à écrire : 8 binds passent *quand l'hôte est au mieux*, et
le dimensionnement doit prendre 16 binds ou attendre l'environnement représentatif de step-201b.

#### Réserves propres à cette mesure

- **Les 37 % sont eux-mêmes un chiffre de co-résidence.** Le Redis du banc est un conteneur sur le même
  portable que le pool, le broker et le pair : il se dispute leurs cœurs, et 205–213 µs de moyenne est
  lent pour un Redis local. En production le store est sur un autre hôte — sans concurrence CPU, mais
  avec un aller-retour réseau. **Le signe de l'écart est inconnu**, et c'est la même leçon que celle qui
  a fait tomber les 2 400/s.
- L'écriture DLR est **synchrone et avant le règlement** par conception (step-201c : un accusé de
  réception ne doit pas être orphelin si la comptabilité échoue). Les 37 % sont le prix de cette
  garantie. S'il faut le baisser — pipeliner, grouper — c'est un correctif, donc sa propre fiche et son
  propre arbitrage de durabilité ; cette fiche produit une attribution, pas un correctif.
- `Billing` reste nil, comme en PR1 : déploiement facturation-off, ce qu'autorise l'invariant (c).
- La décomposition ci-dessus n'est pas un chronométrage. Un chronomètre autour du `submit_sm` dirait sa
  durée ; la décomposition en donne un plafond. Si step-201b a besoin de la durée elle-même, la couture
  devra être arbitrée pour ce qu'elle est : un changement du chemin chaud.

## Contenu

| Chemin | Rôle |
|---|---|
| `k6/messages.js` | script REST, profils et seuils |
| `stub/` | stub du contrat `/v1/messages` à délai réglable et scrutin `Idempotency-Key` — la cible des runs négatifs |
| `bindgen/` | ouverture de N binds SMPP concurrents + injecteur `submit_sm` (logique testable) |
| `steady/` | évaluateur **pur** des clauses de l'état stationnaire (`Evaluate`) + injecteur HTTP paced |
| `promscrape/` | la moitié HTTP commune aux trois lecteurs : URL, redaction, horodatage précoce, garde 200 |
| `smscmetrics/` | lecture du `/metrics` du simulateur → débit absorbé |
| `gatewaymetrics/` | lecture du `/metrics` de la passerelle → verdict trois-valué sur le budget e2e |
| `redpandametrics/` | lecture du `/public_metrics` du broker → latence de service par API, CPU par shard |
| `ceiling/` | balayage du nombre de binds → plafond du pair (logique testable, sans simulateur) |
| `../../cmd/smpp-bindgen` | ligne de commande du générateur |
| `../../cmd/smsc-ceiling` | ligne de commande du balayage de plafond |
| `../../cmd/load-stub` | ligne de commande du stub |
| `../../scripts/load-smoke.sh` | orchestration du couple positif/négatif |
