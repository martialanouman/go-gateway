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

## Contenu

| Chemin | Rôle |
|---|---|
| `k6/messages.js` | script REST, profils et seuils |
| `stub/` | stub du contrat `/v1/messages` à délai réglable et scrutin `Idempotency-Key` — la cible des runs négatifs |
| `bindgen/` | ouverture de N binds SMPP concurrents + injecteur `submit_sm` (logique testable) |
| `smscmetrics/` | lecture du `/metrics` du simulateur → débit absorbé |
| `ceiling/` | balayage du nombre de binds → plafond du pair (logique testable, sans simulateur) |
| `../../cmd/smpp-bindgen` | ligne de commande du générateur |
| `../../cmd/smsc-ceiling` | ligne de commande du balayage de plafond |
| `../../cmd/load-stub` | ligne de commande du stub |
| `../../scripts/load-smoke.sh` | orchestration du couple positif/négatif |
