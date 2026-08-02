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
lance le même script deux fois : contre un stub instantané (doit passer) puis contre le même stub
ralenti à 300 ms (doit échouer). Le second run est la vraie assertion — un run de charge contre un
stub local passe trivialement, donc un vert seul ne signifie rien.

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
make load-binds BINDS=200 ADDR=127.0.0.1:2775          # N binds SMPP concurrents
```

Variables du script k6 : `PROFILE`, `BASE_URL`, `API_KEY`, `SENDER_ID`.

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

## Contenu

| Chemin | Rôle |
|---|---|
| `k6/messages.js` | script REST, profils et seuils |
| `stub/` | stub du contrat `/v1/messages` à délai réglable — la cible du run négatif |
| `bindgen/` | ouverture de N binds SMPP concurrents + injecteur `submit_sm` (logique testable) |
| `smscmetrics/` | lecture du `/metrics` du simulateur → débit absorbé |
| `ceiling/` | balayage du nombre de binds → plafond du pair (logique testable, sans simulateur) |
| `../../cmd/smpp-bindgen` | ligne de commande du générateur |
| `../../cmd/smsc-ceiling` | ligne de commande du balayage de plafond |
| `../../cmd/load-stub` | ligne de commande du stub |
| `../../scripts/load-smoke.sh` | orchestration du couple positif/négatif |
