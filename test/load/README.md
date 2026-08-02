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

### Mesure du 02/08/2026

Conditions : simulateur `smsc-simulator:dev` en conteneur (OrbStack, ports publiés), injecteur sur la
**même machine** (14 cœurs, arm64) ; profil `HealthyConfig` (latence fixe 5 ms) ; **60 s mesurés par
palier**, 10 s de chauffe, 5 s de marge, 5 s entre paliers ; fenêtre d'émission de 32 `submit_sm` en vol
par session (défaut `bindgen`).

| Binds | `submit_sm/s` absorbés | par bind | Latence servie moyenne | Issues non-`success` | Palier |
|---:|---:|---:|---:|---:|---|
| 10 | 1 710 | 171 | 5 ms | 0 | qualifié |
| 20 | **3 329** | 166 | 5 ms | 0 | qualifié |
| 40 | 6 435 | 161 | 5 ms | 0 | qualifié |
| 80 | 12 430 | 155 | 5 ms | 0 | qualifié |
| 160 † | 23 296 | 146 | 5 ms | 0 | qualifié |
| 320 † | 43 498 | 136 | 5 ms | 0 | qualifié |

† hors du balayage de `D3`, ajoutés parce que la courbe ne pliait toujours pas à 80.

**Plafond au nombre de binds du run de référence — 3 329 `submit_sm/s` à 20 binds.** C'est le chiffre
sous lequel le run de référence de `D2` doit se situer. 20 binds parce que `bind_pool_size` est borné à
1..32 par le schéma du plan de contrôle : c'est le plus grand palier du balayage qu'un pod de
`connector-pool-svc` puisse reproduire seul. Le run de référence vise ≥ 1 000 msg/s traversants, soit
≈ 1 300 `submit_sm/s` à 1,3 segment — **moins de la moitié** de ce que le pair tient à ce niveau.

**Plafond du balayage — 12 430 `submit_sm/s` à 80 binds**, et c'est une **borne inférieure, pas un
plafond**. Aucun palier n'a plié : à 320 binds le pair absorbait encore 43 498/s sans une seule issue
non-`success`. Le pair n'a jamais été saturé, donc son vrai plafond n'est pas connu — il est seulement
**au-dessus** de tout ce qui est mesuré ici.

**Ce que les chiffres désignent.** Le débit est **linéaire en nombre de binds**, avec une érosion lente
du débit par bind (171 → 136/s de 10 à 320 binds, ~20 %). Le goulot est donc **par bind**, pas partagé :
le simulateur sérialise le service sur la goroutine de lecture de chaque bind (`serveLatency` appelé
avant toute réponse), ce qui plafonne un bind à 1/5 ms = 200/s en théorie. Les 136–171/s observés
correspondent à 5,8–7,3 ms réels par `submit_sm` : les 5 ms d'attente plus le codec, l'`Append` du
recorder et les compteurs — l'injecteur et le simulateur se disputant les mêmes 14 cœurs. Le
`sync.RWMutex` du recorder était le suspect n° 1 pour une contention **inter-binds** : il n'est pas la
limite à ces débits, sinon le débit par bind s'effondrerait avec le nombre de binds au lieu de perdre
20 % sur un facteur 32.

**Conséquence pour `D1`.** Les 10 400 `submit_sm/s` que la cible NFR implique en sortie (8 000 SMS/s ×
1,3 segment) sont déjà dépassés à 80 binds sur une machine de développement. Le simulateur ne sera pas
la contrainte artificielle de step-201b — à condition de lui donner assez de binds : il en faut
**≥ 80**, pas les ~52 que le modèle 200/s par bind laissait espérer.

**Piège consigné.** `smsc_served_latency_seconds` affiche exactement 5 ms à tous les paliers, y compris
à 320 binds. Ce n'est pas un pair au repos : le simulateur observe la latence **configurée**, pas une
durée mesurée (`internal/smsc/session.go`, `ObserveServedLatency(..., float64(decision.LatencyMS)/1000)`).
Cette métrique ne peut donc **pas** distinguer « le pair sature » de « l'injecteur ne pousse pas »,
contrairement à ce qu'annoncent `D3` et le godoc de `smscmetrics`. Le seul signal de saturation
utilisable est `smsc_submit_sm_outcome_total` (les issues non-`success`, qui disqualifient un palier) et
l'inflexion de la courbe elle-même.

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
