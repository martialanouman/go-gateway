# step-287 — Rejouer la campagne du VPS, injecteur sur une VM séparée

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-282, step-283, step-284, step-285, step-285b, step-285c · **Bloque :** step-409
> Suggestion humaine du 29/09/2026 ; unité faute de multiple de dix libre.

## Pourquoi
step-280 n'a rien pu mesurer de la traversée : les trois goulots qu'elle a nommés (partition par compte,
verrou de solde, routeur en boucle) sont levés par step-282, 284 et 285. Et son ingestion était bornée
par l'hôte : k6 prenait 1,8 cœur des 8 vCPU. Sortir l'injecteur sur une autre VM rend ces cœurs à la
passerelle — sans rendre l'environnement représentatif (ça reste step-409).

## Périmètre
- k6 lancé depuis une VM séparée : visant `rest-api-svc` directement (pas Cloudflare, cf. design de
  step-280), donc un chemin réseau privé ou un port exposé au seul IP de l'injecteur — à choisir, sans
  ouvrir l'API interne à Internet.
- Les quatre runs de step-280 (`sustained`/`peak` × `IDEMPOTENCY`), `e2e-budget`, ratios L0.
- Le simulateur avec télémétrie des fermetures, s'il est publié (journal de step-280).

## Design arrêté (06/10/2026)
Ordre confirmé par l'utilisateur le 06/10/2026 : step-287 passe avant step-292b, et mesure donc l'ancienne
topologie (`mt.inbound`/`mt.routed`) qui sert de référence à ADR-0021.

- **Hôtes.** Passerelle : `contabo169` (169.58.63.248, k3s, 8 vCPU). Injecteur : `contabo75`
  (75.119.149.218, Rocky 10.2, 8 vCPU, 23 Go), qui n'a rien d'autre à faire. RTT mesuré ~5 ms.
- **Chemin réseau.** Un Service `rest-api-svc-load` de type `LoadBalancer` (servicelb, port 30880 → 8080,
  en TLS comme le Service interne, `loadBalancerSourceRanges` limité aux hôtes de test,
  `allocateLoadBalancerNodePorts: false`), posé et retiré par `run.sh`, jamais dans `deploy/test`. Une zone firewalld permanente
  `test-peers` (cible ACCEPT) accepte les hôtes de test sur les deux machines : 75.119.149.218 sur la
  passerelle, 169.58.63.248 sur l'injecteur. C'est une demande de l'utilisateur du 06/10/2026, pour des tests
  futurs ; un autre serveur s'y ajoutera. Il n'y a ni Cloudflare ni
  Traefik : le budget est l'ingestion.
  **Preuve exigée avant le premier run.** Avec k3s, le DNAT de kube-proxy passe avant le filtre d'entrée de
  firewalld, si bien qu'un NodePort peut être joignable sans règle. On vérifie donc deux cas : accepté depuis
  l'injecteur, refusé depuis un tiers.
  **Vérifié le 06/10/2026, et c'est arrivé.** En `NodePort`, le poste de l'exploitant a reçu un 401 malgré la
  zone. On est donc passé en `LoadBalancer` filtré par source : l'injecteur reçoit un 401 (pas de clé), le
  poste tombe en timeout. La zone `test-peers` reste utile aux autres ports, mais elle ne protège pas un
  Service k3s. Si un tiers passe, on ne lance pas : on remplace par une règle iptables `raw`/`mangle` sur la
  source, ou par le réseau privé Contabo.
- **k6 sur l'injecteur.** Binaire k6 1.3.0, la version de l'image du Job, téléchargé depuis la release
  GitHub et vérifié par somme SHA-256. Le script et l'environnement sont identiques au Job `k6-load` :
  même `messages.js`, `K6_INSECURE_SKIP_TLS_VERIFY`, `SENDER_ID=TEST`. Les clés de `seed-load` sont lues
  dans le Secret `k6-load` et écrites dans `/root/k6.env` (0600) par un tube ssh, jamais en ligne de
  commande.
- **`run.sh`** gagne `expose`, `unexpose` et `k6-remote INJECTEUR PROFILE IDEMPOTENCY DURATION`. Il ne
  touche ni au Job `k6` existant ni aux leviers `apply`.
- **Gel.** Aucun merge sur `main` pendant la campagne, car le CD écraserait les leviers. On relève l'image
  déployée en tête de chaque run.
- **Relevés.** Ceux de step-280 pour chaque run : req/s tenues, 202, p99, itérations abandonnées,
  `vmstat`, CPU par pod, `observe`. On y ajoute `vmstat` côté injecteur, qui prouve que k6 n'est plus le
  goulot.

## Journal de la campagne (06-07/10/2026, VPS de test 8 vCPU, injecteur `contabo75` 8 vCPU)

**Aucun verdict n'est rendu ici (→ step-409).**

**Run 1a, `sustained` sans idempotence, image `84b18e7` : perdu à 6 min 53 s.** Une coupure ssh du poste a
tué k6 et les relevés distants. `k6-remote` tourne depuis sous `systemd-run` sur l'injecteur. Le backlog
laissé par ce run (environ 450 000 messages au routeur et 355 000 au pool) a été vidé par l'utilisateur
(seek des groupes à la fin).

**Ce que l'écoulement de ce backlog a montré.** Sans aucune ingestion, l'hôte restait à 96 % de CPU, et
ClickHouse prenait 1,9 cœur, puis 4,6 cœurs à vide. On a trouvé deux causes, payées par **step-287b**
(#278) : les CDR des DLR écrits un accusé à la fois, et des journaux système en `Trace` (33 Go). Après
step-287b, ClickHouse est à 0,2 cœur au repos et l'hôte à 93-94 % d'idle.

**Run 1b, `sustained` sans idempotence, 10 min, image `032721a` (avec step-287b) :**

| Mesure | Valeur |
|---|---:|
| req/s tenues (8 000 visées) | **2 626** |
| 202 | 100 % (1 581 291) |
| p99 / médiane | 3,47 s / 1,47 s |
| itérations abandonnées | 5 346/s (4 000 VUs, le maximum) |
| CPU de l'injecteur | 79 % idle : **l'injecteur n'est plus le goulot** |
| CPU de la passerelle (vmstat, moyenne) | 58 % us, **37 % sy**, 5 % idle |
| `submit_sm` du pool | 79 650 en 600 s, soit **~133/s**, à 0,13 cœur par pod |
| inserts `cdr` | **126 lignes en moyenne** (contre 1 avant step-287b), ~99 ms chacun |

CPU moyen par pod : Postgres 1,9 cœur (l'authentification REST fait une requête Postgres par appel, piège
(i) de step-280), ClickHouse 1,3, `rest-api-svc` 2 × 0,7, billing-svc 0,4, Redpanda 0,36, Redis 0,35,
routeur 2 × 0,23.

**Trouvé en le faisant tourner.** billing-svc lit le CDR en boucle : 1 307 requêtes `ByMessageID` en
20 min, à environ 400 ms chacune, soit le reaper de facturation qui interroge `message_id` hors de la clé de
tri. Une partie de ce volume vient des réservations orphelines laissées par le seek du backlog.

**Ce que le run ne dit pas encore.** Le pool envoie 133 `submit_sm`/s alors qu'il est presque oisif : il
attend quelque chose. Comme l'hôte est saturé (37 % de temps système), il est impossible de dire si
cette attente vient de la contention ou d'une étape sérielle par message. Les trois autres runs
mesureraient la même saturation.

**Ce que le chronomètre de step-287c a montré (07/10/2026, `8a2cc4b`, écoulement sans ingestion).** Sur environ
200 ms par message dans le pool, **172,7 ms** partaient dans la capture de facturation, contre 7,7 ms pour le
`submit_sm`. Environ 80 % des captures expiraient à 200 ms (fail-open). La capture a quitté le chemin chaud
avec **step-287d** (#280, #281, ADR-0024).

**Run 2, `sustained` sans idempotence, 10 min, image `1df24e2` (pool sans capture) :**
- 2 395 req/s, p99 2,54 s, 0 erreur ;
- le pool envoie **~275 `submit_sm`/s**, en 67 ms par message, avec un lag de 245 : il n'est plus le goulot ;
- le routeur passe 226 ms par message, et son lag atteint 1,26 M ;
- le consommateur `billing-svc-settle` règle environ 85 messages/s.

**Run 3, mêmes conditions, image `600d5c0` (chronomètres de step-287e) : le goulot suivant est nommé.**

| Étape | Moyenne |
|---|---:|
| `credit` (réservation, au routeur) | 226,6 ms |
| dont écriture durable (billing-svc) | 184,2 ms |
| — `handoff` (attente de l'écrivain unique du `BillingBatcher`) | 67 ms |
| — `reply` (écriture de son lot) | 110 ms |
| par lot : `begin` 13 + `claim` 17 + `copy` 43 + `commit` 13 | 86 ms |
| toutes les autres étapes du pipeline | < 4 ms |

**Le goulot : l'écrivain unique du `BillingBatcher`.** Il fait 11 lots/s de ~35 écritures, avec quatre
allers-retours Postgres en série par lot, et il est occupé environ 95 % du temps : il plafonne vers
400 écritures/s. Les réservations (270/s) partagent sa file avec les captures du règlement (~100/s, 310 ms
chacune). Le pool envoie 269 `submit_sm`/s sans peine. Le correctif est porté par **step-287f**.

**Trouvé en le faisant tourner.** Les DLR des 24 clients de charge, sans bind ni webhook, partaient en lettre
morte un par un, à ~18/s. Ils ont accumulé 1,1 M de retard et fait échouer deux fois le smoke du déploiement :
`debts/dlr-d-un-client-injoignable-retarde-tous-les-autres.md`. Le groupe a été ramené à la fin par
l'utilisateur.

**Run 4, mêmes conditions, image `0397227` (4 écrivains, step-287f PR1) : l'hôte est saturé.**
- 1 382 req/s, p99 4,43 s ; le pool envoie **348 `submit_sm`/s** (269 au run 3) ;
- `handoff` passe de 67 à 19 ms, un lot de 86 à 62 ms, `credit` de 226,6 à 158,6 ms, 488 écritures durables/s ;
- l'admission HTTP recule pourtant : l'hôte n'a que 6,6 % de CPU libre, et ce que la facturation et le routeur
  gagnent, le service REST le perd (1 366 → 950m). ClickHouse 1,6 cœur, Postgres 1,3. Les dépendances
  passent sur un second nœud : **step-287g**.

**Run 5, mêmes conditions, image `c3ee79a` (dépendances sur contabo75 par le VPC, step-287g) : la latence
de l'écriture durable borne le routeur.**
- 1 971 req/s (+43 %), p99 4,97 s, 22 erreurs sur 1,2 M ; **404 `submit_sm`/s** (+16 %) ;
- CPU libre : passerelle 38 %, contabo75 (dépendances et k6) 26 % ;
- `credit` 154 ms, dont 137 ms d'écriture durable. Un lot coûte 83 ms (`begin` 28, `claim` 11,5, `copy` 25,
  `commit` 18), contre 62 ms au run 4 : chaque aller-retour paie le VPC. Le routeur a un nombre fixe de
  messages en vol, donc son débit est ce nombre divisé par la latence : le CPU libéré ne change rien. Lag du
  routeur 765 000 en fin de run. Le levier suivant est le lot en un seul statement (step-287f PR2).

**Run 6, mêmes conditions, image `362f850` (lot en un seul statement, step-287f PR2) : le lot coûte 64 ms.**
- 1 913 req/s, p99 4,15 s, 0 erreur sur 1,17 M ; **480 `submit_sm`/s** (+19 %) ;
- `credit` 118,6 ms, dont 98,8 ms d'écriture durable (137 au run 5) ; un lot : `begin` 26,7, `write` 18,8,
  `commit` 19,0 ms ; 616 écritures durables/s en lots de 10,4 ;
- CPU libre : passerelle 33 %, contabo75 23 % ; Postgres 2 cœurs, ClickHouse 1,9 ;
- `begin` est devenu la plus grosse étape d'un lot alors qu'un BEGIN fait un aller-retour (~1 ms sur le VPC) :
  l'attente d'une connexion du pool (10) n'est pas séparée du BEGIN. Lag du routeur 678 000 en fin de run.

## Definition of Done
- [ ] les quatre runs faits, chacun avec les relevés de step-280, verdict toujours non rendu (→ step-409)
- [ ] la traversée mesurée avec 24 clients, et le goulot suivant nommé
