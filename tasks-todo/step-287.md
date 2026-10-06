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

## Definition of Done
- [ ] les quatre runs faits, chacun avec les relevés de step-280, verdict toujours non rendu (→ step-409)
- [ ] la traversée mesurée avec 24 clients, et le goulot suivant nommé
