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
- **Chemin réseau.** Un Service `rest-api-svc-load` de type `NodePort` (port fixe 30880 → 8080, en TLS
  comme le Service interne), posé et retiré par `run.sh`, jamais dans `deploy/test`. Une zone firewalld permanente
  `test-peers` (cible ACCEPT) accepte les hôtes de test sur les deux machines : 75.119.149.218 sur la
  passerelle, 169.58.63.248 sur l'injecteur. C'est une demande de l'utilisateur du 06/10/2026, pour des tests
  futurs ; un autre serveur s'y ajoutera. Il n'y a ni Cloudflare ni
  Traefik : le budget est l'ingestion.
  **Preuve exigée avant le premier run.** Avec k3s, le DNAT de kube-proxy passe avant le filtre d'entrée de
  firewalld, si bien qu'un NodePort peut être joignable sans règle. On vérifie donc deux cas : accepté depuis
  l'injecteur, refusé depuis un tiers. Si un tiers passe, on ne lance pas : on remplace par une règle iptables `raw`/`mangle` sur la
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

## Definition of Done
- [ ] les quatre runs faits, chacun avec les relevés de step-280, verdict toujours non rendu (→ step-409)
- [ ] la traversée mesurée avec 24 clients, et le goulot suivant nommé
