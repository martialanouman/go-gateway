# step-287 — Rejouer la campagne du VPS, injecteur sur une VM séparée

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-282, step-283, step-284, step-285 · **Bloque :** step-409
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

## Definition of Done
- [ ] les quatre runs faits, chacun avec les relevés de step-280, verdict toujours non rendu (→ step-409)
- [ ] la traversée mesurée avec 24 clients, et le goulot suivant nommé
