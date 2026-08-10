# step-219 — Métriques agrégées en lecture : le flux pousse, rien ne se lit

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-213 (triage) · **Bloque :** —

## But

Servir les 2 opérations de métriques déclarées au contrat. M11 a livré le **push** (`stream-metrics`,
instantanés agrégés périodiques sur `metrics.stream`) ; le **pull** n'existe pas, donc un tableau de
bord qui s'ouvre n'a rien à afficher avant le premier instantané.

| Opération | Méthode et chemin |
|---|---|
| `get-metrics-summary` | `GET /admin/metrics/summary?window=5m` |
| `get-traffic-metrics` | `GET /admin/metrics/traffic` (séries temporelles par dimension) |

## Le constat

`stream-metrics` (step-182/183) émet des instantanés agrégés, et `docs/specification-technique-tableau-de-bord.md`
décrit un tableau de bord de trafic temps réel ventilé par connecteur, client, compte et groupe. Sans
lecture initiale, le flux ne suffit pas : il donne le mouvement, pas l'état.

## Points d'implémentation clés

- **La source est ClickHouse, pas Prometheus.** Le contrat n'offre que trois dimensions —
  `groupBy: [connector, customer, group]`, **pas** `account`, alors que la spec du tableau de bord en
  cite quatre : c'est le contrat qui est en retrait, et c'est lui qu'il faut servir (l'élargir est une
  décision à part, avec son bump). Ces ventilations sont exactement celles que le catalogue de métriques
  **interdit** en labels — la
  garde de cardinalité de step-180 refuse un label non borné à l'enregistrement. Aller chercher ces
  séries dans Prometheus serait contourner cette garde par la porte de service.
- **Le groupe se résout à la lecture** (§6.17) : le CDR ne porte pas `group_id`, une ventilation par
  groupe est un `customer_id IN (...)` calculé au moment de la requête. Dénormaliser le groupe dans le
  CDR pour simplifier la requête casserait l'exactitude dès qu'un client change de groupe.
- **Le CDR est un `ReplacingMergeTree` versionné.** Toute agrégation doit résoudre sur `max(version)`
  (`argMax`), sinon elle compte l'état `accepted` d'un message déjà `delivered` — le piège relevé au
  cadrage du reaper (step-190) et dans `ByMessageID`.
- **Une fenêtre de lecture n'est pas gratuite.** `window` est un paramètre client : borner les valeurs
  acceptées et la granularité, et poser une limite de lignes, sinon un `window` large sur un cluster
  chargé devient un déni de service par requête admin.
- **Le vocabulaire doit coïncider avec celui du flux.** Deux définitions du « taux de succès » — une
  dans l'agrégat, une dans l'instantané poussé — donneraient deux chiffres différents sur le même écran.
  Reprendre les définitions de `internal/metricstream`, ne pas en réécrire.

## Tests

- Les compteurs agrégés se recoupent avec les lignes CDR insérées : une fixture de N messages dont M
  livrés donne exactement M, et la mutation d'un statut change le résultat.
- Un message présent en deux versions (`accepted` puis `delivered`) est compté **une fois**, dans son
  état final — le test qui n'insère qu'une version passerait sous une requête fausse.
- La ventilation par groupe suit l'appartenance courante : déplacer un client d'un groupe à l'autre
  change la ventilation sans réécrire de CDR.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] les 2 opérations servies ; agrégations résolues sur `argMax(version)` ; fenêtre et lignes bornées
- [ ] définitions identiques à celles du flux temps réel
- [ ] `api/collections/admin-api.yaml` synchronisée ; lignes retirées de `deferred` (step-213)

## Hors périmètre

Les flux temps réel (livrés en M11). Les dashboards Grafana et les règles d'alerte, hors dépôt.
