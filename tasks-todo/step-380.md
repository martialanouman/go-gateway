# step-380 — Métriques agrégées en lecture : le flux pousse, rien ne se lit

> **Jalon :** Surfaces Admin déclarées au contrat, jamais construites (`docs/specification-technique-tableau-de-bord.md`) · **Statut :** À FAIRE
> **Dépend de :** step-320 (triage), step-330 (la ventilation par groupe) · **Bloque :** —

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
- [ ] `api/collections/admin-api.yaml` synchronisée ; lignes retirées de `deferred` (step-320)

## Hors périmètre

Les flux temps réel (livrés en M11). Les dashboards Grafana et les règles d'alerte, hors dépôt.

## Design arrêté

Arbitrages : Fable (11 points), humain (24h refusé ; flux DLR en dette). Contrat 6.4.0 → **6.5.0** (mineur).

**Fenêtre = cohorte.** Un message compte s'il a été soumis dans `[now−window, now)` (`submitted_at`, seule clé
partitionnée et immuable), avec son statut agrégé **à l'instant de la lecture** : sur 5m, `delivered`
sous-compte ce qui est encore en vol — écrit dans la description.

**Fenêtres servies : `5m` et `1h`, pour les deux opérations ; toute autre valeur → 422** à l'exécution. Le
schéma reste `string` (un `enum` sur une opération déjà publiée = rupture oasdiff = majeur). Pas du
traffic fixe : 5m → 10 s (30 points), 1h → 1 min (60 points). `24h` refusé : un scan brut à 8 000/s
lit ~690 M messages ; la spec §6.3 le veut pré-agrégé, et ce pré-agrégat n'existe pas → fiche de dette.

**Agrégation dédiée, légère.** Pas `cdrAggregateSearch` : son niveau interne `argMax` toutes les colonnes,
corps chiffré compris. Trois niveaux sur les seules colonnes utiles (`status`, `latency_ms`,
`connector_id`, `delivered_at`). La précédence de statut §6.6 est **extraite** de `cdrAggOuterCols` en
une constante partagée : une seule définition du statut agrégé dans le dépôt, pour l'explorateur CDR
et pour les métriques. `max_execution_time` posé sur la requête ; dépassement (ou deadline) →
`ErrServiceUnavailable` → 503.

**Définitions.** `submitted` = MT, tous statuts (un rejet est une soumission reçue) ; `delivered` =
statut agrégé `delivered` ; `failed` = `failed` + `expired` ; `rejected` = `rejected` ; `cancelled`,
`enroute`, `accepted` ne comptent que dans `submitted` ; `mo_received` = MO. Invariant :
`delivered + failed + rejected ≤ submitted`. Correspondance avec le flux, par cohérence et non par
identité (le flux n'émet rien des DLR) : `rejected` ≡ `messages_total{status=rejected}` du router (même
événement, même ligne CDR) ; `delivered`/`failed` n'ont qu'une définition, celle partagée ci-dessus.

**Latences.** `e2e_latency_ms_p50/p99` = `quantilesTDigest` de `latency_ms` (DLR − soumission) des MT
livrés, par message et non par segment. `ingest_latency_ms_*` = `null` : mesurée nulle part (fiche
existante `budget-d-ingestion-non-mesurable.md`, étendue). `active_sessions` **omis** : le registre n'a pas
de compteur global exact (`sess:idx` surcompte), et marcher l'index à chaque GET est un DoS → fiche.

**Ventilation.** ClickHouse agrège par `(bucket, clé)` ; Go replie. `connector` : un message jamais
dispatché n'a pas de connecteur → exclu (`Σ series ≤ summary`, écrit). `customer` : l'id. `group` :
ClickHouse par client, Go replie par l'appartenance **courante** (pagination de `CustomerStore.List`),
clients sans groupe → clé `ungrouped`. Puis **top 20 séries par `submitted` + série `other`** (le reste
sommé) : la somme des séries est conservée sans champ neuf. Garde de lignes `LIMIT 100 001` → au-delà, 503
plutôt qu'un résultat tronqué en silence.

**Contrat.** `security: admin:read` (les deux opérations sont aujourd'hui **publiques**), 401/403/422/503,
descriptions (fenêtres, cohorte, `failed`, clés réservées `ungrouped`/`other`). Collection synchronisée ;
lignes retirées de `deferred`.

**Dettes** (même PR) : pré-agrégat 24 h ; `active_sessions` sans compteur ; le flux n'émet pas de
`dlr_total` ; extension de `budget-d-ingestion-non-mesurable.md`.

**Plan.** U1 contrat + collection · U2 `clickhouse` : précédence extraite, `CDRReader.MetricsSummary` /
`TrafficMetrics` (intégration : N messages dont M livrés ; deux versions comptées une fois ; mutation de
statut) · U3 `adminapi` : handlers, fenêtres, repli groupe/top-20, `Deps.Metrics` câblé · U4 dettes · fiche →
`tasks-done`.
