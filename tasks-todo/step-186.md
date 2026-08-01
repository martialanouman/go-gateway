# step-186 — search-messages avec masquage MSISDN par rôle

> **Jalon :** M11 (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-185 · **Bloque :** step-187

## But
Offrir aux opérateurs la recherche de messages (par plage de dates, client, connecteur, statut…) avec
**masquage du MSISDN selon le rôle** et sans jamais exposer de corps.

## Périmètre (ce que fait CETTE PR)
- `api/openapi-admin.yaml` + `internal/adminapi` : `search-messages` (filtres bornés, pagination).
- Lecture CDR (ClickHouse) par dernière version ; masquage MSISDN conditionné au rôle/scope.
- Collection Admin synchronisée.

## Points d'implémentation clés
- **Masquage MSISDN par rôle** (§15) : un rôle sans droit de dévoilement voit le MSISDN masqué ; la règle
  de masquage est partagée avec `get-message-trace` (step-185) et l'export (step-187).
- Aucun corps dans les résultats (invariant a).
- Requêtes bornées (row-cap, plage de dates obligatoire) pour ne pas balayer tout l'historique.
- **`ctx7`** avant toute API `clickhouse-go/v2` de requête paginée.

## Tests (écrits dans la même PR)
- Recherche filtrée renvoie les bons messages, paginés.
- MSISDN masqué pour un rôle restreint, dévoilé pour un rôle habilité.
- Aucun corps dans les résultats.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (a)** respecté
- [ ] masquage MSISDN par rôle testé ; collection synchronisée

## Hors périmètre
Export asynchrone → step-187.

---

## Design arrêté (2026-08-01)

Cinq points que la fiche laissait ouverts, tranchés avant écriture du code.

### D1 — `groupId` : résolu à la lecture, pas dénormalisé
Le CDR n'a pas de colonne `group_id` ; elle vit sur `control_plane.customers`. Le handler résout le
groupe en liste de `customer_id` (`cp.CustomerFilter.GroupID`, déjà là) et filtre le CDR par
`customer_id IN (...)`, plafonné à **500 clients** (422 au-delà).

**Pourquoi pas une colonne dans le CDR :** le groupe d'un client est MUTABLE. Figé à l'envoi, il
répondrait « les messages envoyés quand le client était dans ce groupe » ; résolu à la lecture, il
répond « les messages des clients qui sont aujourd'hui dans ce groupe » — ce qu'un opérateur attend
d'un axe de segmentation organisationnelle. Et cela évite une migration ClickHouse plus une
modification de tous les writers.

Tous les prédicats se combinent en ET : `groupId` **et** `customerId` ensemble donnent
l'intersection (un client hors du groupe rend donc la page vide), jamais l'union — un filtre
supplémentaire ne doit jamais élargir le résultat.

### D2 — Filtre `msisdn` : ouvert à tous, sortie masquée, correspondance EXACTE
Le filtre reste accessible sans `msisdn:reveal` : le parcours support principal part d'un numéro
fourni par le plaignant, et le gater rendrait ce scope obligatoire pour l'usage le plus courant.

**L'oracle est assumé, pas ignoré :** un appelant masqué peut confirmer qu'un numéro qu'il connaît
déjà a du trafic — un bit par requête. Ce qui est fermé, c'est l'énumération : la correspondance est
une **égalité stricte** sur l'E.164 normalisé (`source_addr = ? OR dest_addr = ?`), jamais un
préfixe ni un `LIKE`. Un msisdn non normalisable est un 422, pas une recherche littérale.

### D3 — Plage de dates obligatoire, fenêtre ≤ 31 jours
`from_date` et `to_date` deviennent **requis** : c'est ce qui laisse `PARTITION BY
toDate(submitted_at)` élaguer, une recherche transverse n'ayant pas le préfixe
`(customer_id, account_id)` de la clé de tri. Fenêtre plafonnée à **31 jours** sur 90 de rétention.

Rupture de contrat assumée (`oasdiff` classera `required` en `ERR` → bump **majeur** de
`api/package.json`) : le tableau de bord n'est pas encore construit, le coût réel est nul. Une
fenêtre par défaut silencieuse aurait été pire — l'opérateur croirait chercher tout l'historique.

### D4 — Le curseur keyset remonte dans `internal/storage/clickhouse`
`encodeCursor`/`decodeCursor` quittent `internal/restapi/messages_list.go` pour devenir
`clickhouse.EncodeCDRCursor`/`DecodeCDRCursor`, à côté de `CDRKey` dont ils sont la sérialisation
(2 appels changés côté REST, aucun test touché). Deux copies d'un encodage où la précision
milliseconde est load-bearing divergeraient en silence, et le symptôme — une page perdue quand
plusieurs messages partagent une milliseconde — ne se voit qu'en charge.

### D5 — Garde-fous en constantes, pas en configuration
31 jours et 500 clients sont des garde-fous, pas des réglages produit. En configuration, ils
seraient désactivés le jour où une recherche renvoie 422. À desserrer par une PR si un opérateur y
bute réellement.

## Découpage

**Contrat** — `/admin/messages/search` : `from_date`/`to_date` requis, `msisdn` documenté E.164
exact, réponses `200 MessageSummaryPage` + `422`, sécurité `OperatorBearer [admin:read]`. Suivent :
bump majeur d'`api/package.json`, `api/collections/admin-api.yaml`, entrée dans `m1Operations`
(`contract_test.go`) — le test compare les codes de réponse générés au contrat, à l'identique.

**ClickHouse** — `CDRSearchFilter` + `(*CDRReader).Search`. Troisième variante d'agrégat à côté de
`cdrAggregate` et `cdrAggregateByMessage`, dont le `WHERE` interne s'ancre sur la plage de dates.
Keyset en millisecondes entières (`toUnixTimestamp64Milli`), pas sur le `DateTime64` brut : le piège
de step-029, qui perd une page entière dès que plusieurs messages partagent une milliseconde.

**Handler** — `internal/adminapi/messages_search.go` : `SearchStore` déclarée côté consommateur,
`CustomerLister` pour le groupe, quatre 422 nommant leur champ (fenêtre, msisdn, curseur, groupe),
masquage par `maskAddresses` + `mayRevealMSISDN` — la règle de step-185, pas une seconde.

**Tests** — unitaires sur fakes (chaque filtre atteint le magasin, les quatre 422, masqué sans le
scope / révélé avec, aucun champ de contenu dans le DTO, `next_cursor`/`has_more`) ; intégration
`chtest` (deux clients, isolation, et plusieurs messages sur la même milliseconde pour épingler le
keyset) ; chaque assertion clé validée par mutation.
