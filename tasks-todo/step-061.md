# step-061 — Opt-out : repos suppressions/keywords + Bloom par portée

> **Jalon :** M5 (§9 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-062, step-063

## But
Poser les fondations de l'opt-out : repos `suppressions`/`opt_out_keywords` et un **filtre de Bloom par portée en mémoire** (rechargé à froid) pour un test d'appartenance rapide.

## Périmètre (ce que fait CETTE PR)
- Créer `internal/pipeline/optout` : repos lecture `control_plane.suppressions` et `control_plane.opt_out_keywords` (sqlc).
- Bloom par portée (`platform`/`customer`/`smpp_account`/`inbound_number`) chargé au démarrage depuis `suppressions` (clé = MSISDN normalisé E.164).
- API interne : `MightBeSuppressed(scope, scopeID, msisdn) bool` (Bloom) + `IsSuppressed(...)` (confirmation exacte en base/cache).

## Points d'implémentation clés
- **`ctx7`** avant d'ajouter `github.com/bits-and-blooms/bloom/v3` (§1.2) : API `NewWithEstimates`, `Add`, `Test`, paramètres taille/taux de faux positifs.
- **Propriété clé (pas de faux négatif)** : si un MSISDN a été ajouté, `Test` renvoie toujours vrai. Un faux **positif** est acceptable (confirmé ensuite en base).
- MSISDN normalisé E.164 (`internal/platform/e164`) à l'écriture comme à la lecture (le schéma normalise déjà).
- Rechargement **à froid** au démarrage (hot reload = M7, Hors périmètre).

## Tests (écrits dans la même PR)
- **Property test** (invariant « pas de faux négatif ») : pour tout MSISDN inséré, `Test` = vrai.
- Intégration PG : chargement des scopes, `IsSuppressed` exact.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] property test Bloom vert

## Hors périmètre
Étape MT bloquante (step-062) ; STOP côté MO (step-063) ; Admin opt-out (step-064) ; hot reload (M7).
