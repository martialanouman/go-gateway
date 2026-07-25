# step-045 — Résolution MO (dédié / mot-clé / non routé) + list-unrouted-mo

> **Jalon :** M4 (§8 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-040, step-041, step-043 · **Bloque :** step-048

## But
Router un MO vers le bon compte : numéro dédié → son compte ; numéro partagé → mot-clé ; sinon → file « non routés » visible en Admin.

## Périmètre (ce que fait CETTE PR)
- Dans `mo-dlr-router-svc` : consumer `mo.inbound` → normalisation E.164 du numéro entrant → résolution compte.
- Résolution : `inbound_numbers` dédié (`account_id` non nul) → ce compte ; partagé → match `inbound_keywords` (par `priority`, `match_type`) → compte du mot-clé ; sinon → file non routés.
- Persistance des MO non routés (table/queue) + Admin `list-unrouted-mo` (déjà déclaré `api/openapi-admin.yaml`).
- Le MO résolu produit une intention de remise (consommée à step-048) — ici on **résout et marque**, la remise effective est step-048.

## Points d'implémentation clés
- **Invariant (a)** : matching mot-clé lit le corps **en mémoire**, jamais loggé/stocké en clair.
- E.164 via `internal/platform/e164` (déjà en place).
- Un MO non résolu n'est **jamais abandonné silencieusement** (critère d'acceptation) : trace + compteur + visible en Admin.
- Snapshot en mémoire des `inbound_numbers`/`inbound_keywords` (rechargé à froid ; hot reload plus tard) pour tenir le débit.

## Tests (écrits dans la même PR)
- Intégration : MO sur numéro dédié → compte attendu ; sur numéro partagé + mot-clé → compte du mot-clé ; sans correspondance → `list-unrouted-mo`.
- Priorité/`match_type` des mots-clés respectés.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] MO non résolu visible, jamais perdu

## Hors périmètre
Remise effective (SMPP/webhook, step-046/047/048) ; détection STOP + suppression (M5, step-063).
