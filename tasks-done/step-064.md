# step-064 — Admin opt-out : suppressions + opt-out keywords

> **Jalon :** M5 (§9 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-061 · **Bloque :** —

## But
Exposer la gestion opt-out via l'Admin API : suppressions (CRUD + check + import) et mots-clés d'opt-out.

## Périmètre (ce que fait CETTE PR)
- Handlers `adminapi` (opérations déjà déclarées `api/openapi-admin.yaml`) : `list-suppressions`, `create-suppression`, `delete-suppression`, `check-suppression`, `import-suppressions`, `list/create/update/delete-opt-out-keyword`.
- Repos `internal/controlplane` sur `control_plane.suppressions` et `opt_out_keywords` (sqlc), MSISDN normalisé E.164 à l'écriture.
- `import-suppressions` : ingestion en lot (batch), idempotente sur `suppressions_uq`.
- Synchroniser `api/collections/admin-api.yaml`.

## Points d'implémentation clés
- `check-suppression` : réponse binaire par MSISDN + portées matchées (réutilise `internal/pipeline/optout`, step-061).
- Après une écriture Admin, invalider/recharger le snapshot Bloom (à froid ici ; hot reload M7) — au minimum documenter la latence de prise en compte.
- SQL paramétré (sqlc) ; contraintes `NULLS NOT DISTINCT` du schéma respectées (scope `platform`).

## Tests (écrits dans la même PR)
- Intégration PG : CRUD suppression, `check-suppression`, `import-suppressions` (lot + idempotence).
- CRUD opt-out keywords.
- Contrat admin + synchro collection.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] collection admin synchronisée

## Hors périmètre
Étape MT opt-out (step-062) ; STOP MO (step-063) ; anti-spam admin (step-067).
