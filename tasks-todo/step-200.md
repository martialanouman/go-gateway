# step-200 — Harnais de charge k6/vegeta + générateur de binds SMPP (NFR)

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-201

## But
Livrer la campagne de charge : scripts k6/vegeta (REST) + générateur de binds SMPP, ciblant les NFR —
8 000 SMS/s soutenu, 15 000 en pic — avec les budgets de latence (ingestion p99 < 250 ms, bout-en-bout
p99 < 2 s, disjoncteur fermé).

## Périmètre (ce que fait CETTE PR)
- `deploy/load/` (ou `test/load/`) : scripts **k6** ou **vegeta** pour `POST /messages` + générateur de
  binds SMPP concurrents (réutilise `internal/smpp`).
- Profils : soutenu 8000/s, pic 15000/s ; seuils de latence encodés dans les scripts.
- Documentation courte de lancement (make cible `make load`).

## Points d'implémentation clés
- **k6/vegeta sont des binaires hors `go.mod`** (§1.3/§16) — installés à part, pas une dépendance Go.
  **`ctx7`** pour la syntaxe des scripts k6 (thresholds, scenarios) / l'usage de vegeta.
- Le générateur de binds SMPP est du Go (client `internal/smpp`), pas un binaire externe.
- Les seuils encodent les NFR : le run échoue si p99 dépasse le budget (§16 critère).
- Ne pas polluer le chemin de prod : le harnais vit sous `deploy/`/`test/`, pas `internal/`.

## Tests (écrits dans la même PR)
- Un run local court (débit réduit) passe les seuils encodés — preuve que le harnais mesure bien.
- Le générateur de binds établit N binds concurrents (test unitaire du générateur).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] NFR encodés en seuils ; k6/vegeta hors `go.mod`

## Hors périmètre
Tuning (partitions/batch/pool) → step-201. Chaos → step-202/203.
