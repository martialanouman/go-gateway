# step-187 — Export de messages asynchrone (row-cap, MSISDN masqué)

> **Jalon :** M11 (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-186 · **Bloque :** —

## But
Produire un export de messages asynchrone, plafonné en lignes (row-cap) et **masqué** (MSISDN selon
le rôle, aucun corps), via `create-message-export` / `get-message-export`.

## Périmètre (ce que fait CETTE PR)
- `api/openapi-admin.yaml` + `internal/adminapi` : `create-message-export` (async, renvoie un job) et
  `get-message-export` (statut + lien/fichier).
- Worker d'export : lecture CDR bornée, application du masquage, écriture d'un fichier.
- Collection Admin synchronisée.

## Points d'implémentation clés
- **Asynchrone + row-cap** (§15) : un export ne balaie jamais l'historique sans borne ; job avec statut.
- **Masque** : MSISDN masqué par rôle (règle partagée step-186) ; **aucun corps** dans l'export (invariant a).
- Le fichier produit vit hors chemin chaud ; la destination objet réelle reste infra (comme le tiering).
- Job traçable (`get-message-export`) : statut, compteur de lignes, échéance.

## Tests (écrits dans la même PR)
- `create-message-export` → job ; `get-message-export` → fichier masqué produit.
- Row-cap respecté ; MSISDN masqué ; aucun corps dans le fichier.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (a)** respecté (fichier sans corps)
- [ ] export async + row-cap + masquage testés ; collection synchronisée

## Hors périmètre
Fin de M11. Durcissement/charge/prod → M12.
