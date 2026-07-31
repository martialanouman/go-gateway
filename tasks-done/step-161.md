# step-161 — content_keys : cycle de vie de clé par client (hébergé par billing-svc)

> **Jalon :** M10 (§14 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-160, step-144 · **Bloque :** step-162, step-163, step-164

## But
Gérer une clé de contenu par client (`content_keys`) : création (DEK scellée par la KMS), une seule
active, rotation, destruction — hébergée par `billing-svc`, exposée en gRPC et via Admin
`rotate-content-key`.

## Périmètre (ce que fait CETTE PR)
- `internal/storage/postgres/content_keys.go` (+ sqlc) : CRUD `control_plane.content_keys`
  (`kms_key_ref`, `status` active/destroyed, `content_keys_one_active_idx`).
- `billing-svc` : méthodes gRPC `GetOrCreateContentKey`, `RotateContentKey`, `DestroyContentKey`
  (proto étendu — **`ctx7`** avant régénération).
- `api/openapi-admin.yaml` + `internal/adminapi` : `rotate-content-key` ; collection synchronisée.

## Points d'implémentation clés
- `content_keys` déjà en base (`migrations/0001_init`, §5 du schéma) — pas de migration nouvelle.
- **Une seule clé active par client** (`content_keys_one_active_idx`) : la rotation crée une nouvelle
  active et retrograde l'ancienne (les anciens CDR restent déchiffrables tant que la clé n'est pas
  `destroyed`).
- Hébergement par `billing-svc` (§14 : « `content_keys` hébergé par `billing-svc` ») → cohérence avec le
  périmètre de propriété billing.
- La clé maître ne quitte jamais la KMS ; seule la DEK scellée est stockée (`kms_key_ref`).

## Tests (écrits dans la même PR)
- Création → une active ; rotation → nouvelle active, ancienne conservée déchiffrable.
- Contrainte « une seule active » respectée ; `rotate-content-key` via Admin.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] invariant « une clé active » testé ; collection synchronisée

## Hors périmètre
Chiffrement à l'écriture CDR → step-162. Crypto-shred (destroy) → step-164.
