# step-164 — Crypto-shred : destruction de clé + erase-customer-content

> **Jalon :** M10 (§14 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-163 · **Bloque :** —

## But
Rendre le contenu d'un client illisible **sans réécrire le CDR**, en détruisant sa clé de contenu
(crypto-shred), exposé par l'Admin `erase-customer-content`.

## Périmètre (ce que fait CETTE PR)
- `internal/content/` : opération de destruction de DEK (statut `destroyed`, effacement de la matière
  déchiffrable via la KMS).
- `api/openapi-admin.yaml` + `internal/adminapi` : `erase-customer-content` ; collection synchronisée.

## Points d'implémentation clés
- **Crypto-shred sans réécriture CDR** (§14) : marquer la `content_keys` `destroyed` suffit ; les
  `content_ciphertext` restent en place mais deviennent indéchiffrables. Pas de `DELETE`/`UPDATE` massif
  sur le CDR.
- Idempotent : détruire une clé déjà détruite est un no-op.
- Après destruction, `get-message-content` (step-163) renvoie « illisible ».
- Opération auditée (piste d'audit opérateur).

## Tests (écrits dans la même PR)
- Après `erase-customer-content` : contenu illisible, **CDR non réécrit** (lignes toujours présentes).
- Idempotence ; audit de l'effacement.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] crypto-shred prouvé « illisible sans réécriture » par test

## Hors périmètre
Effacement RGPD complet (client/MSISDN + attestation) → step-166. Rétention/tiering → step-165.
