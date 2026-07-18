# step-160 — Interface KMS + implémentation locale de dev (enveloppe)

> **Jalon :** M10 (§14 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-161, step-162

## But
Poser la primitive de chiffrement enveloppe : une interface `KMS` (envelopper/désenvelopper une clé
de données) et une implémentation locale de dev, de sorte que le fournisseur réel (AWS/GCP/Vault)
soit interchangeable et localisé derrière l'interface.

## Périmètre (ce que fait CETTE PR)
- `internal/content/kms.go` : interface `KMS` (`WrapDataKey`/`UnwrapDataKey`, ou `Encrypt`/`Decrypt`
  d'une DEK) + type d'enveloppe.
- `internal/content/kms_local.go` : implémentation locale de dev (clé maître en mémoire/fichier de dev),
  suffisante pour les tests et le laptop.
- Génération de DEK par client (AES-256) + scellement de la DEK par la KMS (enveloppe).

## Points d'implémentation clés
- **Fournisseur KMS réel hors périmètre** (§14) : décision d'infra. L'interface le rend interchangeable ;
  n'ajouter **aucun** SDK cloud dans cette PR.
- Crypto stdlib (`crypto/aes`, `crypto/cipher` AES-GCM) ; **`ctx7`** avant tout usage d'une lib crypto
  externe. Ne pas rouler sa propre primitive.
- Comparaisons/erreurs sans jamais logguer clé ni clair (prépare l'invariant a).
- L'impl de dev ne doit rien ajouter au chemin de prod (§14 : « L'impl de dev n'ajoute rien »).

## Tests (écrits dans la même PR)
- Round-trip : chiffrer puis déchiffrer un blob rend l'original.
- DEK scellée par la KMS locale se désenveloppe correctement ; une clé altérée échoue proprement.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] aucun SDK cloud ajouté ; interface localisée

## Hors périmètre
Cycle de vie des `content_keys` en base → step-161. Chiffrement au CDR → step-162.
