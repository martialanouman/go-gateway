# step-162 — Chiffrement du contenu à l'écriture CDR + politique content_storage

> **Jalon :** M10 (§14 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-161 · **Bloque :** step-163

## But
Écrire le contenu du message dans le CDR selon la politique du client (`off`/`stored_plaintext`/
`stored_encrypted`), en chiffrant **à l'écriture CDR uniquement** avec la DEK par client, et
re-vérifier que le corps ne fuit jamais ailleurs (invariant a) sous **chaque** valeur de `content_storage`.

## Périmètre (ce que fait CETTE PR)
- `internal/content/` : résolution de la politique effective (`inherit` → défaut client, §5 du schéma).
- Point d'écriture CDR (`internal/storage/clickhouse/cdr.go`) : remplir `content_ciphertext` +
  `content_key_id` quand `stored_encrypted` ; texte clair quand `stored_plaintext` ; rien quand `off`.
- Chiffrement AES-GCM avec la DEK récupérée via `billing-svc` (step-161).

## Points d'implémentation clés
- **Chiffrement à l'écriture CDR UNIQUEMENT** (§14) : jamais sur le chemin d'ingestion ni dans Kafka
  (le corps voyage déjà masqué via le type `Body`).
- **Invariant (a) re-vérifié sous CHAQUE `content_storage`** : `off`, `stored_plaintext`,
  `stored_encrypted` — dans aucun cas le corps n'apparaît en log/span/label. `content_ciphertext` n'est
  jamais loggué (commentaire du schéma : « NEVER in logs »).
- `stored_plaintext` reste soumis à l'invariant (a) : stocké dans la colonne CDR dédiée, jamais ailleurs.
- Le CDR est versionné (§1.10) : le contenu accompagne la première ligne pertinente, immuable ensuite.

## Tests (écrits dans la même PR)
- Chaque mode : `off` → colonne vide ; `plaintext` → clair en colonne ; `encrypted` → chiffré + key_id.
- **Invariant (a)** : test de non-fuite du corps rejoué sous les trois modes (test bloquant).
- Round-trip : `content_ciphertext` déchiffré par la DEK rend l'original.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (a)** re-testé par mode de stockage
- [ ] chiffrement au seul point d'écriture CDR

## Hors périmètre
Lecture gardée/auditée → step-163. Crypto-shred → step-164.
