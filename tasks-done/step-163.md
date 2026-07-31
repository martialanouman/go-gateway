# step-163 — Lecture de contenu gardée et auditée (get-message-content)

> **Jalon :** M10 (§14 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-162 · **Bloque :** step-164

## But
Exposer le contenu chiffré en lecture **uniquement** via le scope `content:read`, avec **audit** de
chaque accès, et garantir qu'une clé détruite rend le contenu illisible (préparation crypto-shred).

## Périmètre (ce que fait CETTE PR)
- `api/openapi-admin.yaml` + `internal/adminapi` : `get-message-content` (déchiffre via la DEK) gardé par
  le scope `content:read`.
- Table `control_plane.content_access_audit` : **édition de `db/schema_passerelle_sms.sql` + nouvelle
  migration `golang-migrate`** (recette CLAUDE.md « Changer le schéma »).
- Écriture d'une ligne d'audit à chaque accès réussi/refusé (opérateur, message_id, horodatage).

## Points d'implémentation clés
- Accès **audité** : chaque `content:read` laisse une trace (§14 : « lecture `content:read` gardée et
  auditée »). Ne jamais logguer le clair déchiffré — seulement le fait de l'accès.
- Clé `destroyed` → déchiffrement impossible → réponse « illisible » sans erreur serveur brute
  (prépare step-164 crypto-shred).
- Le clair renvoyé au client `content:read` transite en réponse HTTP, jamais en log/span (invariant a).
- Scope `content:read` ajouté au vocabulaire des scopes (`internal/auth`).

## Tests (écrits dans la même PR)
- Sans `content:read` → refusé ; avec → clair + ligne d'audit écrite.
- Clé détruite → illisible.
- Nouvelle migration up/down testée ; schéma et migration en accord.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (a)** respecté (clair jamais loggué)
- [ ] audit écrit à chaque accès ; migration cohérente avec `db/schema_passerelle_sms.sql`

## Hors périmètre
Destruction de clé (crypto-shred) et `erase-customer-content` → step-164.
