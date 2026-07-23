# step-024 — smpp-server-svc : listener :2775, auth bind + max_sessions (invariant d)

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-022, step-023 · **Bloque :** step-025, step-026, step-027, step-046

## But
Un ESME peut se binder sur `:2775` : le service authentifie le `system_id`, applique `allowed_bind_types` et le quota `max_sessions` contre le registre gRPC. Ferme l'**invariant (d)** de bout en bout.

## Périmètre (ce que fait CETTE PR)
- Créer `cmd/smpp-server-svc/main.go` : écoute SMPP `:2775` (§1.4), port ops `:9090`, boucle d'acceptation → `internal/smpp/session`.
- Auth bind : résoudre `system_id` → `control_plane.credentials` (`type='smpp_bind'`, `status<>'revoked'`), vérifier le mot de passe **argon2id en temps constant** (`internal/credential.VerifyBindPassword`), charger le `smpp_accounts` (statut, `smpp_enabled`, `allowed_bind_types`, `max_sessions`).
- Requête sqlc `credentials.sql` : `GetBindCredentialBySystemID` (jointure compte). Ajouter à `internal/storage/postgres/queries`.
- Bind : appel gRPC `SessionRegistry.Bind` (client vers session-manager) en passant `max_sessions` ; quota dépassé → `ESME_RBINDFAIL`.
- Refus explicites : compte suspendu/`smpp_enabled=false` → `ESME_RBINDFAIL` ; type non autorisé → `ESME_RBINDFAIL` ; `submit_sm` renvoie `ESME_RSUBMITFAIL` en attendant step-025.

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser le **client gRPC** (`grpc.NewClient`, credentials insecure en interne).
- **Temps constant** obligatoire (§1.9) ; ne jamais logger `system_id`+mot de passe.
- `unbind`/EOF/expiration `enquire_link` → `SessionRegistry.Unbind` libère le jeton.
- Mapper le `code` gRPC (`max_sessions_exceeded`) → `command_status` SMPP via `errs.SMPPStatus`.

## Tests (écrits dans la même PR)
- Intégration (`fakesmsc` en mode ESME ou client de test + testcontainers PG/Redis) : bind valide OK ; mauvais mot de passe → `ESME_RBINDFAIL`.
- **Invariant (d)** e2e : bind au-delà de `max_sessions` → `ESME_RBINDFAIL` ; unbind libère le jeton.
- Type de bind hors `allowed_bind_types` refusé ; `smpp_enabled=false` refusé.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] invariant (d) prouvé de bout en bout (bind réel → registre)

## Hors périmètre
`submit_sm` → pipeline (step-025) ; anti-brute-force (step-026) ; rotation avec grâce (step-027).
