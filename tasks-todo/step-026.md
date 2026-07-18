# step-026 — Anti-brute-force sur le bind SMPP

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-024 · **Bloque :** —

## But
Freiner les tentatives de bind répétées : compteur d'échecs par `system_id`/IP (Redis TTL), backoff progressif et événement de sécurité auditable.

## Périmètre (ce que fait CETTE PR)
- Dans `smpp-server-svc` (ou `internal/smpp/session` côté serveur) : sur échec d'auth, incrémenter un compteur Redis `bindfail:{system_id}` et `bindfail:ip:{ip}` (TTL glissant).
- Au-delà d'un seuil configurable → backoff (délai avant réponse) puis refus `ESME_RBINDFAIL` sans même vérifier le hash (économie CPU argon2).
- Émettre un **événement de sécurité auditable** (log structuré `slog`, jamais de secret ni de corps) et un compteur Prometheus.
- Réinitialisation du compteur sur bind réussi.

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser `go-redis` (INCR + EXPIRE atomiques, idéalement un petit script Lua — pas de read-modify-write côté Go, règle d'or).
- Ne jamais logger le mot de passe ; l'IP et le `system_id` sont des identifiants, autorisés.
- Seuils/fenêtres via `caarlos0/env/v11`, valeurs par défaut sûres.
- Le backoff ne doit pas bloquer une goroutine sans `ctx` (timer respectant l'annulation).

## Tests (écrits dans la même PR)
- Intégration Redis : N échecs → refus rapide + backoff appliqué ; bind réussi remet à zéro.
- L'événement de sécurité est émis sans fuite de secret.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] compteur/backoff atomiques (Lua) et bornés dans le temps

## Hors périmètre
Rotation d'identifiant (step-027) ; corrélation avec un WAF/ingress (exploitation, hors code).
