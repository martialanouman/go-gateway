# step-290 — Sécurité : gosec, govulncheck, secrets, piste d'audit

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## But
Durcir la chaîne : scan d'injection `gosec`, gate `govulncheck` en CI, gestion des secrets, et une
piste d'audit consolidée des actions sensibles.

## Périmètre (ce que fait CETTE PR)
- `make lint`/CI : intégrer **gosec** (scan injection/mauvais usages) en plus de golangci-lint.
- `govulncheck` déjà présent (`make`) → en faire une **gate** bloquante documentée.
- Gestion des secrets : vérifier qu'aucun secret n'est en clair (hash pour mots de passe bind & clés API,
  §1.9), comparaison temps constant partout.
- Piste d'audit consolidée des actions opérateur/sensibles (réutilise les audits M10 : content, GDPR).

## Points d'implémentation clés
- **gosec** est un outil binaire (§16) — **`ctx7`** pour son intégration/exclusions justifiées ; ajouté à
  `make tools`.
- Vérifier les invariants de sécurité déjà posés : **SQL paramétré** (pas d'injection), secrets **hashés**
  (argon2id pour bind, SHA-256 pour clé API, §1.9), révélés une seule fois à la création/rotation.
- Aucune suppression d'alerte gosec sans justification en commentaire.
- La piste d'audit ne contient jamais de corps ni de secret en clair (invariant a).

## Tests (écrits dans la même PR)
- gosec passe (ou n'a que des exclusions justifiées) ; govulncheck vert.
- Un secret est bien hashé/comparé en temps constant (tests existants renforcés).
- Une action sensible produit une entrée d'audit.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] gosec intégré ; govulncheck en gate ; audit sans secret/corps

## Hors périmètre
TLS/mTLS transport → step-300. Auth opérateur OIDC → step-310.
