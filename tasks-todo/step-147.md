# step-147 — Adaptateur de facturation externe (§6.10) derrière une interface

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-144 · **Bloque :** —

## But
Permettre à un fournisseur de facturation externe de décider/consommer le crédit, via un adaptateur à
trois modes, localisé derrière une interface — l'intégration réelle en prod reste optionnelle.

## Périmètre (ce que fait CETTE PR)
- `internal/billing/` : interface `ExternalProvider` + les quatre modes `balance_check`,
  `consume_delegate_async`, `consume_delegate_sync`, `both` (§6.10).
- Sélection du mode selon `external_billing_providers.mode` et `customers.external_billing_provider_id`
  (consolidé sur `customers` en step-142d/ADR-0010 ; `db/schema_passerelle_sms.sql` §3/§4).
- Implémentation stub locale (aucune dépendance réseau réelle) + point de câblage dans `billing-svc`.

## Points d'implémentation clés
- L'adaptateur **existe** ; le branchement à un vrai fournisseur synchrone en prod est hors périmètre
  (§13 « Hors périmètre »).
- `consume_delegate_sync` sur le chemin chaud doit respecter les budgets de latence : borner par timeout,
  fail-closed en cas de dépassement (ne jamais laisser passer un crédit non confirmé).
- `consume_delegate_async` : la confirmation revient hors chemin critique ; réconcilier via le grand livre.
- **`ctx7`** si un client HTTP/gRPC de fournisseur est ajouté.

## Tests (écrits dans la même PR)
- Les trois modes : `balance_check` autorise/refuse ; `sync` timeout → fail-closed ; `async` réconcilie.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] interface localisée (le fournisseur réel est interchangeable)

## Hors périmètre
Intégration d'un fournisseur de prod concret. Endpoints Admin `*-billing-provider` → step-149.
