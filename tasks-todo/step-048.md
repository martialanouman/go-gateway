# step-048 — Décision de remise MO : bind actif (gRPC) ou webhook

> **Jalon :** M4 (§8 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-044, step-045, step-046, step-047 · **Bloque :** —

## But
Boucler la voie retour : un MO résolu (step-045) est remis au compte via un bind SMPP vivant (gRPC `Deliver`, round-robin) **ou**, à défaut, via webhook. Idem transmission DLR au compte.

## Périmètre (ce que fait CETTE PR)
- Dans `mo-dlr-router-svc` : pour un MO/DLR résolu, choisir la voie :
  1. binds vivants du compte (`SessionRegistry.Lookup`) → `SessionRegistry.Deliver` en **round-robin** sur les binds rx/trx ;
  2. sinon webhook (`internal/webhook`, step-047).
- Transmission du DLR au compte (même arbitrage) après mise à jour CDR (step-044).
- Échec de remise → retry / dead-letter cohérent avec §1.6.

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser le **client gRPC** vers session-manager.
- Round-robin sur les binds **vivants** ; un `Deliver` échoué (bind mort) → binder suivant, puis webhook en dernier recours.
- Encodage du `deliver_sm` via `internal/smpp` (le router construit le PDU, le pod le pousse).
- **Invariant (a)** : corps jamais loggé sur tout le chemin de remise.

## Tests (écrits dans la même PR)
- e2e : `fakesmsc` émet un MO sur un numéro entrant → remis au bon compte via bind actif (gRPC) ; sans bind → webhook signé.
- DLR corrélé (step-044) transmis au compte.
- Round-robin réparti sur plusieurs binds vivants ; bascule webhook si aucun bind.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] voie retour complète (MO+DLR) prouvée e2e (bind et webhook)

## Hors périmètre
Détection STOP / opt-out sur MO (M5, step-063) ; comptage/facturation MO (M9).
