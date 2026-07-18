# step-143 — Compteur MO séparé : plancher, arrêt + alerte, jamais bloquant pour le MT

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-142 · **Bloque :** step-144

## But
Le comptage MO (voie retour) est un **compteur**, jamais un blocage : il décrémente un solde MO
distinct du MT, s'arrête et alerte à `mo_billing_floor`, et ne peut **jamais** bloquer un MT.

## Périmètre (ce que fait CETTE PR)
- `internal/billing/` : `RecordMO` — incrément atomique Lua du solde MO
  (`billing:balance:mo:{owner_type}:{owner_id}`) + entrée de grand livre.
- Détection de plancher `mo_billing_floor` : sous le plancher → stop + émission d'un événement d'alerte.
- Soldes **MT et MO strictement séparés** (`direction` distinct dans `balances`, §22 du schéma).

## Points d'implémentation clés
- Le MO ne partage aucun chemin de décision avec le MT : un MO sous plancher n'affecte pas les réserves MT.
- L'alerte de plancher est **émise** ici (événement structuré) ; le transport temps réel
  (`stream-billing-alerts`) est M11 (step-184) — ne pas coupler.
- Atomique en Lua (CLAUDE.md). Idempotence MO par `message_id` MO comme pour le MT.

## Tests (écrits dans la même PR)
- Le compteur MO décrémente ; sous `mo_billing_floor` → stop + événement d'alerte.
- Un MO au plancher ne bloque **jamais** un reserve/capture MT (test croisé).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] séparation MT/MO prouvée par test ; alerte de plancher émise (pas de transport ici)

## Hors périmètre
Endpoint WS `stream-billing-alerts` → step-184 (M11). Adaptateur externe → step-147.
