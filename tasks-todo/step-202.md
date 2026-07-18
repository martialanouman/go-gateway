# step-202 — Chaos : perte Redis (chaque politique de panne) + flapping connecteur

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-200 · **Bloque :** —

## But
Prouver que la passerelle dégrade **conformément aux politiques de panne documentées** sous perte de
Redis (pour **chaque** politique) et sous connecteur instable (flapping), **sans perte de message**.

## Périmètre (ce que fait CETTE PR)
- Suite de chaos (`test/chaos/` ou `internal/e2e`) : injection de perte Redis + flapping du connecteur
  via le **vrai simulateur SMSC** (disponible à M8) et/ou le faux SMSC scriptable.
- Vérification de chaque politique (§1.5) : `router-svc` Redis coupé → reste *ready*, fail-closed sur le
  débit, messages durables dans Kafka ; billing Redis coupé → fail-closed strict.

## Points d'implémentation clés
- **Chaque politique de panne** est vérifiée (§16 critère) : Redis down ne provoque jamais une perte —
  les messages restent dans Kafka (autorité durable), les soldes fail-closed.
- Flapping connecteur : le disjoncteur (M8) ouvre/ferme ; les messages parkés/rejoués ne sont ni perdus
  ni doublement facturés (invariant c toujours vert).
- Le simulateur SMSC (M8, `docs/specification-technique-simulateur-smsc.md`) porte l'injection de pannes.
- Aucune goroutine sans arrêt même sous chaos ; `go test -race`.

## Tests (écrits dans la même PR)
- Redis coupé : chaque politique vérifiée (ready/not-ready, fail-closed) ; zéro perte.
- Flapping connecteur : disjoncteur réagit ; aucun message perdu ni double facturation.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (c)** tenu sous chaos
- [ ] chaque politique de panne Redis vérifiée ; zéro perte de message

## Hors périmètre
Drain de pods/PDB + failover Postgres → step-203. Sécurité → step-204+.
