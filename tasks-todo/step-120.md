# step-120 — Basculer les tests d'intégration au vrai simulateur SMSC

> **Jalon :** M8 (§12 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-115 · **Bloque :** step-130

## But
Introduire le harnais qui pointe les tests de résilience vers le **vrai simulateur SMSC** (binaire externe, injection de pannes), en remplacement progressif du faux SMSC in-repo — prérequis de tous les scénarios M8.

## Périmètre (ce que fait CETTE PR)
- `internal/testutil/smscsim/` : lanceur du simulateur (container ou binaire) + client de pilotage des scénarios de panne (drop, throttle, latence, `ESME_RINVPASWD`), selon `docs/specification-technique-simulateur-smsc.md`.
- Ne dé-`Skip` encore rien : fournit uniquement l'outillage ; les tests existants restent sur `fakesmsc`.
- `Makefile` : cible pour récupérer/lancer le simulateur (analogue à `make fake-smsc`).

## Points d'implémentation clés
- Le simulateur est un **projet/binaire externe**, pas un module Go (§12) → orchestré via testcontainers ou process, pas importé.
- Garder `internal/testutil/fakesmsc` pour les jalons M2→M7 (les tests non-résilience continuent de l'utiliser).
- `t.Skip` guidé par disponibilité du simulateur / `DOCKER_HOST` (OrbStack, mémoire projet).

## Tests (écrits dans la même PR)
- Smoke test : le simulateur démarre, accepte un bind, répond `OK` à un `submit_sm`, et applique un scénario `Throttled` sur commande.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] harnais simulateur opérationnel, scénarios de panne pilotables

## Hors périmètre
Le dé-`Skip` effectif des tests de résilience M2→M7 → step-130 (après breaker/fallback/reconnect).
