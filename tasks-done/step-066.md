# step-066 — Anti-spam : vélocité + réputation (état partagé Redis, fail-open)

> **Jalon :** M5 (§9 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-065 · **Bloque :** —

## But
Compléter le moteur anti-spam (step-065) avec la **vélocité** (MT + MO entrant) et la **réputation**, sur état partagé Redis, en **fail-open avec flag** si Redis est perdu.

## Périmètre (ce que fait CETTE PR)
- Ajouter à `internal/pipeline/antispam` : règles `velocity` (compteurs glissants par source/compte, MT et MO entrant) et `reputation` (score par source).
- Compteurs Redis atomiques (fenêtre glissante) ; actions `block`/`flag`/`throttle`.
- **Fail-open avec flag** : perte de l'état partagé Redis → l'étape **laisse passer** en marquant le message (flag observable), les règles de **contenu statiques** (step-065) restent appliquées.
- Brancher la vélocité MO entrant dans le chemin `mo-dlr-router-svc` (comptage des MO).

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser `go-redis` (fenêtre glissante en Lua atomique — pas de read-modify-write, règle d'or).
- Politique de panne cohérente avec §1.5 : anti-spam vélocité = **fail-open** (disponibilité prioritaire), à distinguer du débit qui est fail-closed (M6).
- Le flag de fail-open est un compteur/label métrique, **jamais** le corps (invariant a).

## Tests (écrits dans la même PR)
- Dépassement de vélocité → action ; sous le seuil → passe.
- Redis coupé → **fail-open** : le message passe avec flag, les règles de contenu restent actives.
- Vélocité MO entrant comptée.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] fail-open avec flag prouvé sur perte Redis

## Hors périmètre
Admin anti-spam (step-067) ; réglage fin de la réputation/ML (ultérieur).
