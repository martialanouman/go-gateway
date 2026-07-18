# step-085 — Brancher l'étape débit dans le pipeline + précédence des plafonds

> **Jalon :** M6 (§10 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-082, step-084 · **Bloque :** step-086

## But
Insérer le token-bucket comme étape **débit** du pipeline (après segmentation, avant envoi SMSC), en appliquant la précédence : plafond dur connecteur `throughput_limit_per_sec` ≥ `rate_limits` opérationnels.

## Périmètre (ce que fait CETTE PR)
- `internal/router` / `internal/pipeline/pipeline.go` : étape débit qui vérifie successivement compte → route → connecteur ; un dépassement rejette avec le code throttle (CDR `rejected`, jamais envoyé).
- **Validation à l'écriture** : côté `internal/adminapi` (create/update rate-limit ou connector), refuser `rate_limits.max_per_sec` > `throughput_limit_per_sec` du connecteur (`db/schema_passerelle_sms.sql` §13 NOTE : contrôle applicatif, pas de CHECK cross-table).
- Émettre le span `pipeline.ratelimit`.

## Points d'implémentation clés
- **Ordre figé** : débit vient après résolution de route et segmentation, avant réservation crédit MT (future M9) et envoi. Ne saute jamais une étape de conformité.
- Le court-circuit L0 (M7) sautera la *résolution de route* mais **pas** le débit.
- Fail-closed conservé (step-084) sur perte Redis.
- Le corps n'entre dans aucun span/label (invariant a).

## Tests (écrits dans la même PR)
- Un message dépassant le plafond compte → rejeté ; sous le plafond → routé.
- Précédence : connecteur à 50/s prime sur route à 100/s (jamais dépassé — critère d'acceptation M6).
- Contrat/validation admin : `rate_limit.max_per_sec` > plafond connecteur → `422`/erreur plate.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] plafond technique du connecteur jamais dépassé ; débit après segmentation

## Hors périmètre
L'AIMD piloté par `submit_sm_resp` → step-086. Le débit `query_sm` → step-087.
