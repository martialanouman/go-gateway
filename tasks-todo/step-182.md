# step-182 — Émettre les mises à jour temps réel vers metrics.stream

> **Jalon :** M11 (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-180 · **Bloque :** step-183

## But
Alimenter le topic Kafka `metrics.stream` avec des événements de métriques échantillonnés depuis le
pipeline et le connector, afin que la gateway WS/SSE (step-183) puisse les diffuser en temps réel.

## Périmètre (ce que fait CETTE PR)
- Producteur `metrics.stream` (§1.6) : émission d'événements bornés (compte/connecteur/route/statut,
  profondeur de file, état disjoncteur) depuis `router-svc`/`connector-pool-svc`.
- Sérialisation stable des événements (schéma versionnable), clé de partition bornée.

## Points d'implémentation clés
- `metrics.stream` est déjà dans la liste canonique des topics (§1.6) — l'utiliser tel quel.
- Événements **bornés** (mêmes labels que step-180) : jamais MSISDN/message_id/corps (invariant a).
- Émission **non bloquante** pour le chemin chaud : best-effort, la perte d'un événement de stream ne
  doit jamais retenir un message (les CDR restent l'autorité).
- **`ctx7`** avant toute API `franz-go` (producteur, clé de partition).

## Tests (écrits dans la même PR)
- Un événement émis atterrit sur `metrics.stream` avec des labels bornés.
- L'émission ne bloque pas le chemin chaud (best-effort testé).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun label non borné (invariant a)
- [ ] émission best-effort non bloquante

## Hors périmètre
Gateway WS/SSE et endpoints stream → step-183/184.
