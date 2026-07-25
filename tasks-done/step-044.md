# step-044 — mo-dlr-router-svc : squelette + corrélation DLR → CDR

> **Jalon :** M4 (§8 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-042, step-043 · **Bloque :** step-048

## But
Créer le service de voie retour et implémenter le chemin DLR : corréler `smsc_msg_id → message_id` (§1.11) et écrire le statut final au CDR.

## Périmètre (ce que fait CETTE PR)
- Créer `cmd/mo-dlr-router-svc/main.go` : consumer Kafka de `dlr.events` (et `mo.inbound`, traité à step-045), port ops `:9090`.
- Package `internal/modlrrouter` : résoudre `dlrmap:{connector_id}:{smsc_msg_id}` (Redis) → `message_id`/`trace_id`.
- Écrire une **nouvelle ligne CDR versionnée** (§1.10) : `delivered`/`failed`/`expired`, `delivered_at`, `latency_ms`, `error_code`.
- DLR **sans mapping** (expiré/inconnu) : journalisé + compteur Prometheus dédié, **jamais** jeté en silence (dead-letter `dlr` optionnel).

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser `franz-go` (consumer group) et `go-redis`.
- Mapper `stat`/`err` SMSC → `Status` CDR (`clickhouse.StatusDelivered/Failed/Expired`) et `error_code` (contrat partagé §11).
- `latency_ms` = `delivered_at − submitted_at` (lu depuis le CDR ou l'enveloppe).
- `readyz` : Kafka vital (fail si injoignable), Redis dégradable (mapping raté → compté).

## Tests (écrits dans la même PR)
- Intégration (Kafka + Redis + ClickHouse testcontainers) : DLR corrélé → nouvelle ligne CDR, dernière version = statut final.
- DLR sans mapping → compté + journalisé, pas de crash, pas de perte silencieuse.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] DLR corrélé met à jour le CDR (ligne versionnée)

## Hors périmètre
Résolution/remise MO (step-045, step-048) ; webhooks (step-047) ; détection STOP (M5, step-063).
