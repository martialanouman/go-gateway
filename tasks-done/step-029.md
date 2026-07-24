# step-029 — API publique : list-messages (pagination par curseur sur le CDR)

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-030

## But
Compléter l'API publique avec `GET /messages` : liste paginée par **curseur** des messages du compte, lue sur le CDR ClickHouse (`operationId: list-messages`, déjà déclaré `api/openapi-public.yaml`).

## Périmètre (ce que fait CETTE PR)
- Créer `internal/restapi/messages_list.go` : handler huma `list-messages` (filtres du contrat : statut, période, `client_ref` ; `limit` + `cursor`).
- Lecture CDR : requête ClickHouse dernière version par `message_id` (`argMax`/`FINAL`, §1.10) scoping `account_id`.
- Curseur opaque encodé (ex. base64 de `(submitted_at, message_id)`), stable et non devinable.
- **Jamais le corps** dans la projection de liste (invariant a).

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser `ClickHouse/clickhouse-go/v2` si de nouvelles API de requête/scan sont nécessaires.
- Curseur = keyset pagination (pas d'`OFFSET`) : `WHERE (submitted_at, message_id) < cursor ORDER BY ... LIMIT n+1`.
- Forme de réponse conforme au contrat (`list-messages`), enveloppe `{data, next_cursor}` selon le schéma déclaré.
- Statut lu = dernière version du `ReplacingMergeTree`.

## Tests (écrits dans la même PR)
- Test de contrat public : `list-messages` conforme.
- Intégration ClickHouse : insertion de N messages, pagination par curseur cohérente et sans doublon/perte ; scoping compte respecté.
- Le corps n'apparaît pas dans la réponse.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] pagination keyset stable, curseur opaque

## Hors périmètre
`get-account` (step-028), `cancel-message` (step-030), `Idempotency-Key` (step-031).
