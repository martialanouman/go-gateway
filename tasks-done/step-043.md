# step-043 — Réception deliver_sm : classification MO vs DLR → mo.inbound / dlr.events

> **Jalon :** M4 (§8 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-044, step-045

## But
Le `connector-pool-svc` reçoit les `deliver_sm` du SMSC, les classe (MO vs DLR) et les publie sur les topics de la voie retour.

## Périmètre (ce que fait CETTE PR)
- Dans `connector-pool-svc` : handler `deliver_sm` sur le bind sortant, répond `deliver_sm_resp`.
- Classification par `esm_class` (bit accusé de réception → DLR ; sinon MO).
- Publier : DLR → `dlr.events` (parse du champ receipt : `smsc_msg_id`, `stat`, `err`, horodatages) ; MO → `mo.inbound` (source, destination = numéro entrant, corps masqué).
- Clés/format d'enveloppe cohérents avec §1.6.

## Points d'implémentation clés
- **Invariant (a)** : le corps d'un MO passe en `Body` masquant, jamais loggé.
- Parsing du receipt DLR (format SMPP 3.4 `id:... stat:... err:...`) robuste aux variantes ; en cas d'échec de parse, journaliser + compter, ne pas jeter en silence.
- `deliver_sm_resp` renvoyé même si la publication échoue ? Non : publier d'abord (durabilité Kafka) puis accuser, pour ne pas perdre le MO/DLR. Documenter le choix.
- `connector_id` propagé dans l'enveloppe (nécessaire à la corrélation DLR, clé `dlrmap`).

## Tests (écrits dans la même PR)
- Intégration : `fakesmsc` émet un MO → message publié sur `mo.inbound` ; émet un DLR → publié sur `dlr.events` avec `smsc_msg_id` extrait.
- `esm_class` correctement discriminé (table de tests MO/DLR).
- Le corps n'apparaît dans aucun log.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] classification MO/DLR et publication prouvées

## Hors périmètre
Corrélation DLR → CDR (step-044) ; résolution/remise MO (step-045, step-048).
