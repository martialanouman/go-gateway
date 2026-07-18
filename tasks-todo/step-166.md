# step-166 — Effacement RGPD (client + MSISDN) + attestation asynchrone

> **Jalon :** M10 (§14 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-164, step-165 · **Bloque :** —

## But
Livrer l'effacement RGPD : par client (crypto-shred + purge CDR) et par MSISDN (suppression ligne à
ligne à travers tous les clients, en gardant l'opt-out), sous forme de job asynchrone avec attestation.

## Périmètre (ce que fait CETTE PR)
- Table `control_plane.gdpr_erase_jobs` : **édition `db/schema_passerelle_sms.sql` + migration
  `golang-migrate`** (recette « Changer le schéma »).
- `api/openapi-admin.yaml` + `internal/adminapi` : `gdpr-erase` (client), effacement MSISDN,
  `get-gdpr-erase-job` (attestation) ; collection synchronisée.
- Worker asynchrone d'effacement (job avec statut/attestation).

## Points d'implémentation clés
- **Client** : crypto-shred (step-164) **+** purge CDR par drop de partition quand possible (step-165).
- **MSISDN** : suppression ligne à ligne **à travers les clients**, mais **garder l'opt-out** (§14 :
  l'effacement retire contenu + métadonnées mais conserve la suppression opt-out — obligation de ne pas
  re-contacter).
- **Attestation** : `get-gdpr-erase-job` renvoie la preuve d'exécution (portée, horodatage, compteurs) —
  jamais le contenu effacé.
- Job **asynchrone** : row-cap/backpressure pour ne pas saturer le chemin chaud.
- Aucun corps ni clair dans les logs/attestation (invariant a).

## Tests (écrits dans la même PR)
- Effacement MSISDN : contenu + métadonnées retirés across clients ; **opt-out conservé** ; attestation émise.
- Effacement client : crypto-shred + purge ; attestation.
- Migration up/down ; schéma et migration en accord.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] opt-out conservé après effacement MSISDN ; attestation vérifiée ; collection synchronisée

## Hors périmètre
Fin de M10. Observabilité/temps réel → M11.
