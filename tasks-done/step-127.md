# step-127 — Auto-reconnexion opt-in (backoff + jitter), `link_status` distinct du breaker

> **Jalon :** M8 (§12 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-128

## But
Reconnecter automatiquement un bind coupé quand l'opérateur l'a activé, avec backoff exponentiel + jitter, en distinguant strictement l'état du lien (`link_status`) de l'état applicatif (`breaker_state`).

## Périmètre (ce que fait CETTE PR)
- `internal/connector/reconnect/` (naissance du paquet) : boucle de reconnexion **opt-in** lisant `auto_reconnect_enabled` + params (`reconnect_initial_delay_ms`, `reconnect_multiplier`, `reconnect_max_delay_ms`, `reconnect_jitter_pct`, `reconnect_max_attempts`) de `db/schema_passerelle_sms.sql` §9.
- `internal/connectorpool` : exposer `link_status` (up|down) **séparé** de `breaker_state`.
- `ESME_RINVPASWD` au (re)bind → **arrêt** définitif de la boucle (identifiants faux, ne pas marteler le SMSC).

## Points d'implémentation clés
- **opt-in** : sans `auto_reconnect_enabled`, un bind coupé reste `link_status=down` et attend un `rebind-connector` manuel (step-128).
- **`link_status` ≠ `breaker_state`** : jamais conflatés (§12, colonne schéma le rappelle) — un lien up peut avoir un breaker open et inversement.
- Backoff + jitter bornés par `reconnect_max_delay_ms` / `reconnect_max_attempts` (0 = infini) ; boucle avec condition d'arrêt.
- `ESME_RINVPASWD` = condition d'arrêt dure (critère d'acceptation M8).

## Tests (écrits dans la même PR)
- Bind coupé + auto-reconnexion activée → revient (backoff observé, horloge injectée).
- Sans auto-reconnexion → `link_status=down`, pas de retry.
- `ESME_RINVPASWD` → boucle stoppée, pas de nouvelle tentative.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] `link_status` et `breaker_state` distincts ; `ESME_RINVPASWD` stoppe le retry

## Hors périmètre
Les endpoints Admin de pilotage (rebind, statut, politique) → step-128.
