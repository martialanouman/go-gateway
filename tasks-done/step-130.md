# step-130 — Dé-`Skip` des tests de résilience + scénarios d'injection de pannes

> **Jalon :** M8 (§12 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-120, step-122, step-124, step-125, step-126, step-127, step-129 · **Bloque :** —

## But
Clore M8 : réactiver tous les tests de résilience laissés `t.Skip("… — M8")` depuis M2→M7 et prouver les critères d'acceptation via le vrai simulateur (injection de pannes).

## Périmètre (ce que fait CETTE PR)
- Retirer les `t.Skip("… — M8")` disséminés dans `internal/connectorpool`, `internal/routing`, `internal/e2e` et les brancher sur `internal/testutil/smscsim` (step-120).
- Scénarios d'acceptation end-to-end (§12) :
  - connecteur dégradé → disjoncteur `open` → trafic via `fallback_chain` → excédent parqué (`mt.reroute-park`) puis rejoué ;
  - agrégat de disjoncteur correct avec binds sur **plusieurs pods** ;
  - `bind_pool_size=4` → débit accru + segments d'un message sur un seul bind (ordre) ;
  - bind coupé + auto-reconnexion → revient ; sans → `link_status=down` + rebind manuel ;
  - `ESME_RINVPASWD` → auto-retry stoppé.

## Points d'implémentation clés
- Ces tests deviennent **verts à vie** : ne jamais les re-`Skip` (§12 critère explicite).
- Réutiliser le pilotage de pannes du simulateur (drop/throttle/latence/mot de passe invalide).
- Garder `-race` vert sous injection de pannes ; aucune goroutine fuyante lors des bascules.

## Tests (écrits dans la même PR)
- Les cinq scénarios ci-dessus, verts.
- Grep de garde : plus aucun `Skip("… — M8")` restant dans le dépôt (test ou script CI).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] tous les tests de résilience M2→M7 dé-`Skip`és et verts

## Hors périmètre
Facturation (M9), contenu/chiffrement (M10) — inchangés par M8.
