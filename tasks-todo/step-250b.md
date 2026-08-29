# step-250b — Chaos : flapping connecteur (disjoncteur + invariant c)

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-250 · **Bloque :** —

## But
Prouver que sous un connecteur **instable** — le disjoncteur ouvrant et refermant plusieurs fois — aucun
message n'est perdu et **aucun n'est doublement facturé** (invariant c).

## Pourquoi une fiche à part
step-250 portait les deux moitiés de « chaos ». La moitié Redis a été livrée séparément : elle n'a
jamais eu besoin d'un pair SMPP, et les réunir aurait fait une PR que la revue n'aurait pas tenue à
la maille où les trois derniers défauts ont été trouvés.

## Le gap, précisément
Beaucoup est déjà couvert et ne doit **pas** être réécrit : `breaker_test.go` couvre le cycle
`Closed→Open→HalfOpen→Closed` du disjoncteur seul ; `reroute_test.go` couvre l'avancée dans la
`fallback_chain`, le saut d'un candidat ouvert, le parking sur `mt.reroute-park` ; `sim_reconnect_test.go`
couvre la coupure et le rétablissement du lien TCP ; `billing_settle_test.go` compte les `Capture`/
`Release` sur des scénarios à une seule tentative.

Ce qu'**aucun** test ne fait : faire flapper le disjoncteur **plusieurs cycles** au niveau du pool
complet en comptant, par `message_id`, qu'aucun n'est ni capturé ni libéré deux fois pendant que des
messages sont parqués puis rejoués. Le plus proche, `TestSimBreakerFallbackParkReplay`
(`sim_fallback_test.go:37`), compte bien N entrées / N sorties **une par une** — le bon patron — mais
n'ouvre le disjoncteur **qu'une fois** et n'asserte **rien** sur la facturation.

## Points d'implémentation clés
- Le harnais est en place : `startSimPool` (`sim_harness_test.go:70`) monte un pool réel avec un vrai
  Redis pour l'agrégat cross-pod ; `tcpproxy.Cut/Resume` fait flapper le lien à **adresse stable** ;
  `breaker.New(cfg, now)` prend une **horloge injectable**, donc plusieurs cycles se jouent sans
  attendre les `Cooldown` réels.
- `BreakerConfig` n'a **aucune variable d'environnement** : le service tourne sur les défauts
  (`MinRequests=20`, `FailureRate=0.5`, `Window=10s`, `Cooldown=30s`) et seuls les tests le surchargent.
- L'idempotence à prouver vit dans `resolveTerminal` (`billing.go:357`) : capture et release sont
  mutuellement exclusives et idempotentes par `message_id`. Le rejeu Kafka d'un même message pendant un
  flap est le cas qui l'exerce.
- Aucune goroutine sans arrêt même sous chaos ; `go test -race`.

## À trancher dans la PR
**Les tests `sim_*.go` sont aujourd'hui skippés en CI** : le workflow `Test (race)` n'a pas d'étape
`make smsc-sim`, donc l'image `smsc-simulator:dev` manque et ces tests **passent sans rien exercer**.
Soit on ajoute l'étape au workflow, soit on écrit ce scénario sur `fakesmsc` + `tcpproxy` (qui tournent
toujours). Livrer un test de chaos qui ne s'exécute pas en CI serait le pire des deux.

## Tests (écrits dans la même PR)
- Plusieurs cycles ouvre/ferme du disjoncteur sous trafic : aucun message perdu (réconciliation par
  identifiant, comme step-250), aucun `message_id` capturé **et** libéré, ni capturé deux fois.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (c)** tenu sous flapping
- [ ] le test s'exécute réellement en CI (pas un skip silencieux)

## Hors périmètre
Perte Redis (step-250, livrée). Drain de pods/PDB + failover Postgres → step-260. Sécurité → step-290+.
