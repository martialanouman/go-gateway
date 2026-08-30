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

## Design arrêté

**Le test s'écrit sur `fakesmsc` + un pool en mémoire, pas sur le harnais `sim_*`.** Trois raisons
vérifiées, dont deux dirimantes :

1. `smscsim` n'a **aucune mutation de scénario à chaud** (sa doc : « un test qui a besoin d'un
   comportement différent relance un simulateur »). `fakesmsc.Config.OnSubmit` est une closure évaluée à
   chaque submit : un `atomic.Bool` suffit à alterner malade/sain. Pour ce test, fakesmsc est l'outil
   juste, pas un pis-aller.
2. Le harnais `sim_*` **ne peut pas tester l'invariant (c)** : `startSimPool` ne renseigne jamais
   `deps.Billing` (donc `noopSettler`) et `injectRouted*` ne fixe jamais `Billable`. L'assertion serait
   creuse par construction.
3. Les tests `sim_*` **ne tournent pas en CI** — trou plus large que cette step, parti en **step-250c**.

**Le double enregistre, il ne compte pas** (leçon de step-245). `spySettler` ne tient que deux entiers ;
« A réglé deux fois, B jamais réglé » leur laisse exactement les valeurs d'un run correct. L'invariant
(c) est une propriété **par message**, donc le double est indexé par `message_id`.

**Le flapping suit le disjoncteur, pas l'horloge** : la sonde bascule le SMSC en observant l'état
rapporté, ce qui produit de vrais cycles au lieu d'un script temporel qui ne prouverait que le SMSC a
changé d'avis à l'heure dite. `ResponseTimeout` (100 ms) reste sous `Cooldown` (300 ms) — invariant
documenté du disjoncteur, qui attribue chaque issue à l'état *courant*.

**Trois pièges rencontrés, tous côté fixture :**

- `recordingProducer` signale chaque `Produce` dans un canal de **16** que rien ne draine : au 17ᵉ record
  le pool se bloque dans sa propre goroutine. Un collecteur non bloquant le remplace.
- Un consommateur qui rejoue **tous** les records à chaque passe n'est pas Kafka : il réinjecte des
  messages déjà commités, et le pool *paraît* les régler plusieurs fois. Le double ne rejoue que le
  **non commité**.
- `retryKey` est `(partition, offset)` : des records partageant un offset partagent **une seule** entrée
  de fenêtre de rejeu, et l'horloge d'échec du premier message gouverne tous les autres.

## Tests (écrits dans la même PR)
- Plusieurs cycles ouvre/ferme du disjoncteur sous trafic : aucun message perdu (réconciliation par
  identifiant, comme step-250), aucun `message_id` capturé **et** libéré, ni capturé deux fois.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (c)** tenu sous flapping
- [ ] le test s'exécute réellement en CI (pas un skip silencieux)

## Hors périmètre
Perte Redis (step-250, livrée). Drain de pods/PDB + failover Postgres → step-260. Sécurité → step-290+.
