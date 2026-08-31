# step-260 — Chaos : drain gracieux + PDB + binds préservés ; failover Postgres

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-250 · **Bloque :** —

## But
Prouver qu'un redémarrage de pods se fait **sans coupure des binds** (drain gracieux + PDB).

## Périmètre (ce que fait CETTE PR)
- Drain gracieux : les trois obligations du `[MUST]` de `docs/guide-codage-go.md` §5 — unbind SMPP au
  drain, offsets Kafka commités, `submit_sm` en vol terminés dans la fenêtre.
- Retrait du load balancer avant l'arrêt : `/readyz` bascule, puis on attend.
- Binds préservés : un ESME se rebinde immédiatement sur le pod de remplacement, à `max_sessions=1`.

**Le failover Postgres est parti en step-260b** — autre sous-système, autre outillage (il exige un
`pgtest.Cuttable` qui n'existe pas), et la prémisse de cette fiche à son sujet était fausse : le solde
n'est pas réhydraté « depuis le grand livre » mais depuis `control_plane.balances`.

## Design arrêté

**Ce que la step a trouvé.** Contrairement à step-250 et step-250b, qui n'ont changé aucune ligne de
production, celle-ci a corrigé **trois défauts réels**. Aucun n'avait de symptôme visible, pour une
raison unique : *rien ne testait le drain*.

| Obligation §5 `[MUST]` | Avant |
|---|---|
| `smpp-server-svc` unbind gracieux | ❌ socket coupée sèche — `sendUnbind` n'était joignable que par `forceClose` (révocation, step-032) |
| consumers Kafka valident leurs offsets **puis** s'arrêtent | ❌ `CommitRecords(ctx, …)` sur le ctx déjà annulé, échec requalifié en arrêt propre |
| `connector-pool-svc` termine les `submit_sm` en vol | ✅ tenu, mais non testé |
| retrait LB avant l'arrêt (plan §1.5) | ❌ `OpsServer` n'avait aucun état de cycle de vie |

Le défaut Kafka est le plus coûteux : **un SIGTERM gracieux perdait autant qu'un `kill -9`**, donc le
pool re-soumettait au SMSC un lot déjà envoyé. `reroute.go` le dit — la facturation est idempotente par
`message_id`, « but the extra submit itself is not undone » : un SMS en double chez l'abonné, à chaque
rolling deploy.

**Les quatre décisions.**

1. **Un hook de pré-drain sur le superviseur** (`OnDrain`), parce que le drain a un instant qu'aucun
   composant ne peut occuper : *avant* que le premier ne tombe. `Group` a dû être détaché de son parent
   pour l'offrir — ses composants mouraient à l'instant du SIGTERM, ce qu'un test a révélé.
2. **`/readyz` répond 503 « draining » sans sonder** ses dépendances : le verdict ne peut plus changer.
   `/healthz` reste 200, sans quoi le kubelet redémarrerait le pod qu'on retire.
3. **`DRAIN_DELAY` (5 s par défaut) est indispensable, pas cosmétique.** Basculer sans attendre ferme le
   listener avant que kube-proxy n'ait retiré l'endpoint. L'ordre compte dans les deux sens : attendre
   avant de basculer dépenserait la grâce en s'annonçant *ready*.
4. **`cancelWork()` passe devant l'unbind.** L'unbind est une courtoisie bornée par un délai d'écriture
   de 5 s ; un pair qui a cessé de lire retiendrait sinon chaque `submit_sm` en vol pendant toute cette
   fenêtre.

**Ce qui n'a PAS été corrigé, et pourquoi.** Après un `kill -9`, l'entrée `pod_id:bind_id` survit
jusqu'à 60 s et consomme un slot `max_sessions` — un pod redémarré porte un **nouveau** `pod_id`, donc
la règle « un rebind ne double-compte pas » de `bind.lua` ne le couvre pas. C'est le TTL jouant son rôle
de filet ; le drain **propre**, lui, libère le jeton aussitôt. Le test le documente au lieu de le
« réparer ».

**La garde qui compte est textuelle.** `DrainHook` peut être parfait et le drain cassé pour un service
dont la `main` ne l'appelle jamais — et rien ne le verrait avant la production. Un test parcourt les dix
`cmd/*/main.go` et exige l'appel ; son plancher (dix services) l'empêche de garder du vide.

## Points d'implémentation clés
- Le drain SMPP existait déjà côté connector (`connectorpool.Run` détache l'unbind après cancel) : c'est
  le seul tiers du `[MUST]` qui était tenu. Il a désormais son test — le détachement est exactement le
  genre de subtilité délibérée qu'un refactor retire comme du poids mort.
- **Binds préservés** (§16 critère) : `/readyz` bascule avant l'arrêt, puis `DRAIN_DELAY` s'écoule.
- Aucune goroutine sans arrêt ; `go test -race`.

## Tests (écrits dans la même PR)
- `supervisor` : le hook de pré-drain court avant le premier composant drainé (`Ordered` et `Group`).
- `observability` : `/readyz` → 503 « draining » à dépendances saines ; `/healthz` reste 200 ; le hook
  bascule **puis** attend.
- `smpp/session` : un ctx annulé émet un `unbind` (le test affirmait l'inverse — il décrivait fidèlement
  le code, et le code avait tort) ; les workers en vol sont libérés même quand le pair a cessé de lire.
- `smppserver` (intégration) : à `max_sessions=1`, le pod drainé émet son `unbind` et l'ESME se rebinde
  aussitôt sur un pod de **nouvelle identité**.
- `storage/kafka` (intégration) : un enregistrement traité au moment du SIGTERM n'est **pas** redélivré.
- `connectorpool` : le pool drainé envoie son `Unbind` au SMSC.
- Garde : les dix `cmd/*/main.go` enregistrent le hook.

## Definition of Done
- [x] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [x] critères couverts par tests · godoc sur l'exporté
- [x] rolling deploy sans coupure des binds (drain) prouvé ; le PDB qui devra le respecter → step-270

## Hors périmètre
Failover Postgres → **step-260b**. Manifests k8s (PDB déclaré) → step-270, qui devra rendre le
`terminationGracePeriodSeconds` du pod supérieur à `SHUTDOWN_TIMEOUT` + `DRAIN_DELAY`. Sécurité →
step-290+.

Le drain de `supervisor.Ordered` reste **non borné globalement** : il attend `<-dones[i]` sans timeout,
donc un composant qui ignorerait son contexte bloquerait jusqu'au SIGKILL du kubelet. Aucun composant
actuel ne l'exhibe — noté dans step-260b.
