# step-250 — Chaos : perte Redis (chaque politique de panne)

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-200 · **Bloque :** —

## But
Prouver que la passerelle dégrade **conformément aux politiques de panne documentées** sous perte de
Redis, **sans perte de message**.

La matrice de référence est **`docs/guide-codage-go.md` §16**. Ce n'est pas « §1.5 » : ce §1.5
(`plan-execution-passerelle.md`) ne documente que la readiness de `router-svc`.

## Périmètre (ce que fait CETTE PR)
- Couper un **vrai** Redis et vérifier chaque politique documentée — aucun test du dépôt ne le faisait :
  les scénarios « redis down » existants injectent tous un faux qui rend `errors.New("redis down")`,
  ce qui prouve la branche, pas la politique.
- Trois politiques + la readiness §1.5, chacune prouvée **dans le paquet qui la porte** (elles n'ont
  besoin que de Redis) ; le « zéro perte » est prouvé une fois, au niveau du routeur.
- Compléter la matrice §16 des **quatre** politiques Redis codées mais non documentées.

## Design arrêté

**La coupure : un `tcpproxy` devant le Redis de `redistest`** (`redistest.Cuttable` /
`CuttableConfig`), pas l'arrêt du conteneur. Ce n'est pas un pis-aller : le conteneur est partagé par
tout un paquet de tests et n'est délibérément pas exposé — l'arrêter déciderait du sort des tests
frères. Le proxy coupe **un seul client**. `Cut()` accepte puis ferme, donc l'échec est une socket
morte immédiate et non une attente de `DialTimeout` ; le client est bâti par `redisstore.NewClient`,
le constructeur de production, pour hériter des vrais délais plutôt que de délais de test.

**Les quatre vérifications :**

| Politique (§16) | Comportement attendu sous coupure | Où |
|---|---|---|
| Redis (rate-limit) | fail-closed : le débit reste **borné** par le plafond local du pod | `internal/pipeline/ratelimit` |
| Redis (anti-spam à état) | fail-open avec `flag` ; les règles de **contenu** continuent de bloquer | `internal/pipeline/antispam` |
| Redis (cache de solde) | fail-closed : erreur, aucune entrée de grand livre | `internal/billing` |
| Readiness §1.5 | `router-svc` reste *ready* ; *not ready* si Kafka est injoignable | `cmd/router-svc` |

**Le « zéro perte » se prouve par une réconciliation, pas par un total.** Chaque `message_id` soumis
doit reparaître **exactement une fois**, d'un seul côté : produit sur `mt.routed`, ou écrit au CDR en
`rejected` avec un code. Un total est satisfait par un message perdu compensé par un doublon — les deux
pires fautes possibles, qui s'annulent au comptage.

**L'assertion la plus subtile est le code d'erreur.** Refuser ne suffit pas : *comment* on refuse décide
du sort du message. `router.handle` branche sur `errs.CodeOf` — une erreur **codée** devient une ligne
CDR `rejected` et **commite** l'offset ; une erreur **non codée** est une faute transitoire, l'offset
reste non commité et le message est redélivré. Donner un code à l'indisponibilité de Redis rejetterait
définitivement tout message en vol pendant un hoquet. C'est la perte que le critère interdit, et aucune
assertion `err != nil` ne la verrait.

## Tests (écrits dans la même PR)
- `TestRateLimitFallsBackToTheLocalCeilingWhenRedisIsCut` — admet exactement la capacité, puis refuse.
- `TestAntispamFlagsInsteadOfBlockingWhenRedisIsCut` — le doublon **bloque** avant la coupure (contrôle),
  passe `flag` après ; la règle de contenu bloque toujours.
- `TestReserveFailsClosedWhenRedisIsCut` — erreur **non codée**, grand livre intact, rejeu facturé une fois.
- `TestRouterReadinessFollowsTheFailurePolicy` — `/readyz` 200 Redis coupé, 503 Kafka injoignable.
- `TestRedisOutageAccountsForEveryMessage` — réconciliation identifiant par identifiant.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté
- [ ] chaque politique documentée vérifiée sur un Redis réellement coupé ; zéro perte de message
- [ ] la matrice §16 complétée des politiques Redis qu'elle omettait

## Hors périmètre
Flapping connecteur → step-250b. Drain de pods/PDB + failover Postgres → step-260. Sécurité → step-290+.
Le volet « **et testée** » du [MUST] §16 pour les quatre politiques neuves : fiche à ouvrir.
