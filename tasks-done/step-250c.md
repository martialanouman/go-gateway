# step-250c — Faire tourner la suite de résilience M8 en CI

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## But
**Dix fonctions de test ne s'exécutent jamais en CI.** Elles ne sont pas rouges : elles sautent, et un
saut se lit comme un succès. Toute la suite de résilience M8 est dans ce cas.

## Le constat, vérifié
`.github/workflows/ci.yml` (job `Test (race)`) lance `go test -race -timeout 10m ./...` sans aucune étape
`make smsc-sim`. Or `smscsim.Launch` (`internal/testutil/smscsim/smscsim.go:59`) saute explicitement
quand l'image `smsc-simulator:dev` est absente — et elle ne vit dans aucun registre.

Les tests concernés (11 sites d'appel à `smscsim.Launch`) :

| Fichier | Tests |
|---|---|
| `internal/connectorpool/sim_smoke_test.go` | 1 |
| `internal/connectorpool/sim_bindpool_test.go` | 1 |
| `internal/connectorpool/sim_fallback_test.go` | 1 (dont `TestSimBreakerFallbackParkReplay`) |
| `internal/connectorpool/sim_reconnect_test.go` | 3 |
| `internal/connectorpool/sim_scenarios_test.go` | 1 (agrégat de disjoncteur cross-pod) |
| `internal/testutil/smscsim/` | 3 (les tests du simulateur lui-même) |

## Périmètre (ce que fait CETTE PR)
- Construire l'image dans le workflow, avant `go test`.
- **Une garde générale** : en CI, un test d'intégration qui saute échoue.
- Épingler le simulateur.

## Design arrêté

**La moitié « réparer ce que ça révèle » est vide.** La version initiale de cette fiche l'annonçait non
bornée et en faisait la justification d'une fiche séparée. Vérifié avant d'écrire une ligne : **les dix
tests passent tous**, 53 s sous `-race` contre l'image épinglée. Il n'y avait rien à réparer.

**La garde est générale, pas limitée au simulateur.** En la concevant, un cas plus grave est apparu : si
Docker tombait sur le runner, `redistest`/`pgtest`/`kafkatest`/`chtest` sauteraient TOUS —
`testcontainers.SkipIfProviderIsNotHealthy` ne sait que sauter — et la CI resterait verte. Toute la
couverture d'intégration disparaîtrait en silence ; `smscsim` n'était qu'une instance.

`internal/testutil/ciguard` porte la règle : **hors CI sauter est correct** (un portable sans Docker doit
pouvoir lancer `make test`), **en CI sauter est un mensonge**. `RequireDocker` remplace la fonction de
testcontainers en refaisant ses deux contrôles (`ProviderDocker.GetProvider`, puis `provider.Health`) et
en routant le verdict par la garde.

La décision est une fonction pure et le corps prend une petite interface (`Helper`/`Skipf`/`Fatalf`) :
sans cela on ne peut asserter ni le saut ni l'échec, un vrai `*testing.T` ne pouvant rapporter qu'il a
été sauté sans sauter aussi le test qui l'observe.

**`SMSC_SIM_REF` passe de `main` à `v0.7.0`.** Avec `main`, la CI de ce dépôt pouvait virer au rouge
parce qu'un AUTRE dépôt avait bougé, sans qu'une ligne ici l'explique. Le mécanisme d'épinglage existait
déjà et le `make help` le documentait ; seul le défaut ne l'était pas.

**Coût mesuré, et une contrainte qui n'avait pas été anticipée.** Construction de l'image 42 s, tests
+53 s — mais la première exécution CI a échoué, deux fois de suite, sur un Redpanda mort au démarrage
(`exit 139`, SIGSEGV) dans `storage/kafkaprovision`, alors qu'un autre paquet avait démarré le sien sans
problème dans la même exécution.

Ni l'image ni le code : la place manquait. `go test ./...` lance les paquets en parallèle et **six
d'entre eux démarrent chacun leur propre Redpanda**, d'autres une ClickHouse, un Postgres, un Redis.
Cette step ajoute cinq conteneurs simulateur et allonge `connectorpool` à 57 s, donc son chevauchement
avec les paquets Kafka ; celui qui démarre en dernier tape le plafond.

D'où **`-p 2`** sur le job : borner le nombre de PAQUETS simultanés, pas les tests. Rien n'est exclu ni
sauté ; les conteneurs ne sont simplement plus tous détenus au même instant. Le module redpanda plafonne
déjà chaque instance (`--smp=1 --memory=1G`) : le problème est leur **nombre**, pas leur taille.

Job final : **6,8 min** (contre 3,9 avant), pour un `-timeout 10m` par paquet. Mettre l'image en cache
reste inutile tant qu'on est sous ~8 min.

## Tests (écrits dans la même PR)
- `ciguard` : le saut reste un saut hors CI, devient un échec en CI, la variable lue est la bonne, et un
  provider en panne est nommé dans le message.
- **La vérification décisive n'est pas un test Go** : image retirée, `CI=1` → échec nommant l'image
  manquante et la commande qui la construit ; sans `CI` → saut. Les deux constatés.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [x] les 10 tests s'exécutent réellement en CI, et sont verts
- [x] un saut d'intégration en CI échoue au lieu de passer — pour les cinq helpers, pas le seul simulateur

## Hors périmètre
Écrire de nouveaux tests de résilience (step-250b, livrée). Le drain de pods → step-260.
