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
- Ajouter la construction de l'image au workflow, avant `go test`.
- **Puis réparer ce que ça révèle.** C'est la moitié non bornée, et c'est elle qui justifie une fiche à
  part plutôt qu'un ajout à step-250b : ces tests n'ont jamais tourné en CI, donc certains peuvent être
  rouges, lents, ou instables sous la contention d'un runner partagé.

## Points d'implémentation clés
- Le temps de build de l'image s'ajoute à chaque exécution : mesurer, et mettre en cache si la durée du
  job double.
- Un test qui saute doit rester **visible**. Un saut silencieux est ce qui a permis à ce trou de durer :
  envisager une garde qui échoue si `smscsim` saute alors qu'on est en CI (`CI=true`), plutôt que de
  compter sur une relecture du workflow.
- Ne pas confondre avec le tag `loadref`, qui isole délibérément les mesures longues : ici les tests sont
  censés tourner, et personne ne l'a remarqué.

## Tests (écrits dans la même PR)
- La garde « en CI, un saut de `smscsim` est une erreur » — c'est le test qui empêche la régression de
  revenir, et le seul qui puisse échouer pour la bonne raison aujourd'hui.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] les 10 tests s'exécutent réellement en CI, et sont verts
- [ ] un saut de `smscsim` en CI échoue au lieu de passer

## Hors périmètre
Écrire de nouveaux tests de résilience (step-250b, livrée). Le drain de pods → step-260.
