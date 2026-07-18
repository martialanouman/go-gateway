# step-140 — Poser le contrat gRPC billing + l'outillage protoc

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-141, step-144

## But
Livrer le premier service gRPC du dépôt : `api/proto/billing.proto` et l'outillage de
génération (`make proto`), de sorte que `internal/billing/pb` compile. Aucune logique métier — le
socle sur lequel `billing-svc` et ses clients se grefferont.

## Périmètre (ce que fait CETTE PR)
- `api/proto/billing.proto` : service `Billing` avec `Reserve`, `Capture`, `Release`, `GetBalances`,
  `RecordMO` (messages request/response, `message_id`, `owner_type`, `owner_id`, `direction`, `credits`).
- Outillage : cible `make proto` (protoc **ou** buf) + `protoc-gen-go`/`protoc-gen-go-grpc` ajoutés à
  `make tools` (§1.3). Code généré sous `internal/billing/pb` (§1.7).
- `go.mod` : promouvoir `google.golang.org/grpc` et `google.golang.org/protobuf` en dépendances directes.

## Points d'implémentation clés
- **`ctx7` obligatoire** avant d'ajouter/figer les versions de `grpc`, `protobuf`, `protoc-gen-go`,
  `protoc-gen-go-grpc` : ne pas deviner. Récupérer la doc à jour et les bons numéros.
- Contrat gRPC = source de vérité de l'API billing ; les champs `direction` (`mt`/`mo`),
  `owner_type` (`customer`/`smpp_account`) reflètent `db/schema_passerelle_sms.sql` (tables `balances`,
  `billing_ledger`) — mêmes énumérations.
- `Reserve`/`Capture`/`Release` portent tous `message_id` : c'est la **clé d'idempotence** (invariant c).
- Port métier `billing-svc` = 7001 (§1.4) ; documenté dans le proto en commentaire, pas encore servi ici.

## Tests (écrits dans la même PR)
- Test de compilation implicite : `internal/billing/pb` build (couvert par `make build`).
- Test unitaire trivial : instancier les messages générés (garde que la génération est à jour et versionnée).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] `make proto` reproductible ; code généré commité ; versions figées via `ctx7`

## Hors périmètre
Serveur gRPC (step-144), repos Postgres (step-141), scripts Lua (step-142). Aucune intégration
router/connector ici.
