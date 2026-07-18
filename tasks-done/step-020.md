# step-020 — Proto SessionRegistry + génération du code gRPC

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-022, step-046

## But
Poser le contrat gRPC du registre de sessions (`api/proto/session.proto`) et la chaîne de génération, socle des services de sessions (session-manager, smpp-server, mo-dlr-router).

## Périmètre (ce que fait CETTE PR)
- Créer `api/proto/session.proto` : service `SessionRegistry` avec `Bind`, `Unbind`, `Lookup`, `Deliver` (RPC vers le pod détenteur), messages associés (`account_id`, `pod_id`, `bind_id`, `bind_type`, `system_id`, PDU `deliver_sm` sérialisé).
- Ajouter la cible `make proto` (protoc ou buf) + `make tools` installant `protoc-gen-go`/`protoc-gen-go-grpc` (§1.3).
- Générer le code sous `internal/session/pb` (§1.7 : protos dans `api/proto/`, généré sous `internal/…/pb`).

## Points d'implémentation clés
- **`ctx7` obligatoire avant d'ajouter** `google.golang.org/grpc` et `google.golang.org/protobuf` (§1.2) : récupérer versions et API à jour, ne rien deviner.
- `package` proto et `go_package` alignés sur le module `github.com/martialanouman/go-gateway` (§1.1).
- `Deliver` transporte un `deliver_sm` déjà encodé par `internal/smpp` (le registre ne parle pas SMPP) : champ `bytes pdu` + `bind_id` cible.
- `bind_type` reflète `allowed_bind_types` (`tx`/`rx`/`trx`) du schéma (`control_plane.smpp_accounts`).
- Le code généré est commité (pas de génération en CI bloquante) ; documenter la commande de régénération.

## Tests (écrits dans la même PR)
- Test de compilation : le paquet `internal/session/pb` build et s'importe.
- Test léger de round-trip proto (marshal/unmarshal d'un `DeliverRequest`) pour figer la stabilité du contrat.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] `make proto` régénère un diff vide à partir du `.proto` commité

## Hors périmètre
Implémentation du serveur/registre (step-021, step-022) ; usage de `Deliver` par la voie retour (step-046, M4).
