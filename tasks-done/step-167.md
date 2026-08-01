# step-167 — Extraire la garde des clés de contenu dans un service dédié (content-key-svc)

> **Jalon :** M10 (§14 `docs/plan-execution-passerelle.md`) · **Statut :** FAIT (ADR-0011)
> **Dépend de :** step-161, step-163, step-164 · **Bloque :** —

## But
Sortir la KMS et le cycle de vie des `content_keys` de `billing-svc` vers un service dédié
`content-key-svc` (gRPC :7002), pour que le dépositaire de la clé maître soit un service **minimal,
à faible surface et auditable**, sans rapport avec la comptabilité de crédits.

**Pourquoi cette déviation du plan.** §14 prescrit « `content_keys` hébergé par `billing-svc` », et
step-161 l'a suivi. La revue de conception d'alors n'a comparé cette option qu'à une pire (la KMS
dupliquée dans `admin-api-svc`) : l'option « service dédié » n'a jamais été évaluée. À l'usage elle est
meilleure, et le coût de la bascule ne fera qu'augmenter :

- **cohésion nulle** — facturation et chiffrement de contenu n'ont rien à voir ;
- **rayon d'explosion** — la clé maître vit aujourd'hui dans un process du chemin chaud (reserve/capture
  à 8 000 msg/s) à large surface de dépendances (Redis, provider de facturation HTTP, ledger) ;
- **couplage de disponibilité** dans les deux sens ;
- **conformité** — « dépositaire dédié, minimal, audité » est bien plus tenable devant un auditeur.

La bascule est peu coûteuse parce que le découpage existe déjà : `ContentKeyServer` ne dépend que de
`content.KMS` et d'une interface de store (aucun couplage facturation), `service ContentKeys` est déjà
distinct de `service Billing` dans le proto, et seuls **2 binaires** l'appellent (`admin-api-svc`,
`router-svc`).

## Périmètre (ce que fait CETTE PR)
- `docs/adr/` : **ADR** actant la déviation de §14 (contexte, options, décision, conséquences) ; mise à
  jour de la ligne §14 de `docs/plan-execution-passerelle.md` pour qu'elle cesse de dire l'inverse.
- `api/proto/contentkeys.proto` : extraire `service ContentKeys` + ses messages de `billing.proto`, avec
  son propre `go_package` (`internal/contentkeys/pb`) ; régénérer (`make proto`).
- `internal/contentkeys/` : déplacer le serveur (`internal/billing/contentkeys_grpc.go` + son test) ;
  aucune logique métier ne change.
- `cmd/content-key-svc/main.go` : squelette de service canonique (config, logger, tracing, pool Postgres,
  KMS, serveur gRPC :7002, port ops, superviseur). `loadContentKMS` y déménage.
- `cmd/billing-svc` : retirer la KMS, l'enregistrement `RegisterContentKeysServer` et `CONTENT_KMS_MASTER_KEY`.
- `internal/config` : section/adresse client `CONTENT_KEY_ADDR` (défaut `localhost:7002`) ; les appelants
  (`admin-api-svc`, `router-svc`) dialent ce service au lieu de `BILLING_ADDR`.
- Déploiement : entrée `.goreleaser.yaml`, port 7002 (7000 = session-manager, 7001 = billing).
  `docker-compose.yml` n'héberge que l'infra (Postgres/Redis/Kafka/ClickHouse), pas les services : rien à
  y ajouter — la prémisse initiale de cette step était fausse sur ce point.

## Points d'implémentation clés
- **Aucun changement de comportement** : ni la crypto, ni le cycle de vie des clés, ni les RPC. C'est un
  déplacement de frontière de process. Les tests des paquets `internal/` doivent suivre le déménagement
  quasiment inchangés — si l'un doit être réécrit, c'est le signe d'un couplage à corriger.
- **Aucune migration de données** : `control_plane.content_keys` ne bouge pas.
- **Séquence de bascule sans coupure** (à écrire dans le runbook de l'ADR) : (1) déployer
  `content-key-svc` avec le **même** `CONTENT_KMS_MASTER_KEY` — les deux process servent alors les mêmes
  clés ; (2) repointer les clients ; (3) retirer la KMS de `billing-svc`. La clé maître est
  transitoirement dans deux process pendant l'étape 1 : c'est borné, assumé et documenté.
- **La KMS ne doit plus figurer nulle part dans `billing-svc`** après la bascule (ni import, ni config).
- Le service dédié garde la surface minimale : Postgres + KMS + gRPC. Pas de Redis, pas de Kafka, pas de
  client HTTP.
- `ContentKeyStore` reste déclarée côté consommateur ; `*postgres.ContentKeyRepo` la satisfait sans
  modification.

## Tests (écrits dans la même PR)
- Les tests du serveur de clés (get-or-create, rotation, unwrap par key_id, `destroyed` refusé avant
  unwrap, crypto-shred) passent à l'identique après déménagement.
- `cmd/content-key-svc` : test de garde de configuration en production (comme les autres binaires).
- `admin-api-svc` / `router-svc` : la garde de config production exige `CONTENT_KEY_ADDR` explicite.
- Bout en bout inchangé : chiffrement au CDR (step-162) et lecture gardée (step-163) fonctionnent en
  dialant le nouveau service.

## Definition of Done
- [x] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [x] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [x] ADR écrit et §14 du plan mis à jour ; plus aucune référence KMS dans `billing-svc`
- [x] `.goreleaser.yaml` à jour ; séquence de bascule documentée (runbook de l'ADR, avec la fenêtre de
      rollback et la métrique à surveiller)

## Hors périmètre
Fournisseur KMS réel (AWS/GCP/Vault) — toujours une décision d'infra. Aucun changement des RPC, de la
crypto ou du schéma.
