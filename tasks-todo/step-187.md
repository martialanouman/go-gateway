# step-187 — Export de messages asynchrone (row-cap, MSISDN masqué)

> **Jalon :** M11 (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-186 · **Bloque :** —

## But
Produire un export de messages asynchrone, plafonné en lignes (row-cap) et **masqué** (MSISDN selon
le rôle, aucun corps), via `create-message-export` / `get-message-export`.

## Périmètre (ce que fait CETTE PR)
- `api/openapi-admin.yaml` + `internal/adminapi` : `create-message-export` (async, renvoie un job) et
  `get-message-export` (statut + lien/fichier).
- Worker d'export : lecture CDR bornée, application du masquage, écriture d'un fichier.
- Collection Admin synchronisée.

## Points d'implémentation clés
- **Asynchrone + row-cap** (§15) : un export ne balaie jamais l'historique sans borne ; job avec statut.
- **Masque** : MSISDN masqué par rôle (règle partagée step-186) ; **aucun corps** dans l'export (invariant a).
- Le fichier produit vit hors chemin chaud ; la destination objet réelle reste infra (comme le tiering).
- Job traçable (`get-message-export`) : statut, compteur de lignes, échéance.

## Tests (écrits dans la même PR)
- `create-message-export` → job ; `get-message-export` → fichier masqué produit.
- Row-cap respecté ; MSISDN masqué ; aucun corps dans le fichier.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (a)** respecté (fichier sans corps)
- [ ] export async + row-cap + masquage testés ; collection synchronisée

## Hors périmètre
Fin de M11. Durcissement/charge/prod → M12.

---

## Design arrêté (2026-08-01)

### D1 — Un scope dédié : `cdr:export_bulk`
Septième constante de `internal/auth`, exigée par **les deux** opérations. Exporter 100 000 lignes
n'est pas lire une page : `admin:read` le donnerait à tout lecteur, `admin:write` le donnerait à qui
provisionne des clients et le refuserait à un support en lecture seule — l'inverse du besoin.
`get-message-export` l'exige aussi, parce que le statut porte l'URL de l'artefact.

### D2 — Deux formats : `csv` et `jsonl`
Parquet quitte l'enum du contrat. Le tiering (step-165) laisse ClickHouse écrire son Parquet
côté serveur ; ici c'est impossible — **le masquage est une règle Go**, donc le fichier est écrit par
ce process. Ajouter un encodeur colonnaire pour un besoin non exprimé serait une dépendance à tenir.
Le contrat ne doit pas promettre ce qui n'existe pas.

### D3 — `mask_msisdn: false` exige `msisdn:reveal`, sinon 403
Refuser, jamais masquer en silence : un opérateur qui demande des numéros en clair et reçoit un
fichier masqué sans le savoir en tirerait de fausses conclusions. C'est le point de sécurité de la
step — sans lui, le drapeau contourne tout le masquage par rôle.

### D4 — Les filtres sont TYPÉS, `additionalProperties: false`
`MessageExportRequest.filters` était un objet libre. Il reprend les prédicats de `search-messages`,
`from_date`/`to_date` requis, fenêtre ≤ 31 jours. Un filtre inconnu est **refusé**, pas ignoré :
ignoré, il élargirait l'export au lieu de le restreindre. Rupture de contrat → bump majeur.

### D5 — Row-cap 100 000 : le job ÉCHOUE, il ne tronque pas
Au-delà du plafond, le job passe `failed` avec un message qui dit de restreindre la fenêtre. Un
export tronqué en silence est pire qu'un export refusé : l'opérateur croit tenir l'exhaustif. Le
contrat gagne donc un champ `error` sur `ExportJob` (ajout, non rupturant) — un `failed` sans raison
n'est pas exploitable.

### D6 — Destination fichier, l'objet réel reste infra
Le worker écrit dans un répertoire configuré (`EXPORT_DIR`) et `download_url` porte l'URI
correspondante, comme `FileDestination` du tiering. Répertoire non configuré → 503 à la création
(la capacité est absente du déploiement, ce n'est pas la faute de l'appelant).

### D7 — Échéance annoncée, purge non implémentée
`expires_at` = création + 24 h. La suppression effective de l'artefact appartient à l'infra, comme
la rétention froide. **Dette explicite**, pas un oubli : le job annonce quand l'artefact n'est plus
garanti, rien ne le supprime encore.

## Découpage

1. Table `control_plane.message_export_jobs` + migration 0013 + repo Postgres.
2. Scope `cdr:export_bulk`.
3. Contrat : filtres typés, `error` sur ExportJob, enum de format réduit, bump majeur, collection,
   `m1Operations`.
4. Handler + worker (`async.Runner`, contexte de bookkeeping détaché comme le job RGPD) + écrivains
   csv/jsonl réutilisant `maskAddresses`.
5. Câblage (`EXPORT_DIR`), DoD, PR.
