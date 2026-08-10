# step-216 — Réécriture de sender ID (§6.16) : ni l'admin, ni l'évaluation

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-213 (triage) · **Bloque :** —

## But

Construire la réécriture d'adresse source décrite en §6.16 : **deux volets**, la surface d'administration
des règles et leur évaluation dans `connector-pool-svc` juste avant l'envoi. C'est la seule des sept
surfaces manquantes qui **touche le chemin chaud** — à découper en deux PRs.

| Opération | Méthode et chemin |
|---|---|
| `list-sender-rewrite-rules` | `GET /admin/sender-rewrite-rules` |
| `create-sender-rewrite-rule` | `POST /admin/sender-rewrite-rules` |
| `update-sender-rewrite-rule` | `PATCH /admin/sender-rewrite-rules/{id}` |
| `delete-sender-rewrite-rule` | `DELETE /admin/sender-rewrite-rules/{id}` |
| `test-sender-rewrite-rule` | `POST /admin/sender-rewrite-rules/{id}/test` |

## Le constat

`control_plane.sender_id_rewrite_rules` est en base, complète : portées, types, priorité, motifs de
correspondance, contraintes de cohérence (`platform ⇒ scope_id NULL`, `static ⇒ rewrite_to NOT NULL`) et
son index `(scope, scope_id, priority) WHERE status = 'active'`. Le modèle sqlc est même déjà généré
(`ControlPlaneSenderIDRewriteRule`) — inutile de le régénérer. La spec décrit la sémantique. Mais **ni
repo, ni admin, ni évaluation** n'existent : la §6.16 est le seul cas où le
chemin critique lui-même est amputé : `docs/specification-technique-passerelle-sms.md` décrit
connector-pool comme évaluant la réécriture avant l'envoi, et il ne l'évalue pas.

## Périmètre — deux PRs

**PR1 — repo + admin.** Les 5 opérations, sur le modèle des autres CRUD admin (`routes.go`,
`sender_ids.go`). `test-sender-rewrite-rule` évalue une règle contre un échantillon **sans écrire** :
c'est le seul endroit où un opérateur peut vérifier une règle avant de la publier.

**PR2 — évaluation dans connector-pool.** L'étage manquant du chemin chaud.

## Points d'implémentation clés

- **Où, exactement** (§6.16) : dans `connector-pool-svc`, **après** la résolution du connecteur et
  **après** que l'anti-spam et le routage ont évalué le sender ID **original**. La réécriture est une
  décision du fournisseur, pas une revendication du client : elle **n'est pas ré-autorisée** par §6.19.
  L'inverser — réécrire avant l'autorisation — ferait contourner silencieusement une étape de
  conformité, dans le même esprit que l'invariant (b) sans en être un cas.
- **L'original est préservé sur le CDR** (`original_source_addr`). Une réécriture qui écrase la trace
  rend l'incident client indiagnosticable.
- **Précédence `connector → account → customer → platform`, première correspondance gagnante.** Le tri
  se fait sur la portée d'abord, `priority` ensuite. Un tri sur la seule `priority` donnerait un résultat
  plausible et faux, et aucun test à une seule règle ne le verrait : la fixture doit porter **au moins
  deux portées concurrentes**.
- **Quatre types** : `static`, `fallback_pool` (round-robin), `truncate`, `sanitize`. Le round-robin est
  un état partagé entre pods — trancher explicitement où il vit (Redis atomique, ou déterministe par
  hachage du message) plutôt que de laisser un compteur local produire une distribution différente par
  pod.
- **Chemin chaud** : la résolution doit lire un instantané en mémoire, jamais la base par message. Le
  patron existe (`internal/pipeline/senderid` `LoadSnapshot`, watcher de config) — le réutiliser, et
  mesurer le coût ajouté avant de conclure quoi que ce soit sur le débit (step-201f mesure le pool).
- La colonne `direction` autorise `mo` : décider si PR2 le couvre ou si `mt` seul est servi, et l'écrire.

## Tests

- Précédence : deux règles de portées différentes correspondent au même message ; la plus spécifique
  gagne. Muter l'ordre de tri doit faire tomber le test.
- L'original reste sur le CDR après réécriture.
- L'anti-spam et l'autorisation voient l'adresse **d'origine** : un message dont le sender réécrit
  serait refusé à l'ingestion doit quand même partir. C'est la propriété qui distingue §6.16 de §6.19.
- `test-sender-rewrite-rule` n'écrit rien : vérifié par l'état de la base après appel.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] les 5 opérations servies ; l'évaluation câblée dans connector-pool avec l'ordre §6.16 respecté
- [ ] `original_source_addr` renseigné ; aucun invariant (a/b/c/d) violé
- [ ] `api/collections/admin-api.yaml` synchronisée ; lignes retirées de `deferred` (step-213)

## Hors périmètre

L'optimisation du chemin d'envoi (step-201f et sa suite). Les règles de réécriture MO si PR2 se limite à
`mt` — auquel cas la fiche le dit explicitement et `direction='mo'` reste refusé à la création.
