# step-350 — Réécriture de sender ID (§6.16) : ni l'admin, ni l'évaluation

> **Jalon :** Surfaces Admin déclarées au contrat, jamais construites (§6.16 `docs/specification-technique-passerelle-sms.md`) · **Statut :** À FAIRE
> **Dépend de :** step-320 (triage), step-201f (PR2 seulement) · **Bloque :** —

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

**PR1 — repo + CRUD.** Les **4** opérations de CRUD, sur le modèle des autres CRUD admin (`routes.go`,
`sender_ids.go`). `test-sender-rewrite-rule` **n'en fait pas partie** : évaluer une règle contre un
échantillon exige le moteur d'évaluation, et le dupliquer dans PR1 pour le remplacer en PR2 est du
travail jeté. À la fin de PR1, cette cinquième opération reste donc dans `deferred`, avec pour raison
« attend le moteur de PR2 » — c'est exactement l'usage que step-320 prévoit pour cette liste.

**PR2 — moteur d'évaluation + câblage.** L'étage manquant du chemin chaud, **et**
`test-sender-rewrite-rule`, qui l'expose sans écrire : c'est le seul endroit où un opérateur peut
vérifier une règle avant de la publier, et il doit répondre exactement ce que le pool ferait.

**Ordre vis-à-vis de la mesure de débit.** PR2 ajoute un étage au chemin d'envoi. Elle ne doit pas
merger entre step-201f (qui attribue le plafond du pool) et step-280 (la campagne NFR) : step-280
mesurerait un pipeline différent de celui que step-201f a caractérisé, et le dimensionnement que
step-201f doit à step-270 deviendrait périmé sans que personne ne le voie. Soit PR2 attend step-280,
soit elle déclare invalider la mesure et fait relancer le banc.

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

Une DoD par PR — chacune doit être atteignable seule.

**PR1**
- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] les 4 opérations de CRUD servies ; `test-sender-rewrite-rule` toujours en `deferred`, avec sa raison
- [ ] `api/collections/admin-api.yaml` synchronisée ; les 4 lignes retirées de `deferred`
- [ ] aucun changement du chemin d'envoi

**PR2**
- [ ] `make check` vert
- [ ] évaluation câblée dans connector-pool avec l'ordre §6.16 respecté ; `original_source_addr` renseigné
- [ ] `test-sender-rewrite-rule` servi, et il répond ce que le pool ferait
- [ ] aucun invariant (a/b/c/d) violé ; la 5ᵉ ligne retirée de `deferred`
- [ ] l'effet sur le débit du pool est mesuré, ou la mesure de step-201f est explicitement déclarée à
      relancer

## Hors périmètre

L'optimisation du chemin d'envoi (step-201f et sa suite). Les règles de réécriture MO si PR2 se limite à
`mt` — auquel cas la fiche le dit explicitement et `direction='mo'` reste refusé à la création.
