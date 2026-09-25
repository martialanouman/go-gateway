# step-350 — Réécriture de sender ID (§6.16) : ni l'admin, ni l'évaluation

> **Jalon :** Surfaces Admin déclarées au contrat, jamais construites (§6.16 `docs/specification-technique-passerelle-sms.md`) · **Statut :** LIVRÉE (PR1 #210, PR2)
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

## Design arrêté — PR1

Arbitrages : la spec (§6.16) ne tranche aucun des points ci-dessous ; Fable les a tranchés sans
contredire la spec, validés par l'humain le 2026-09-24.

1. **`direction='mo'` refusé à la création (422).** PR2 ne sert que `mt` ; une règle que personne
   n'évalue ment à l'opérateur. `direction` est absent de `SenderRewriteRuleUpdate`, donc immuable.
   La réécriture MO (normalisation reply-to, §6.16) devient une fiche `debts/`.
2. **Motifs : regex RE2 en correspondance totale** (PR2 ancre en `^(?:p)$`), compilés au bord (422).
   NULL = tout. Le préfixe de chiffres des routes (`internal/routing/snapshot.go:160`) ne couvre pas
   un sender ID alphanumérique ; l'écart de lecture d'un champ homonyme est écrit dans la
   `description` du contrat.
3. **Requis par type, validés au bord sur l'état FUSIONNÉ** (Get + patch au PATCH, `rewrite_type`
   étant modifiable) : `static` → `rewrite_to` non vide ; `fallback_pool` → tableau non vide de
   chaînes non vides ; `truncate` → `max_length` ; `sanitize` → `sanitize_charset_json` NULL
   (défaut `[A-Za-z0-9]`) ou `{"allowed": "<caractères conservés>"}` non vide, les autres caractères
   étant supprimés. Les champs sans objet pour le type sont **ignorés**, pas rejetés : sous COALESCE,
   les rejeter interdirait tout changement de type.
4. **Contrat : schémas inchangés**, contraintes conditionnelles au bord et dans `description` ;
   ajout de `security:` (`admin:read` / `admin:write`) et des 401/403/404/422 manquants → bump
   **mineur** 6.0.0 → 6.1.0.
5. **Précédents repris** : PATCH par COALESCE (la dette `patch-null-ne-peut-pas-effacer-un-champ`
   est étendue ; pour un motif, `.*` ≡ NULL) ; `created_by` NULL (pas d'identité opérateur avant
   step-310) ; `scope_id` non vérifié contre la table visée (précédent antispam) ; appariement
   `platform ⇔ scope_id NULL` validé au bord (422).
6. **Ordre de la liste = ordre d'évaluation du pool** : portée (`connector`, `smpp_account`,
   `customer`, `platform`), puis `priority` (plus bas d'abord), puis `id`. PR2 réutilise la requête.

## Design arrêté — PR2

Arbitrages : la spec (§6.16) fixe l'emplacement et la précédence, rien d'autre ; Fable a tranché D1-D8
(deux corrections, un défaut évité), validés par l'humain le 2026-09-24.

1. **Moteur partagé** `internal/pipeline/senderrewrite` : instantané immuable des règles **actives**,
   chargé par la requête de PR1 (filtre `status` en Go ; le moteur trie lui-même chaque portée par
   priority puis id, sans dépendre de l'ordre de son appelant). Évaluation
   `connector` (celui de `routed.ConnectorID`, jamais `deps.ConnectorID` qui vaut `uuid.Nil` sur un pool
   non filtrant) → `smpp_account` → `customer` → `platform` ; la première règle dont les motifs
   correspondent gagne, même si sa sortie est identique. Regex stockée invalide → règle ignorée + WARN au
   build. Sortie vide (sanitize qui retire tout) → l'original, la règle compte comme correspondue.
2. **Forme des adresses** : motif sender contre `routed.From` **verbatim** (jamais normalisé ; c'est ce
   que §6.19 compare) ; motif dest contre `routed.To`, **chiffres seuls, sans `+`** (`e164.Normalize`).
   La sortie repasse par `sourceAddr()` pour le TON/NPI. Le contrat le dit.
3. **`fallback_pool`** : `pool[fnv32a(message_id) % n]` — même sender pour tous les segments, stable sous
   redélivrance et reroute, aucun état partagé. Distribution statistique, pas un tourniquet strict ; le
   contrat le dit.
4. **Pool** : réécriture après `preDispatch` (disjoncteur, reroute), juste avant `buildSubmit`.
   `routed` n'est **jamais** muté (reroute, dead-letter, drainer le réencodent) : une copie `sent` porte
   l'adresse réécrite vers la PDU, `submitOutcome` et `dlrmap`. `config.Watcher` sur
   `config.ChannelSnapshotInvalidation`, `atomic.Pointer` ; rebuild en échec → instantané courant gardé ;
   chargement en échec au boot → refus de démarrer.
5. **Traçabilité** : `OutcomeMT`/`outcomeWire` gagnent `original_from,omitempty`, renseigné seulement si
   l'adresse a changé ; la projection remplit `original_source_addr` (commentaire `outcome.go:145`
   corrigé). **Voie retour** : `dlrmap` garde aussi `original_source_addr,omitempty` ; `modlrrouter`
   l'utilise pour l'adresse client du reçu DLR (§6.16 : « sans que le client le sache » ; le webhook DLR
   ne porte aucune adresse) et
   le recopie sur la ligne `delivered`. Niveau message du CDR : `any(source_addr)` →
   `argMax(source_addr, status NOT IN ('accepted','rejected'))`. Compatible dans les deux sens pendant un
   déploiement progressif : aucun décodeur ne refuse une clé inconnue, et un champ absent se lit vide.
6. **`test-sender-rewrite-rule`** : la règle `{id}` seule, même `disabled`, même code que le pool, sans
   écriture. Corps : `message_id` **optionnel** (absent ⇒ `uuid.Nil`) pour reproduire le choix du pool.
   `security: admin:read`, 401/403/404/422, bump mineur 6.2.0.
7. **Débit** : `BenchmarkRewrite` (0 règle / 5 règles, ns/op). La mesure step-201f
   (`TestPoolSubmitCeiling`) est **déclarée à relancer** — ligne dans step-280 et dans l'en-tête de
   step-201f. Pas de campagne A/B maintenant (bruit ±30 % sur cet hôte).
8. `mo` reste refusé (dette existante). Aucune métrique neuve : le CDR trace chaque réécriture.

**Révisions de revue (arbitrées par Fable, 2026-09-24)** :
- **B1 — l'API publique rend l'expéditeur soumis.** Le niveau message du CDR porte l'adresse envoyée ;
  `GET /messages` rend `original_source_addr` s'il existe. §6.16 fait foi contre la description « actually
  used on the wire » du contrat public, écrite avant toute réécriture et corrigée (description seule).
- **B2 — 20 octets au plus** pour `rewrite_to` et chaque entrée du pool (`source_addr` SMPP) : 422 au
  bord, sur l'état fusionné ; une règle stockée qui dépasse est ignorée au build avec un WARN, comme une
  regex invalide — jamais par message. Aucun `maxLength` au schéma (rupture). `truncate` inchangé : il ne
  fait que raccourcir.
- **B3 — « un sender refusé à l'ingestion part quand même »** est une propriété de l'ordre des paquets :
  une garde d'imports (le routeur n'atteint pas `senderrewrite`, le pool n'atteint pas `senderid`).
- **M1 — la recherche par expéditeur** filtre aussi `original_source_addr`, sinon chercher l'original
  d'un message réécrit ne garde que la ligne `accepted` et rend un statut faux.
- **Multipart — l'expéditeur est épinglé par message** (demande humaine, arbitrage Fable) : sans cela,
  un segment rerouté seul vers un connecteur aux règles différentes, ou soumis après un rechargement,
  part sous un autre expéditeur, et le combiné ne réassemble pas le SMS — livré, facturé, illisible.
  Pour un message de plus d'un segment, le premier `submit_sm_resp` OK écrit `rewrite:{message_id}`
  (`SET NX`, TTL de la table DLR) ; les segments suivants, sur ce connecteur ou un autre, lisent l'épingle
  au lieu de réévaluer. La cohérence du réassemblage l'emporte sur les règles du connecteur de secours :
  un segment qu'il refuse est un échec visible (CDR `failed`, DLR), un SMS illisible ne l'est pas. Un
  message d'un seul segment ne touche pas Redis. Redis injoignable : fail-open (règles locales + WARN),
  comme la table DLR. Fenêtre résiduelle nommée : deux segments soumis au même instant sur deux
  connecteurs peuvent tous deux lire « absent ».
- **Le refus de démarrer** sans règles (et sans limites de débit, même cas voisin) est prouvé par un rôle
  Postgres privé de la seule table en cause.

## Tests

- Précédence : deux règles de portées différentes correspondent au même message ; la plus spécifique
  gagne. Muter l'ordre de tri doit faire tomber le test.
- L'original reste sur le CDR après réécriture.
- L'anti-spam et l'autorisation voient l'adresse **d'origine** : un message dont le sender réécrit
  serait refusé à l'ingestion doit quand même partir. C'est la propriété qui distingue §6.16 de §6.19.
- `test-sender-rewrite-rule` n'écrit rien : vérifié par l'état de la base après appel.

## Definition of Done

Une DoD par PR — chacune doit être atteignable seule.

**PR1** — livrée (branche `step-350-sender-rewrite-crud`)
- [x] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [x] les 4 opérations de CRUD servies ; `test-sender-rewrite-rule` toujours en `deferred`, avec sa raison
- [x] `api/collections/admin-api.yaml` synchronisée ; les 4 lignes retirées de `deferred`
- [x] aucun changement du chemin d'envoi

Revue : 3 axes puis un 2ᵉ tour sur les correctifs. Deux bloquants, tous deux dans les tests : l'ordre
d'évaluation n'était pas prouvé (sans règle `smpp_account`, un tri alphabétique passait), et le PATCH
n'était vérifié que sur trois champs. Coupe : un test de câblage isolé, un défaut de priorité manuel, un
commentaire qui niait une course réelle — nommée depuis par un `ponytail:` (Get puis Update sans verrou).
Ce que PR2 hérite : le charset est une liste littérale (`"A-Z"` garde trois caractères) ; `rewrite_to` et
les entrées du pool ne sont pas bornés à la longueur d'un `source_addr` SMPP.

**PR2** — livrée (branche `step-350-pr2-rewrite-engine`)
- [x] `make check` vert
- [x] évaluation câblée dans connector-pool avec l'ordre §6.16 respecté ; `original_source_addr` renseigné
- [x] `test-sender-rewrite-rule` servi, et il répond ce que le pool ferait
- [x] aucun invariant (a/b/c/d) violé ; la 5ᵉ ligne retirée de `deferred`
- [x] l'effet sur le débit du pool est mesuré, ou la mesure de step-201f est explicitement déclarée à
      relancer — `BenchmarkRewrite` : ~8 ns sans règle, ~150 ns et 1 allocation au pire sur 5 règles ;
      `TestPoolSubmitCeiling` déclaré à relancer (step-280, en-tête de step-201f)

Revue : 3 axes puis un 2ᵉ tour sur les correctifs. Trois bloquants, tous corrigés et arbitrés par Fable
(« Révisions de revue » ci-dessus) : l'API publique montrait l'adresse réécrite au client ; rien ne bornait
une adresse réécrite aux 20 octets de `source_addr`, et une seule règle pouvait ouvrir le disjoncteur d'un
connecteur ; la propriété §6.19 n'avait aucun test — elle est gardée par `go list -deps` sur les deux
binaires. « N'écrit rien » est vérifié par un compteur d'écritures sur le store, pas par l'état d'une
base : le handler ne voit que l'interface.

## Hors périmètre

L'optimisation du chemin d'envoi (step-201f et sa suite). Les règles de réécriture MO si PR2 se limite à
`mt` — auquel cas la fiche le dit explicitement et `direction='mo'` reste refusé à la création.
