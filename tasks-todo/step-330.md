# step-330 — Groupes de clients (§6.17) : la table existe, rien ne la remplit

> **Jalon :** Surfaces Admin déclarées au contrat, jamais construites (§6.17 `docs/specification-technique-passerelle-sms.md`) · **Statut :** À FAIRE
> **Dépend de :** step-320 (triage) · **Bloque :** step-380

## But

Servir les 7 opérations de groupes que `api/openapi-admin.yaml` déclare et qu'aucun handler
n'implémente. La segmentation par groupe est aujourd'hui **à moitié construite** : la table, la clé
étrangère, l'affectation à la création et les filtres de lecture existent — ce qui manque, c'est de
pouvoir **créer un groupe** et **changer une appartenance** après coup.

| Opération | Méthode et chemin |
|---|---|
| `list-customer-groups` | `GET /admin/customer-groups` |
| `create-customer-group` | `POST /admin/customer-groups` |
| `get-customer-group` | `GET /admin/customer-groups/{id}` |
| `update-customer-group` | `PATCH /admin/customer-groups/{id}` |
| `delete-customer-group` | `DELETE /admin/customer-groups/{id}` |
| `list-group-customers` | `GET /admin/customer-groups/{id}/customers` |
| `set-customer-group` | `PATCH /admin/customers/{id}/group` |

## Le constat

Ce qui existe déjà, servi : `control_plane.customer_groups` et son modèle sqlc généré,
`customers.group_id` en `ON DELETE SET NULL`, l'affectation **à la création**
(`customerCreateBody.GroupID`, `internal/adminapi/customers.go`), et **deux** filtres `?groupId=` —
`list-customers` et `list-smpp-accounts`.

Ce qui manque, exactement : **aucun groupe ne peut être créé par l'API**, donc les filtres et le champ
de création ne peuvent référencer qu'un UUID inséré à la main ; et **l'appartenance ne peut plus
changer** après la création du client. Le code le sait déjà : `customerUpdateBody` porte le commentaire
« group_id is absent (group membership has its own endpoint) » — cet endpoint est
`set-customer-group`, précisément l'une des 7 non implémentées.

**Ne pas « réparer » cela en ajoutant `group_id` à `customerUpdateBody`** : ce serait contredire une
décision de conception explicite, et créer un second chemin d'affectation.

## Points d'implémentation clés

- **La suppression est non destructive** (§6.17) : elle détache les clients (`group_id → NULL`) et ne
  supprime **jamais** un client ni ses comptes. Le `ON DELETE SET NULL` du schéma le garantit déjà côté
  base ; le handler ne doit pas le contredire par une cascade applicative.
- **Un groupe ne porte rien** : ni solde, ni quota, ni portée de configuration (routes, scripts,
  anti-spam, réécriture). Ce n'est pas un niveau d'héritage. Toute tentation d'y accrocher un réglage
  appartient à une autre fiche et à une révision de la spec.
- **Jamais sur le chemin critique, jamais un label Prometheus.** L'appartenance change ; le CDR porte
  `customer_id` et pas `group_id`. Un filtre par groupe se résout en `customer_id IN (...)` au moment de
  la lecture — c'est ce qui rend la ventilation exacte quand un client change de groupe.
- Structure **plate** : zéro ou un groupe par client, pas de hiérarchie. Le schéma ne porte pas de
  parent ; ne pas en inventer un.

## Design arrêté

### Le contrat change — la prédiction de cette fiche était fausse

Les 7 opérations ne portent **aucun bloc `security:`**. Le document en a un global
(`api/openapi-admin.yaml:31-32`, `OperatorBearer: []`), mais `auth.Middleware` ne lit que
`ctx.Operation().Security` et huma ne fusionne jamais le global dans l'opération. Conséquence
vérifiée : servir ces 7 sans `scopeSecurity(...)` les rendrait **publiques** — créer, supprimer et
réaffecter des clients sans token. La piste d'audit, qui lit le principal posé par ce middleware,
enregistrerait en prime des mutations ne nommant personne.

Mettre les scopes dans le code sans toucher au contrat serait le « mensonge publié » que le dépôt a
déjà refusé en **step-149** : *un endpoint sécurisé qui cache ses échecs d'auth publie un mensonge ;
l'intention du contrat — la cohérence — est la source de vérité, pas ses coquilles.* Les 7 sont dans
la situation des 10 opérations billing d'alors : un bloc pré-déclaré avant que la convention
`security` par opération ne se généralise, jamais relu.

| Opération | `security` | Codes ajoutés | Pourquoi ce code |
|---|---|---|---|
| `list-customer-groups` | `admin:read` | 403, 422 | `?status=` hors `active\|archived`, validé comme `list-customers` |
| `create-customer-group` | `admin:write` | 403 | — |
| `get-customer-group` | `admin:read` | 401, 403, 422 | `Id` est `format: uuid` → huma répond 422 avant le handler |
| `update-customer-group` | `admin:write` | 401, 403, 409 | `name` est `UNIQUE` → renommage en collision |
| `delete-customer-group` | `admin:write` | 401, 403, 422 | uuid malformé |
| `list-group-customers` | `admin:read` | 401, 403, 422 | uuid malformé, `cursor`/`limit` invalides |
| `set-customer-group` | `admin:write` | 401, 403, 422 | uuid malformé **et** FK vers un groupe inexistant |

Additif : `oasdiff breaking` sur cette révision complète sort **0 ERR** (règles déclenchées :
`api-security-added`, `response-non-success-status-added`, toutes INFO). Donc **bump mineur**
`api/package.json` 4.2.1 → 4.3.0, comme 2.0.0 → 2.1.0 en step-149.

Pas de 409 sur `delete` : le `ON DELETE SET NULL` ne bloque rien. Le 422 de `set-customer-group`
tranche « groupe inexistant ? » : `internal/storage/postgres/pgerr.go:37-40` traduit déjà une
violation de FK en 422 et l'assume en commentaire — donc **aucun pré-contrôle d'existence du groupe**,
donc aucune fenêtre de course entre le contrôle et l'écriture.

### La garde : « pas de `security:` » ne doit plus vouloir dire « public »

Les steps 340→390 héritent du même bloc pré-déclaré sans `security:` — le piège se retendrait six
fois. Un test dans `contract_test.go` exige que **toute opération générée porte un `security` non
vide**. Le middleware n'est pas touché : le passer en fail-closed change un comportement global et
mérite sa propre justification, hors du périmètre de cette step.

### Ce qui s'écrit

| Fichier | Rôle |
|---|---|
| `internal/controlplane/customergroup.go` | `CustomerGroup`, `NewCustomerGroup`, `CustomerGroupPatch`, `CustomerGroupFilter` |
| `internal/controlplane/enums.go` | `CustomerGroupStatus` + `Valid()` — **pas** de `Rank()` : ce n'est pas un cycle de vie restrictif |
| `internal/storage/postgres/queries/customer_groups.sql` | create · get · list · update (COALESCE partiel) · delete (`:execrows`) |
| `internal/storage/postgres/customer_groups.go` | `CustomerGroupRepo` **sans pool** (aucune transaction), sur le modèle de `ExternalBillingProviderRepo` |
| `internal/adminapi/customer_groups.go` | DTO + les 6 opérations `/admin/customer-groups` |
| `internal/adminapi/customers.go` | `set-customer-group` **ici**, pas avec les groupes : il mute `customers`, son chemin et son tag sont `Customers` |
| `deps.go` · `api.go` · `wiring.go` | une ligne chacun |

**Décisions.** `set-customer-group` passe par une méthode dédiée `CustomerStore.SetGroup` — **pas** de
`GroupID` ajouté à `cp.CustomerPatch`, qui porte déjà le commentaire inverse. Le problème classique du
PATCH nullable ne se pose pas : le contrat déclare `group_id` **required** et `[string, "null"]`, donc
la valeur est toujours présente et `null` signifie détacher — aucun sentinel « absent vs null ».
`list-group-customers` n'écrit **aucun SQL neuf** : un `Get` sur le groupe pour honorer le 404, puis le
`CustomerStore.List` existant avec `cp.CustomerFilter{GroupID: &id}` — c'est littéralement le
« `customer_id IN (...)` au moment de la lecture » qu'impose §6.17. `list-customer-groups` n'est pas
paginé (le contrat rend un tableau nu), `ORDER BY name`. `delete` en `:execrows`, 0 ligne → 404.
`created_by` est exposé en lecture et jamais rempli, comme `sender_ids` et `routing_scripts`, en
attendant step-310.

**Ce qui ne s'écrit pas.** Aucune migration : la table existe depuis `0001_init`. Aucun code d'audit :
`audited()` (`internal/adminapi/audit.go:119-127`) couvre déjà toute requête non lecture-seule.

## Tests

- CRUD sur repo réel (`testcontainers`), et la suppression vérifiée par ce qu'elle **préserve** : les
  clients existent toujours après, avec `group_id` à NULL.
- Les deux filtres `?groupId=` (`list-customers`, `list-smpp-accounts`) deviennent exerçables de bout en
  bout, sur un groupe créé par l'API : ils retournent le client du groupe et **pas** un client hors
  groupe. Une fixture où les deux clients seraient dans le même groupe ne prouverait rien.
- Un changement d'appartenance par `set-customer-group` change ce que ces filtres retournent, et le
  retour à `null` détache sans supprimer.
- Contrat : les 7 opérations sortent de la liste `deferred` de step-320 et entrent dans la liste servie.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] les 7 opérations servies, conformes au contrat — **contrat corrigé**, `security` et codes
      d'échec d'auth ajoutés aux 7, bump **mineur** `api/package.json` 4.2.1 → 4.3.0. La prédiction
      « aucun changement de contrat attendu » de cette fiche était fausse : voir `## Design arrêté`
      et le précédent step-149
- [ ] une opération générée sans `security` fait échouer la suite (la garde pour steps 340→390)
- [ ] `api/collections/admin-api.yaml` synchronisée (test bloquant) et le compte de son `README.md`
- [ ] les 7 lignes retirées de la liste `deferred` posée par step-320 (elle vit dans le test de
      contrat, pas dans la fiche)

## Hors périmètre

Toute portée de configuration par groupe. La ventilation par groupe dans les vues analytiques
(`search-messages`, exports) : lecture seule, à traiter là où ces surfaces vivent.
