# step-214 — Groupes de clients (§6.17) : la table existe, rien ne la remplit

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-213 (triage) · **Bloque :** —

## But

Servir les 7 opérations de groupes que `api/openapi-admin.yaml` déclare et qu'aucun handler
n'implémente. La segmentation par groupe est aujourd'hui **à moitié construite** : la table, la clé
étrangère et jusqu'au filtre de lecture existent — seule la surface qui permet d'assigner un groupe
manque.

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

`control_plane.customer_groups` existe, `customers.group_id` la référence en `ON DELETE SET NULL`, et
`list-smpp-accounts` **implémente déjà** le filtre `?groupId=` (`internal/adminapi/accounts.go`). Ce
filtre ne peut aujourd'hui rien retourner : rien ne permet d'affecter un client à un groupe. La moitié
aval est donc écrite et non exerçable — un test de bout en bout du filtre est le premier bénéfice de
cette step.

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

## Tests

- CRUD sur repo réel (`testcontainers`), et la suppression vérifiée par ce qu'elle **préserve** : les
  clients existent toujours après, avec `group_id` à NULL.
- Le filtre `list-smpp-accounts?groupId=` retourne enfin quelque chose : un compte dont le client est
  dans le groupe, et **pas** un compte d'un client hors groupe. Une fixture où les deux clients seraient
  dans le même groupe ne prouverait rien.
- Contrat : les 7 opérations sortent de la liste `deferred` de step-213 et entrent dans la liste servie.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] les 7 opérations servies, conformes au contrat déclaré (aucun changement de contrat attendu)
- [ ] `api/collections/admin-api.yaml` synchronisée (test bloquant)
- [ ] `tasks-todo/step-213.md` : les 7 lignes retirées de `deferred`

## Hors périmètre

Toute portée de configuration par groupe. La ventilation par groupe dans les vues analytiques
(`search-messages`, exports) : lecture seule, à traiter là où ces surfaces vivent.
