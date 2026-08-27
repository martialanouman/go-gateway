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
- [ ] les 7 opérations servies, conformes au contrat déclaré (aucun changement de contrat attendu)
- [ ] `api/collections/admin-api.yaml` synchronisée (test bloquant)
- [ ] les 7 lignes retirées de la liste `deferred` posée par step-320 (elle vit dans le test de
      contrat, pas dans la fiche)

## Hors périmètre

Toute portée de configuration par groupe. La ventilation par groupe dans les vues analytiques
(`search-messages`, exports) : lecture seule, à traiter là où ces surfaces vivent.
