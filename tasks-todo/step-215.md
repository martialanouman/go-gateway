# step-215 — Webhooks : le repo est livré depuis M4, l'admin n'a jamais été écrite

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-213 (triage) · **Bloque :** —

## But

Servir les 4 opérations de webhooks déclarées au contrat. Aujourd'hui un client ne peut **pas**
configurer où sont poussés ses MO et ses DLR : la remise fonctionne, sa configuration n'existe pas.

| Opération | Méthode et chemin |
|---|---|
| `list-webhooks` | `GET /admin/smpp-accounts/{id}/webhooks` |
| `create-webhook` | `POST /admin/smpp-accounts/{id}/webhooks` |
| `update-webhook` | `PATCH /admin/smpp-accounts/{id}/webhooks/{webhookId}` |
| `delete-webhook` | `DELETE /admin/smpp-accounts/{id}/webhooks/{webhookId}` |

## Le constat

Tout l'aval existe depuis M4 : `control_plane.webhooks`, `internal/storage/postgres/webhooks.go`,
l'envoi signé HMAC-SHA256 avec retries (step-047), la remise MO arbitrée entre bind actif et webhook
(step-048), et depuis step-192 le topic `webhook.retry` sur son propre groupe de consommation. Ce qui
manque est la seule chose qu'un opérateur touche.

## Points d'implémentation clés

- **`secret` est un secret** : la règle du dépôt s'applique — stocké en hash, révélé **une seule fois**
  à la création et à la rotation, jamais relu par `list` ni `get`. Le champ masqué doit refuser d'être
  ré-écrit tel quel (la garde sentinelle de step-149 existe déjà pour ce cas, la réutiliser plutôt que
  d'en écrire une seconde).
- **`webhooks_uq UNIQUE (account_id, event_type)`** : un compte a au plus un webhook `mo` et un `dlr`.
  Une création en double est un 409 déterministe, pas une erreur de base remontée telle quelle. Le
  résumé du contrat le dit déjà (« one per event_type ») — le handler doit le dire aussi.
- **`status = disabled` ≠ suppression.** Désactiver doit couper la remise sans perdre l'URL ni le
  secret. Vérifier ce que le consommateur de remise lit réellement : s'il ignore `status`, la bascule
  est décorative, et c'est un défaut de cette step, pas une amélioration future.
- **`retry_policy_json`** existe en base et le runner de retry a ses propres bornes (8 essais, back-off
  30 s ×2 plafonné à 10 min, âge max 6 h). Ne pas exposer un réglage que le runner n'honore pas :
  soit la colonne est câblée, soit elle reste hors du corps de requête.

## Tests

- CRUD sur repo réel ; le secret **n'apparaît dans aucune réponse** après création (assertion sur le
  corps sérialisé, pas sur la struct — c'est la sérialisation qui fuit).
- Le doublon `(account_id, event_type)` produit le code d'erreur du contrat, pas un 500.
- Un webhook `disabled` n'est pas remis : test au niveau du consommateur de remise, seul endroit où la
  propriété est vraie ou fausse. La muter au niveau du handler ne prouverait rien.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] les 4 opérations servies ; secret jamais relu ; unicité et `disabled` vérifiés côté remise
- [ ] `api/collections/admin-api.yaml` synchronisée
- [ ] `tasks-todo/step-213.md` : les 4 lignes retirées de `deferred`

## Hors périmètre

Le mécanisme de remise, ses retries et son dead-letter (livrés en M4 et step-192). La rotation
programmée du secret.
