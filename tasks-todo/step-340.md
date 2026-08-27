# step-340 — Webhooks : le repo est livré depuis M4, l'admin n'a jamais été écrite

> **Jalon :** Surfaces Admin déclarées au contrat, jamais construites (§6.18 `docs/specification-technique-passerelle-sms.md`) · **Statut :** À FAIRE
> **Dépend de :** step-320 (triage) · **Bloque :** —

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

- **`secret` est un secret d'un genre différent de ceux du dépôt, et la règle générale ne s'y applique
  pas.** Les mots de passe de bind et les clés API sont *vérifiés* par la passerelle, donc hachés. Ce
  secret-ci est *utilisé* : `webhook.Sign(wh.Secret, …)` en a besoin **en clair** à chaque remise. Le
  hacher rendrait toutes les signatures invérifiables côté client. Le contrat a déjà tranché la bonne
  forme — `secret` est **write-only, jamais retourné** (`Webhook` n'a pas ce champ ; `WebhookCreate` et
  `WebhookUpdate` l'acceptent en entrée) : c'est l'opérateur qui **fournit** la valeur, rien n'est
  « révélé ». Donc pas de hash, pas de sentinelle masquée (celle de step-149 sert à `auth_config_json`,
  qui, lui, est retourné masqué). La règle à tenir est plus étroite et déjà écrite dans le code : le
  secret ne doit **jamais** être persisté hors du plan de contrôle — la doc de `webhook.RetrySink`
  l'interdit explicitement pour les records de retry, un opérateur pouvant les lire.
- **`webhooks_uq UNIQUE (account_id, event_type)`** : un compte a au plus un webhook `mo` et un `dlr`.
  Une création en double est un 409 déterministe, pas une erreur de base remontée telle quelle. Le
  résumé du contrat le dit déjà (« one per event_type ») — le handler doit le dire aussi.
- **`status = disabled` ≠ suppression.** Désactiver doit couper la remise sans perdre l'URL ni le
  secret. Vérifier ce que le consommateur de remise lit réellement : s'il ignore `status`, la bascule
  est décorative, et c'est un défaut de cette step, pas une amélioration future.
- **`retry_policy_json` est déjà câblée et déjà au contrat** — la sortir serait une rupture.
  `webhook.parseRetryPolicy` lit `max_attempts`, `initial_backoff_ms`, `max_backoff_ms` et `multiplier`,
  sous des plafonds durs (20 essais, 5 min). Le vrai constat est ailleurs : **le runner différé
  (step-192) ne pace que sur ses propres constantes** — seul `max_attempts` traverse
  (`retriesExhausted`), le back-off de la politique n'est pas honoré sur ce chemin. Une surface qui
  laisse configurer `initial_backoff_ms` sans que le retry différé s'en serve promet un réglage inerte :
  soit le runner l'honore, soit la fiche le documente comme non honoré sur le chemin différé.

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
- [ ] les 4 lignes retirées de la liste `deferred` posée par step-320 (elle vit dans le test de
      contrat, pas dans la fiche)

## Hors périmètre

Le mécanisme de remise, ses retries et son dead-letter (livrés en M4 et step-192). La rotation
programmée du secret.
