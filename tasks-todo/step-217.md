# step-217 — Sessions SMPP : le flux temps réel existe, la lecture REST non

> **Jalon :** Surfaces Admin déclarées au contrat, jamais construites (§6.18 `docs/specification-technique-passerelle-sms.md`) · **Statut :** À FAIRE
> **Dépend de :** step-213 (triage) · **Bloque :** —

## But

Servir les 3 opérations de sessions déclarées au contrat. Un opérateur peut aujourd'hui **regarder** les
sessions vivre (`stream-sessions`, step-184) mais ne peut ni les lister à froid, ni en déconnecter une.

| Opération | Méthode et chemin |
|---|---|
| `list-sessions` | `GET /admin/sessions` (filtres `accountId`, `connectorId`, curseur) |
| `list-account-sessions` | `GET /admin/smpp-accounts/{id}/sessions` (binds vivants vs `max_sessions`) |
| `disconnect-session` | `DELETE /admin/sessions/{id}` |

## Le constat

Toutes les pièces existent : le registre de sessions Redis (step-021), son service gRPC (step-022), la
déconnexion forcée (step-032) et `internal/adminapi/disconnector.go` déjà câblé pour la suspension de
compte. Il manque la lecture et l'action explicite.

`list-account-sessions` est la seule des trois à porter une information que le flux ne donne pas :
**binds vivants comparés à `max_sessions`** — c'est-à-dire à quel point un compte est près de se voir
refuser un bind (invariant d).

## Points d'implémentation clés

- **Le registre est la source, pas la base.** Les sessions vivent dans Redis, pas dans PostgreSQL : une
  lecture qui joindrait `smpp_accounts` pour « lister les sessions » retournerait des comptes, pas des
  sessions. La différence se voit le jour où un pod meurt.
- **La pagination par curseur sur un état volatil n'est pas celle du CDR.** Une page 2 peut manquer une
  session fermée entre-temps ; c'est acceptable et doit être **écrit**, pas découvert. Ne pas promettre
  une cohérence d'instantané que Redis ne donne pas.
- **`disconnect-session` est une action, pas une suppression.** Réutiliser le chemin de step-032 avec un
  motif dédié (`operator_disconnect`), pour que la raison arrive au client et dans l'audit. Un `DELETE`
  qui se contenterait de retirer la clé du registre laisserait la connexion TCP ouverte : la session
  disparaîtrait de la liste et continuerait de servir.
- **La déconnexion est inter-pods** : la session est détenue par un pod `smpp-server-svc` précis, et
  l'ordre passe par le registre — le même mécanisme que `SessionRegistry.Deliver`. Ne pas supposer que
  l'API admin et le détenteur du bind sont dans le même processus.

## Tests

- `list-account-sessions` : un compte à `max_sessions = 2` avec 2 binds vivants doit apparaître **plein**
  ; la fixture doit distinguer les deux nombres (une fixture où vivants = max = 0 passe sous n'importe
  quelle formule).
- `disconnect-session` : après l'appel, la connexion est **réellement fermée** côté pair — assertion sur
  le pair de test, pas sur l'absence de la clé Redis. C'est la couche où le défaut vivrait.
- Une session inconnue → 404, jamais un 204 silencieux.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] les 3 opérations servies ; la déconnexion vérifiée côté pair, avec motif
- [ ] `api/collections/admin-api.yaml` synchronisée ; lignes retirées de `deferred` (step-213)

## Hors périmètre

Le flux `stream-sessions` (livré). La politique de reconnexion des connecteurs sortants (step-127/128).
