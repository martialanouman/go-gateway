# step-360 — Sessions SMPP : le flux temps réel existe, la lecture REST non

> **Jalon :** Surfaces Admin déclarées au contrat, jamais construites (§6.18 `docs/specification-technique-passerelle-sms.md`) · **Statut :** LIVRÉE
> **Dépend de :** step-320 (triage) · **Bloque :** —

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

- [x] `make check` vert (lint · `test -race` · govulncheck · contrats) — 2026-09-25
- [x] les 3 opérations servies ; la déconnexion vérifiée côté pair, avec motif
  (`internal/smppserver/operator_disconnect_e2e_test.go` : DELETE HTTP → unbind puis EOF chez le pair
  visé, le voisin du même compte répond encore)
- [x] `api/collections/admin-api.yaml` synchronisée ; lignes retirées de `deferred` (step-320)

## Hors périmètre

Le flux `stream-sessions` (livré). La politique de reconnexion des connecteurs sortants (step-127/128).

## Design arrêté

Arbitré par Fable le 2026-09-25 (spec → Fable, aucun point remonté à l'humain). Six décisions :

1. **Métadonnées dans le registre, même slot que le quota.** `sess:{acct}:meta` (hash, champ = bind_id →
   JSON `{pod_id, system_id, bind_type, remote_addr, window_size, connected_at}`) est écrit par `bind.lua`
   en `HSETNX` **dans la branche acceptée seulement** (un bind refusé ne laisse rien ; `connected_at`
   survit au refresh), effacé par `unbind.lua`, et purgé au balayage (`ZRANGEBYSCORE` avant
   `ZREMRANGEBYSCORE`, sinon une meta expirée vit tant que le compte a un bind vivant). Le registre
   horodate `connected_at`. `last_enquire_link` = null : le suivre coûterait une écriture par
   enquire_link. `connector_id` = null, `direction` = `user`.
2. **Index global = pointeur, pas vérité.** `sess:idx` (zset lex, score 0, membre `bind_id|account_id`),
   `ZADD` **après** acceptation à chaque Bind (le refresh le rejoue : auto-réparation), `ZREM` après
   unbind. Seul un pod mort laisse une orpheline ; le lecteur vérifie chaque entrée contre
   `sess:{acct}` + meta et purge paresseusement. Curseur = membre complet (opaque), `ZRANGE BYLEX (cursor`.
   Une session fermée entre deux pages manque ; une ouverte sous le curseur aussi ; une page peut être
   courte, `has_more` reste exact. **Écrit dans la description du contrat.** SCAN rejeté (COUNT n'est
   qu'un indice, balaie le keyspace partagé). Filtre `accountId` : lecture directe du compte, triée et
   paginée en mémoire (≤ max_sessions entrées).
3. **Nouvelle RPC `ListSessions`** ; `Lookup` (chemin chaud MO/DLR) inchangée.
4. **`disconnect-session`** : `id` = bind_id. `DISCONNECT_SCOPE_SESSION` (et `disconnect.ScopeSession`
   dans `valid()`, sinon `Decode` le rejette au pod). `Server.Disconnect` résout le bind par `sess:idx`
   → `NotFound` (404) s'il n'est pas vivant, sinon publie ; le pod détenteur matche `bindID` et
   `forceClose` (unbind puis fermeture). Motif `operator_disconnect`, audité par `audited()`. 204 = ordre
   publié. Deux fenêtres écrites, non corrigées : un pod d'avant la step ignore l'ordre pendant un
   rollout ; un bind accepté répond 404 pendant ≤ 1 RTT avant son `ZADD`.
5. **`connectorId`** : 422 explicite (renvoie vers `get-connector-status`) + 422 déclaré au contrat +
   fiche de dette. Les binds sortants n'ont ni UUID de session ni registre ; une page vide mentirait.
6. **Sécurité** : `admin:read` sur les deux lectures, `admin:write` sur la déconnexion, 401/403 au
   contrat. Aucune contrainte de validation durcie → bump **mineur**. `list-account-sessions` :
   `max_sessions` lu en base (404 si compte inconnu), `active` = vivantes, **peut dépasser** max (§6.3).

### Revue (tour 1) — ce qu'elle a changé au design

- Un échec du `ZADD` d'index après admission **ne refuse plus le bind** : le slot est déjà pris, et le
  refuser le laissait compté une TTL entière pour un client à qui l'on a dit non. Le refresh rejoue
  l'écriture.
- Un unbind dont le jeton a déjà expiré désindexe quand même le bind (`Registry.Unindex`, borné au
  compte de l'unbind) : l'index ne grossit plus faute de lecteur. L'échec d'écriture de l'index est
  journalisé (`WARN`), sans quoi une panne durable viderait la liste en silence. Reste une orpheline possible si le `ZREM` lui-même échoue — purgée à
  la lecture suivante.
- `bind_type` : une seule table, `pb.BindType.Name()`, fermée sur tx/rx/trx (`""` sinon) ; l'Admin API
  n'importe plus `internal/session`.
- Pannes gRPC du registre (`Unavailable`, `DeadlineExceeded`) → 503 ; `limit` borné à 500 côté gRPC.
- **Ordre de déploiement** : `session-manager-svc` et `smpp-server-svc` avant `admin-api-svc`. Un
  session-manager d'avant la step répond `Unimplemented` à `ListSessions` et `InvalidArgument` au scope
  session ; un pod d'avant la step ignore l'ordre de déconnexion (204 sans effet) ; un refresh servi
  par l'ancien Lua ne renouvelle pas la meta, et la session sort de la liste jusqu'au refresh suivant.
- Rejeté : prélude Lua commun (chaque script cesserait d'être lisible seul) ; revérifier le score avant
  la purge (la course ne coûte qu'une absence jusqu'au refresh suivant, ≤ TTL/2).

Tour 2 (sur le seul commit de correctifs) : aucun bloquant ; appliqués ci-dessus. Écartés : `Canceled`
mappé à part (bruit de log seulement), forme du `FromError` dans `list`.
