# step-399 — Une invalidation perdue laisse une config périmée sans borne : resynchroniser périodiquement

> **Jalon :** Dette ouverte par step-395 · **Statut :** À FAIRE
> **Dépend de :** step-395, step-398 · **Bloque :** —

## Pourquoi cette fiche existe

step-395 a borné la durée d'un rebuild **échoué** : le watcher le rejoue seul, et
`config_rebuild_last_success_timestamp_seconds` le rend visible. Elle ne couvre pas un rebuild qui
**n'a jamais été demandé**, parce que l'invalidation s'est perdue en route. Rien n'échoue alors, rien
n'est rejoué, et le pod sert la config d'avant le changement jusqu'à la mutation suivante — la nuit,
sans borne.

Trois portes mènent à ce cas, toutes silencieuses :

- **L'Admin API annonce au mieux.** `PublishConfigChanges` publie `config:changed` quand le handler
  rend la main ; `internal/adminapi/deps.go:242` l'assume : « a lost announcement only defers the rebuild
  to the next admin mutation ». Redis injoignable à cet instant, et le changement n'est jamais annoncé.
- **Le pub/sub Redis ne stocke rien.** Un pod dont l'abonnement est en reconnexion
  (`redisstore.Subscribe` se réabonne, mais ne rattrape rien) perd l'invalidation publiée pendant ce temps.
- **config-sync relaie sans mémoire.** Un config-sync en redémarrage ne reçoit pas `config:changed`, donc
  ne republie rien sur `breaker:events`.

Ce que ça coûte : les mêmes divergences que step-395 — un opt-out retiré qui s'applique encore, une route
ou une règle de réécriture qui ne prend jamais — mais **sans aucun signal** : la métrique de step-395 montre
un dernier succès récent, puisque aucun rebuild n'a échoué.

## Périmètre proposé (à arbitrer avant le design)

Un rebuild périodique dans `config.Watcher`, même sans notification, qui borne la durée de toute config
périmée quelle qu'en soit la cause. `config_rebuild_last_success_timestamp_seconds` devient alors une vraie
mesure de fraîcheur, sur laquelle une alerte d'âge a un sens.

Points à trancher (spec → Fable → humain, cf. le déroulé des steps) :

1. **La période**, et si elle est une option du Watcher (`WithResync`) ou réglable par config. Elle fixe le
   pire cas de fraîcheur **et** la charge.
2. **La charge.** Chaque rebuild du routeur relit six composants (routes, deux Bloom, sender ID, scripts,
   facturation, contenu) ; un rebuild par pod et par période, multiplié par les réplicas. À chiffrer
   (volume des Bloom en particulier) avant de fixer la période.
3. **L'interaction avec le rejeu de step-395.** Un rebuild périodique pendant un backoff : coalescer, ou
   laisser le backoff gouverner ? La règle de step-395 — un seul timer, jamais deux rebuilds concurrents —
   doit survivre.
4. **Quels services.** Le routeur et le connector-pool ont un état ; config-sync n'en a pas, et une
   resynchronisation n'y aurait aucun sens.
5. **Le cas de l'Admin API.** Rendre l'annonce durable (outbox) serait l'autre remède ; il ne couvre pas
   les deux autres portes, d'où la préférence pour la resynchronisation. À confirmer.

## Design arrêté

Arbitrage Fable le 2026-09-27, validé par l'utilisateur. La spec ne chiffre aucune fraîcheur ; son schéma
d'architecture nomme déjà « config sync (pub/sub / polling) », et `modlrrouter/stop.go` logue depuis step-398
« routers catch up on resync » : cette fiche tient ce contrat.

1. **Réglage** : `CONFIG_RESYNC_INTERVAL`, champ de premier niveau de `config.Config` (même contrat que
   `DRAIN_DELAY`) : défaut 5 min, 0 désactive ; toute autre valeur sous 1 min est refusée (revue : `5ms` pour `5m`
   ferait rebâtir la flotte en continu contre le plan de contrôle partagé). Passé au Watcher par `WithResync(d)`, qui
   ignore `d ≤ 0` ; sans l'option, pas de resynchronisation. 5 min et non 1 h : la même période borne l'envoi
   à un désabonné dont l'annonce `optout:changed` s'est perdue.
2. **Mécanisme — un tick synthétique** : un timer de resync armé au démarrage de `Run` et réarmé après chaque
   rebuild **réussi** et désarmé par un échec (la resync ne tourne qu'hors panne : le backoff de step-395
   gouverne seul), gigue ±10 % ; à l'échéance il dépose un tick dans `ticks`, rien d'autre. Coalescence,
   rejeu et « jamais deux rebuilds concurrents » restent ceux de step-395. Fraîcheur bornée par
   1,1 × période depuis le dernier succès. La gigue décorrèle les pods qu'une notification commune aligne.
3. **Charge** : `buildFilter` pagine par 1 000 ; 5 M de lignes MNP × 4 réplicas / 5 min ≈ 70 requêtes
   d'index/s sur la flotte.
4. **Services** : le snapshot watcher du routeur, le watcher opt-out du routeur et le rewrite watcher du
   connector-pool ; pas config-sync (sans état). Amendé à la revue : la closure du snapshot recharge l'opt-out
   APRÈS les routes et le Bloom exact, donc une panne d'`exact_routes` suspendait la borne d'un STOP perdu ; la
   même période sur le watcher opt-out la rend indépendante (§6.20). `Enforcer.Reload` est sérialisé.
5. **Outbox Admin écartée** : elle ne couvre ni le pub/sub non durable ni config-sync redémarrant.

Conséquences assumées : `config_rebuild_total{outcome="ok"}` ne mesure plus l'activité Admin ; l'écriture
non atomique entre composants devient périodique ; un arrêt pendant un rebuild compte `+1 error` plus souvent
(step-395). Dette ouverte dans la PR : `buildFilter` matérialise tous les MSISDN avant de dimensionner le
filtre (~150 Mo transitoires toutes les 5 min, `debts/bloom-exact-routes-materialise-tous-les-msisdn.md`).

## Chaîne de preuves (esquisse)

1. Rouge dans `internal/config` : sans aucune notification, un rebuild a lieu au bout de la période.
2. Mutation : la période ignorée, le réarmement absent, ou réarmé sur échec font tomber les tests.
3. Bout en bout : un changement en base **sans** publication atteint le résolveur du routeur dans la
   période.
4. `make check` vert.

## Hors périmètre

Rendre le rebuild atomique entre composants (déjà écarté par step-395). Une règle d'alerte PromQL versionnée
sur l'âge de la config : le dépôt n'en versionne aucune aujourd'hui.

Le cache read-through `exactroute:{msisdn}` : la resync borne le Bloom, pas lui. Une invalidation Admin
échouée y laisse une cible périmée jusqu'au TTL (6 h) — borne déjà assumée
(`internal/routing/exact/invalidator.go:29`, TTL à `internal/routing/exact/resolver.go:19`), indépendante de la resynchronisation. Le design arrêté la
comptait d'abord parmi les dettes de cette PR ; ce n'en est pas une neuve.
