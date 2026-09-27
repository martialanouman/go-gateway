# step-398 — Une invalidation perdue laisse une config périmée sans borne : resynchroniser périodiquement

> **Jalon :** Dette ouverte par step-395 · **Statut :** À FAIRE
> **Dépend de :** step-395 · **Bloque :** —

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

## Chaîne de preuves (esquisse)

1. Rouge dans `internal/config` : sans aucune notification, un rebuild a lieu au bout de la période.
2. Mutation : la période ignorée ou infinie fait tomber le test ; un rebuild périodique concurrent d'un
   rejeu aussi.
3. Bout en bout : un changement en base **sans** publication atteint le résolveur du routeur dans la
   période.
4. `make check` vert.

## Hors périmètre

Rendre le rebuild atomique entre composants (déjà écarté par step-395). Une règle d'alerte PromQL versionnée
sur l'âge de la config : le dépôt n'en versionne aucune aujourd'hui.
