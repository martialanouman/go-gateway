# step-287c — Chronomètre du pool : où un `submit_sm` passe son temps

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-287b · **Bloque :** step-287 (reprise de la campagne)
> Née de la campagne step-287 du 07/10/2026, demande humaine du même jour ; unité faute de multiple de dix
> libre.

## Pourquoi
Pendant le run 1b de step-287, le pool envoyait ~133 `submit_sm`/s sur 52 binds, à 0,13 cœur par pod :
**~390 ms par message et par bind**, alors que le simulateur répond en 5 ms. Le reste du temps se passe
ailleurs. Le pool n'émet qu'un span (`connector.submit`) et aucune métrique par étape, donc rien ne dit
où. Sans cette réponse, impossible de trancher entre contention de l'hôte (séparer les magasins) et étape
sérielle à corriger dans le code.

## Design arrêté (07/10/2026)
- **Un histogramme `connector_submit_stage_seconds{connector_id, stage}`** dans le catalogue de métriques,
  exposé par le pool. Le label `stage` est déjà autorisé par la garde (`labels.go`), avec un vocabulaire
  fermé tiré du code :
  - par message, dans l'ordre de `processOne` : `limit` (jeton du connecteur, Redis), `claim` (claim
    d'annulation, Redis), `throttle` (pacing AIMD), `sender` (épinglage de l'expéditeur), `submit`
    (aller-retour SMPP), `dlrmap` (correspondance DLR, Redis), `capture` (capture ou libération de
    facturation), `outcome` (publication `mt.outcome`) ;
  - par lot de poll : `shard` (durée de traitement d'un shard) et `batch` (durée du lot entier). L'écart
    entre les deux mesure la barrière : un lot attend son shard le plus lent.
- Pas de span enfant : une trace sert à suivre un message, pas à mesurer un débit, et l'environnement de
  test n'a pas de collecteur.
- Toujours actif, en production comme en test : c'est l'outil qui manquera le jour où un connecteur réel
  ralentira.

## Definition of Done
- [ ] chaque étape d'un envoi réussi est observée une fois, `shard` et `batch` une fois par lot (test,
      mutation)
- [ ] métrique exposée par le pool (`TestOpsExposesTheMetricsThisServiceFeeds`)
- [ ] un run de 10 min sur le VPS, avec la répartition par étape relevée dans le journal de step-287
