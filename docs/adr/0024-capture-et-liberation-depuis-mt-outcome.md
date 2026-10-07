# ADR-0024 : La capture et la libération d'un message envoyé se font depuis `mt.outcome`, dans billing-svc

**Status:** Accepted
**Date:** 2026-10-07
**Deciders:** Équipe plateforme. Demande humaine du 07/10/2026 (campagne step-287) ; arbitrage tranché par
Fable le même jour.
**Réf spec:** passerelle §4.2, §6.9 ; ADR-0012 (CDR projeté depuis `mt.outcome`), ADR-0023 (groupe dédié sur
`mt.outcome`) ; step-287c, step-287d

## Context

`connector-pool-svc` capturait la réservation de chaque message envoyé, et libérait celle de chaque message
refusé, par un appel gRPC synchrone à billing-svc, entre le `submit_sm_resp` et la publication de
`mt.outcome`. Le chronomètre de step-287c l'a mesuré sur le VPS de test le 07/10/2026 : **172,7 ms par
message** passés dans `capture`, contre 7,7 ms pour le `submit_sm`. Environ 80 % des captures expiraient à
`BILLING_SETTLE_TIMEOUT` (200 ms) et passaient en fail-open. Le reaper les rattrapait au rythme d'une par
seconde environ, et le pool plafonnait à 195 `submit_sm`/s.

L'appel était fail-open pour une raison solide : une erreur propagée aurait rejoué le record de `mt.routed`,
donc renvoyé le SMS. Mais il restait sur le chemin chaud, et c'est exactement ce qu'ADR-0012 avait retiré
pour ClickHouse.

## Decision

**1. Le pool ne fait plus aucun appel de facturation après le `submit_sm_resp`.** Il publie `mt.outcome`, qui
est fail-closed et acquitté avant le commit, et porte désormais `billable` et `owner_type`.

**2. Un groupe dédié `billing-svc-settle` dans billing-svc lit `mt.outcome`.**
- `enroute` déclenche une capture, `failed` une libération ; le reste, et `billable=false`, ne coûtent rien.
- Il passe par `Accountant.Capture`/`Release`, inchangés : verrou terminal, idempotence par
  `(message_id, entry_type)`, invariant c.
- Il travaille en concurrence bornée sur le lot de poll.
- Un échec fait échouer son record, qui est rejoué sur les offsets de ce groupe, jamais sur ceux de
  `mt.routed`. **Une panne de facturation ne peut plus renvoyer de SMS : c'est une propriété de la
  structure, plus un fail-open à tenir.**
- Il démarre au dernier offset : un premier déploiement ne règle pas toute la rétention de `mt.outcome`.

**3. Les chemins sans envoi gardent le `Release` gRPC fail-open du pool** : `cancelBeforeDispatch` et la
chaîne de repli épuisée. Ils ne passent pas par `mt.outcome` (ADR-0023 §3), et ils sont rares.

**4. Le CDR ne porte plus de montant de règlement venu du pool.** `billed`/`credits_charged` ne faisaient
déjà pas autorité : la ligne DLR (rang 40, `Billed: false`) les écrase pour tout message livré. Le grand
livre est la seule autorité.

**5. Le reaper reste le filet** : un retard au-delà de `MIN_AGE`, les records antérieurs au déploiement, et
les libérations fail-open des chemins sans envoi.

## Options Considered

### Option A : un consommateur de `mt.outcome` dans billing-svc (retenue)
Le même geste qu'ADR-0012, sur le même patron qu'ADR-0023. Le seul acte fail-closed après l'envoi reste un
produce Kafka.

### Option B : garder l'appel, mais rendre la capture rapide (RPC par lot, sans verrou par message)
Écartée. Capture et libération sont deux `entry_type`, et l'index unique ne peut pas arbitrer entre elles :
sans verrou, un message pourrait être capturé **et** libéré. Un appel par lot resterait sériel par poll dans
le pool, et sur le chemin chaud.

### Option C : relever `BILLING_SETTLE_TIMEOUT`
Écartée. Chaque message serait plus lent, sur un chemin sériel par bind, pour sauver un champ CDR déjà faux
après le DLR.

## Consequences

- La spec nommait le pool comme acteur de la capture (§4.2, §6.9 point 3). Elle est amendée ; les garanties
  de §6.9 sont conservées.
- Le retard de règlement devient visible par le lag du groupe `billing-svc-settle` sur `mt.outcome`.
- `billing_capture_failed_total` disparaît du pool.
- Un CDR qui dirait vrai sur la facturation demanderait un rang de règlement :
  `debts/cdr-sans-montant-de-reglement.md`.
- **Ordre de déploiement :** le groupe `billing-svc-settle` est déployé avant que le pool cesse de capturer.
  Pendant la transition, les deux règlent ; l'idempotence garde une seule entrée.
