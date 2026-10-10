# step-287i — Profiler un service sous charge : `pprof` sur le port d'exploitation

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-287f · **Bloque :** la reprise de step-287
> Demande humaine du 10/10/2026, née du run 8 de step-287 : mesurer billing-svc avant de l'optimiser.

## Pourquoi
Au run 8, Postgres attend billing-svc : les transactions du lot passent en moyenne 1,8 connexion
`idle in transaction` (0,74 après `BEGIN`, 0,61 après `WriteBillingBatch`) contre 0,35 en exécution, alors
qu'un aller-retour VPC coûte 1 ms et que billing-svc n'a pas de limite CPU. Le temps se perd dans le
processus. Les binaires sont compilés en `-s -w` : `perf` sur l'hôte n'en dit rien. Seule une trace
d'exécution Go montre ce que fait la goroutine entre la réponse de Postgres et la requête suivante
(ordonnanceur, GC, sérialisation, attente d'un canal).

## Design arrêté
- `OPS_PPROF` (bool, défaut `false`) dans `Config` ; à `true`, `NewOpsServer` monte `net/http/pprof`
  sous `/debug/pprof/` sur le port d'exploitation (9090), interne et absent des contrats.
- Faux par défaut : aucun service en production n'expose de profil sans qu'on l'ait demandé.
- L'environnement de test l'active pour tous les services (`deploy/test/patches/gateway-config.yaml`).
- `http.Server.WriteTimeout` reste absent : `/debug/pprof/trace?seconds=5` et `profile?seconds=30`
  écrivent pendant toute leur durée.
- Hors périmètre : l'optimisation elle-même, choisie sur la trace, dans une step suivante.

## Definition of Done
- [ ] `/debug/pprof/` répond 404 sans `OPS_PPROF`, 200 avec (test, muté)
- [ ] `OPS_PPROF` lu par `config.Load`, faux par défaut (test)
- [ ] activé dans `deploy/test`, rendu à jour
- [ ] run 9 : trace de 5 s et profil CPU de 30 s de billing-svc sous charge, lus et versés au journal de
      step-287
