# step-285 — Le routeur ne sort pas d'un backlog quand la facturation est active

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-280 · **Bloque :** step-409
> Ouverte par la campagne step-280 ; unité faute de multiple de dix libre.

## Ce que la campagne a vu

VPS de test, 24 clients en facturation postpayée, ~3,3 M messages dans `mt.inbound` après un run
`sustained`. Sans **aucune** ingestion, Postgres au repos, le routeur n'a consommé **0 message en 60 s** :

1. il relit le lot non commité de `mt.inbound` ;
2. `handleBatch` lance une goroutine par partition et chaque voie réserve le crédit de ses messages ;
   ces réservations en rafale dépassent `RESERVE_TIMEOUT` (200 ms) — `DeadlineExceeded`, classé
   transitoire, à raison (`internal/pipeline/credit/reserver.go:76`) ;
3. l'erreur remonte de `Consumer.RunBatch` (`internal/storage/kafka/consumer.go:229`), le superviseur
   abat le processus (« component failed, shutting down ») ;
4. Kubernetes le relance après un backoff qui monte à 5 min, et tout recommence au point 1.

7 redémarrages en 16 min, CrashLoopBackOff. Pendant un run, c'est ce qui a fait tomber la traversée à
zéro. En production, c'est la reprise après **n'importe quelle** panne qui accumule un backlog.

## Pourquoi ce n'est pas un réglage

Élargir `RESERVE_TIMEOUT` repousse le seuil, ne le supprime pas : la rafale de reprise grandit avec le
backlog. Le défaut est qu'une erreur **transitoire** a une conséquence **fatale** (le processus), et que
la reprise refait la même rafale. La mécanique « une erreur de traitement abat le groupe » est déjà fichée
pour `mo-dlr-router-svc` (`debts/content-key-svc-est-sur-le-chemin-de-la-remise-sans-etre-une-dependance-de-readiness.md`) ;
ici elle ne se résorbe pas d'elle-même.

## À arbitrer (spec → Fable → humain)

- **Où vit la reprise** : backoff dans le consommateur (rejouer la voie en échec sans redémarrer le
  processus, comme `errLaneHalted` le prépare déjà) plutôt que redémarrage ; ou concurrence de réservation
  bornée vers billing-svc ; ou les deux.
- Ce que la réponse change pour les autres consommateurs du superviseur (même mécanique partout).
- Si billing-svc doit borner lui-même sa file (il était à 0,75 cœur sous une request de 50 m).

## Definition of Done
- [ ] un test d'intégration qui rejoue un backlog sous une réservation lente et prouve que le routeur
      avance sans redémarrer — rouge lu sur le code actuel
- [ ] la reprise d'un backlog de step-280 rejouée sur le VPS de test : lag décroissant, zéro redémarrage
- [ ] la dette `content-key-svc…` statuée à la lumière de la même décision (payée ou explicitement non)
