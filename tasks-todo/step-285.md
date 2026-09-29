# step-285 — Le routeur se tue en boucle sur un backlog quand la facturation est active

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-280 · **Bloque :** step-409
> Ouverte par la campagne step-280 ; unité faute de multiple de dix libre.

## Ce que la campagne a vu

VPS de test, 24 clients en facturation postpayée, ~3,3 M messages dans `mt.inbound` après un run
`sustained`. Sans **aucune** ingestion, le routeur n'a consommé **0 message en 60 s** :

1. il relit le lot non commité de `mt.inbound` ;
2. `handleBatch` lance une goroutine par partition et chaque voie réserve le crédit de ses messages ;
   ces réservations en rafale dépassent `RESERVE_TIMEOUT` (200 ms) — `DeadlineExceeded`, classé
   transitoire, à raison (`internal/pipeline/credit/reserver.go:76`) ;
3. l'erreur remonte de `Consumer.RunBatch` (`internal/storage/kafka/consumer.go:229`), le superviseur
   abat le processus (« component failed, shutting down ») ;
4. Kubernetes le relance après un backoff qui monte à 5 min, et tout recommence au point 1.

7 redémarrages en 16 min, CrashLoopBackOff. Le routeur a fini par en sortir **de lui-même** : à partir de
~23:45 UTC, il a vidé les 3,3 M messages en ~65 min (~820 `submit_sm/s` au simulateur, lag à 0 avant
00:49). Mais pendant la phase de boucle, la traversée est tombée à zéro. En production, ce serait
chaque reprise d'un backlog, après n'importe quelle panne, qui passerait par des minutes d'arrêt complet
et par un backoff Kubernetes qui monte à 5 min.

## Le même verrou que le goulot 2 de step-280

La réservation n'est pas qu'un aller-retour Redis : elle écrit durablement de façon synchrone
(`RecordDurable` → `AdjustBalance … ON CONFLICT DO UPDATE`, `internal/billing/billing.go:274`), sur la
ligne `control_plane.balances` du client, que la capture verrouille aussi (delta 0, `billing.go:382`).
Pendant la rafale de reprise, Postgres n'est donc pas au repos : les réservations d'un même client se
sérialisent derrière ce verrou, et c'est vraisemblablement ce qui les pousse au-delà de 200 ms. À vérifier
en tête de step : `pg_stat_activity` pendant un redémarrage.

## Pourquoi ce n'est pas un réglage

Élargir `RESERVE_TIMEOUT` repousse le seuil, ne le supprime pas : la rafale de reprise grandit avec le
backlog, et le verrou par client la sérialise. Le défaut est qu'une erreur **transitoire** a une conséquence **fatale** (le processus), et que
la reprise refait la même rafale. La mécanique « une erreur de traitement abat le groupe » est déjà fichée
pour `mo-dlr-router-svc` (`debts/content-key-svc-est-sur-le-chemin-de-la-remise-sans-etre-une-dependance-de-readiness.md`) ;
ici, la reprise dépend du hasard des redémarrages et du backoff, pas d'une conception.

## À arbitrer (spec → Fable → humain)

- **Où vit la reprise** : backoff dans le consommateur (rejouer la voie en échec sans redémarrer le
  processus, comme `errLaneHalted` le prépare déjà) plutôt que redémarrage ; ou concurrence de réservation
  bornée vers billing-svc ; ou les deux.
- Ce que la réponse change pour les autres consommateurs du superviseur (même mécanique partout).
- Si billing-svc doit borner lui-même sa file (il était à 0,75 cœur sous une request de 50 m).
- Si l'écriture durable synchrone sur une ligne par client (réserve **et** capture) est le bon modèle à
  8 000/s : c'est aussi le plafond par client de step-280.

## Definition of Done
- [ ] un test d'intégration qui rejoue un backlog sous une réservation lente et prouve que le routeur
      avance sans redémarrer — rouge lu sur le code actuel
- [ ] la reprise d'un backlog de step-280 rejouée sur le VPS de test : lag décroissant dès la première
      minute, zéro redémarrage
- [ ] la dette `content-key-svc…` statuée à la lumière de la même décision (payée ou explicitement non)
