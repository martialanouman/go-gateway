# step-285 — Le routeur se tue en boucle sur un backlog quand la facturation est active

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-280 · **Bloque :** step-287
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

**Rejoué le 30/09/2026 après step-284 (verrou retiré), image `v0.0.1-sha-cf28500ffff7` :** le routeur tombe
quand même. Un **seul** client, `sustained` 10 min (~4 500 req/s en entrée) : les deux routeurs en
CrashLoopBackOff (6 redémarrages chacun en ~11 min), `rpc error: code = DeadlineExceeded` sur la réserve,
~1 900 `submits_total` sur la fenêtre. Depuis step-282, un client s'étale sur les 12 partitions : la
concurrence des réserves qu'il fallait 24 clients pour atteindre est là dès le premier. Relevé au repos
après coup : Postgres inactif (aucune attente), `balance_deltas` vide, billing-svc hors du haut de
`kubectl top`. L'hypothèse du verrou ci-dessous n'explique donc plus les dépassements ; la source reste
**non attribuée** — billing-svc n'expose ni latence gRPC ni compteurs de son pool Postgres (10 connexions).
Première chose à poser en tête de step.

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

## Design arrêté (03/10/2026 — spec muette, tranché par Fable, appliqué)

1. **Rejeu en place dans `kafka.Consumer`, `Run` et `RunBatch`, pour tous les consommateurs.** Une erreur de
   handler ne remonte plus : le consommateur rejoue le suffixe non commité du lot, sans repoller, avec un
   backoff exponentiel plafonné (1 s → 30 s, jitter ±20 %), jusqu'au succès ou à l'annulation du ctx. Un
   `slog.Warn` par tentative (groupe, topic, partition, offset, tentative, délai, erreur).
2. **`pending` est le complément de ce que `committablePrefix` rend committable**, jamais « `results[i] != nil` » :
   le curseur de fetch est déjà au-delà du lot, un enregistrement ni rejoué ni commité serait sauté par le
   commit suivant. `committablePrefix` rend `(commit, pending, err)`.
3. **Restent fatals** : erreur de fetch, rupture de contrat du handler, échec de commit hors annulation.
4. **Écartés** : recréer le client kgo (un rééquilibrage par échec, corrélé entre répliques, chaque
   déplacement republie — ADR-0012) ; `BlockRebalanceOnPoll` (budget sous `RebalanceTimeout`, puis
   expulsion) ; `SetOffsets` (déconseillé par kgo en groupe) ; élargir `RESERVE_TIMEOUT`. franz-go n'a pas
   de `max.poll.interval` : les battements de cœur vivent dans leur goroutine, un rejeu long n'expulse pas.
5. **Concurrence vers billing-svc : non bornée de plus** — les voies la bornent déjà (≤ 1 réserve en vol
   par partition assignée).
6. **Attribution** : un histogramme `billing_reserve_stage_seconds{stage="total"|"durable"}` dans
   `Accountant.Reserve`, injecté comme la jauge du replieur. Ni intercepteur gRPC, ni stats pgx.
7. **Dette `content-key-svc…` : payée pour son sujet** (la jambe de remise rejoue seule, les autres
   groupes du pod continuent) ; ce qui n'est pas payé y est écrit.
8. **Docs à corriger** : `Handler`/`BatchHandler`/`Run`/`RunBatch`, `router.Run`, `reserver.go:15-17`
   (le délai de 200 ms ne protège d'aucun rééquilibrage, c'est un fail-fast).
9. **Dettes ouvertes** : doublons par tentative au pool de connecteurs (produce `mt.outcome` après
   `submit_sm`) ; rejeu pendant un rééquilibrage ; tête de ligne silencieuse d'un poison.
10. **Amendement de revue (PR2)** : un appel annulé garde ce qu'il n'a pas commité (`Consumer.held`) et
    l'appel suivant le traite avant de repoller — connector-pool rappelle `RunBatch` sur le même client
    après chaque chute de bind ; sans cela, le curseur déjà avancé sautait ces enregistrements. Et
    `mt-replay` s'arrête toujours sur le premier échec (`Replayer.Run` annule son propre ctx).
11. **Amendement après le rejeu VPS du 03/10 (tranché par Fable)** : zéro redémarrage, mais ~9 msg/s. Le
    backoff croissait à chaque échec d'un même lot (`attempt` jamais remis à zéro) et chaque attente gelait
    toutes ses partitions : 2,4 % de réserves au-delà de 200 ms suffisaient à arrêter le routeur. Règle :
    une tentative qui a traité quelque chose remet `attempt` à 0 et rejoue sans attendre ; seule une
    tentative sans aucun progrès attend. Une panne franche garde son backoff exponentiel.
    Chaque rejeu reste journalisé (`idle_attempts`, `delay=0` s'il a progressé). `RESERVE_TIMEOUT` passe à
    1 s (décision utilisateur) : 200 ms tranchait 2,4 % de réserves que billing-svc servait en 42 ms.

**Test de DoD** : e2e en processus (`internal/e2e`, harnais du routeur de référence) — backlog produit
avant le démarrage, réserve qui rend `DeadlineExceeded` brut puis réussit ; `Run` ne rend rien, le lag
du groupe atteint 0, chaque message est sur `mt.routed`. Rouge sur le code actuel : `Run` rend l'erreur.

**Plan de PR** : PR1 = histogramme billing ; PR2 = rejeu en place + tests + docs + dettes ; puis rejeu VPS.
