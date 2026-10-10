# La capture lit le grand livre trois fois, sous un verrou Redis, avant d'écrire

> **Statut :** PAYÉE le 10/10/2026 par step-287k · **Nature :** technique
> **Née de :** step-285b (design arrêté, point 6) · **Portée par :** step-287k

**Ce qu'on a fait à la place.** step-285b groupe les écritures durables, une transaction pour N
mouvements. La capture garde un chemin unitaire avant son écriture : elle prend le verrou terminal
(Redis), lit `LedgerEntryExists` deux fois (sa propre entrée, puis l'entrée opposée) et `ReserveEntry`. Ces
trois allers-retours par message restent hors du lot.

**Pourquoi.** Ces lectures portent l'exclusion mutuelle entre capture et libération : la première entrée
terminale durable gagne. Les regrouper demande un autre design, par exemple une réclamation
d'idempotence partagée par les deux terminaux. Les sondes de step-285b montraient que le commit et le WAL
dominaient ; on les a attaqués d'abord.

**Ce qu'il en coûte.** Trois requêtes par message livré, sur le même Postgres que les écritures groupées.
Pas encore mesuré une fois le lot en place.

**À quoi on reconnaîtra qu'il faut la payer.** La mesure VPS de step-285b ou le verdict de step-409
montrent la capture en tête des requêtes Postgres, ou la latence de `Capture` qui borne le débit du pool
de connecteurs.

Sources : `internal/billing/billing.go` (`resolveTerminal`, `Capture`) ·
`internal/storage/postgres/billing_batch.go`

**Voir aussi** `debts/lectures-du-grand-livre-par-message-id-non-elaguees.md` (step-408) : depuis que le grand
livre a une partition par jour, ces trois lectures verrouillent chacune toutes les partitions.

**Payée.** La trace de billing-svc au run 9 de step-287 a montré la capture en tête de l'attente du pool
Postgres (93 %). step-287k remplace les trois lectures par une seule, `GetMessageEntries`, sous le même
verrou et avec la même décision. Le verrou Redis par message reste.
