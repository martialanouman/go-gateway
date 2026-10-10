# step-287f — Le grand livre s'écrit à quatre : écrivains parallèles et lot en un seul statement

> **Jalon :** M12 · **Statut :** LIVRÉE
> **Dépend de :** step-287e · **Bloque :** step-287 (reprise de la campagne)
> Née du run 3 de step-287 (07/10/2026), demande humaine du même jour ; unité faute de multiple de dix libre.

## Pourquoi
Run 3 de step-287 (VPS de test, `600d5c0`) : l'écriture durable d'une réservation coûte **184 ms**, soit
67 ms d'attente de l'écrivain unique (`handoff`) et 110 ms d'écriture de son lot (`reply`). Un lot fait 86 ms de
Postgres en quatre allers-retours (`begin` 13, `claim` 17, `copy` 43, `commit` 13). L'écrivain unique, à 11
lots/s de ~35 écritures, est occupé environ 95 % du temps. Il plafonne vers 400 écritures/s, alors que le
besoin est de 270 réservations/s et ~100 captures/s.

## Design arrêté (07/10/2026)
L'arbitrage a été rendu par Fable, sans contradiction avec la spec ni avec ADR-0022.

- **4 écrivains, constante** (`batchWriters`), qui tirent sur la même file, chacun dans sa transaction.
  - **L'invariant c tient.** La clé primaire `(message_id, entry_type)` de `billing_idempotency` arbitre : sous
    READ COMMITTED, un `INSERT … ON CONFLICT DO NOTHING` qui croise une clé insérée par une transaction en
    vol attend son issue. Avant step-285b, chaque écriture était déjà sa propre transaction concurrente.
  - **La sémantique du grand livre est intacte.** `balance_deltas` est commutatif (ADR-0022 §2), et
    `balance_after` est épinglé par Redis, non monotone par `created_at` (ADR-0022 §4). Le verrou terminal
    sérialise capture et libération en amont, et l'ambiguïté du COMMIT reste par lot.
  - **`ORDER BY` sur la réclamation par lot.** Deux lots portant deux paires identiques en ordre inverse
    s'interbloqueraient (40P01) ; un ordre d'insertion déterministe l'exclut.
  - Le plafond passe à ~1 600 écritures/s. Le pool Postgres, à 10 connexions, en laisse pour les lectures,
    `XactStatus`, le replieur et les replis unitaires. Ce n'est pas un réglage : la valeur ne change pas.
- **Le lot en un statement, entre `Begin` et `Commit` (PR2).** Un CTE unique fait la réclamation, l'insertion
  des deltas et celle du grand livre, à partir d'un `jsonb_to_recordset`, et rend le xid dans le même
  résultat, avant que le COMMIT parte. `commitOutcome` ne change pas. Deux variantes sont rejetées, parce que
  le xid ne serait plus lisible avant le commit, ce qui rouvrirait l'ambiguïté refusée le 04/10 :
  - le pipeline `BEGIN…COMMIT` en un seul envoi ;
  - un CTE en autocommit.

  Les étapes `claim` et `copy` fusionnent en `write`.
- **Pas de file séparée pour le règlement.** Sa part est bornée par `settleConcurrency` (32). Si la mesure
  montre encore une pression, on abaisse cette constante.

## Definition of Done
- [x] PR1 : 4 écrivains. Une écriture qui bloque sur une clé en vol n'arrête pas les autres (tombe sous
      `batchWriters = 1`) et rend `applied=false` une fois la clé commitée. Somme du grand livre = solde sous
      concurrence ; `-race`.
- [x] PR2 : CTE unique ; les tests du batcher restent verts sans changement de contrat.
  - **Écart arbitré par Fable (09/10/2026).** Le solde rendu à un rejeu inclut maintenant les voisins du même
    lot (le solde durable au COMMIT), et non plus le solde lu avant les deltas du lot. Aucun appelant ne le
    lit (`credit/reserver.go` ne lit que `GetReserved`), et avec 4 écrivains un voisin d'un autre lot
    l'incluait déjà. `TestBatcherAnswersAReplayWithTheDurableBalance` attend -6.
  - **`jsonb_array_elements` plutôt que `jsonb_to_recordset`.** sqlc 1.30 ne résout pas les colonnes d'une
    liste de définition (`column "ord" does not exist`).
- [x] VPS après chaque PR (runs 4 et 6 de step-287) : `billing_durable_stage_seconds`, lots/s et taille, réservations/s, CPU Postgres.
