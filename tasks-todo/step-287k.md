# step-287k — Une capture lit le grand livre une fois, pas trois

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-287i (trace du run 9) · **Bloque :** la reprise de step-287
> Demande humaine du 10/10/2026, née du run 9 de step-287.

## Pourquoi
Trace de billing-svc au run 9 (5 s) : 53 s d'attente cumulée d'une connexion du pool Postgres, soit ~10
goroutines bloquées en permanence pour un pool de 10. 93 % de cette attente vient de
`SettleConsumer` → `Capture` → `resolveTerminal`, qui lit le grand livre trois fois par message, chaque
lecture avec son propre acquire : `LedgerEntryExists(capture)`, `LedgerEntryExists(release)`,
`GetReserveEntry`. Le lot de réservation attend derrière (`begin` 26,6 ms).

## Design arrêté
- **`GetMessageEntries :one`** rend, en un aller-retour, la présence d'une écriture `reserve`, `capture`
  et `release` pour un `message_id` : `COALESCE(bool_or(entry_type = …), false)` sur
  `billing_ledger WHERE message_id = @message_id AND entry_type IN (…)`. Même table, même index
  (`billing_ledger_idem_idx`, partition par partition) que les trois lectures qu'elle remplace : la garde
  reste celle du grand livre, sur toutes les partitions.
- **`cp.MessageEntries{Reserve, Capture, Release bool}`** et `Has(entryType)` ; `LedgerStore` gagne
  `MessageEntries(ctx, messageID)` et perd `LedgerEntryExists`, qui n'a plus d'appelant hors des tests
  (les tests passent à `MessageEntries`).
- **`resolveTerminal`** garde son verrou Redis et son ordre de décision : terminal déjà écrit → no-op ;
  terminal opposé écrit → cède ; capture sans réserve → anomalie ; sinon `RecordDurable`. Seule la
  lecture change.
- Hors périmètre : la branche `no_reservation` de `Capture` et `Release`, qui lisent encore
  `ReserveEntry` pour le montant ; elles ne servent que si la réservation Redis a expiré.

## Definition of Done
- [ ] `GetMessageEntries` : chaque type présent ou absent, isolément, y compris sur deux partitions
      (intégration, mutations du SQL généré)
- [ ] `resolveTerminal` ne lit plus qu'une fois ; les tests existants (idempotence, cession, absence de
      réserve, concurrence capture/libération) restent verts et tuent les mutations de la décision
- [ ] mesuré à un run de step-287 : l'attente du pool par `Capture` baisse dans la trace
