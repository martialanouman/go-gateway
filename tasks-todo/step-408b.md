# step-408b — Les lectures du grand livre par `message_id` élaguent, ou ne lisent plus le grand livre

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-408 · **Bloque :** step-410
> Suffixe de lettre : aucun numéro libre entre step-408, dont elle dépend, et step-410, qu'elle bloque.

## Pourquoi cette fiche existe

step-408 a donné au grand livre une partition par jour. Les lectures qui le consultent par `message_id` seul
ne bornent pas `created_at` et n'élaguent donc rien : chacune planifie et verrouille **chaque partition et ses
trois index**. Ce sont `GetMessageEntries` (`internal/storage/postgres/queries/billing.sql`, step-287k) et
`GetReserveEntry` (`:175`, quand la réservation Redis a expiré), la LATERAL de `ListOrphanedReservations` (`:186`)
et `ConsumedCredits` (`:50`). Avant step-408, il n'y avait qu'une partition, et le coût n'existait pas.

En ordre de grandeur, **estimation non mesurée** : vers une centaine de partitions, environ 400 verrous par
lecture, dont 64 au plus en fast-path. Une vingtaine de lectures concurrentes dépassent alors la table partagée
des défauts PG (`max_locks_per_transaction` 64 × `max_connections` 100). Les captures échouent en « out of
shared memory » (53200) **en quelques mois de production**, puisque rien n'est purgé
(`debts/grand-livre-ni-detache-ni-archive-ni-purge.md`). Le temps de planification, lui, croît dès le premier
jour, linéairement.

Décision humaine (04/10/2026) : une step avant le go-live, plutôt qu'une dette payable après. Fable avait
recommandé de ne pas la traiter dans step-408, parce que le remède change la sémantique de l'idempotence et
mérite ses propres tests.

Dettes qu'elle paie : `debts/lectures-du-grand-livre-par-message-id-non-elaguees.md`. Elle touche les mêmes
lectures que `debts/capture-lit-le-grand-livre-trois-fois-avant-d-ecrire.md` (portée par step-409) : le design
dira s'il paie aussi celle-là.

## Périmètre

1. **Mesurer d'abord.** Le nombre de verrous d'une capture et le temps de planification, avec 8, 100 et 400
   partitions, sur PG18 et avec les réglages réels du VPS. Si la mesure dément l'estimation, la fiche le dit
   et la step se réduit en conséquence.
2. **Faire élaguer ou contourner** chacune des quatre lectures. Deux pistes à arbitrer au design :
   - lire l'existence dans `billing_idempotency` (non partitionnée). Mais `ClaimIdempotency` réclame *avant* le
     mouvement, donc « réclamation présente » n'implique pas « mouvement écrit » : il faut prouver que la
     réclamation et le mouvement sont dans la même transaction sur tous les chemins (lot `COPY` de step-285b,
     `RecordDurable`, Topup, Transfer) ;
   - borner `created_at` à partir de l'horodatage de l'UUIDv7 du `message_id`, avec une marge. Cas limites :
     un `message_id` généré longtemps avant son mouvement (reprise, rejeu de dead-letter, capture tardive), et
     un mouvement sans `message_id`.
3. **Un test qui tombe** si une de ces lectures cesse d'élaguer, par exemple avec `EXPLAIN` sur une base à
   N partitions, qui compte les partitions parcourues.

## Hors périmètre

Détacher, archiver et purger (`debts/grand-livre-ni-detache-ni-archive-ni-purge.md`) : cela borne le nombre de
partitions à ~395, mais ne rend aucune lecture élaguable.

## Definition of Done

- [ ] mesure consignée (verrous et planification, à 8 / 100 / 400 partitions) ;
- [ ] les quatre lectures élaguent, ou ne lisent plus le grand livre, et un test le prouve ;
- [ ] invariant (c) : la facturation reste idempotente sous double livraison, y compris d'un jour à l'autre ;
- [ ] la fiche de dette passe à PAYÉE, avec la date et la PR.
