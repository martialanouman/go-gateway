# Les lectures du grand livre par message_id parcourent toutes les partitions

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-408 (`internal/storage/postgres/queries/billing.sql:104`) · **Portée par :** step-408b

**Ce qu'on a fait à la place.** step-408 crée une partition du grand livre par jour, sans toucher aux lectures
qui le consultent par `message_id` seul : `GetMessageEntries` (une fois par capture depuis step-287k) et
`GetReserveEntry` (`:175`, quand la réservation Redis a expiré), la LATERAL de `ListOrphanedReservations` (`:186`)
et `ConsumedCredits` (`:50`). Aucune ne borne `created_at`, donc aucune n'élague : chacune planifie et
verrouille chaque partition et ses trois index.

**Pourquoi.** Le remède change la sémantique de l'idempotence, pas le DDL. Lire l'existence dans
`billing_idempotency` (non partitionnée) suppose que la réclamation implique le mouvement, or
`ClaimIdempotency` la prend *avant* lui. Borner `created_at` depuis l'horodatage de l'UUIDv7 du `message_id`
est un autre choix, avec ses propres cas limites. Arbitré par Fable en revue de step-408 : une step à part, pas
une ligne de step-408. Ce sont les mêmes lectures que
`debts/capture-lit-le-grand-livre-trois-fois-avant-d-ecrire.md` (portée par step-409) : un redesign qui
paie l'une doit dire s'il paie l'autre.

**Ce qu'il en coûte.** Le temps de planification croît linéairement avec le nombre de partitions (doc PG18,
ddl-partitioning). Et les verrous, quatre relations par partition : en ordre de grandeur, vers une centaine
de partitions, quelque 400 verrous par lecture, dont 64 au plus logent hors de la table partagée (fast-path).
Une vingtaine de lectures concurrentes (2 réplicas × 10 connexions) dépasseraient la table partagée des défauts
PG (`max_locks_per_transaction` 64 × `max_connections` 100 = 6 400) : « out of shared memory » sur les
captures. **Estimation non mesurée** : le dépôt ne fixe aucun des deux réglages. Comme rien n'est purgé
(`debts/grand-livre-ni-detache-ni-archive-ni-purge.md`), on y arrive en quelques mois de production. Monter
`max_locks_per_transaction` recule le plafond, pas le coût de planification.

**À payer** avant le go-live, ou avant 60 jours de production au plus tard. On reconnaîtra l'urgence à la
latence de `billing_reserve_stage_seconds` qui croît de jour en jour, ou à une erreur 53200 sur une capture.
