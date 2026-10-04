# Le débit d'un client est borné par la latence de sa réserve : partitions ÷ latence

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-285 (rejeu VPS du 03/10/2026) · **Portée par :** step-409 (verdict)

**Ce qu'on a fait à la place.** step-285 a supprimé le crash et le backoff qui figeaient le routeur. Elle
n'a pas touché au débit : une voie (une partition) réserve un message après l'autre, de façon synchrone,
avant de le publier. Le plafond d'un client vaut donc à peu près le nombre de partitions divisé par la
latence d'une réserve.

**Pourquoi.** La fiche de step-285 renvoyait à la mesure la question « l'écriture durable synchrone est-elle
le bon modèle à 8 000/s ? ». Sa DoD portait sur la reprise sans redémarrage, pas sur le débit.

**Ce qu'il en coûte.** Mesure du VPS : ~225 messages/s pour un client. Une réserve prend 41 ms, dont 33 ms
d'écriture durable : réclamation d'idempotence, delta et grand livre dans une transaction à commit
synchrone. Postgres monte à 1,2 cœur, et les routeurs restent presque inactifs. Loin des 8 000/s, et la
latence ne baisse pas en ajoutant des réplicas : seul le nombre de partitions élève le plafond.

**À quoi on reconnaîtra qu'il faut la payer.** Le verdict de step-409, ou un client dont le débit
contractuel dépasse partitions ÷ latence. Pistes : réserver par lot de voie (un aller-retour pour N
messages), plusieurs réserves en vol par voie avec publication dans l'ordre, ou une écriture durable moins
chère (commit asynchrone du grand livre, ce qui rouvre ADR-0022).

**Payée en partie par step-285b (#255, 04/10/2026).** Le coût Postgres par message : un commit pour ~8
mouvements, CPU de Postgres divisé par deux, attente du WAL de 36 % à 11 % des échantillons. Le plafond par
client, lui, n'a presque pas bougé (273 → 294 réserves/s) : il tient maintenant à la concurrence par voie,
une réserve à la fois, qui ne forme que de petits lots. Relevés : `tasks-done/step-285b.md`, journal.

**Payée en partie par step-285c (#258, 04/10/2026).** Huit réserves en vol par voie, publiées dans
l'ordre : 301 → 951 réserves/s pour un client, lots de 8,9 → 33,9 mouvements, CPU de Postgres +16 %. Le
plafond vaut maintenant voies × 8 ÷ latence, et la latence d'une réserve double (58 ms d'écriture durable,
attente `WALWrite`). Reste ouverte : 951/s contre 8 000/s visés ; le verdict revient à step-409. Relevés :
`tasks-done/step-285c.md`, journal.

Sources : `internal/router/router.go` (`runLane`, `laneWindow`) ·
`internal/billing/billing.go` (`Reserve`, `RecordDurable`) · `cmd/billing-svc/wiring.go`
(`billing_reserve_stage_seconds`)
