# Un rééquilibrage pendant un lot ralenti rejoue des envois déjà partis

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-283 (revue, arbitrage Fable A) · **Portée par :** step-287 (à chiffrer)

**Ce qu'on a fait à la place.** Au plafond du connecteur, chaque `submit_sm` attend son jeton
(`internal/connectorpool/submit.go:218`). Un lot de poll dure alors à peu près « enregistrements / débit ». Sa
taille n'est bornée que par les octets (`internal/storage/kafka/consumer.go:116
184`), et le consumer ne bloque
pas le rééquilibrage. Si un rééquilibrage tombe pendant ce lot, `CommitRecords` échoue et le nouveau
propriétaire rejoue les `submit_sm` déjà partis.

**Pourquoi.** Les bornes d'ADR-0012 et d'ADR-0014 portent sur un nombre : un poll par partition et par
incident. Elles ne changent pas. ADR-0014 laisse volontairement le taux d'incidents non borné, et refuse une
seconde variable de borne. L'attente AIMD (step-086) allongeait déjà les lots. Enfin,
`PollRecords(ctx, n)` déplacerait la base de mesure de step-287.

**Ce qu'il en coûte.** Sous backpressure soutenue, un redéploiement ou un scale du pool produit plus de
SMS en double que sous l'ancien rejet au routeur. Le nombre de doublons reste dans la borne, mais la borne
est atteinte plus souvent.

**À quoi on reconnaîtra qu'il faut la payer.** Des doublons mesurés au rejeu de step-287, ou un lag
`mt.routed` durable sur un connecteur plafonné. Options : `PollRecords` borné à quelques secondes de
plafond, ou `BlockRebalanceOnPoll`.
