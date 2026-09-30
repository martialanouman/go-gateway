# Une réservation expire pendant une longue backpressure

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-283 (revue, arbitrage Fable E) · **Portée par :** —

**Ce qu'on a fait à la place.** Le crédit est réservé au routeur. Il est capturé au pool, après
l'envoi. La réservation vit `defaultHoldTTL`, soit 5 min (`internal/billing/billing.go:64`). Si un
connecteur plafonné retient un message plus longtemps, la réservation expire. La capture passe alors par le
chemin lent : elle relit le montant réservé dans le grand livre.

**Pourquoi.** Ce chemin lent est correct. Le parking de step-126 attendait déjà plus de 5 min. step-283 ne
change que la fréquence à laquelle on l'emprunte.

**Ce qu'il en coûte.** Sous backpressure soutenue, chaque capture relit le grand livre au lieu du
cache : c'est plus de charge Postgres, au moment précis où le système est déjà saturé.

**À quoi on reconnaîtra qu'il faut la payer.** Des captures sur le chemin lent mesurées pendant une
backpressure (step-287). Les options : allonger le TTL, ou prolonger la réservation au moment de l'envoi.
