# Le `deliver_sm` sortant voyage avec un `sequence_number` factice

> **Statut :** OUVERTE (acceptée) · **Nature :** technique
> **Née de :** le code lui-même · **Portée par :** —

Le champ est un « **placeholder**: the owning pod's send window allocates its own ». C'est correct de
bout en bout : le pod propriétaire réalloue avant d'émettre.

**Ce qu'il en coûte.** Faible, et c'est pourquoi cette fiche dit « à documenter plutôt qu'à
corriger » : le PDU circule néanmoins sur le fil gRPC dans un état invalide entre deux services. Tout
consommateur futur du lien `mo-dlr-router → smpp-server` qui ferait confiance au champ se tromperait.

**À quoi on reconnaîtra qu'il faut la payer.** Un troisième service branché sur ce lien. Pas avant.

Source : `internal/modlrrouter/build.go:15`
