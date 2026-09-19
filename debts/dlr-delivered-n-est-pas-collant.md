# Un DLR `failed` tardif écrase un `delivered` déjà écrit

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** le code lui-même (« deferred […] at M4+ ») · **Portée par :** —

Le rang est un entier simple : `delivered=40`, `failed/expired=50`. Le raffinement qui rendrait
`delivered` **collant** est « deferred (the CDR schema anticipates widening the rank at M4+) ». M4
est dépassé depuis longtemps, et l'aveu est dupliqué à deux endroits sans qu'aucun ne nomme de step.

**Ce qu'il en coûte.** Le topic garantit explicitement « No ordering » et « At-least-once ». Un
receipt `failed` anormalement tardif écrase donc un `delivered` déjà écrit. Le texte qualifie le cas
d'« anomalous » — mais un CDR faux est une facture fausse.

**À quoi on reconnaîtra qu'il faut la payer.** Un message livré, facturé, et rapporté en échec. Ou
l'élargissement du rang, que le schéma CDR anticipe déjà.

Sources : `internal/pipeline/envelope.go:198` · `internal/modlrrouter/modlrrouter.go:183`
