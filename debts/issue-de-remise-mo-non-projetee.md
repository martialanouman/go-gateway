# L'issue de la remise d'un MO au compte n'est pas projetée au CDR

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-402 (arbitrage Fable) · **Portée par :** —

**Ce qu'on a fait à la place.** Un MO routé a une seule ligne CDR, en statut `accepted`, écrite depuis
`mo.routed` (`projectMORouted`, `internal/ingest/accepted.go`). Ce qu'il devient ensuite n'atteint
jamais le CDR : remis à un bind, confié au webhook ou parqué au dead-letter (`Deliverer.Deliver`,
`internal/modlrrouter/deliverer.go`). Un MO lit `accepted` à vie, et le contrat public le dit
(`MessageStatus`).

**Pourquoi.** Écrire `delivered` dès le routage mentirait pour un MO parti au dead-letter. Projeter
l'issue réelle demande de suivre trois chemins, dont un asynchrone : le webhook a ses propres reprises
et son propre dead-letter.

**Ce qu'il en coûte si on ne la paie jamais.** Le support ne peut pas dire, depuis le CDR Explorer, si
un client a reçu son MO. Il doit croiser `mo.dead-letter` et `webhook.dead-letter`.

**À quoi on reconnaîtra qu'il faut la payer.** À la première demande « le client dit ne pas avoir reçu
ce MO », ou à un filtre de statut MO dans le tableau de bord. Le rang au-dessus de `accepted` est libre
pour cette ligne, sous la même clé de tri.
