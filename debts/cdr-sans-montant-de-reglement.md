# Le CDR ne dit pas ce qu'un message a coûté

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-287d (ADR-0024) · **Portée par :** —

**Ce qu'on a fait à la place.** `cdr.billed`/`credits_charged` ne sont plus remplis par le pool depuis
step-287d : la capture se fait dans billing-svc, depuis `mt.outcome`. Ils ne faisaient déjà pas autorité
avant : la ligne DLR (`internal/modlrrouter/modlrrouter.go:245`, rang 40, `Billed: false`) les écrase pour
tout message livré, via `argMax(billed, version)` (`internal/storage/clickhouse/cdr.go:311`). Le grand livre
(`control_plane.billing_ledger`, lu par l'Admin API depuis step-149) est la seule autorité.

**Pourquoi.** Écrire le montant dans le CDR depuis billing-svc mettrait ClickHouse dans le chemin d'écriture
de la facturation, ce qu'ADR-0023 a écarté. Et un montant qui survive au DLR demande un rang ou un statut de
règlement dans le modèle versionné du CDR. Aucun écran ne l'a demandé.

**Ce qu'il en coûte.** Le CDR Explorer ne peut pas afficher ce qu'un message a coûté : il faut lire le grand
livre à côté. Côté client, `get-message`/`list-messages` (`internal/restapi/messages.go:243`) rendent désormais
`credits_charged: null` pour un message resté `enroute` aussi, et plus seulement pour un message livré.

**À quoi on reconnaîtra qu'il faut la payer.** Le tableau de bord demande le coût par message dans le CDR
Explorer ou dans l'export.
