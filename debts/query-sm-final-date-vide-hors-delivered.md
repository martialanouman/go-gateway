# `query_sm` ne date pas un état final autre que DELIVERED

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-390b · **Portée par :** —

**Ce qu'on a fait à la place.** `query_sm_resp` porte un `final_date` seulement pour un message
`delivered`, tiré de `delivered_at`. Pour `expired`, `failed` et `cancelled`, l'état est final mais
`final_date` reste vide (`internal/smppserver/ops.go`, `onQuery`). SMPP 3.4 §4.8.2 réserve le champ
vide aux messages qui ne sont pas encore dans un état final.

**Pourquoi.** Le CDR n'a pas d'horodatage d'issue : `delivered_at` n'est rempli que pour `delivered`
(`internal/modlrrouter/modlrrouter.go:210`), l'agrégat ne le remonte que pour un message entièrement
livré (`internal/storage/clickhouse/cdr.go:366`), et `version` est un rang, pas une date. Remplir
`delivered_at` pour les autres issues aurait changé le sens d'une colonne publique (REST get-message,
export et recherche Admin, `latency_ms`, `cdr_events.at`). Ajouter une colonne `finalized_at` suppose une
migration ClickHouse et le passage de tous les writers d'issue, hors de proportion pour un champ
informatif d'une opération de polling que la spec déconseille (§6.22). Arbitrage Fable, 2026-09-27.

**Ce qu'il en coûte.** Un ESME qui date l'échec d'un message à partir de `final_date` lit un champ vide.
L'état, lui, est juste.

**À quoi on reconnaîtra qu'il faut la payer.** Un client dont le logiciel rejette un `query_sm_resp` à
état final sans date, ou une autre raison d'ajouter un horodatage d'issue au CDR (une surface REST qui
voudrait `failed_at`, par exemple) : les deux se paient par la même colonne.

Source : `internal/smppserver/ops.go` (`onQuery`), `internal/modlrrouter/modlrrouter.go:210`
