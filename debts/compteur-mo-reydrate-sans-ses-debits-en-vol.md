# Le compteur MO se réhydrate sans ses débits en vol

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-284 · **Portée par :** —

**Ce qu'on a fait à la place.** step-284 soustrait les réserves MT en vol à la réhydratation du cache de solde
(ADR-0022, `billing:inflight:mt:…`). Le compteur MO a la même fenêtre : `recordmo.lua` débite Redis avant
que `RecordDurable(mo_charge)` committe (`internal/billing/billing.go`, `RecordMO`), et `rehydrateMO` lit
le seul durable.

**Pourquoi.** Arbitrage Fable du 30/09/2026 : le compteur MO ne bloque rien (§6.9), il s'arrête à
`mo_billing_floor`. L'écart ne fait pas dépasser un solde prépayé ; il retarde l'arrêt de l'accumulation.

**Ce qu'il en coûte.** À chaque réhydratation sous trafic MO, le compteur surestime le solde d'au plus les
crédits MO en vol : quelques MO de plus accumulés au-delà du plancher, et l'alerte
`mo_balance_floor_reached` en retard d'autant.

**À quoi on reconnaîtra qu'il faut la payer.** Un client dont le compteur MO dépasse son plancher de plus
qu'un message, ou une exigence de plancher MO strict.
