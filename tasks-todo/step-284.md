# step-284 — L'écriture durable du solde devient asynchrone, le plancher reste atomique

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-280 · **Bloque :** step-287
> Décision humaine du 29/09/2026 (goulot 3 de step-280) ; unité faute de multiple de dix libre.

## Pourquoi
Réserve **et** capture font une écriture durable synchrone (`RecordDurable` → `AdjustBalance … ON
CONFLICT DO UPDATE`, `internal/billing/billing.go:274` et `:382`) sur **une** ligne
`control_plane.balances` par client. Sur le VPS : 6-7 transactions en attente de verrou, captures au-delà
de 200 ms, 369 `submit_sm/s` pour un client — et vraisemblablement les `RESERVE_TIMEOUT` de step-285.

Décision humaine : rendre cette écriture asynchrone **sans risquer de dépassement de solde, sauf pour les
clients qui en ont le droit** (overdraft, postpayé sans plafond dur).

## Ce qui tient déjà
Le plancher est appliqué atomiquement par `reserve.lua` dans Redis, avant toute écriture durable : le
refus « solde insuffisant » ne dépend pas de Postgres. C'est ce qui rend l'asynchrone possible.

## À arbitrer (spec → Fable → humain)
- **Le piège de la réhydratation** : le cache de solde expire (10 min, `billing.go:65-69`) et se
  réhydrate depuis le solde durable, qui ne reflète aujourd'hui les réserves en cours que **parce que**
  l'écriture est synchrone. En asynchrone, une réhydratation sur un durable en retard rendrait du crédit
  déjà réservé : dépassement. Options : réhydrater durable + deltas non encore persistés ; bloquer la
  réhydratation tant que le filigrane de persistance n'a pas rattrapé ; ou ne plus faire expirer le cache
  (et borner la divergence autrement — la raison d'être du TTL, step-142b).
- **Le transport** : agrégation par client en mémoire puis écriture groupée, ou journal durable
  (`billing.events`, step-400) consommé par un écrivain unique par client. Une perte de processus ne doit
  perdre aucun débit (invariant c : idempotence par `message_id`).
- **Panne Redis** : aujourd'hui fail-closed ; ce que devient la reconstruction du cache.
- Plan §6.9/§13, ADR-0010 et le modèle du grand livre (réserve débite, capture crédite 0, libération
  rembourse) à relire : la somme du grand livre doit rester égale au solde, en différé.

## Definition of Done
- [ ] design arrêté + ADR
- [ ] invariant c vert sous double livraison, et un test de réhydratation pendant un retard de persistance
      qui échoue sur une implémentation naïve (rouge lu)
- [ ] aucun dépassement pour un client prépayé strict sous charge concurrente (test d'intégration)
- [ ] rejouer la mesure mono-client de step-280 : plus de verrou en attente dans `pg_stat_activity`
