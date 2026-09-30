# billing-svc reste en `Recreate` après la transition vers les deltas de solde

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-284 (revue de la PR3) · **Portée par :** —

**Ce qu'on a fait à la place.** `deploy/k8s/billing-svc.yaml` déclare `strategy: Recreate`, gardé par
`internal/deploy/billing_strategy_test.go`. Un déploiement progressif mêlerait une réplique antérieure à
ADR-0022, qui réhydrate le cache depuis `balances` sans les deltas non repliés, et une nouvelle.

**Pourquoi.** Seule la **transition** vers les deltas mêle deux lectures incompatibles du solde. Une fois
toutes les répliques au-delà d'ADR-0022, deux versions successives lisent le solde de la même façon, et le
déploiement progressif redevient sûr. On a gardé `Recreate` faute d'un moyen simple de le limiter à cette
seule version.

**Ce qu'il en coûte.** Chaque déploiement de billing-svc le coupe entièrement : le routeur retient les
messages facturés (offsets non committés) le temps de la bascule, puis rattrape un backlog (ADR-0022 §6,
step-285).

**À quoi on reconnaîtra qu'il faut la payer.** Dès que la transition est déployée partout (test et
production) : repasser en `RollingUpdate`, et ne garder la garde que pour une future rupture de lecture.
