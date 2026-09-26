# Les métriques 24 h ne sont pas servies : le pré-agrégat n'existe pas

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-380 (design arrêté, arbitrage humain) · **Portée par :** —

**Ce qu'on a fait à la place.** `get-metrics-summary` et `get-traffic-metrics` n'acceptent que `5m` et
`1h` ; `24h` répond 422 (`internal/adminapi/metrics.go`, `metricsWindows`). La spec du tableau de bord
(§6.3) liste pourtant la bascule 5 min / 1 h / 24 h.

**Pourquoi.** Les deux lectures agrègent le CDR brut, message par message. À 8 000 SMS/s, 24 h font
~690 M messages par requête : un déni de service à la main de n'importe quel opérateur. La spec le
prévoit elle-même : « longues via instantané REST pré-agrégé ».

**Ce que la payer demande.** Une table d'agrégats (par heure × connecteur × client) et sa migration
ClickHouse. Une vue matérialisée naïve sur `cdr` compte chaque **version** insérée — `accepted` puis
`delivered` = deux fois : il faut agréger l'état final, par un job qui relit les heures closes, ou une
MV par état terminal.

**À quoi on reconnaîtra qu'il faut la payer.** Le tableau de bord qui livre la bascule 24 h.
