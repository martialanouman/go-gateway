# Le flux temps réel n'émet rien des DLR : deux taux de succès peuvent diverger

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-380 (design arrêté, arbitrage humain) · **Portée par :** —

**Ce qu'on a fait à la place.** La DoD de step-380 voulait des définitions « identiques à celles du
flux ». Le flux n'en a pas pour la remise : le router émet `messages_total{status=routed|rejected}`, le
pool `submits_total{connector_id,status}`, et `modlrrouter` rien. Seul `rejected` coïncide (même
événement, même ligne CDR). `delivered`/`failed` n'ont qu'une définition dans le dépôt, la précédence
§6.6 partagée entre l'explorateur CDR et les métriques (`cdrStatusPrecedence`).

**Ce qu'il en coûte.** Un tableau de bord qui calculerait un taux de succès en direct depuis
`submits_total` (accepté par le SMSC) et au chargement depuis le CDR (DLR reçu) afficherait deux
chiffres différents sur le même écran.

**Ce que la payer demande.** Faire émettre `dlr_total{status}` par `modlrrouter`, avec la même
précédence (`failed` = failed + expired).

**À quoi on reconnaîtra qu'il faut la payer.** Le premier widget « taux de succès » en temps réel.
