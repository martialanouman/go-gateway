# Les DLR d'un client injoignable retardent ceux de tous les autres

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** campagne step-287 (VPS de test, 07/10/2026) · **Portée par :** —

**Ce qu'on a fait à la place.** Le groupe `mo-dlr-router-svc-dlr-delivery` traite `dlr.events`
enregistrement par enregistrement. Pour un compte sans bind ni webhook actif, chaque DLR fait, en série
(`internal/modlrrouter/deliverer.go:140-170`) :
- une recherche de session (`tryBinds`) ;
- une lecture Postgres du webhook (`webhooks.Get`) ;
- un log `Warn` ;
- un produce synchrone vers le topic de lettre morte.

**Pourquoi.** Le chemin a été écrit pour l'exception : un client qui perd sa connexion quelques instants. Pas
pour un client qui n'a jamais eu de moyen de recevoir ses DLR.

**Ce qu'il en coûte.** Mesuré le 07/10/2026 : les 24 clients de charge de la campagne, sans bind ni webhook,
ont laissé **1 115 342 DLR** en retard dans ce groupe, écoulés à **~18/s**. Les DLR des autres comptes
partagent les mêmes partitions et attendent derrière. Le smoke du déploiement de `600d5c0` a échoué deux fois
faute de recevoir ses DLR à temps ; il a fallu ramener le groupe à la fin. En production, un seul gros client
A2P sans réception de DLR suffirait à retarder les accusés de tous les autres.

**À quoi on reconnaîtra qu'il faut la payer.** Dès qu'un client sans moyen de réception existe en
production, ou que le lag de `mo-dlr-router-svc-dlr-delivery` dépasse quelques secondes de trafic. Les pistes
sont, au choix :
- un traitement par lot de poll ;
- un cache négatif « ce compte n'a ni bind ni webhook », relu au rechargement de configuration ;
- un lot de produces vers la lettre morte.
