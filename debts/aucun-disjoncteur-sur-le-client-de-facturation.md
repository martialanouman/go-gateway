# Aucun disjoncteur sur le client de facturation

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-190 (`tasks-done/step-190.md:87`) · **Portée par :** —

`Settler` borne chaque appel par son `timeout` et échoue **ouvert**, pour une raison écrite et juste :
propager l'erreur « would redeliver and re-submit a known-bad message, burning SMSC TPS ». Mais rien
ne compte les échecs successifs — `grep -i breaker internal/billing/ internal/connectorpool/settle/`
ne rend rien.

**Ce qu'il en coûte.** Non écrite. En pratique : un `billing-svc` durablement indisponible fait payer
**un timeout complet par message** sur le chemin d'envoi. La campagne NFR (step-280) ne mesurera pas
ce cas, puisqu'elle mesure un `billing-svc` vivant.

**À quoi on reconnaîtra qu'il faut la payer.** La première panne de `billing-svc` en charge, ou le
jour où le débit d'envoi devient sensible à la latence d'un service qui n'est pas sur son chemin.

Source : `internal/connectorpool/settle/settle.go:109`
