# `ingest_duration_seconds` est déclarée, enregistrée, et jamais observée

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-201 (`tasks-done/step-201.md:148`) · **Portée par :** —

La métrique existe dans le catalogue et n'a **aucun site d'observation** hors tests. Le harnais de
charge contourne déjà en lisant les échantillons de l'injecteur plutôt que l'exposition.

**Ce qu'il en coûte.** Écrit : le budget d'ingestion (§1.2, p50 < 50 ms, p99 < 250 ms) n'est mesuré
nulle part. Et quand il le sera, ses bornes l'encadrent mal — `ExponentialBuckets(0.001, 2, 13)` place
p50 entre 32 et 64 ms, p99 entre 128 et 256 ms : « aucun des deux seuils n'est décidable depuis son
exposition ». `message_e2e_duration_seconds` a reçu son correctif ; celle-ci non.

**À quoi on reconnaîtra qu'il faut la payer.** **Avant** que quiconque publie un verdict sur le budget
d'ingestion — c'est-à-dire avant step-280, qui le publiera sans cette métrique si rien ne bouge.

Sources : `internal/observability/metrics/catalog.go:39` · `:158`

**Suite (step-380).** `get-metrics-summary` publie `ingest_latency_ms_p50/p99` à `null` pour la même
raison : ni l'exposition ni le CDR ne portent cette latence. Payer cette dette rend le champ servable.
