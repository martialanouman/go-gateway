# Le drainer de `mt.reroute-park` n'estampille pas `ReplayedAt`

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-201 (`tasks-done/step-201.md:449`) · **Portée par :** —

Le rejeu **manuel** pose bien `routed.ReplayedAt = &now`. Le drain **automatique**, lui, ré-encode
l'enregistrement tel quel, avec son `SubmittedAt` d'origine.

**Ce qu'il en coûte.** Écrit : après 10 minutes de parking, les messages « sortent au-delà de la
dernière borne finie (327,68 s) et atterrissent dans le bucket `+Inf`, ce qui fait rendre `Fail` au
vérificateur ». Couplé à `CONNECTOR_MAX_MESSAGE_AGE` : le jour où ce filtre sera activé, un drain
automatique dead-lettrerait **en masse** des messages parfaitement valides.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier drain automatique après incident — ou
l'activation de `MAX_MESSAGE_AGE`, qui doit venir après celle-ci.

Sources : `internal/connectorpool/drainer.go:100` · `internal/connectorpool/replay.go:131`
