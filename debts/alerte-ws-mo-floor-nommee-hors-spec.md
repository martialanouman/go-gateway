# L'alerte WS de plancher MO s'appelle `mo_floor_reached`, la spec dit `mo_balance_floor_reached`

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-400 (04/10/2026) · **Portée par :** —

**Ce qu'on a fait à la place.** Le flux `stream-billing-alerts` émet `alert: mo_floor_reached`
(`internal/metricstream/metricstream.go:81`, `api/openapi-admin.yaml:3234`). La spec nomme l'événement
`mo_balance_floor_reached` (`specification-technique-passerelle-sms.md` §835,
`specification-technique-tableau-de-bord.md:357`). Le topic durable `billing.events` (step-400) prend le
nom de la spec ; le WS garde l'ancien.

**Pourquoi.** Le contrat WS est publié et le tableau de bord en génère ses clients : renommer la valeur
est une rupture. step-400 avait pour sujet la source durable, pas ce renommage.

**Ce qu'il en coûte si on ne la paie jamais.** Deux noms pour une transition. Un BFF qui corrèle
l'affichage (WS) et la détection (topic) doit traduire l'un vers l'autre.

**À quoi on reconnaîtra qu'il faut la payer.** Au prochain bump majeur du contrat Admin, ou dès que le
BFF corrèle les deux flux.
