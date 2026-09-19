# Le fail-open de la capture laisse un débit ouvert jusqu'au passage du reaper

> **Statut :** OUVERTE (atténuée) · **Nature :** produit
> **Née de :** step-146 · **Portée par :** — (atténuée par step-190, livrée)

`connector-pool` règle **fail-open** : une panne de facturation laisse des débits de réserve sans
rien pour les fermer, et « the customer stays charged for a message that may never have been sent ».
Le filet existe et il est sérieux — le reaper est piloté par le grand livre, arbitré par le CDR, et
refuse de relâcher en aveugle : « refunding a message that was really sent is a free delivery ».

**Ce qu'il en coûte.** La fenêtre résiduelle, entre la panne et le passage du reaper (`minAge`,
15 min), pendant laquelle un client est débité d'un message peut-être jamais parti.

**À quoi on reconnaîtra qu'il faut la payer.** Si la fenêtre de 15 minutes devient visible en
facturation — c'est-à-dire si les pannes de `billing-svc` cessent d'être rares.

Source : `internal/billing/reaper.go:104`
