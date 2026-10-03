# Le retard du groupe qui marque la première émission d'un sender ID n'est ni publié ni alerté

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** ADR-0023 (sender ID utilisé non supprimable) · **Portée par :** —

**Ce qu'on a fait à la place.** Le groupe `router-svc-sender-first-use` consomme `mt.outcome`
(`cmd/router-svc/wiring.go`, `newFirstUseMark`) sous `runResilient` : une panne se lit dans les logs
ERROR, rien d'autre. `pollQueueDepth` (`cmd/router-svc/main.go:108`) ne lit que les groupes
`mt.inbound` et `-outcome-cdr`, et `queue_depth_records` est indexé par topic : `mt.outcome` y est
déjà celui de la projection CDR.

**Pourquoi.** Publier ce retard demande un label de groupe sur une jauge indexée par topic, ou une
jauge neuve et son alerte. C'est hors du sujet de la règle (409 sur un sender ID utilisé), et la
passerelle n'est pas encore en production.

**Ce qu'il en coûte.** Un `MarkFirstUsed` qui échoue en boucle (droit UPDATE retiré, Postgres en
panne) laisse tout sender ID utilisé supprimable, en silence. Si le retard dépasse la rétention
Kafka de `mt.outcome`, les marques sont perdues pour de bon.

**À quoi on reconnaîtra qu'il faut la payer.** Avant le go-live, ou au premier sender ID supprimé
alors que des CDR le citent.
