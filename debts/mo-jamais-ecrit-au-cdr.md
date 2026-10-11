# Aucune ligne MO n'est écrite au CDR : `mo_received` lu au CDR vaut toujours 0

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-402 (arbitrage Fable, portée) · **Portée par :** —

**Ce qu'on a fait à la place.** step-402 ajoute `mo_received` aux tranches de `get-traffic-metrics` et le
lit au CDR (`internal/storage/clickhouse/metrics.go:123`), comme le `mo_received` de
`get-metrics-summary` (`metrics.go:74`). Mais aucun écrivain ne produit de ligne `direction = 'mo'` :
`mo-dlr-router-svc` publie `mo.routed` (`internal/modlrrouter/moresolver.go:211`), et rien d'autre. Le
seul MO vivant est le compteur `mo_received_total` du flux temps réel, posé par step-402 dans
connector-pool.

**Pourquoi.** La spec le demande (§4.3, `docs/specification-technique-passerelle-sms.md:554`, « → CDR
écrit »). Mais écrire la ligne MO suppose d'arrêter son statut, son idempotence quand `mo.inbound` est
relivré, et le chiffrement de son corps. Cela doublait une PR dont le sujet est un contrat de lecture,
qui ne dépend pas de l'écrivain.

**Ce qu'il en coûte si on ne la paie jamais.** Les graphiques MO du tableau de bord restent plats au
chargement et sur toute plage passée, alors que le flux temps réel montre du MO. Le CDR Explorer et
l'export n'ont aucun MO non plus.

**Ce que la payer demande.** Que l'écrivain renseigne `connector_id` (le connecteur qui a reçu le MO)
et le `customer_id` du compte résolu, sous un `message_id` propre au MO (les métriques groupent par
direction, l'explorateur non). Un MO sans `connector_id` disparaît de la vue par connecteur. Un MO non
routé n'a pas de client et reste hors du CDR.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier écran Trafic du tableau de bord
(go-gateway-bo step-081) qui trace le MO sur une plage passée. Au plus tard au go-live (step-410).
