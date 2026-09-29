# Pas de file prioritaire sur un connecteur partagé

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** `docs/adr/0020-categorie-de-trafic-declaree-et-priorite-sortante.md` (option D) · **Portée par :** ADR-0021 (Proposed), step à créer

**Ce qu'on a fait à la place.** La priorité d'un OTP s'obtient en lui **réservant** des connecteurs
(`priority_tier`). Sur un connecteur qui accepte tout (`priority_tier = 0`), OTP et marketing partagent la
même partition FIFO de `mt.routed` : le `priority_flag` part sur le fil, mais le message attend son tour
dans la passerelle.

**Pourquoi.** Une file prioritaire double les partitions de `mt.routed`, impose une politique anti-famine,
et duplique le chemin reroute / parking / rejeu / dead-letter. Aucune mesure ne montrait encore qu'une
réservation ne suffit pas (ADR-0020, *Trade-off Analysis*).

**Ce qu'il en coûte.** Un client qui n'a pas de connecteur réservé voit ses OTP retardés d'autant que le
backlog marketing du connecteur qu'ils partagent. Si ce connecteur est plafonné, ce sont des minutes.

**À quoi on reconnaîtra qu'il faut la payer.** Un opérateur de réseau n'offre qu'un seul lien, qu'on ne peut
donc pas réserver, et la latence p99 des OTP sur ce lien dépasse le budget bout-en-bout (2 s). Ou bien le
nombre de connecteurs réservés devient un coût d'exploitation.
