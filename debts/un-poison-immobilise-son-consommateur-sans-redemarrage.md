# Un enregistrement qui échoue toujours immobilise son consommateur, sans redémarrage ni alerte

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-285 (design arrêté, point 9) · **Portée par :** —

**Ce qu'on a fait à la place.** `Consumer.Run` et `RunBatch` rejouent en place un enregistrement en échec,
sans repoller, jusqu'à ce qu'il passe. La cadence suit un backoff de 1 à 30 s. Un enregistrement qui
échouera toujours bloque donc tout le poll de son consommateur, toutes partitions comprises. Exemples :
un `mt.inbound` indécodable (il n'y a pas de dead-letter avant M7), ou une dépendance absente comme
`content-key-svc`. Le seul signal est un `WARN` par tentative. Il n'y a ni `CrashLoopBackOff` ni compteur,
et `/readyz` reste vert.

**Pourquoi.** Avant step-285, le même enregistrement faisait redémarrer le processus en boucle : même
blocage, mais visible, et tous les composants du pod tombaient avec lui. Le rejeu en place garde le reste
du pod vivant et ne déplace aucune partition. Il fallait un dead-letter pour faire mieux, et ce n'était pas
le sujet de la step.

**Ce qu'il en coûte.** Un flux arrêté que personne ne voit, tant que la règle d'alerte sur le lag
(ADR-0012 § Surveiller, hors dépôt) n'est pas déployée.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier poison en production, ou le premier `WARN`
« replaying in place » dont le numéro de tentative dépasse la dizaine. Deux paiements possibles :
`kafka_consume_retries_total{group}` via un crochet dans `Consumer`, et un dead-letter pour `mt.inbound`.

Sources : `internal/storage/kafka/consumer.go` (`Consumer.replay`) · `internal/router/router.go`
(`handle`, « no dead-letter path until M7 »)
