# Le broker Redpanda de test se fige en CI

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** CI de `main` des 07/10/2026 (runs 37553369358 et 37564993358) · **Portée par :** —

**Ce qu'on a fait à la place.** Deux fois, `TestConsumerReplaysAFailedRecordInPlace` est resté 9 minutes
dans `kgo.(*Client).waitUnknownTopic` (`internal/storage/kafka/kafka_integration_test.go`, premier
`Produce`), jusqu'au délai du paquet. Les tests précédents du même paquet avaient publié sur `mt.routed` sans
problème. Un broker en pause (`docker pause`) reproduit exactement cette attente en local : le broker ne
répondait plus. Les produces des tests de `internal/storage/kafka` sont désormais bornés
(`testProduceTimeout`, 15 s). Une récidive échoue donc en 15 s avec « records have timed out », au lieu de
geler le paquet 10 minutes. Le broker, lui, n'est pas corrigé.

**Pourquoi.** La cause du gel côté CI n'est pas établie : budget de conteneurs du runner
(`exit 139` = `fs.aio-max-nr`, déjà vu), ou Redpanda `--smp=1` affamé sous `-race`. Il faudrait les logs du
conteneur au moment du gel, que la CI ne garde pas.

**Ce qu'il en coûte.** Un `main` rouge saute le déploiement de test et demande un `gh run rerun --failed`.

**À quoi on reconnaîtra qu'il faut la payer.** Une récidive malgré la borne (le test échoue alors en 15 s,
avec l'erreur) : il faudra faire parler le conteneur, en déversant `docker logs` dans le job en cas d'échec.
