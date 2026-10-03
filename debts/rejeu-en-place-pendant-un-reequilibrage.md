# Un rejeu en place continue sur une partition que le groupe vient de réattribuer

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-285 (design arrêté, points 4 et 9) · **Portée par :** —

**Ce qu'on a fait à la place.** Le consommateur rejoue un lot en échec sans repoller et sans bloquer les
rééquilibrages : `BlockRebalanceOnPoll` n'est pas activé. Supposons qu'un membre rejoigne le groupe
pendant le rejeu : l'ancien propriétaire continue de traiter le suffixe pendant que le nouveau le refetch
depuis le dernier commit. En protocole de groupe classique, franz-go envoie le commit tardif avec la
génération courante sans vérifier la possession (`consumer_group.go`, `commit`) : le broker l'accepte, et
l'ancien membre peut ramener l'offset commité du nouveau propriétaire en arrière. Ni erreur ni redémarrage
(le filtre par possession n'existe que sur la branche KIP-848).

**Pourquoi.** Bloquer les rééquilibrages impose un budget sous `RebalanceTimeout`. Au-delà, le membre est
expulsé, et une lenteur de deux minutes devient pire qu'avant. Il faut aussi changer `Close` pour tous les
consommateurs. La fenêtre de doublons était déjà acceptée : `producer.go`, `FailClosedProduceTimeout`.

**Ce qu'il en coûte.** Un traitement concurrent du suffixe par deux membres. Pour le routeur, ce sont des
`mt.routed` republiés, bornés par le poll. La fenêtre dure le temps du lot plus celui du rejeu, au lieu du
seul lot. Un offset ramené en arrière fait retraiter au nouveau
propriétaire, à son prochain redémarrage, ce qu'il avait déjà commité : des doublons, jamais une perte.

**À quoi on reconnaîtra qu'il faut la payer.** Des doublons corrélés à un déploiement pendant une panne
de facturation. Paiement : `OnPartitionsRevoked` abandonne le rejeu des partitions révoquées, ce qui coûte
moins cher que `BlockRebalanceOnPoll`.

Sources : `internal/storage/kafka/consumer.go` (`RunBatch`, `Consumer.replay`) ·
`internal/storage/kafka/producer.go` (`FailClosedProduceTimeout`)
