# Un rejeu en place continue sur une partition que le groupe vient de réattribuer

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-285 (design arrêté, points 4 et 9) · **Portée par :** —

**Ce qu'on a fait à la place.** Le consommateur rejoue un lot en échec sans repoller et sans bloquer les
rééquilibrages : `BlockRebalanceOnPoll` n'est pas activé. Supposons qu'un membre rejoigne le groupe
pendant le rejeu, à cause d'un rollout ou d'un scale. L'ancien propriétaire continue de traiter le suffixe,
et le nouveau le refetch depuis le dernier commit. franz-go filtre le commit tardif sur la partition
révoquée, puis `commit` rend une erreur fatale, et le processus redémarre.

**Pourquoi.** Bloquer les rééquilibrages impose un budget sous `RebalanceTimeout`. Au-delà, le membre est
expulsé, et une lenteur de deux minutes devient pire qu'avant. Il faut aussi changer `Close` pour tous les
consommateurs. La fenêtre de doublons était déjà acceptée : `producer.go`, `FailClosedProduceTimeout`.

**Ce qu'il en coûte.** Un traitement concurrent du suffixe par deux membres. Pour le routeur, ce sont des
`mt.routed` republiés, bornés par le poll. La fenêtre dure le temps du lot plus celui du rejeu, au lieu du
seul lot.

**À quoi on reconnaîtra qu'il faut la payer.** Des doublons corrélés à un déploiement pendant une panne
de facturation. Paiement : `OnPartitionsRevoked` abandonne le rejeu des partitions révoquées, ce qui coûte
moins cher que `BlockRebalanceOnPoll`.

Sources : `internal/storage/kafka/consumer.go` (`RunBatch`, `Consumer.replay`) ·
`internal/storage/kafka/producer.go` (`FailClosedProduceTimeout`)
