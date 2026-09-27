# Un jour de CDR dont l'archive inscrite est incomplète n'est jamais supprimé

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-407 (`internal/storage/clickhouse/retention.go:198`) · **Portée par :** —

Le catalogue `control_plane.cdr_archives` garde un seul objet par jour, et la ligne ne se réécrit jamais.
Quand un jour est déjà inscrit (un DROP a échoué après l'inscription, ou une écriture tardive a recréé la
partition d'un jour supprimé), l'archiveur compare la partition à l'objet inscrit sur les paires
`(message_id, version)` (`coveredBy`, `retention.go:205`). Si l'objet les couvre toutes, la partition se
supprime seule. Sinon elle reste, et chaque passe se solde par `archive_failed`.

**Pourquoi.** Réécrire la ligne ferait mentir le lecteur de step-420 sur l'objet qui fait foi, et supprimer
sans couverture perdrait des lignes que seul un objet non inscrit détient. Arbitré par Fable pendant la
revue de step-407 : l'automatisation couvre le cas fréquent (DROP raté, partition inchangée), le reste
demande un humain.

**Ce qu'il en coûte.** Aucune procédure n'existe : le jour occupe le disque de ClickHouse et l'alerte sur
`cdr_retention_partitions_total{outcome="archive_failed"}` sonne jusqu'à ce qu'un opérateur choisisse,
à la main, entre étendre le catalogue à plusieurs objets par jour et accepter la perte des lignes
manquantes.

**À payer quand** `archive_failed` persiste sur un même jour au-delà de deux passes, ou avant d'écrire
une version de CDR sur un message de plus de 90 jours.
