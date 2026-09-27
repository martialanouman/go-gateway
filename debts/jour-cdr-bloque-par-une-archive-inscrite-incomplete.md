# Un jour de CDR déjà inscrit avec un autre objet n'est jamais supprimé par la rétention

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-407 (`internal/storage/clickhouse/retention.go:197`) · **Portée par :** —

Le catalogue `control_plane.cdr_archives` garde un seul objet par jour, et la ligne ne se réécrit jamais.
Quand la passe retrouve un jour déjà inscrit avec un autre objet que celui qu'elle vient d'écrire (un DROP a
échoué après l'inscription, ou une écriture tardive a recréé la partition d'un jour supprimé), elle garde la
partition et se solde par `archive_failed`, à chaque passe.

**Pourquoi.** Tranché par l'utilisateur en revue de step-407, après deux mécaniques automatiques rejetées :
comparer des comptes laisse perdre une version tardive (`ReplacingMergeTree` fusionne dans la même
partition) ; comparer le contenu `(message_id, segment_seq, version)` relit une journée entière en mémoire,
des centaines de millions de clés au débit visé. L'identité d'objet ne perd rien et ne coûte rien.

**Ce qu'il en coûte.** Le jour occupe le disque de ClickHouse et l'alerte
`cdr_retention_partitions_total{outcome="archive_failed"}` sonne jusqu'à l'intervention. Procédure : comparer
la partition à l'objet inscrit (`SELECT … FROM s3(cdr_archive, filename = '<objet>', format = 'Parquet')`) ;
s'il la couvre, `ALTER TABLE cdr DROP PARTITION '<jour>'` à la main ; sinon, décider entre plusieurs objets
par jour au catalogue et la perte des lignes manquantes.

**À payer quand** `archive_failed` persiste sur un même jour, ou avant d'écrire une version de CDR sur un
message de plus de 90 jours.
