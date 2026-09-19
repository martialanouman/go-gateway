# Le TLS client vers Kafka et ClickHouse n'existe pas, et sa fiche non plus

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-300 (`tasks-todo/step-300.md:78`) · **Portée par :** — (step-305 est annoncée, le fichier n'existe pas)

step-300 a cartographié les quatre magasins : PostgreSQL et Redis sont chiffrables **sans code**
(`sslmode=require`, schéma `rediss://`) ; Kafka et ClickHouse **non** — `dialOpts` ne pose que
`DialTimeout`, et les `Options` ClickHouse sont construites à la main, sans champ `TLS`. D'où :
« hors périmètre, donc, et **step-305 est ouverte pour le porter** ».

**Ce qu'il en coûte.** Non écrite. En pratique : les corps de message chiffrés au repos transitent en
clair vers ClickHouse et Kafka, sur le réseau du cluster.

**À quoi on reconnaîtra qu'il faut la payer.** Elle a déjà son échéance : la dernière PR de step-300.
Le risque n'est pas l'oubli du besoin, c'est que **`step-305.md` n'existe ni dans `tasks-todo/` ni
dans `tasks-done/`** — c'est le schéma « la dette sans fiche » que ce plan dit avoir déjà payé
plusieurs fois.

Sources : `internal/storage/kafka/` · `internal/storage/clickhouse/clickhouse.go:27`
