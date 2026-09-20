# Le TLS client vers Kafka et ClickHouse n'existe pas, et sa fiche non plus

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-300 (`tasks-done/step-300.md`) · **Portée par :** **step-305**, créée le 2026-09-19 par step-300d

step-300 a cartographié les quatre magasins : PostgreSQL et Redis sont chiffrables **sans code**
(`sslmode=require`, schéma `rediss://`) ; Kafka et ClickHouse **non** — `dialOpts` ne pose que
`DialTimeout`, et les `Options` ClickHouse sont construites à la main, sans champ `TLS`. D'où :
« hors périmètre, donc, et **step-305 est ouverte pour le porter** ».

**Ce qu'il en coûte.** Non écrite. En pratique : les corps de message chiffrés au repos transitent en
clair vers ClickHouse et Kafka, sur le réseau du cluster.

**À quoi on reconnaîtra qu'il faut la payer.** Elle a déjà son échéance : la dernière PR de step-300.
Le risque n'était pas l'oubli du besoin, c'était que **`step-305.md` n'existe ni dans `tasks-todo/` ni
dans `tasks-done/`** — le schéma « la dette sans fiche » que ce plan dit avoir déjà payé plusieurs
fois.

**Ce point-là est payé** : step-300d a créé `tasks-todo/step-305.md` (2026-09-19), qui porte désormais
le travail et nomme le fork resté ouvert — l'ancre de confiance, les quatre magasins n'étant pas des
pods de ce dépôt. La fiche reste **OUVERTE** parce que le code, lui, n'est pas écrit ; elle n'est plus
orpheline. Elle disparaîtra d'ici quand step-305 sera livrée : une dette dont le paiement est tout le
sujet d'une step ouverte n'a pas à vivre en double (`.claude/rules/debts.md`).

Sources : `internal/storage/kafka/` · `internal/storage/clickhouse/clickhouse.go:27` · `tasks-todo/step-305.md`
