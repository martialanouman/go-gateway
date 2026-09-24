# Deux tables du schéma n'avaient qu'un modèle sqlc et rien d'autre

> **Statut :** PAYÉE le 2026-09-24 · **Nature :** technique
> **Née de :** le schéma lui-même · **Payée par :** step-330 (#204, groupes) et step-350 (#210 puis PR2,
> réécriture)

**Payée.** Les groupes sont servis depuis step-330 : `customers.group_id` n'est plus une FK que rien ne
peut peupler. La réécriture a son CRUD depuis step-350 PR1, et depuis PR2 `connector-pool-svc` l'évalue
juste avant chaque `submit_sm` : la §6.16 existe au runtime.

Ce que la fiche avait prévu et qui n'a pas eu lieu : l'index partiel
`(scope, scope_id, priority) WHERE status='active'` n'a toujours **aucun lecteur**. Le pool charge toute
la table en mémoire (quelques règles) et la relit à chaque invalidation, jamais par portée ; l'index reste
entretenu pour personne, à un coût d'écriture négligeable sur une table admin.

**Ce que la fiche disait avant d'être payée.** `customer_groups` et `sender_id_rewrite_rules` étaient
complètes en base, modèles sqlc générés, sans repo, admin ni évaluation ; une FK morte et un index
entretenu pour zéro lecteur.

Sources : `db/schema_passerelle_sms.sql:72` (`customer_groups`) · `:526` (`sender_id_rewrite_rules`) ·
`:549` (l'index) · `cmd/connector-pool-svc/wiring.go` (chargement des règles)
