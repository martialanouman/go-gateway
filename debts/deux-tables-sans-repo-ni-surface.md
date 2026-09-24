# Deux tables du schéma n'avaient qu'un modèle sqlc et rien d'autre

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** le schéma lui-même · **Portée par :** step-350 (PR2, évaluation de la réécriture)

`customer_groups` et `sender_id_rewrite_rules` étaient complètes en base, leurs modèles sqlc générés, et
n'avaient **ni repo, ni admin, ni évaluation**.

**Où on en est.** Les groupes sont servis depuis step-330 (#204) : `customers.group_id` n'est plus une
FK que rien ne peut peupler. La réécriture a son repo et son CRUD admin depuis step-350 PR1 : les règles
se créent, se listent dans l'ordre d'évaluation, et **rien ne les évalue**.

**Ce qu'il en coûte d'ici step-350 PR2.** L'index partiel `(scope, scope_id, priority) WHERE
status='active'` est maintenu par PostgreSQL au profit de **zéro lecteur**, et une règle active ne
change aucun envoi : la §6.16 n'existe pas au runtime, alors que l'Admin API laisse l'opérateur croire
le contraire.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier opérateur qui crée une règle et voit partir
le sender ID d'origine.

Sources : `db/schema_passerelle_sms.sql:72` (`customer_groups`) · `:526` (`sender_id_rewrite_rules`) ·
`:549` (l'index)
