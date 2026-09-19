# Deux tables du schéma n'ont qu'un modèle sqlc et rien d'autre

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** le schéma lui-même · **Portée par :** step-330 (groupes) et step-350 (réécriture)

`customer_groups` et `sender_id_rewrite_rules` sont complètes en base, leurs modèles sqlc sont
générés, et **ni repo, ni admin, ni évaluation**. Les fiches le disent : « table and sqlc model only:
no repo » et « no admin surface: no group is creatable ».

**Ce qu'il en coûte.** `customers.group_id` est une clé étrangère vers une table que rien ne peut
peupler : la colonne est structurellement `NULL`. Pour la réécriture, l'index partiel
`(scope, scope_id, priority) WHERE status='active'` est maintenu par PostgreSQL au profit de **zéro
lecteur**, et la §6.16 de la spec n'existe pas au runtime.

**À quoi on reconnaîtra qu'il faut la payer.** Les deux steps existent. Ce fichier note ce que le
schéma coûte **d'ici là** : une FK morte et un index entretenu pour personne.

Sources : `db/schema_passerelle_sms.sql:72` · `:516`
