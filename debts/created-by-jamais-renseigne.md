# `created_by` est publié par le contrat et reste nul pour toujours

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-310 · **Portée par :** —

**Ce qu'on a fait à la place.** Quatre tables du plan de contrôle portent `created_by uuid REFERENCES
dashboard.operators(id)` (`db/schema_passerelle_sms.sql:77`, `:375`, `:497`, `:581`), et le contrat Admin
publie ce champ, en lecture seule et nullable, sur les groupes de clients, les scripts de routage et leurs
versions, les règles de réécriture et les sender IDs. Rien ne l'écrit. step-310 ne l'écrit pas non plus.

**Pourquoi.** Le commentaire de `internal/adminapi/customer_groups.go` renvoyait le champ à « real operator
auth (step-310) ». Mais la colonne attend un **humain** du tableau de bord, alors que l'Admin API n'authentifie
qu'un **jeton de service** : le `sub` OIDC du BFF, d'un script ou de la collection. ADR-0017 l'avait déjà
écrit : « la passerelle ne voit pas l'humain derrière un jeton de service du BFF ». Aucun `sub` ne peut
satisfaire cette clé étrangère.

**Ce qu'il en coûte si on ne la paie jamais.** Le tableau de bord affiche un « créé par » toujours vide, et
un client généré depuis le contrat traite un champ qui n'aura jamais de valeur. L'imputation vit ailleurs :
`dashboard.audit_log` pour l'humain, `control_plane.audit_log` pour le jeton.

**À quoi on reconnaîtra qu'il faut la payer.** Dès que le tableau de bord veut afficher l'auteur d'une
ressource. Il faudra alors choisir : soit le BFF transmet l'identifiant de l'opérateur dans un en-tête que la
passerelle croit parce que le mTLS authentifie le BFF, soit le champ sort du contrat, ce qui est une rupture et
donc un bump MAJEUR.

Source : `internal/adminapi/customer_groups.go:16`
