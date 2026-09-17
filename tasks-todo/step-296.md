# step-296 — Deux actions d'opérateur qui échappent à la piste d'audit

> **Jalon :** Dette ouverte par step-290d · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## Pourquoi cette fiche existe

step-290c a posé `control_plane.audit_log` et la règle « pas de ligne, pas d'action » : toute mutation
de l'Admin API écrit son intention **avant** le handler, et un échec d'écriture rend 503. Deux actions
d'opérateur restent hors de cette règle, pour deux raisons différentes.

## Constat 1 — `mt-replay` n'a ni authentification ni trace

`cmd/mt-replay` remet sur `mt.routed` des messages placés en dead-letter (step-129). C'est une action de
plan de données : elle fait repartir des SMS vers de vrais abonnés, elle est facturée, et elle ne laisse
**aucune trace** de qui l'a lancée. Son seul contrôle d'accès est implicite : il faut des identifiants
Kafka et un accès ClickHouse.

Ce n'est pas une faille d'authentification à proprement parler — qui a ces identifiants peut déjà
produire sur `mt.routed` sans passer par l'outil. C'est un **trou d'imputabilité** : après un rejeu de
masse, rien ne dit qui, quand, ni combien.

**À trancher :** une ligne `audit_log` écrite par l'outil (avec quel `operator` ? il n'a pas de principal),
ou un déplacement de l'action derrière l'Admin API, qui a déjà l'identité et l'audit. La seconde voie
coûte un endpoint et supprime la question de l'identité ; elle change la nature de l'outil.

## Constat 2 — `test-billing-provider` n'est pas audité, et le sera nécessairement

`POST /admin/billing-providers/{id}/test-connection` est classé **lecture** par `readOnlyRequest`
(`internal/adminapi/configchange.go`), donc ni publié comme changement de configuration, ni audité. Ce
classement est juste aujourd'hui : le handler ne fait que charger le fournisseur et répondre un OK de
façade — « stub provider: real HTTP connectivity probe deferred »
(`internal/adminapi/billing_admin.go`).

Il cessera de l'être le jour où la sonde HTTP réelle arrivera (suite de step-147) : l'opération deviendra
un **appel sortant vers un tiers, avec des identifiants stockés**, déclenché sous `admin:write`. Une
sonde est un moyen d'exfiltration commode — elle prouve qu'une URL répond, et le `base_url` est
modifiable par la même API.

**Ce que la fiche exige :** quand la sonde devient réelle, l'opération sort de `readOnlyPostSuffixes` et
devient auditée. `internal/adminapi/audit_internal_test.go` épingle déjà la liste des quatre POST
exonérés, donc la retirer est une modification visible, pas un oubli possible.

## Definition of Done

- [ ] Un rejeu de dead-letter laisse une trace nominative, ou la fiche écrit pourquoi ce n'est pas
      possible et ce qui le remplace.
- [ ] `test-billing-provider` est audité dès que sa sonde sort du stub ; le test des suffixes exonérés
      le reflète.

## Hors périmètre

La lecture de la piste d'audit → step-315. L'authentification réelle des opérateurs → step-310.
