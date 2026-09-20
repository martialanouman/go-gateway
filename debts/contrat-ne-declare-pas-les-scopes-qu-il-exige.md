# La moitié de l'Admin API exige un scope que son contrat ne déclare pas

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-330 (`tasks-done/step-330.md`, `## Design arrêté`) · **Portée par :** —

step-330 a ajouté `security:` aux 7 opérations de groupes, en suivant le précédent de step-149 : *un
endpoint sécurisé qui cache ses échecs d'auth publie un mensonge.* En tentant d'ajouter la
comparaison `security` contrat ↔ servi au test qui compare déjà les codes et les schémas,
**50 opérations sur 110 l'ont fait échouer**. Elles n'ont aucun bloc `security:` dans `api/openapi-admin.yaml`
pendant que le code exige un scope via `scopeSecurity(...)`.

Familles entières concernées : exact-routes, suppressions, opt-out keywords, inbound numbers &
keywords, antispam rules, routing scripts, rate plans, billing providers, `get-billing-ledger`,
`get-message-trace`, `list-unrouted-mo`.

**Ce qu'il en coûte.** Le tableau de bord (dépôt séparé) génère ses clients depuis ce YAML. Pour ces
50 opérations, il croit qu'un jeton opérateur quelconque suffit : ni les scopes requis, ni les `401`
et `403` que le service renvoie vraiment ne sont dans le contrat. Un opérateur à qui il manque
`admin:write` reçoit un 403 que son client ne sait pas lire. C'est le même défaut que step-149 a
corrigé pour les 10 opérations de facturation, et que step-330 vient de corriger pour 7 autres —
laissé en place pour les 50 restantes.

**Pourquoi ce n'est pas fait ici.** Hors du périmètre de step-330, qui sert les groupes de clients.
C'est un changement additif (donc `oasdiff` non cassant, bump mineur) mais qui touche la moitié du
document et mérite sa propre PR et sa propre relecture.

**Ce qui garde le sol en attendant.** `TestEveryGeneratedOperationRequiresAScope`
(`internal/adminapi/contract_test.go`) empêche le cas grave — une opération servie **sans aucun
scope**, donc publique. Ce qu'il ne voit pas, c'est le désaccord entre ce qui est publié et ce qui
est exigé.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier opérateur à scope partiel, ou la première
fois que le tableau de bord doit distinguer « pas le droit » de « pas connecté ». La réparation :
ajouter `security:` et les `401`/`403` aux 50, puis ajouter
`reflect.DeepEqual(cOp["security"], gOp["security"])` à
`TestGeneratedSpecMatchesTheContractForEveryM1Operation` — la comparaison qui a révélé l'écart, et
qui ne peut pas y vivre tant que les 50 sont muettes.

Sources : `internal/adminapi/contract_test.go` (`TestEveryGeneratedOperationRequiresAScope`) ·
`internal/auth/middleware.go` (l'application) · `api/openapi-admin.yaml` (les 50 blocs muets)
