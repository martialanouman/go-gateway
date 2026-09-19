# `CONNECTOR_MAX_MESSAGE_AGE` vaut `0` : la SLA d'âge est inactive en production

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-201 (`tasks-done/step-201.md:454`) · **Portée par :** —

Le défaut est `0`, et « a zero MaxMessageAge disables the check ». Aucun manifeste de `deploy/k8s` ne
le positionne : le défaut **est** la configuration livrée.

**Ce qu'il en coûte.** Écrit : « le seul filtre qui écarte un `SubmittedAt` nul ou un message resté
des heures en backlog n'est pas actif en configuration par défaut. Un `SubmittedAt` nul donnerait
~6,4e10 s, **empoisonnant `_sum` pour la vie du pod**. » S'y ajoute le cas métier : un message vieux
de plusieurs heures part au SMSC au lieu d'être dead-lettré.

**À quoi on reconnaîtra qu'il faut la payer.** Un `_sum` de latence aberrant, ou un client qui reçoit
un SMS écrit la veille. **À lire avec la dette du drainer** : activer ce filtre sans corriger le
drainer ferait dead-lettrer en masse des messages valides.

Sources : `cmd/connector-pool-svc/main.go:68` · `internal/connectorpool/submit.go:45`
