# Le test d'ordre du routeur suppose que deux lanes se chevauchent

> **Statut :** OUVERTE · **Nature :** test
> **Née de :** CI de `main` du 11/10/2026 sur `6ee4377` (run 38091858573, essai 1) · **Portée par :** —

**Ce qu'on a fait à la place.** `TestRouterKeepsAPartitionInOrderUnderLoad` a échoué une fois sous `-race`
(`peak concurrent Produce = 1`), puis a passé au `gh run rerun --failed`. Le test exige que les deux lanes
publient en même temps au moins une fois (`internal/router/lanes_test.go:272`). Les délais du producteur de
test (8 à 40 ms sur la partition 0, 1 ms sur la partition 1) rendent ce chevauchement probable, pas certain :
sous la charge du runner, la partition 1 peut finir avant que la partition 0 ne commence. On a relancé.

**Pourquoi.** L'assertion est voulue : sans elle, le test passerait sur un routeur séquentiel et ne
prouverait rien de l'éventail qu'il garde. La rendre certaine demande une barrière dans le producteur de
test, et la campagne step-287 gelait `main`.

**Ce qu'il en coûte.** Un `main` rouge saute le déploiement de test et demande un `gh run rerun --failed`.

**À quoi on reconnaîtra qu'il faut la payer.** Une deuxième occurrence. Le correctif : le producteur de test
retient le premier `Produce` de la partition 0 jusqu'à ce qu'un `Produce` de la partition 1 soit entré, sous
une échéance, au lieu de compter sur des délais.
