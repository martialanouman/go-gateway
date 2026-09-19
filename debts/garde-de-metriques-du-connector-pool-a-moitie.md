# La garde d'exposition des métriques du `connector-pool` ne couvre que sa moitié basse

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-201 (`tasks-done/step-201.md:457`) · **Portée par :** —

Le test appelle `poolCatalogueCollectors(catalog)` **directement**, pas le câblage. `router-svc` a
reçu le patron complet — via `newRouterApp` puis `app.ops.Registry()` — le pool non.

**Ce qu'il en coûte.** Écrit : « supprimer l'enregistrement dans `wiring.go` laisserait la suite verte
et `/metrics` sans aucune métrique du catalogue ». Une garde verte sur un chemin jamais exécuté ne dit
rien de ce chemin.

**À quoi on reconnaîtra qu'il faut la payer.** Elle est à moitié payée : le patron existe déjà chez
`router-svc`, il suffit de l'appliquer. À faire avant de s'appuyer sur ces métriques pour un verdict.

Source : `cmd/connector-pool-svc/opsmetrics_test.go:28`
