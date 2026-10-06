# La liste des composants supervisés d'un service n'est gardée par aucun test

> **Statut :** PAYÉE le 2026-10-06 · **Nature :** technique
> **Née de :** step-398 · **Payée par :** #272

**Payée.** `internal/platform/supervisor/components_guard_test.go` vérifie le type de chaque `cmd/` supervisé.
Tout champ d'une struct `*App` dont le type a une méthode `Run(context.Context, …)` doit apparaître dans un
argument d'un `Add` du superviseur, sinon le test tombe. Elle couvre l'oubli d'un ajout aussi bien qu'une
suppression, et les dix `main.go` restent inchangés. Prouvée en retirant les lignes `g.Add` du watcher de
débit (`rest-api-svc`), du watcher opt-out (`router-svc`) et du watcher de réécriture (`connector-pool-svc`).
Ce qui suit est l'aveu d'origine.

**Ce qu'on a fait à la place.** step-398 ajoute à `router-svc` un second watcher de config, qui recharge
l'opt-out sur une annonce STOP. Les tests prouvent qu'il est construit et qu'il fonctionne quand on le lance
(`cmd/router-svc/optout_reload_test.go`), mais rien ne prouve que `main.go` le lance : sa ligne
`g.Add("opt-out watcher", app.optOutWatcher.Run)` peut disparaître sans qu'aucun test ne tombe. C'est vrai
de chaque `g.Add` de chaque service : `newXxxApp` est testé depuis step-193, la liste des composants qu'en
tire `run()` ne l'est nulle part.

**Pourquoi.** L'extraire en une fonction testable touche les dix `main.go` ; c'est un chantier de patron, pas
un correctif de STOP.

**Ce qu'il en coûte.** Un composant construit et jamais lancé est invisible : ses tests passent, le service
démarre, `/readyz` est vert. Pour le watcher opt-out, ce serait exactement le défaut que step-398 ferme — un
STOP appliqué seulement à la prochaine mutation Admin.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier composant oublié dans un `g.Add`, ou la prochaine
step qui ajoute un composant supervisé à un service.

**Déclencheur atteint (step-289, 05/10/2026).** step-289 ajoute un watcher de limites de débit à
`rest-api-svc` et `smpp-server-svc` (`g.Add("rate-limit watcher", …)`). Les tests le lancent eux-mêmes
(`cmd/rest-api-svc/ratelimit_test.go`), donc supprimer la ligne `g.Add` ne fait tomber aucun test : une
limite réglée au tableau de bord ne s'appliquerait plus qu'au redémarrage du pod. La dette n'a pas été
payée dans step-289, dont ce n'est pas le sujet ; elle attend une step dédiée.

Source : `cmd/router-svc/main.go` (`g.Add("opt-out watcher", …)`)
Source : `cmd/rest-api-svc/main.go`, `cmd/smpp-server-svc/main.go` (`g.Add("rate-limit watcher", …)`)
