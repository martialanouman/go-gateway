# La liste des composants supervisés d'un service n'est gardée par aucun test

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-398 · **Portée par :** —

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

Source : `cmd/router-svc/main.go` (`g.Add("opt-out watcher", …)`)
