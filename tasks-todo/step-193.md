# step-193 — Extraire le câblage de router-svc et connector-pool-svc en constructeurs testables

> **Jalon :** Audit pré-production (structure) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-193b

## But
Rendre le câblage des services testable **avant** que M12 ne l'alourdisse. Les `func run()` des mains ont
grossi sans filet : **378 lignes** pour `cmd/router-svc`, **290** pour `cmd/connector-pool-svc` — et
**aucun `*_test.go` n'existe sous `cmd/`**. Or step-205 (TLS/mTLS), step-206 (OIDC) et step-207 (probes k8s)
vont tous ajouter du câblage dans ces mêmes fonctions. Poser le patron maintenant coûte une PR ; le poser
après trois empilements en coûte beaucoup plus.

Cette PR ne change **aucun comportement** : c'est un refactoring pur, à sortie observable identique.

## Périmètre (ce que fait CETTE PR)
- `cmd/router-svc` et `cmd/connector-pool-svc` : extraire le câblage de `run()` vers des constructeurs
  nommés, chacun assemblant un sous-graphe cohérent de dépendances et **retournant une erreur** plutôt que
  de terminer le processus.
- `run()` se réduit à : lire la config → appeler les constructeurs → enregistrer les composants supervisés.
- Tests de câblage sur les deux services (les premiers de `cmd/`).

## Points d'implémentation clés
- **Refactoring à comportement constant.** Aucun changement d'ordre d'initialisation, de valeur par défaut,
  de message de log ni de sémantique d'arrêt. Toute divergence observée est un bug de la PR, pas une
  amélioration à saisir.
- **Découper par sous-graphe, pas par ligne.** Un constructeur = un ensemble de dépendances qui vont
  ensemble (magasins, pipeline, observabilité, composants supervisés). Découper arbitrairement en tranches
  de 50 lignes ne ferait que **déplacer** la complexité — le critère est qu'un lecteur tienne moins de
  concepts en tête, pas que les fichiers soient plus courts.
- **Ce qui rend le tout testable, c'est la valeur de retour.** Un constructeur qui `log.Fatal` ou appelle
  `os.Exit` reste intestable : chaque étape faillible retourne son erreur enveloppée.
- Respecter la convention §2 : interfaces déclarées **côté consommateur**. Ne pas introduire d'interface
  nouvelle si le type concret suffit — ne pas fabriquer d'abstraction pour l'abstraction.
- Ne pas toucher au comportement de `supervisor` ni au drain gracieux : ils sont déjà couverts par ailleurs.

## Tests (écrits dans la même PR)
- Chaque constructeur retourne une erreur enveloppée exploitable quand une dépendance est indisponible
  (au lieu de tuer le processus).
- Le graphe se construit intégralement avec des dépendances de test, sans démarrer les écoutes réseau.
- Une config invalide est rejetée à la construction, pas au premier message traité.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] **aucun changement de comportement** : mêmes logs de démarrage, même ordre d'initialisation
- [ ] `run()` ramené à la lecture de config + appel des constructeurs + enregistrement supervisé

## Hors périmètre
Les trois autres mains (`mo-dlr-router-svc`, `admin-api-svc`, `smpp-server-svc`) → step-193b, une fois le
patron validé ici. Aucune fonctionnalité nouvelle.
