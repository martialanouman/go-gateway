# step-287l — La fenêtre du routeur devient un réglage, pour la balayer en campagne

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-285c (fenêtre de 8 par lane) · **Bloque :** la reprise de step-287
> Demande humaine du 10/10/2026, née du run 10 de step-287.

## Pourquoi
Au run 10, le routeur envoie 565 messages/s pour 100 ms de `credit` : ~57 en vol (loi de Little) pour un
plafond de 12 partitions × `laneWindow` 8 = 96, et 3 M de retard. La fenêtre est une constante
(`internal/router/router.go:114`), dont le commentaire `ponytail:` prévoit déjà d'en faire un réglage « si une
campagne de mesure doit la balayer ». Des fenêtres plus larges grossissent aussi les lots de billing-svc
(~8 écritures par lot aujourd'hui).

## Design arrêté
- `ROUTER_LANE_WINDOW` (int, défaut 8, refusé sous 1) au premier niveau de `Config`, porté par
  `router.Deps.LaneWindow` ; une valeur nulle dans `Deps` garde 8, pour les tests et les autres appelants.
- Rien d'autre ne change dans `runLane` : publication dans l'ordre des offsets, arrêt de la lane au premier
  échec, barrière de lot.
- Pas de jauge des messages en vol : le routeur a pprof (step-287i), une trace montre l'attente en tête de
  lane sans code de plus.
- Runs 11 et 12 : même image, seule la variable change (16 puis 32), avec une trace du routeur.
- Hors périmètre : lever la barrière de lot, publier en asynchrone, ajouter des partitions.

## Definition of Done
- [ ] `ROUTER_LANE_WINDOW` lu, 8 par défaut, refusé sous 1 (test de config, muté)
- [ ] la lane lance au plus `LaneWindow` messages à la fois (test du routeur, muté sur 8 et sur la valeur)
- [ ] câblé dans router-svc
- [ ] runs 11 et 12 versés au journal de step-287, avec une trace du routeur
