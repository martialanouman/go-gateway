# `CONNECTOR_AUTO_RECONNECT` du manifest n'a aucun effet

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-280 · **Portée par :** —

`deploy/k8s/connector-pool-svc.yaml` pose `CONNECTOR_AUTO_RECONNECT: "true"`. Mais dès que le pool a un
plan de contrôle, `reloadConfig` (`internal/connectorpool/lifecycle.go:61`) remplace la politique de
l'environnement par celle du connecteur en base, où `auto_reconnect_enabled` vaut `false` par défaut
(opt-in, §6.13). La valeur du manifest ne sert qu'au tout premier instant.

Conséquence observée sur le VPS de test : au premier reset du simulateur, les deux pods se sont garés
(« link down, parking until reconfigure ») et le connecteur n'a plus rien envoyé pendant 85 min, alors
que le manifest promettait la reconnexion.

**Ce qu'on a fait à la place.** `test-env seed-load` active la politique par l'Admin API pour la
campagne. `seed` (le smoke) ne le fait pas.

**Pourquoi.** Retirer la variable ou la rendre prioritaire est une décision sur la source de vérité de la
config du pool (env contre plan de contrôle) qui touche la spec §6.13, pas une retouche de manifest.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier connecteur de production créé sans l'opt-in : il
se garera au premier incident opérateur, et le manifest aura dit le contraire. La payer : retirer
`CONNECTOR_AUTO_RECONNECT` du manifest (et du `main.go` s'il ne sert plus), ou faire avertir l'Admin API à
la création d'un connecteur sans auto-reconnexion, comme le guide §7.4 le recommande déjà.
