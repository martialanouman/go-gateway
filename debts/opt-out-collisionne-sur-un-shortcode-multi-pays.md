# L'opt-out indexe l'adresse seule : un shortcode réutilisé entre pays collisionne

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-062/063 · **Portée par :** — (le rendez-vous était step-063, **déjà livrée**)

L'index de l'enforcer diverge délibérément de l'unicité du schéma, et la raison est solide : « an
MT's From carries no country, so it **cannot** be resolved otherwise ». Pour un agrégateur
mono-pays, c'est exact. Le commentaire renvoie la question à step-063 — qui est dans `tasks-done/`.
Le rendez-vous a donc eu lieu sans que rien ne soit tranché.

**Ce qu'il en coûte.** Écrit : « only a bare shortcode reused across countries could collide — out of
scope until multi-country ». Le jour d'une expansion, un STOP envoyé à un shortcode dans un pays
supprimerait les MT de ce shortcode **dans tous les pays** : un sur-blocage silencieux sur un chemin
réglementaire.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier pays supplémentaire. Pas avant, et pas
après.

Source : `internal/pipeline/optout/enforcer.go:26`
