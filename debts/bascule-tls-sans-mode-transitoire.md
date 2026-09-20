# La bascule TLS est dure : pas de mode permissif, pas de double port

> **Statut :** OUVERTE (acceptée) · **Nature :** technique
> **Née de :** step-300, fork tranché le 2026-09-17 · **Portée par :** — (contrepartie assumée)

Le serveur n'écoute qu'en TLS. Ni reniflage du premier octet, ni double port pendant une transition.
La raison rend la question presque théorique : « **cette passerelle n'a jamais été déployée** […]
quand la première release partira, TLS sera là depuis le début. Construire un mécanisme de
transition, c'est le porter ensuite **pour toujours**. »

**Ce qu'il en coûte.** Écrit : « si un jour un cluster tourne en clair, la bascule coûtera une fenêtre
d'indisponibilité, ou le double port qu'on n'aura pas construit ».

**À quoi on reconnaîtra qu'il faut la payer.** Un cluster en service avant que TLS n'y soit activé —
c'est-à-dire un ordre de déploiement qu'on n'a pas prévu. La procédure se documente ; elle ne se code
pas aujourd'hui.

Source : `tasks-done/step-300.md` (§ « Bascule : dure, sans mode transitoire »)
