# Les scripts de routage voient un nombre de segments provisoire, et rien ne les en empêche

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** l'ordre du pipeline (§6.1) · **Portée par :** —

La segmentation a lieu **après** la résolution de route : quand un script s'exécute, « the real
segment count is **not yet known**. A script must not route on it. »

**Ce qu'il en coûte.** Une règle écrite sur le nombre de segments donne un résultat faux, et rien ne
l'empêche à l'exécution. L'interdit vit dans un commentaire Go qu'un auteur de script de routage ne
lira jamais : il devrait être dans la documentation du langage de script, ou rejeté à la validation.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier script client qui route sur la longueur. On
ne le saura qu'en lisant son script.

Source : `internal/pipeline/pipeline.go:39`
