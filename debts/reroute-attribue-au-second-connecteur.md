# Un message rerouté est attribué au connecteur qui a fini par répondre

> **Statut :** OUVERTE (acceptée) · **Nature :** technique
> **Née de :** step-201 (`tasks-done/step-201.md:482`) · **Portée par :** —

C'est volontaire, et la raison est écrite : « c'est la bonne lecture de "bout en bout" » — le client a
bien attendu la somme des deux tentatives.

**Ce qu'il en coûte.** Écrit : « un tableau de bord par connecteur montrera le second portant une
latence causée par le premier ». De quoi ouvrir un disjoncteur sur le mauvais connecteur, en
exploitation, sur la foi d'une métrique juste mal lue.

**À quoi on reconnaîtra qu'il faut la payer.** La première fois qu'un opérateur soupçonne un
connecteur sain à cause de ce graphe.

Source : `tasks-done/step-201.md:482`
