# La réconciliation de facturation n'a qu'un balayage, pas de reprise durable

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-190 (`tasks-done/step-190.md:79`) · **Portée par :** —

step-190 a arbitré entre un topic `billing.settle-retry` et un balayage périodique, et a choisi le
balayage — « il reste ajoutable plus tard comme optimisation de latence, par-dessus le balayage ».
Le topic n'a jamais été créé (`grep settle-retry` : zéro).

**Ce qu'il en coûte.** Écrit par step-260b : le `MIN_AGE` de 15 minutes du reaper est **plus long que
le TTL du cache de solde**. Une réserve peut donc dépenser pendant une dizaine de minutes un crédit
qui n'existe plus. step-260b a corrigé le crédit fantôme du `Release` ; l'écart entre les deux
fenêtres, lui, est structurel.

**À quoi on reconnaîtra qu'il faut la payer.** Un client qui dépasse son solde pendant une panne de
facturation, ou le jour où l'on veut resserrer le TTL du cache sans toucher au reaper.

Source : `tasks-done/step-190.md:79`
