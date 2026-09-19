# L'anti-brute-force appelle Redis sans échéance propre

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-250d (`tasks-done/step-250d.md:91`) · **Portée par :** —

`throttleBlocks` interroge Redis avec le contexte de la connexion, sans `context.WithTimeout`. La
fiche explique pourquoi elle n'a pas prouvé le cas : « hors de portée de l'outil : `Cut()` produit une
socket morte, jamais un Redis lent ».

**Ce qu'il en coûte.** Écrit : « une découverte de panne devenue lente ferait de l'anti-brute-force un
vecteur de DoS » — chaque bind attend Redis **avant même** l'argon2id, donc avant tout travail utile.

**À quoi on reconnaîtra qu'il faut la payer.** Un Redis lent, pas coupé. La fiche renvoyait au proxy
retardateur de step-396 — mais step-396 ne porte que PostgreSQL : **le cas Redis n'a aucun porteur.**

Source : `internal/smppserver/listener.go:258`
