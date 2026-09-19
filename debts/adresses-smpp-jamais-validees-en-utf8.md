# `source_addr` et `destination_addr` ne sont jamais validés en UTF-8

> **Statut :** OUVERTE · **Nature :** technique et produit
> **Née de :** step-260c (`tasks-done/step-260c.md:264`) · **Portée par :** —

step-260c a posé `utf8.ValidString` chez **un seul** appelant, `system_id` au bind, parce que c'est
celui qui produisait une inversion de politique de panne. La fiche le dit elle-même : « un garde posé
chez un seul appelant plutôt qu'au codec laisse la famille entière ouverte ». `submit.go` ne valide
rien.

**Ce qu'il en coûte.** Écrit : « de l'UTF-8 invalide y devient silencieusement `U+FFFD` au marshal
JSON, puis part en CDR ». Un MSISDN mutilé est un MSISDN qu'un effacement RGPD **par numéro** ne
retrouvera jamais, et sur lequel aucune réconciliation de facturation ne tombera juste.

**À quoi on reconnaîtra qu'il faut la payer.** Le jour où quelqu'un regarde la fidélité des adresses
en CDR — ou une demande d'effacement qui ne trouve pas ses lignes.

Sources : `internal/smppserver/submit.go` (absence) · `internal/smppserver/bind.go:51` (le garde isolé)
