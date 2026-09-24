# Un rejeu de dead-letter est imputé à un nom que l'opérateur se donne

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-296 · **Portée par :** — (step-310 authentifie l'Admin API, pas l'outil)

**Ce qu'on a fait à la place.** `mt-replay` écrit sa ligne dans `control_plane.audit_log` avant de
rejouer, sous `-operator <nom>`, stocké `declared:<nom>`. Le nom est **déclaré**, jamais vérifié : l'outil
n'a pas de principal.

**Pourquoi.** Le design arrêté de step-296 l'écrit : « C'est **déclaratif** : l'imputabilité réelle viendra
d'identifiants par opérateur (step-310). » Or step-310 ne porte que l'OIDC de l'Admin API ; aucune step ne
donne à l'outil, ni à Kafka, une identité par opérateur.

**Ce qu'il en coûte si on ne la paie jamais.** La ligne dit qui **prétend** avoir rejoué, pas qui l'a fait ;
et quiconque détient les identifiants Kafka produit sur `mt.routed` sans l'outil, donc sans ligne du tout.
Après un rejeu de masse contesté, la trace ne prouve rien.

**À quoi on reconnaîtra qu'il faut la payer.** Dès qu'un audit ou un incident exige d'imputer un rejeu à une
personne, ou dès que Kafka reçoit des identifiants (SASL/mTLS) par opérateur plutôt que par service.

Source : `cmd/mt-replay/main.go:20`
