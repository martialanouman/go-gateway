# Défauts SMPP du connecteur non modifiables à l'Admin API

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-292 (PR1) · **Portée par :** —

**Ce qu'on a fait à la place.** `priority_flag_default` est lu par le pool depuis step-292
(`cmd/connector-pool-svc/wiring.go:574`, `connectorConfigSource.Load`) et envoyé quand la priorité effective
d'un message est 0 (ADR-0020 §3). Mais ni `connectorCreateBody` ni `connectorUpdateBody`
(`internal/adminapi/connectors.go:135`, `:154`) ne l'acceptent : seule la valeur par défaut du schéma (0)
est atteignable, et un opérateur ne la change qu'en SQL. Il en va de même pour tous ses voisins `*_default`
(`registered_delivery_default`, `esm_class_default`, `validity_period_default`…), servis en lecture
seulement.

**Pourquoi.** ADR-0020 (action item 2) ne demande qu'une description de `priority_flag_default`. Ouvrir
l'écriture d'un seul `*_default` laisserait ses voisins dans le même trou ; les ouvrir tous est un bump
MINEUR qui dépasse la step.

**Ce qu'il en coûte.** Le marketing part toujours avec `priority_flag = 0`. Un opérateur dont le SMSC
attend une autre valeur passe par la base.

**À quoi on reconnaîtra qu'il faut la payer.** Le formulaire de connecteur du tableau de bord expose
`priority_flag_default` (ADR-0020, action item 8), ou un opérateur de réseau exige un autre défaut.
