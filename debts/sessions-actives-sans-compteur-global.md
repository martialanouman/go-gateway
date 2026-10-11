# `active_sessions` est absent du résumé de métriques

> **Statut :** PAYÉE le 2026-10-11 (step-403) · **Nature :** produit
> **Née de :** step-380 (design arrêté, point 6) · **Portée par :** —

**Ce qu'on a fait à la place.** `get-metrics-summary` omet `active_sessions`, que la spec du tableau de
bord (§6.3) liste parmi ses widgets.

**Pourquoi.** Le registre de sessions n'a pas de compteur global exact. `sess:idx` est purgé
paresseusement : son `ZCARD` compte des binds expirés, et un chiffre faux est pire qu'un champ absent.
Parcourir l'index par pages de 500 à chaque GET serait un autre DoS admin.

**Ce que la payer demande.** Un compteur tenu par le registre (dans `bind.lua` / `unbind.lua`, réconcilié
par le balayage des expirés), ou laisser le tableau de bord le lire sur le flux `sessions`.

**À quoi on reconnaîtra qu'il faut la payer.** Le widget « sessions actives » du tableau de bord.

**Payée (step-403).** Ni compteur réconcilié ni flux : un ZSET `sess:live` noté par l'expiration de chaque bind
(`internal/session/registry.go`, `liveKey`), compté par la RPC `CountSessions` après purge des expirés. Sa
justesse est celle de la clé du compte, aux mêmes bornes de TTL.
