# `max_per_day` exposé au client, appliqué nulle part

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-283 (arbitrage F5) · **Portée par :** —

**Ce qu'on a fait à la place.** Seule la fenêtre par seconde s'applique : le seau du compte à l'admission,
celui du connecteur à l'envoi. `max_per_day` est chargé dans l'instantané et ignoré par `toBucket`
(`internal/pipeline/ratelimit/enforcer.go:177`), mais `GET /v1/account` le rend au client
(`internal/restapi/account.go:102`, `api/openapi-public.yaml:570`).

**Pourquoi.** step-283 déplaçait le débit avant l'ACK ; une fenêtre journalière est un mécanisme de plus
(fuseau de la journée, compteur à échéance, refus en `429` jusqu'à minuit), qu'aucune mesure ni aucun client
ne réclamait. La spec §6.4 ne promet que la fenêtre par seconde.

**Ce qu'il en coûte.** Un client lit un plafond journalier que la passerelle ne tient pas. S'il s'en sert
pour borner sa dépense, il la croit bornée ; si un opérateur s'en sert pour plafonner un client, rien ne
le plafonne.

**À quoi on reconnaîtra qu'il faut la payer.** Un contrat qui vend un volume journalier, ou un opérateur qui
renseigne `max_per_day` en production. Deux issues : l'appliquer à l'admission (un compteur Redis à
échéance à côté du seau, même `429`), ou le retirer de `GET /v1/account` (bump MAJEUR du contrat public).
