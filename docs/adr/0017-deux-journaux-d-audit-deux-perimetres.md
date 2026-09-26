# ADR-0017 : Deux journaux d'audit, deux périmètres — la passerelle fait foi pour ce qui l'atteint

**Status:** Accepted
**Date:** 2026-09-26
**Deciders:** Équipe plateforme (arbitrage utilisateur, step-315)
**Réf spec:** §6.14 ; spec tableau de bord §3.1 et « Dashboard-owned resources » ; step-290c → step-315

## Context

Deux tables s'appellent `audit_log`. `control_plane.audit_log` (step-290c) enregistre chaque mutation de
l'Admin API, chaque lecture qui démasque un numéro, et chaque rejeu de `mt-replay`. Elle est écrite avant
le handler. `dashboard.audit_log` (spec tableau de bord §3.1, livrée par le BFF `go-gateway-bo`) enregistre
les actions d'un opérateur humain, sous son `operator_id`. La spec du tableau de bord range
`GET /audit-log` parmi ses ressources propres. Rien ne disait laquelle fait foi, ni si le BFF devait
projeter la nôtre.

Ce que chacune voit seule :
- le BFF ne voit pas un appel qui atteint l'Admin API sans passer par lui (jeton d'opérateur direct, script,
  collection) ni `mt-replay` ;
- la passerelle ne voit pas l'humain derrière un jeton de service du BFF, ni les actions propres au BFF
  (connexion, MFA, rôles), qui ne l'atteignent jamais.

## Decision

- **`control_plane.audit_log` fait foi pour tout ce qui atteint la passerelle**, quel que soit l'appelant.
  Elle se lit par `GET /admin/audit-log`, sous le scope `audit:read`, et le BFF la **lit** sans la projeter.
- **`dashboard.audit_log` fait foi pour les actions propres au BFF**, et pour l'identité humaine d'une
  action proxyfiée.
- L'écran de consultation du BFF (sa step-184) montre les deux sources. Une action proxyfiée y apparaît
  deux fois, l'une sous l'humain, l'autre sous le jeton : ce sont deux faits, pas un doublon.

## Consequences

- La spec passerelle gagne une opération que la spec du tableau de bord attribuait au BFF. Celle-ci est
  annotée en ce sens.
- Relier une ligne de passerelle à son humain demande que le BFF transmette une corrélation, par exemple
  `X-Request-Id`, que la colonne `request_id` conserve déjà. Rien ne l'impose aujourd'hui.
