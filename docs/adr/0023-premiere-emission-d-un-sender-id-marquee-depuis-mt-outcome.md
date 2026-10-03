# ADR-0023 : La première émission d'un sender ID est marquée depuis `mt.outcome`

**Status:** Accepted
**Date:** 2026-10-03
**Deciders:** Équipe plateforme. Règle utilisateur du 03/10/2026 (dette 064 du tableau de bord) ; arbitrage
marque vs lecture CDR tranché par Fable le même jour.
**Réf spec:** passerelle §6.19 (sender ID), §6.16 (réécriture) ; ADR-0012 (CDR projeté depuis `mt.outcome`)

## Context

Un sender ID qui a servi à envoyer au moins un SMS ne doit plus se supprimer, seulement se désactiver :
ses CDR s'y rattachent. `delete-sender-id` supprimait sans condition et `control_plane.sender_ids` ne
retenait aucun usage. L'autorisation à l'envoi est lock-free sur un snapshot immuable : aucune écriture
synchrone par message n'y est admissible.

Deux voies : lire le CDR au moment de la suppression, ou poser une marque hors du chemin chaud.

## Decision

1. **Une colonne `sender_ids.first_used_at timestamptz`**, posée une fois, jamais effacée.
2. **Posée par un groupe de consommateurs dédié sur `mt.outcome`** dans router-svc (`<svc>-sender-first-use`),
   à côté de la projection CDR mais sans partager son groupe : aucune ne peut ralentir l'autre. Par lot, un
   seul `UPDATE … FROM unnest(…) WHERE first_used_at IS NULL`, idempotent sous rejeu (at-least-once), avec
   le plus ancien `submitted_at` du lot pour chaque couple.
3. **Compte toute issue de `mt.outcome`, `enroute` comme `failed`.** Son unique constructeur
   (`connectorpool.submitOutcome`) part d'un `submit_sm_resp` : l'adresse est passée sur le fil de
   l'opérateur. Les échecs sans envoi (chaîne de repli épuisée) écrivent le CDR directement et n'y passent pas.
4. **L'adresse marquée est l'adresse soumise**, `cmp.Or(OriginalFrom, From)` : c'est elle que l'autorisation
   a vérifiée. L'adresse réécrite (§6.16) n'a pas été soumise par le client ; la marquer pourrait déclarer
   « utilisé » un autre de ses sender ID.
5. **La suppression est atomique** : `DELETE … AND first_used_at IS NULL` ; zéro ligne, puis relecture pour
   distinguer 404 (absent) de 409 `conflict` (déjà utilisé).

Écartée : la lecture CDR à la suppression. Le TTL du CDR (90 jours) rendrait supprimable un sender ID
utilisé il y a quatre mois, exposer `first_used_at` dans `list-sender-ids` coûterait un agrégat ClickHouse
par appel, et ClickHouse entrerait dans le chemin d'écriture Admin.

## Consequences

- **Messages émis avant la mise en service :** le groupe neuf démarre au début de `mt.outcome` et rejoue
  sa rétention Kafka ; ce qui en est sorti n'est pas reconstruit. La passerelle n'est pas encore en
  production (environnement de test seulement) : rien à rattraper.
- La marque arrive en différé, de la latence du groupe : un sender ID supprimé dans les secondes qui
  suivent son tout premier envoi reste supprimable. La marque tombe alors sur une ligne absente, sans erreur.
- Un `UPDATE` par lot de `mt.outcome`, même quand tous les couples sont déjà marqués. Index
  `sender_ids_uq (customer_id, address)` ; à revoir si Postgres le montre.
