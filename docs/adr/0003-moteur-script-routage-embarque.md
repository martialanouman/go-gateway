# ADR-0003 : Moteur de script de routage embarqué (goja/Lua) vs FaaS

**Status:** Accepted
**Date:** 2026-07-14
**Deciders:** Équipe plateforme
**Réf spec:** §6.2, §7

## Context

Certaines logiques de routage ne s'expriment pas en règles déclaratives. Le fournisseur doit pouvoir écrire un **script** qui reçoit les données d'un SMS et retourne un ID de route. Ce script s'exécute sur le **chemin critique** (chaque message d'un compte scripté), à débit élevé, et doit être isolé (multi-tenant) sans compromettre la latence.

## Decision

Exécuter les scripts dans un **moteur embarqué en processus** : **goja** (JavaScript pur Go) principal, **gopher-lua** (Lua) alternatif. Contrat `resolveRoute(message) → routeId | null`. Garde-fou **primaire = plafond d'instructions/bytecode** (déterministe, insensible aux pauses GC), timeout mur en filet (défaut 2 ms), plafond mémoire. Runtimes **poolés** et réinitialisés par invocation (isolement inter-comptes, pas d'allocation par message). Aucun accès réseau/fichier.

## Options Considered

### Option A : moteur embarqué goja/Lua (retenue)
| Dimension | Évaluation |
|---|---|
| Latence | Excellente (in-process, pas de saut réseau) |
| Isolation | Bonne (sandbox strict) |
| Domaine de panne | Local |
| Complexité | Moyenne (bac à sable, quotas) |

**Pros :** pas de saut réseau ni de domaine de panne externe sur le chemin critique ; garde par compteur d'instructions déterministe ; enveloppe de capacité isolable par compte.
**Cons :** bac à sable à maintenir ; un compte scripté a une capacité propre inférieure, à isoler sur des pools/quotas séparés.

### Option B : FaaS externe (fonction hébergée, appel réseau)
| Dimension | Évaluation |
|---|---|
| Latence | Mauvaise (saut réseau par message) |
| Isolation | Excellente (isolation OS) |
| Domaine de panne | Externe (nouvelle dépendance critique) |
| Complexité | Élevée (déploiement, réseau) |

**Pros :** isolation forte, langages libres.
**Cons :** un saut réseau **par message** au débit cible est rédhibitoire pour la latence ; ajoute un domaine de panne sur le chemin critique.

## Trade-off Analysis

À 8 000+ msg/s, un appel réseau par message (Option B) est incompatible avec les budgets de latence. L'embarqué (A) supprime ce coût ; le risque — un script qui monopolise le CPU — est neutralisé par le **plafond d'instructions** (garde déterministe, préférée à un simple timeout mur sensible aux pauses GC).

## Consequences

- **Plus facile :** exprimer une logique complexe sans redéploiement ; garder la latence basse.
- **Plus difficile :** dimensionner (les comptes scriptés ont une enveloppe propre, HPA à ajuster) ; sécuriser le bac à sable.
- **À revisiter :** si un besoin de langage non supporté émerge.

## Action Items

1. [ ] `internal/routing/script` : pool de runtimes goja + gopher-lua, garde instructions + timeout + mémoire.
2. [ ] Cycle de vie `draft → validate → test → publish` (un actif par portée).
3. [ ] Métriques par script : latence p50/p99, taux timeout/erreur/ID invalide ; pools/quotas isolés.
