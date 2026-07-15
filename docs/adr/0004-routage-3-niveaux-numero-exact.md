# ADR-0004 : Routage à 3 niveaux avec court-circuit numéro exact

**Status:** Accepted
**Date:** 2026-07-14
**Deciders:** Équipe plateforme
**Réf spec:** §6.1, §7

## Context

Le routage doit gérer la **portabilité des numéros (MNP)** : le matching par préfixe MSISDN suppose que le préfixe identifie l'opérateur, ce qui est faux pour un numéro porté. Il faut aussi permettre une logique scriptée et un matching déclaratif classique, sans payer un coût réseau pour les ~99 % de messages sans cas particulier.

## Decision

Résolution de route à **trois niveaux, premier gagnant** : **L0** correspondance de numéro exact (`exact_routes`, court-circuit) → **L1** script de routage → **L2** matching déclaratif (préfixe-trie / MCC-MNC). Le niveau L0 est adossé à un **filtre de Bloom en mémoire** (jamais de faux négatif) : « absent » = pas d'override, sans appel réseau. Le court-circuit L0 saute **uniquement la résolution de route**, jamais les étapes de conformité (E.164, sender ID, opt-out, anti-spam) ni l'aval (segmentation, débit, crédit).

## Options Considered

### Option A : 3 niveaux avec numéro exact + Bloom (retenue)
| Dimension | Évaluation |
|---|---|
| Correction MNP | Résolue |
| Coût chemin chaud | Quasi nul (Bloom) |
| Complexité | Moyenne |

**Pros :** résout la portabilité ; coût quasi nul pour les 99 % sans override (Bloom en mémoire) ; combine exactitude (numéro exact), flexibilité (script) et simplicité (déclaratif).
**Cons :** trois chemins de résolution à maintenir ; table `exact_routes` volumineuse à alimenter (import MNP).

### Option B : matching par préfixe seul
**Pros :** simple.
**Cons :** **faux** en marché porté — route un numéro porté vers le mauvais opérateur. Rédhibitoire.

### Option C : lookup base systématique par message
**Pros :** toujours exact.
**Cons :** un accès réseau/base par message au débit cible ; inacceptable en latence.

## Trade-off Analysis

Le préfixe seul (B) est incorrect ; le lookup systématique (C) est trop coûteux. La combinaison numéro-exact-avec-Bloom résout la correction **et** la performance : le Bloom transforme « chercher un override » en une vérification mémoire sans faux négatif, réservant l'accès Redis aux rares « peut-être ». La règle absolue — **le raccourci ne saute jamais la conformité** — est un invariant testable.

## Consequences

- **Plus facile :** router correctement en marché porté ; garder le chemin chaud rapide.
- **Plus difficile :** maintenir `exact_routes` (imports MNP en masse, rafraîchissement du Bloom via config-sync).
- **Garde-fou :** si la cible d'un numéro exact est indisponible, on retombe sur L1/L2 plutôt que dead-letter.

## Action Items

1. [ ] `internal/routing` : Bloom en mémoire des clés `exact_routes`, rafraîchi par config-sync.
2. [ ] Test d'invariant (b) : un message routé L0 traverse toutes les étapes de conformité.
3. [ ] Import MNP asynchrone (`POST /admin/exact-routes/import`).
