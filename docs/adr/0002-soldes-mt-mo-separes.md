# ADR-0002 : Soldes MT et MO séparés ; MO = compteur postpayé

**Status:** Accepted
**Date:** 2026-07-14
**Deciders:** Équipe plateforme
**Réf spec:** §6.9, §7

## Context

Le module de facturation (opt-in) doit compter les crédits SMS. Un MT (sortant) est sollicité par le client ; un MO (entrant) ne l'est pas — le SMSC l'a déjà remis avant toute décision de crédit. Se pose la question : un solde commun MT+MO, ou deux soldes distincts ?

## Decision

**Deux soldes séparés par direction.** Le **solde MT** est un vrai solde bloquant (réserve → capture/libère ; en prépayé sans découvert, zéro bloque l'envoi). Le **solde MO** est un **compteur postpayé qui ne bloque rien** : le MO est toujours remis, compté jusqu'à `mo_billing_floor`, après quoi l'accumulation cesse avec une alerte. Un dépassement MO n'a **aucun** effet sur le MT.

## Options Considered

### Option A : soldes MT et MO séparés (retenue)
| Dimension | Évaluation |
|---|---|
| Complexité | Faible (supprime un couplage) |
| Robustesse | Élevée |
| Surface d'abus | Réduite |

**Pros :** supprime le couplage où de l'entrant non contrôlé viderait les crédits d'envoi ; ferme un vecteur de **déni de service économique** (inonder un long-code pour couper les MT) ; élimine tout un sous-système (blocage MT conditionné au MO).
**Cons :** un solde MO ne peut rien bloquer — le recours contre une facture MO impayée est commercial (suspendre le client), pas un blocage par message.

### Option B : solde commun MT+MO
**Pros :** un seul compteur, conceptuellement simple.
**Cons :** couple l'entrant non sollicité au budget d'envoi ; ouvre le vecteur d'abus ci-dessus ; impose une logique de blocage MT sur un événement (MO) que le client ne contrôle pas.

## Trade-off Analysis

Le point décisif est la **sécurité économique** : avec un solde commun, un tiers malveillant peut épuiser les crédits d'un client en lui envoyant des MO. La séparation supprime ce vecteur et simplifie le système, au prix assumé qu'un solde MO ne bloque rien (contrepartie acceptable : le MO est de toute façon déjà remis).

## Consequences

- **Plus facile :** raisonner sur la facturation ; se protéger de l'abus ; désactiver proprement (les deux axes sont indépendants).
- **Plus difficile :** communiquer aux clients que le MO est postpayé non bloquant.
- **Verrou associé :** `balance_scope` (propriétaire du solde) n'est changeable que si **tous** les soldes sont à zéro.

## Action Items

1. [ ] Table `balances` clé `(owner_type, owner_id, direction)` — déjà au DDL.
2. [ ] `billing-svc` : réserve/capture/libère MT en Lua ; compteur MO sans réservation.
3. [ ] Alerte `mo_balance_floor_reached` sur le stream `billing-alerts`.
