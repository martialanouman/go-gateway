# ADR-0007 : Opt-out scopé au canal avec union des portées à l'application

**Status:** Accepted
**Date:** 2026-07-14
**Deciders:** Équipe plateforme
**Réf spec:** §6.20, §6.21, §7

## Context

Quand un destinataire répond STOP, à quoi se désabonne-t-il ? Un désabonnement trop large (toute la plateforme) casserait des flux transactionnels légitimes d'autres clients ; trop étroit ne respecterait pas l'intention réglementaire. Le contrôle doit aussi être appliqué sur le chemin critique MT sans coût prohibitif.

## Decision

Le désabonnement vise le **canal** — le numéro entrant auquel le destinataire a répondu STOP (portée par défaut `inbound_number`), avec des portées plus larges disponibles (`smpp_account`, `customer`, `platform`). À l'**application** (étape MT bloquante), on bloque si le destinataire figure dans **l'une quelconque** des portées applicables (union : platform OU customer OU account OU inbound_number du `source_addr`). La recherche utilise un **filtre de Bloom par portée en mémoire** (jamais de faux négatif) ; seul un « peut-être » lit Redis.

## Options Considered

### Option A : scopé au canal + union à l'application (retenue)
| Dimension | Évaluation |
|---|---|
| Justesse réglementaire | Élevée |
| Effet de bord | Minimal (canal ciblé) |
| Coût chemin chaud | Quasi nul (Bloom) |

**Pros :** un STOP sur un shortcode ne coupe que ce canal ; l'union à l'application rend un opt-out large effectif quand il existe ; Bloom → coût quasi nul, jamais de faux négatif (la propriété qui compte : un faux négatif enverrait à un désabonné).
**Cons :** modèle à plusieurs portées à raisonner ; un expéditeur alphanumérique n'a pas de chemin retour (seules les portées compte/client/plateforme s'y appliquent).

### Option B : opt-out global (plateforme) uniquement
**Pros :** simple.
**Cons :** un STOP sur un canal marketing couperait les OTP transactionnels d'un autre client vers le même numéro — inacceptable.

### Option C : opt-out par client uniquement
**Pros :** intermédiaire.
**Cons :** ne distingue pas les canaux d'un même client (un STOP sur une campagne coupe tout) ; ne colle pas à l'intention « ce canal-là ».

## Trade-off Analysis

Le global (B) et le par-client (C) sur-bloquent. Scoper au **canal** respecte l'intention du STOP, tandis que l'**union à l'application** garantit qu'un opt-out volontairement large reste effectif. Le Bloom rend le contrôle quasi gratuit sur le chemin chaud avec la seule propriété qui compte ici : pas de faux négatif.

## Consequences

- **Plus facile :** respecter STOP sans casser d'autres flux ; contrôle peu coûteux.
- **Plus difficile :** raisonner sur l'union des portées ; gérer le cas alphanumérique (avertissement UI pour les comptes sans numéro entrant).
- **Rétention :** les suppressions n'expirent jamais (les expirer serait une violation).

## Action Items

1. [ ] `internal/pipeline/optout` : Bloom par portée en mémoire, étape MT bloquante avant anti-spam/routage/facturation.
2. [ ] Détection STOP côté MO écrivant une `suppressions` scopée sur le numéro entrant + auto-réponse (MT jamais facturé).
3. [ ] `POST /admin/suppressions/check` pour diagnostiquer « bloqué par quelle portée ».
