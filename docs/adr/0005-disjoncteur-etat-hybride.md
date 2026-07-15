# ADR-0005 : Disjoncteur par connecteur à état hybride

**Status:** Accepted
**Date:** 2026-07-14
**Deciders:** Équipe plateforme
**Réf spec:** §6.8, §6.15, §7

## Context

Un connecteur SMSC qui se dégrade au débit cible est un risque réel. Il faut un disjoncteur par connecteur, mais les binds d'un connecteur sont répartis sur **plusieurs pods** (pool de binds, scalabilité horizontale). Le routeur (`router-svc`) doit connaître l'état sans payer une **dépendance synchrone par message**, et les messages déjà routés doivent pouvoir être redirigés même si le connecteur cible vient de s'ouvrir.

## Decision

Disjoncteur par connecteur à **état hybride**, sur deux surfaces :

1. **Décisions de routage futures** : chaque `connector-pool-svc` publie l'état agrégé (`breaker:state`, dérivé par majorité du hash `breaker:binds` alimenté par chaque pod) à chaque transition, avec notification `breaker:events`. `router-svc` ne le lit **qu'en construisant son instantané**, jamais par message.
2. **Messages déjà routés** : chaque message porte un `fallback_chain` en en-tête ; si `connector-pool-svc` reçoit un message pour un connecteur ouvert, il republie vers le connecteur suivant (borné, avec `mt.reroute-park` pour l'excédent).

`link_status` (up/reconnecting/down) et `breaker_state` (closed/open/half_open) restent **distincts**.

## Options Considered

### Option A : état hybride (Redis agrégé + fallback_chain) (retenue)
| Dimension | Évaluation |
|---|---|
| Dépendance chemin chaud | Nulle (par message) |
| Cohérence multi-pod | Bonne (agrégat par majorité) |
| Complexité | Élevée |

**Pros :** pas de lookup synchrone par message ; le routeur voit les pannes via son instantané ; les messages en vol se rerootent unilatéralement.
**Cons :** deux mécanismes à maintenir ; agrégation multi-pod non triviale.

### Option B : état purement local par pod
**Pros :** simple, rapide.
**Cons :** chaque pod décide dans son coin ; `router-svc` n'a aucune visibilité globale → il continue de router vers un connecteur mort.

### Option C : lookup Redis synchrone par message
**Pros :** état toujours frais.
**Cons :** un accès Redis **par message** au débit cible ; latence et dépendance inacceptables.

## Trade-off Analysis

Le local pur (B) prive le routeur de visibilité ; le lookup synchrone (C) est trop coûteux. L'hybride (A) sépare les deux besoins : l'agrégat Redis (mis à jour **par transition**, pas par message) informe les décisions futures ; le `fallback_chain` porté sur le message gère les décisions déjà prises. Aucun des deux n'impose de dépendance synchrone sur le chemin chaud.

## Consequences

- **Plus facile :** isoler un opérateur défaillant sans dépendance par message ; rerouter le trafic en vol.
- **Plus difficile :** l'agrégation d'état multi-pod (hash de sous-binds + majorité) ; borner le reroutage de masse.
- **Lien :** le disjoncteur ne reconnecte jamais (ADR distinct / §6.13) ; recommandation normative d'activer l'auto-reconnexion sur tout connecteur disjoncté.

## Action Items

1. [ ] `internal/connector` : machine à états + agrégation `breaker:binds` → `breaker:state` par majorité (CAS sur transition).
2. [ ] `fallback_chain` résolu au routage, porté en en-tête ; draineur borné + `mt.reroute-park`.
3. [ ] Exposer `link_status` et `breaker_state` séparément via `/admin/connectors/{id}/status`.
