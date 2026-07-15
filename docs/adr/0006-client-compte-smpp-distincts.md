# ADR-0006 : Client et compte SMPP distincts ; cardinalité des identifiants

**Status:** Accepted
**Date:** 2026-07-14
**Deciders:** Équipe plateforme
**Réf spec:** §6.18, §3.1, §7

## Context

Un client B2B a souvent plusieurs intégrations techniques (par application, environnement, marque) qui partagent une relation commerciale unique (solde, tarif, sender IDs). Il faut modéliser « un client, plusieurs comptes techniques » sans dupliquer la facturation ni laisser proliférer les identifiants.

## Decision

Deux entités distinctes : un **client** (`customers`) détient **1..N comptes SMPP** (`smpp_accounts`). La relation commerciale (solde, plan tarifaire, sender IDs, réputation, politique de contenu) est portée par le **client** ; l'intégration technique (identifiants, canaux, débit, sessions, webhooks) par le **compte**. Chaque compte a **exactement 1 identifiant de bind SMPP + 1 clé API**, imposé par une **contrainte de schéma** (`UNIQUE(account_id, type)`), pas une convention de prose. Le statut effectif = `min(customer.status, account.status)`.

## Options Considered

### Option A : client et compte distincts, cardinalité en contrainte (retenue)
| Dimension | Évaluation |
|---|---|
| Expressivité | Élevée (1 client, N comptes) |
| Sûreté | Élevée (règle inviolable) |
| Coût | 1 entité + 1 jointure au provisioning |

**Pros :** exprime le modèle réel ; borne la cardinalité des clés (règle inviolable) ; chemin critique inchangé (l'auth résout compte→client à l'ingestion et propage les deux ID en en-têtes, aucune jointure par message).
**Cons :** une entité et une jointure de plus au provisioning.

### Option B : entité unique « compte » portant tout
**Pros :** modèle plat, plus simple à première vue.
**Cons :** duplique la config commerciale (solde, tarif, sender IDs) par intégration ; ne modélise pas « plusieurs apps d'un même client » ; réconciliation de facturation pénible.

### Option C : cardinalité des identifiants en convention (code applicatif)
**Pros :** schéma plus souple.
**Cons :** règle contournable (bug = clés multiples) ; ce que la base peut garantir, elle doit le garantir.

## Trade-off Analysis

L'entité unique (B) casse dès qu'un client a deux applications partageant un solde. La séparation (A) colle au modèle B2B réel pour un coût marginal (une jointure au provisioning, jamais sur le chemin chaud). Mettre la cardinalité **dans le schéma** (vs C) rend la règle inviolable pour un coût nul.

## Consequences

- **Plus facile :** onboarding multi-intégration ; facturation mutualisée ; borne des identifiants.
- **Plus difficile :** rien de significatif (une jointure au provisioning).
- **Corollaire :** suspendre un client suspend tous ses comptes (statut effectif).

## Action Items

1. [ ] DDL : `customers` 1─N `smpp_accounts`, `credentials` avec `UNIQUE(account_id, type)` — déjà en place.
2. [ ] Auth d'ingestion : résoudre identifiant → compte → client, propager les deux ID en en-têtes Kafka.
3. [ ] Répartition des niveaux config (tableau §3.1) respectée dans l'Admin API.
