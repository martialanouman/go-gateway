# ADR-0009 : Annulation d'un message réservée au canal SMPP (pas de surface REST)

**Status:** Accepted
**Date:** 2026-07-24
**Deciders:** Équipe plateforme
**Réf spec:** §1.10, §6.22 ; step-030

## Context

Le plan d'exécution (step-030, §7 du plan) prévoyait l'annulation d'un message pas-encore-envoyé sur **deux surfaces** : `POST /messages/{id}/cancel` (REST) **et** `cancel_sm` (SMPP), avec une preuve de parité entre les deux. Le contrat public `api/openapi-public.yaml` déclarait l'opération `cancel-message` en conséquence (marquée `deferred` jusqu'à cette étape).

Au moment d'implémenter, l'équipe a tranché que l'annulation devait être une **opération SMPP uniquement** : seuls les ESME bindés en SMPP peuvent annuler un message. Les clients REST n'ont aucun moyen d'annuler.

## Decision

L'annulation est **exclusivement SMPP** (`cancel_sm`). Il n'y a **aucune surface REST** :

- L'opération `cancel-message` est **retirée** de `api/openapi-public.yaml` (le contrat public ne promet pas une route qui n'existe pas), et l'entrée `deferred` correspondante disparaît du test de conformité.
- La logique d'annulation vit dans `internal/cancel` (`Canceller`), consommée par le seul `smpp-server-svc` via le hook `cancel_sm`. Elle reste un domaine isolé et testable, mais sans exigence de parité multi-surface.
- Le mécanisme « pas encore envoyé » est un flag Redis `cancel:{message_id}` que `connector-pool-svc` consulte avant `submit_sm`, plus une ligne CDR `cancelled` (rang 60). Le double-appel est naturellement idempotent (`ESME_ROK`).

Mapping SMPP (via `errs.SMPPStatusForError`, aucun code ajouté) : message inconnu → `ESME_RINVMSGID` ; déjà envoyé → `ESME_RCANCELFAIL` ; encore en file (ou déjà annulé) → `ESME_ROK`.

## Options Considered

### Option A : SMPP-only, route REST retirée du contrat (retenue)
**Pros :** le contrat public n'expose aucune opération morte ; une seule surface à maintenir et à raisonner ; l'annulation reste alignée sur la sémantique `cancel_sm` native que les agrégateurs SMPP attendent ; moins de câblage (rest-api-svc n'a pas besoin de Redis).
**Cons :** dévie du plan d'exécution initial (parité REST/SMPP abandonnée) ; un futur besoin d'annulation REST devra rouvrir le contrat.

### Option B : garder la route REST, renvoyer 405 `operation_not_supported`
**Pros :** la forme du contrat reste stable.
**Cons :** expose une opération publiquement documentée qui refuse toujours — trompeur pour un intégrateur.

### Option C : implémenter la double surface comme prévu au plan
**Pros :** conforme au plan ; parité prouvée.
**Cons :** ajoute une surface REST que le produit ne veut pas ; couple rest-api-svc à Redis et à l'écriture CDR pour un cas d'usage jugé hors du modèle client REST.

## Consequences

- **Plus facile :** une seule surface d'annulation ; contrat public honnête ; `rest-api-svc` inchangé (pas de dépendance Redis).
- **Plus difficile :** `connector-pool-svc` gagne une dépendance Redis. Elle est requise **au démarrage** (le client PING au boot, comme partout) mais **fail-open au runtime** : si le flag ne peut être lu, le message est envoyé plutôt que de figer toute la livraison sortante — l'annulation étant best-effort. Un futur besoin REST rouvrira le contrat et cet ADR.
- **Traçabilité :** `tasks-done/step-030.md` (SMPP-only), le plan d'exécution et la spec technique (§5, §6.22) renvoient à cet ADR.
- **Fiabilité de l'annulation (limite connue) :** l'annulabilité se décide sur la projection CDR `accepted`, écrite hors-chemin (quelques dizaines de ms, droppable sous saturation). Un `cancel_sm` arrivant avant que cette projection soit durable répond `ESME_RINVMSGID` (message « inconnu ») alors que le message est en file et partira — même fenêtre que le `404` de `get-message`. L'ESME doit retenter. Poser le flag inconditionnellement corrigerait la fenêtre mais casserait le scoping strict par compte (un compte pourrait annuler le message d'un autre) : le scoping prime.
- **Course intrinsèque (hors périmètre) :** une annulation concurrente d'un `submit_sm` déjà parti reste possible ; le CDR enregistre alors `cancelled` (rang 60) bien que le message ait quitté la file. Corollaire à revoir en **M4** : `cancelled` (rang 60) supersède `delivered`/`failed` (40/50), donc un DLR terminal arrivant après une annulation en course serait masqué — à réconcilier quand la voie retour MO/DLR arrivera. Le multi-segment partiellement parti reste hors périmètre (M6).
